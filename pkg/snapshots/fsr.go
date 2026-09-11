// Copyright 2020 FairwindsOps Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshots

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/fairwindsops/gemini/pkg/fsr"
	"github.com/fairwindsops/gemini/pkg/kube"
	snapshotgroup "github.com/fairwindsops/gemini/pkg/types/snapshotgroup/v1"
)

// FSRPollInterval is the requeue cadence while a snapshot is in a transitional
// state ("enabling" or "disabling"). AWS FSR warmup is minutes-scale
// (~60min/TiB), so polling faster wastes API calls and requeue work.
const FSRPollInterval = 60 * time.Second

var (
	fsrClient        fsr.Client
	defaultFSRAZs    []string
	fsrGlobalEnabled = false
)

// SetFSRClient installs the AWS FSR client used by ReconcileFSR. main.go calls
// this once at startup; tests inject a fake.
func SetFSRClient(c fsr.Client) { fsrClient = c }

// SetDefaultFSRAZs installs the cluster-wide fallback AZ list used when a
// SnapshotGroup omits spec.fastSnapshotRestore.availabilityZones.
func SetDefaultFSRAZs(azs []string) { defaultFSRAZs = azs }

// SetFSRGlobalEnabled flips the cluster-wide FSR opt-in. The default is false
// — operators must explicitly enable FSR (typically via GEMINI_FSR_ENABLED).
// When false, ReconcileFSR short-circuits regardless of per-SnapshotGroup
// configuration. This is a pure skip — snapshots already FSR-enabled in AWS
// stay enabled; cleanup happens via per-SG `enabled: false` or manual aws ec2
// calls.
func SetFSRGlobalEnabled(v bool) { fsrGlobalEnabled = v }

// ReconcileFSR drives the FSR state machine for a single SnapshotGroup.
//
// Returns the duration after which the controller should re-enqueue this SG.
// Zero means no time-based requeue is needed (state is steady or terminal);
// the existing informer event path will pick up future changes.
//
// AWS is authoritative on every pass. This lets Gemini recover from stale or
// failed annotations and from a partial multi-AZ enable.
//
// Rotation order keeps one recoverable hot snapshot throughout:
//  1. disable FSR on snapshots older than the immediately previous snapshot;
//  2. enable/retry each missing AZ on the newest ReadyToUse snapshot;
//  3. wait for AWS state=enabled and >=1 creation credit in every target AZ;
//  4. only then disable FSR on the previous snapshot.
func ReconcileFSR(sg *snapshotgroup.SnapshotGroup) (time.Duration, error) {
	if !fsrGlobalEnabled {
		return 0, nil
	}
	// No fastSnapshotRestore block at all: Gemini never touched FSR for this SG,
	// so there is nothing to enable and nothing to clean up from our side.
	if sg.Spec.FastSnapshotRestore == nil {
		return 0, nil
	}
	perSGEnabled := sg.Spec.FastSnapshotRestore.Enabled
	if fsrClient == nil {
		if perSGEnabled {
			klog.Warningf("%s/%s: fastSnapshotRestore.enabled=true but no FSR client configured; skipping",
				sg.ObjectMeta.Namespace, sg.ObjectMeta.Name)
		}
		return 0, nil
	}

	azs := sg.Spec.FastSnapshotRestore.AvailabilityZones
	if len(azs) == 0 {
		azs = defaultFSRAZs
	}
	if len(azs) == 0 {
		if !perSGEnabled {
			// Opted out with no AZs recorded: we can't safely target any AZ for
			// Disable. Operators that need cleanup should either restore the AZs
			// block or disable FSR manually via aws ec2.
			return 0, nil
		}
		return 0, fmt.Errorf("%s/%s: fastSnapshotRestore enabled but no AZs configured (set spec.fastSnapshotRestore.availabilityZones or %s)",
			sg.ObjectMeta.Namespace, sg.ObjectMeta.Name, fsr.DefaultAZsEnvVar)
	}

	snapshots, err := ListSnapshots(sg)
	if err != nil {
		return 0, fmt.Errorf("list snapshots: %w", err)
	}

	if !perSGEnabled {
		requeue, _, err := reconcileDisable(sg, snapshots, azs, true)
		return requeue, err
	}

	target := newestReadyToUse(snapshots)
	if target == nil {
		return 0, nil
	}
	previous := previousSnapshot(snapshots, target)

	// Free quota held by N-2 and older before trying the newest snapshot. N-1 is
	// deliberately preserved until the new snapshot has usable credits.
	stale := snapshotsExcept(snapshots, target, previous)
	requeueStale, staleCold, err := reconcileDisable(sg, stale, azs, true)
	if err != nil {
		return 0, err
	}
	if !staleCold {
		return minNonZero(requeueStale, FSRPollInterval), nil
	}

	requeueEnable, err := reconcileEnable(sg, target, azs)
	if err != nil {
		return 0, err
	}
	if fsrState(target) != FSRStateEnabled {
		return minNonZero(requeueStale, requeueEnable), nil
	}

	if previous == nil {
		return minNonZero(requeueStale, requeueEnable), nil
	}
	requeuePrevious, _, err := reconcileDisable(sg, []*GeminiSnapshot{previous}, azs, true)
	if err != nil {
		return 0, err
	}
	return minNonZero(minNonZero(requeueStale, requeueEnable), requeuePrevious), nil
}

// reconcileEnable converges the newest snapshot from AWS state rather than
// trusting annotations. Missing AZs are enabled one at a time so one quota
// failure cannot hide a successful sibling and every missing AZ is retried.
func reconcileEnable(sg *snapshotgroup.SnapshotGroup, target *GeminiSnapshot, azs []string) (time.Duration, error) {
	snapshotID, err := resolveSnapshotID(target)
	if err != nil {
		return FSRPollInterval, nil
	}
	states, err := fsrClient.Describe(context.TODO(), snapshotID)
	if err != nil {
		return 0, fmt.Errorf("FSR Describe(%s): %w", snapshotID, err)
	}

	missing := fsr.MissingAZs(states, azs)
	if len(missing) > 0 {
		// A target already moving toward disabled must finish that transition
		// before AWS will accept a fresh enable request.
		if !fsr.IsColdInAll(states, missing) {
			return FSRPollInterval, nil
		}
		now := strconv.FormatInt(time.Now().Unix(), 10)
		if err := patchSnapshotAnnotations(target, map[string]string{
			FSRStateAnnotation:     FSRStateEnabling,
			FSREnabledAtAnnotation: now,
		}); err != nil {
			return 0, fmt.Errorf("annotate %s as enabling: %w", target.Name, err)
		}
		for _, az := range missing {
			if err := fsrClient.Enable(context.TODO(), snapshotID, []string{az}); err != nil {
				return 0, fmt.Errorf("FSR Enable(%s, %s): %w", snapshotID, az, err)
			}
		}
		return FSRPollInterval, nil
	}

	if !fsr.IsWarmInAll(states, azs) {
		return FSRPollInterval, nil
	}
	creditsReady, err := fsrClient.CreditsReady(context.TODO(), snapshotID, azs)
	if err != nil {
		return 0, fmt.Errorf("FSR credits(%s): %w", snapshotID, err)
	}
	if !creditsReady {
		if fsrState(target) != FSRStateEnabling {
			if err := patchSnapshotAnnotations(target, map[string]string{FSRStateAnnotation: FSRStateEnabling}); err != nil {
				return 0, fmt.Errorf("annotate %s as enabling: %w", target.Name, err)
			}
		}
		return FSRPollInterval, nil
	}
	if fsrState(target) != FSRStateEnabled {
		if err := patchSnapshotAnnotations(target, map[string]string{FSRStateAnnotation: FSRStateEnabled}); err != nil {
			return 0, fmt.Errorf("annotate %s as enabled: %w", target.Name, err)
		}
	}
	return 0, nil
}

// reconcileDisable converges each supplied snapshot to cold in every target AZ.
// The bool result is true only when all snapshots are already fully disabled.
func reconcileDisable(sg *snapshotgroup.SnapshotGroup, snapshots []*GeminiSnapshot, azs []string, canInitiate bool) (time.Duration, bool, error) {
	var requeue time.Duration
	allCold := true
	for _, snap := range snapshots {
		snapshotID, err := resolveSnapshotID(snap)
		if err != nil {
			allCold = false
			requeue = minNonZero(requeue, FSRPollInterval)
			continue
		}
		states, err := fsrClient.Describe(context.TODO(), snapshotID)
		if err != nil {
			return 0, false, fmt.Errorf("FSR Describe(%s): %w", snapshotID, err)
		}
		if fsr.IsColdInAll(states, azs) {
			if fsrState(snap) != FSRStateDisabled {
				if err := patchSnapshotAnnotations(snap, map[string]string{FSRStateAnnotation: FSRStateDisabled}); err != nil {
					return 0, false, fmt.Errorf("annotate %s as disabled: %w", snap.Name, err)
				}
			}
			continue
		}

		allCold = false
		requeue = minNonZero(requeue, FSRPollInterval)
		if !canInitiate {
			continue
		}
		active := fsr.ActiveAZs(states, azs)
		if len(active) == 0 {
			// AWS is already in the disabling transition; only poll.
			continue
		}
		if fsrState(snap) == FSRStateDisabling {
			continue
		}
		if err := fsrClient.Disable(context.TODO(), snapshotID, active); err != nil {
			return 0, false, fmt.Errorf("FSR Disable(%s): %w", snapshotID, err)
		}
		now := strconv.FormatInt(time.Now().Unix(), 10)
		if err := patchSnapshotAnnotations(snap, map[string]string{
			FSRStateAnnotation:      FSRStateDisabling,
			FSRDisabledAtAnnotation: now,
		}); err != nil {
			return 0, false, fmt.Errorf("annotate %s as disabling: %w", snap.Name, err)
		}
	}
	return requeue, allCold, nil
}

func previousSnapshot(snapshots []*GeminiSnapshot, target *GeminiSnapshot) *GeminiSnapshot {
	for i, snap := range snapshots {
		if snap.Name == target.Name && snap.Namespace == target.Namespace && i+1 < len(snapshots) {
			return snapshots[i+1]
		}
	}
	return nil
}

func snapshotsExcept(snapshots []*GeminiSnapshot, excluded ...*GeminiSnapshot) []*GeminiSnapshot {
	out := make([]*GeminiSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		skip := false
		for _, item := range excluded {
			if item != nil && snap.Name == item.Name && snap.Namespace == item.Namespace {
				skip = true
				break
			}
		}
		if !skip {
			out = append(out, snap)
		}
	}
	return out
}

// fsrState reads the fsr-state annotation off a snapshot (empty string if unset).
func fsrState(s *GeminiSnapshot) string {
	if s == nil || s.VolumeSnapshot == nil {
		return ""
	}
	return s.VolumeSnapshot.ObjectMeta.Annotations[FSRStateAnnotation]
}

// minNonZero returns the smaller of two durations, ignoring zeros. When both
// are zero, returns zero.
func minNonZero(a, b time.Duration) time.Duration {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// newestReadyToUse returns the snapshot with the highest timestamp whose
// VolumeSnapshot.Status.ReadyToUse is true. Snapshots are pre-sorted desc
// by timestamp by ListSnapshots, so we just pick the first ready one.
func newestReadyToUse(snapshots []*GeminiSnapshot) *GeminiSnapshot {
	for _, s := range snapshots {
		if s.VolumeSnapshot == nil || s.VolumeSnapshot.Status == nil {
			continue
		}
		if s.VolumeSnapshot.Status.ReadyToUse != nil && *s.VolumeSnapshot.Status.ReadyToUse {
			return s
		}
	}
	return nil
}

// resolveSnapshotID maps a Gemini-managed VolumeSnapshot to its AWS EBS snapshot
// ID by following Status.BoundVolumeSnapshotContentName -> VSC.Status.SnapshotHandle.
func resolveSnapshotID(snap *GeminiSnapshot) (string, error) {
	contentName, err := getSnapshotContentName(snap)
	if err != nil {
		return "", err
	}
	details, err := getSnapshotContentDetails(contentName)
	if err != nil {
		return "", err
	}
	if details.SnapshotHandle == "" {
		return "", fmt.Errorf("VolumeSnapshotContent %s has empty snapshotHandle", contentName)
	}
	return details.SnapshotHandle, nil
}

// patchSnapshotAnnotations applies a strategic-merge patch that adds (or
// overwrites) the given annotations on a VolumeSnapshot. Other annotations
// are preserved by the merge patch semantics.
//
// On success the patched annotations are also merged into the in-memory
// snap.VolumeSnapshot so that subsequent reads in the same reconcile see
// the new values. Without this, code paths like ReconcileFSR that run an
// enable pass (which may flip fsr-state to "enabled") and then gate the
// disable pass on reading that same annotation would always see the stale
// pre-patch value and skip disable until the next reconcile event.
func patchSnapshotAnnotations(snap *GeminiSnapshot, anns map[string]string) error {
	body := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": anns,
		},
	}
	patch, err := json.Marshal(body)
	if err != nil {
		return err
	}
	client := kube.GetClient()
	if _, err = client.SnapshotClient.Namespace(snap.Namespace).Patch(
		context.TODO(), snap.Name, types.MergePatchType, patch, metav1.PatchOptions{},
	); err != nil {
		return err
	}
	if snap.VolumeSnapshot != nil {
		if snap.VolumeSnapshot.ObjectMeta.Annotations == nil {
			snap.VolumeSnapshot.ObjectMeta.Annotations = map[string]string{}
		}
		for k, v := range anns {
			snap.VolumeSnapshot.ObjectMeta.Annotations[k] = v
		}
	}
	return nil
}
