package fsr

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
)

func TestAZStateHelpers(t *testing.T) {
	states := []AZState{
		{AvailabilityZone: "az-a", State: "enabled"},
		{AvailabilityZone: "az-b", State: "disabling"},
	}
	assert.Equal(t, []string{"az-b", "az-c"}, MissingAZs(states, []string{"az-a", "az-b", "az-c"}))
	assert.Equal(t, []string{"az-a"}, ActiveAZs(states, []string{"az-a", "az-b", "az-c"}))
	assert.False(t, IsColdInAll(states, []string{"az-b"}), "disabling still occupies the lifecycle transition")
}

type cloudWatchStub struct {
	balances map[string]*float64
}

func (s *cloudWatchStub) GetMetricStatistics(_ context.Context, input *cloudwatch.GetMetricStatisticsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricStatisticsOutput, error) {
	az := ""
	for _, dimension := range input.Dimensions {
		if dimension.Name != nil && *dimension.Name == "AvailabilityZone" && dimension.Value != nil {
			az = *dimension.Value
		}
	}
	balance, ok := s.balances[az]
	if !ok {
		return &cloudwatch.GetMetricStatisticsOutput{}, nil
	}
	now := time.Now()
	return &cloudwatch.GetMetricStatisticsOutput{Datapoints: []cwtypes.Datapoint{{Timestamp: &now, Average: balance}}}, nil
}

func TestCreditsReadyRequiresOneCreditInEveryAZ(t *testing.T) {
	one := 1.0
	zero := 0.0
	client := &awsClient{cloudWatch: &cloudWatchStub{balances: map[string]*float64{
		"az-a": &one,
		"az-b": &zero,
	}}}

	ready, err := client.CreditsReady(context.Background(), "snap-123", []string{"az-a", "az-b"})
	assert.NoError(t, err)
	assert.False(t, ready)

	client.cloudWatch = &cloudWatchStub{balances: map[string]*float64{"az-a": &one, "az-b": &one}}
	ready, err = client.CreditsReady(context.Background(), "snap-123", []string{"az-a", "az-b"})
	assert.NoError(t, err)
	assert.True(t, ready)
}
