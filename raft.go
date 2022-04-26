package main

import (
	"fmt"
	"os"

	"github.com/corymonroe-coinbase/aeron-go/aeron"
	"github.com/corymonroe-coinbase/aeron-go/aeron/atomic"
	"github.com/corymonroe-coinbase/aeron-go/aeron/idlestrategy"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logbuffer"
	"github.com/corymonroe-coinbase/aeron-go/cluster"
	"github.com/corymonroe-coinbase/aeron-go/cluster/codecs"
)

type Service struct {
	cluster cluster.Cluster
}

func (s *Service) OnStart(cluster cluster.Cluster, image *aeron.Image) {
	fmt.Printf("OnStart called\n")
	s.cluster = cluster
}

func (s *Service) OnSessionOpen(session cluster.ClientSession, timestamp int64) {}

func (s *Service) OnSessionClose(
	session cluster.ClientSession,
	timestamp int64,
	closeReason codecs.CloseReasonEnum,
) {
}

func (s *Service) OnSessionMessage(
	session cluster.ClientSession,
	timestamp int64,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	var result int64
	for {
		result = session.Offer(buffer, offset, length, nil)
		if result >= 0 {
			return
		}
		s.cluster.IdleStrategy().Idle(0)
	}
}

func (s *Service) OnTimerEvent(correlationId, timestamp int64) {}

func (s *Service) OnTakeSnapshot(publication *aeron.Publication) {}

func (s *Service) OnRoleChange(role cluster.Role) {
	fmt.Printf("OnRoleChange called: %v\n", role)
}

func (s *Service) OnTerminate(cluster cluster.Cluster) {
	fmt.Printf("OnTerminate called - role=%v logPos=%d\n", cluster.Role(), cluster.LogPosition())
}

func (s *Service) OnNewLeadershipTermEvent(
	leadershipTermId int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberId int32,
	logSessionId int32,
	timeUnit codecs.ClusterTimeUnitEnum,
	appVersion int32,
) {
	fmt.Printf("OnNewLeadershipTermEvent called: %d, %d, %d\n", leadershipTermId, logPosition, timestamp)
}

func main() {
	ctx := aeron.NewContext()
	if _, err := os.Stat("/dev/shm"); err == nil {
		path := fmt.Sprintf("/dev/shm/aeron-%s", aeron.UserName)
		ctx.AeronDir(path)
		fmt.Println("using path: ", path)
	}

	opts := cluster.NewOptions()
	opts.IdleStrategy = &idlestrategy.Busy{}
	if opts.ClusterDir = os.Getenv("CLUSTER_DIR"); opts.ClusterDir == "" {
		opts.ClusterDir = "/tmp/aeron-go-poc/cluster"
	}

	service := &Service{}
	agent, err := cluster.NewClusteredServiceAgent(ctx, opts, service)
	if err != nil {
		panic(err)
	}

	agent.StartAndRun()
}
