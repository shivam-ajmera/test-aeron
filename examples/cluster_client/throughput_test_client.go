package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/corymonroe-coinbase/aeron-go/aeron"
	"github.com/corymonroe-coinbase/aeron-go/aeron/atomic"
	"github.com/corymonroe-coinbase/aeron-go/aeron/idlestrategy"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logbuffer"
	"github.com/corymonroe-coinbase/aeron-go/cluster/client"
)

type TestContext struct {
	ac                    *client.AeronCluster
	messageCount          int
	latencies             []int64
	nextSendKeepAliveTime int64
}

func (ctx *TestContext) OnConnect(ac *client.AeronCluster) {
	fmt.Printf("OnConnect - sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		ac.ClusterSessionId(), ac.LeaderMemberId(), ac.LeadershipTermId())
	ctx.ac = ac
	ctx.nextSendKeepAliveTime = time.Now().UnixMilli() + time.Second.Milliseconds()
}

func (ctx *TestContext) OnDisconnect(cluster *client.AeronCluster, details string) {
	fmt.Printf("OnDisconnect - sessionId=%d (%s)\n", cluster.ClusterSessionId(), details)
	ctx.ac = nil
}

func (ctx *TestContext) OnMessage(cluster *client.AeronCluster, timestamp int64,
	buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	recvTime := time.Now().UnixNano()
	msgNo := buffer.GetInt32(offset)
	sendTime := buffer.GetInt64(offset + 4)
	latency := recvTime - sendTime
	if msgNo < 1 || int(msgNo) > len(ctx.latencies) {
		fmt.Printf("OnMessage - sessionId=%d timestamp=%d pos=%d length=%d latency=%d\n",
			cluster.ClusterSessionId(), timestamp, header.Position(), length, latency)
	} else {
		ctx.latencies[msgNo-1] = latency
		ctx.messageCount++
		// WriteToArchive(ctx, buffer, offset, length, ctx.messageCount)
	}
}

func (ctx *TestContext) OnNewLeader(cluster *client.AeronCluster, leadershipTermId int64, leaderMemberId int32) {
	fmt.Printf("OnNewLeader - sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		cluster.ClusterSessionId(), leaderMemberId, leadershipTermId)
}

func (ctx *TestContext) OnError(cluster *client.AeronCluster, details string) {
	fmt.Printf("OnError - sessionId=%d: %s\n", cluster.ClusterSessionId(), details)
}

func (ctx *TestContext) sendKeepAliveIfNecessary() {
	if now := time.Now().UnixMilli(); now > ctx.nextSendKeepAliveTime && ctx.ac != nil && ctx.ac.SendKeepAlive() {
		ctx.nextSendKeepAliveTime += time.Second.Milliseconds()
	}
}

// func WriteToArchive(
// 	ctx *TestContext,
// 	buffer *atomic.Buffer,
// 	offset int32,
// 	length int32,
// 	offerCnt int) {

// 	ret := ctx.archPub.Offer(buffer, offset, length, nil)

// 	switch ret {
// 	case aeron.NotConnected:
// 		fmt.Printf("%d, Not connected (yet)", offerCnt)

// 	case aeron.BackPressured:
// 		fmt.Printf("%d: back pressured", offerCnt)
// 	default:
// 		if ret < 0 {
// 			fmt.Printf("%d: Unrecognized code: %d", offerCnt, ret)
// 		} else {
// 			fmt.Printf("%d: success!", offerCnt)
// 		}
// 	}
// 	if !ctx.archPub.IsConnected() {
// 		fmt.Println("no subscribers detected")
// 	}
// }

func main() {
	ctx := aeron.NewContext()
	if aeronDir := os.Getenv("AERON_DIR"); aeronDir != "" {
		ctx.AeronDir(aeronDir)
		fmt.Println("aeron dir: ", aeronDir)
	} else if _, err := os.Stat("/dev/shm"); err == nil {
		path := fmt.Sprintf("/dev/shm/aeron-%s", aeron.UserName)
		ctx.AeronDir(path)
		fmt.Println("aeron dir: ", path)
	}

	opts := client.NewOptions()
	if idleStr := os.Getenv("NO_OP_IDLE"); idleStr != "" {
		opts.IdleStrategy = &idlestrategy.Busy{}
	}
	opts.IngressChannel = "aeron:udp"
	opts.IngressEndpoints = "0=localhost:20000"
	//opts.EgressChannel = "aeron:udp?alias=cluster-egress|endpoint=localhost:11111"

	listener := &TestContext{
		latencies: make([]int64, 100),
	}
	clusterClient, err := client.NewAeronCluster(ctx, opts, listener)
	if err != nil {
		panic(err)
	}

	for !clusterClient.IsConnected() {
		opts.IdleStrategy.Idle(clusterClient.Poll())
	}

	// options := archive.DefaultOptions()
	// options.RequestChannel = *examples.Config.RequestChannel
	// options.RequestStream = int32(*examples.Config.RequestStream)
	// options.ResponseChannel = *examples.Config.ResponseChannel
	// options.ResponseStream = int32(*examples.Config.ResponseStream)

	// arch, err := archive.NewArchive(options, ctx)
	// if err != nil {
	// 	fmt.Println("Failed to connect to media driver: " + err.Error())
	// }
	// defer arch.Close()

	// channel := *examples.Config.SampleChannel
	// stream := int32(*examples.Config.SampleStream)

	// if _, err := arch.StartRecording(channel, stream, false, true); err != nil {
	// 	fmt.Println("StartRecording failed: " + err.Error())
	// 	os.Exit(1)
	// }

	// publication := <-arch.AddPublication(channel, stream)
	// fmt.Printf("Publication found %v", publication)
	// defer publication.Close()

	// idler := idlestrategy.Sleeping{SleepFor: time.Millisecond * 1000}
	// idler.Idle(0)

	// listener.arch = arch
	// listener.archPub = publication

	sendBuf := atomic.MakeBuffer(make([]byte, 250))
	padding := atomic.MakeBuffer(make([]byte, 200))
	for round := 1; round <= 10; round++ {
		fmt.Printf("starting round #%d\n", round)
		listener.messageCount = 0
		sentCt := 0
		beginTime := time.Now().UnixNano()
		latencies := listener.latencies
		for i := range latencies {
			latencies[i] = 0
		}
		ct := len(latencies)
		for i := 1; i <= ct; i++ {
			sendBuf.PutInt32(0, int32(i))
			sendBuf.PutInt64(4, time.Now().UnixNano())
			sendBuf.PutBytes(12, padding, 0, 200)
			for {
				if r := clusterClient.Offer(sendBuf, 0, sendBuf.Capacity()); r >= 0 {
					sentCt++
					break
				}
				clusterClient.Poll()
				listener.sendKeepAliveIfNecessary()
			}
		}
		for listener.messageCount < sentCt {
			pollCt := clusterClient.Poll()
			if pollCt == 0 {
				listener.sendKeepAliveIfNecessary()
			}
			opts.IdleStrategy.Idle(pollCt)
		}
		now := time.Now()
		totalNs := now.UnixNano() - beginTime
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("round #%d complete, count=%d min=%d 10%%=%d 50%%=%d 90%%=%d max=%d throughput=%.2f\n",
			round, sentCt, latencies[ct-sentCt]/1000, latencies[ct/10]/1000, latencies[ct/2]/1000, latencies[9*(ct/10)]/1000,
			latencies[ct-1]/1000, (float64(sentCt) * 1000000000.0 / float64(totalNs)))

		for time.Since(now) < 1*time.Second {
			listener.sendKeepAliveIfNecessary()
			opts.IdleStrategy.Idle(clusterClient.Poll())
		}
	}
	clusterClient.Close()
	fmt.Println("done")
	time.Sleep(time.Second)
}
