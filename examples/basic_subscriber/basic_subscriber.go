/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/corymonroe-coinbase/aeron-go/aeron"
	"github.com/corymonroe-coinbase/aeron-go/aeron/atomic"
	"github.com/corymonroe-coinbase/aeron-go/aeron/idlestrategy"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logbuffer"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logging"
	"github.com/corymonroe-coinbase/aeron-go/examples"
)

var logger = logging.MustGetLogger("basic_subscriber")

func main() {
	flag.Parse()

	if !*examples.ExamplesConfig.LoggingOn {
		logging.SetLevel(logging.INFO, "aeron")
		logging.SetLevel(logging.INFO, "memmap")
		logging.SetLevel(logging.INFO, "driver")
		logging.SetLevel(logging.INFO, "counters")
		logging.SetLevel(logging.INFO, "logbuffers")
		logging.SetLevel(logging.INFO, "buffer")
	}

	to := time.Duration(time.Millisecond.Nanoseconds() * 10000)
	ctx := aeron.NewContext().AeronDir(aeron.DefaultAeronDir + "/aeron-" + aeron.UserName).MediaDriverTimeout(to)

	a, err := aeron.Connect(ctx)
	if err != nil {
		fmt.Printf("Failed to connect to media driver: %s\n", err.Error())
	}
	defer a.Close()

	subscription := <-a.AddSubscription("aeron-spy:aeron:udp?tags=32|session-id=-826749489|alias=log", int32(100))
	defer subscription.Close()
	fmt.Printf("Subscription found %v", subscription)

	// tmpBuf := &bytes.Buffer{}
	counter := 1
	handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		offset += 32
		// bytes := buffer.GetBytesArray(offset, length)

		msgNo := buffer.GetInt32(offset)
		sendTime := buffer.GetInt64(offset + 4)
		fmt.Printf("msg:%d sendTime:%d\n", msgNo, sendTime)

		// tmpBuf.Reset()
		// buffer.WriteBytes(tmpBuf, offset+32, length)
		// fmt.Printf("%8.d: Gots me a fragment offset:%d length: %d payload: %s (buf:%s)\n", counter, offset, length, string(bytes), string(tmpBuf.Next(int(length))))
		// fmt.Println(bytes)
		// fmt.Println(header.FrameLength(), header.Offset())

		counter++
	}

	idleStrategy := idlestrategy.Sleeping{SleepFor: time.Millisecond}

	for {
		fragmentsRead := subscription.Poll(handler, 10000)
		idleStrategy.Idle(fragmentsRead)
	}
}
