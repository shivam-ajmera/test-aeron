// Copyright (C) 2021 Talos, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package archive

import (
	"bytes"
	"fmt"
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/archive/codecs"
	"time"
)

// Proxy class for encapsulating encoding and sending of control protocol messages to an archive
type Proxy struct {
	Publication  *aeron.Publication
	Marshaller   *codecs.SbeGoMarshaller
	SessionId    int64
	IdleStrategy idlestrategy.Idler
	Timeout      time.Duration
	Retries      int
}

// Create a proxy with default settings
func NewProxy(publication *aeron.Publication, idleStrategy idlestrategy.Idler, sessionId int64) *Proxy {
	proxy := new(Proxy)
	proxy.Publication = publication
	proxy.IdleStrategy = idleStrategy
	proxy.Marshaller = codecs.NewSbeGoMarshaller()
	proxy.Timeout = ArchiveDefaults.ControlTimeout
	proxy.Retries = ArchiveDefaults.ControlRetries
	proxy.SessionId = sessionId

	return proxy
}

// Registered for logging
func ArchiveProxyNewPublicationHandler(channel string, stream int32, session int32, regId int64) {
	logger.Debugf("ArchiveProxyNewPublicationHandler channel:%s stream:%d, session:%d, regId:%d", channel, stream, session, regId)
}

// Offer to our request publication
func (proxy *Proxy) Offer(buf bytes.Buffer) int64 {
	bytes := buf.Bytes()
	length := int32(buf.Len())
	buffer := atomic.MakeBuffer(bytes, length)

	var ret int64
	for retries := proxy.Retries; retries > 0; retries-- {
		ret = proxy.Publication.Offer(buffer, 0, length, nil)
		switch ret {
		case aeron.NotConnected:
			return ret // Fail immediately
		case aeron.PublicationClosed:
			return ret // Fail immediately
		case aeron.MaxPositionExceeded:
			return ret // Fail immediately
		default:
			if ret > 0 {
				return ret // Succeed
			} else {
				// Retry (aeron.BackPressured or aeron.AdminAction)
				proxy.IdleStrategy.Idle(0)
			}
		}
	}

	// Give up, returning the last failure
	logger.Debugf("Proxy.Offer giving up [%d]", ret)
	return ret

}

// From here we have all the functions that create a data packet and send it on the
// publication

// ConnectRequest
func (proxy *Proxy) Connect(responseChannel string, responseStream int32, correlationId int64) error {

	// Create a packet and send it
	bytes, err := ConnectRequestPacket(responseChannel, responseStream, correlationId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

//  ClosesSession
func (proxy *Proxy) CloseSession() error {
	// Create a packet and send it
	bytes, err := CloseSessionRequestPacket(proxy.SessionId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// StartRecording
// Uses the more recent protocol addition StartdRecordingRequest2 which added autoStop
func (proxy *Proxy) StartRecording(correlationId int64, stream int32, sourceLocation codecs.SourceLocationEnum, autoStop bool, channel string) error {

	bytes, err := StartRecordingRequest2Packet(proxy.SessionId, correlationId, stream, sourceLocation, autoStop, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// StopRecording
func (proxy *Proxy) StopRecording(correlationId int64, stream int32, channel string) error {
	// Create a packet and send it
	bytes, err := StopRecordingRequestPacket(proxy.SessionId, correlationId, stream, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// Replay
func (proxy *Proxy) Replay(correlationId int64, recordingId int64, position int64, length int64, replayChannel string, replayStream int32) error {

	// Create a packet and send it
	bytes, err := ReplayRequestPacket(proxy.SessionId, correlationId, recordingId, position, length, replayStream, replayChannel)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// StopReplay
func (proxy *Proxy) StopReplay(correlationId int64, replaySessionId int64) error {
	// Create a packet and send it
	bytes, err := StopReplayRequestPacket(proxy.SessionId, correlationId, replaySessionId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// ListRecordings
func (proxy *Proxy) ListRecordings(correlationId int64, fromRecordingId int64, recordCount int32) error {
	// Create a packet and send it
	bytes, err := ListRecordingsRequestPacket(proxy.SessionId, correlationId, fromRecordingId, recordCount)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// ListRecordingsForUri
// Lists up to recordCount recordings that match the channel and stream
func (proxy *Proxy) ListRecordingsForUri(correlationId int64, fromRecordingId int64, recordCount int32, stream int32, channel string) error {

	bytes, err := ListRecordingsForUriRequestPacket(proxy.SessionId, correlationId, fromRecordingId, recordCount, stream, channel)

	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// ListRecording
func (proxy *Proxy) ListRecording(correlationId int64, fromRecordingId int64) error {
	// Create a packet and send it
	bytes, err := ListRecordingRequestPacket(proxy.SessionId, correlationId, fromRecordingId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// ExtendRecording
// Uses the more recent protocol addition ExtendRecordingRequest2 which added autoStop
func (proxy *Proxy) ExtendRecording(correlationId int64, recordingId int64, stream int32, sourceLocation codecs.SourceLocationEnum, autoStop bool, channel string) error {
	// Create a packet and send it
	bytes, err := ExtendRecordingRequest2Packet(proxy.SessionId, correlationId, recordingId, stream, sourceLocation, autoStop, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// RecordingPosition
func (proxy *Proxy) RecordingPosition(correlationId int64, recordingId int64) error {
	// Create a packet and send it
	bytes, err := RecordingPositionRequestPacket(proxy.SessionId, correlationId, recordingId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// TruncateRecording
func (proxy *Proxy) TruncateRecording(correlationId int64, recordingId int64, position int64) error {
	// Create a packet and send it
	bytes, err := TruncateRecordingPacket(proxy.SessionId, correlationId, recordingId, position)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// StopRecordingSubscription
func (proxy *Proxy) StopRecordingBySubscriptionId(correlationId int64, subscriptionId int64) error {
	// Create a packet and send it
	bytes, err := StopRecordingSubscriptionPacket(proxy.SessionId, correlationId, subscriptionId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}

// StopRecordingIdentity
func (proxy *Proxy) StopRecordingByIdentity(correlationId int64, recordingId int64) error {
	// Create a packet and send it
	bytes, err := StopRecordingByIdentityPacket(proxy.SessionId, correlationId, recordingId)
	if err != nil {
		return err
	}

	if ret := proxy.Publication.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Publication.Offer failed: %d", ret)
	}

	return nil
}
