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
	"github.com/lirm/aeron-go/archive/codecs"
	logging "github.com/op/go-logging"
	"log"
	"os"
	"testing"
)

// Rather than mock or spawn an archive-media-driver we're just seeing
// if we can connect to one and if we can we'll run some tests. If the
// init fails to connect then we'll skip the tests
// FIXME: this plan fails as aeron-go calls log.Fatalf() !!!
var context *ArchiveContext
var archive *Archive
var haveArchive bool = false

type TestCases struct {
	sampleStream  int32
	sampleChannel string
	replayStream  int32
	replayChannel string
}

var testCases = []TestCases{
	{int32(*TestConfig.SampleStream), *TestConfig.SampleChannel, int32(*TestConfig.ReplayStream), *TestConfig.ReplayChannel},
}

func TestMain(m *testing.M) {
	var err error
	context = NewArchiveContext()
	context.AeronDir(*TestConfig.AeronPrefix)
	archive, err = ArchiveConnect(context)
	if err != nil || archive == nil {
		log.Printf("archive-media-driver connection failed, skipping all archive_tests")
		return
	} else {
		haveArchive = true
	}

	result := m.Run()
	archive.Close()
	os.Exit(result)
}

// This should always pass
func TestConnection(t *testing.T) {
	if !haveArchive {
		return
	}
}

// Test adding a recording
func TestStartStopRecording(t *testing.T) {
	if !haveArchive {
		return
	}

	recordingId, err := archive.StartRecording(testCases[0].sampleChannel, testCases[0].sampleStream, codecs.SourceLocation.LOCAL, true)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	t.Logf("id:%#v", recordingId)

	res, err := archive.StopRecordingByRecordingId(recordingId)
	if err != nil {
		t.Log(err, res)
		t.Fail()
	}
}

// Test adding a recording
func TestListRecordingsForUri(t *testing.T) {
	if !haveArchive {
		return
	}

	if testing.Verbose() {
		logging.SetLevel(logging.DEBUG, "archive")
	}

	// Add a recording to make sure there is one
	recordingId, err := archive.StartRecording(testCases[0].sampleChannel, testCases[0].sampleStream, codecs.SourceLocation.LOCAL, true)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	t.Logf("id:%#v", recordingId)

	count, err := archive.ListRecordingsForUri(0, 100, "aeron", testCases[0].sampleStream)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	t.Logf("count:%d", count)

	// Clean up
	res, err := archive.StopRecordingByRecordingId(recordingId)
	if err != nil {
		t.Log(err, res)
		t.Fail()
	}
}

// Test starting a replay
func TestStartStopReplay(t *testing.T) {
	if !haveArchive {
		return
	}

	// Add a recording to make sure there is one
	recordingId, err := archive.StartRecording(testCases[0].sampleChannel, testCases[0].sampleStream, codecs.SourceLocation.LOCAL, true)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	t.Logf("recordingId:%#v", recordingId)

	count, err := archive.ListRecordingsForUri(0, 100, "aeron", testCases[0].sampleStream)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if count == 0 {
		t.Log("FIXME:No recordings to start")
		t.Fail()
	}

	recordingId = archive.Control.Results.RecordingDescriptors[count-1].RecordingId
	t.Logf("id:%#v", recordingId)
	replayId, err := archive.StartReplay(recordingId, 0, -1, testCases[0].replayChannel, testCases[0].replayStream)
	if err != nil {
		t.Logf("StartReplay failed: %d, %s", replayId, err.Error())
		t.Fail()
	}

	// Clean up
	res, err := archive.StopReplay(replayId)
	if err != nil {
		t.Logf("StopReplay(%d) failed:%d %s", replayId, res, err.Error())
		t.Fail()
	}

	res, err = archive.StopRecordingByRecordingId(recordingId)
	if err != nil {
		t.Logf("StopRecordingByRecordingId(%d) failed:%d %s", replayId, res, err.Error())
		t.Fail()
	}
}
