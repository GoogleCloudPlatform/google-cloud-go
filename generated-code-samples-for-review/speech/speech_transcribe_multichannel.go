// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go_gapic. DO NOT EDIT.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	speech "cloud.google.com/go/speech/apiv1"
	speechpb "google.golang.org/genproto/googleapis/cloud/speech/v1"
)

// [START speech_transcribe_multichannel]

// sampleRecognize: Transcribe a short audio file with multiple channels
//
// localFilePath: Path to local audio file, e.g. /path/audio.wav
func sampleRecognize(localFilePath string) error {
	ctx := context.Background()
	c, err := speech.NewClient(ctx)
	if err != nil {
		return err
	}

	// localFilePath := "resources/multi.wav"
	// Path to local audio file, e.g. /path/audio.wav
	localFilePathBytes, err := ioutil.ReadFile(localFilePath)
	if err != nil {
		return err
	}

	req := &speechpb.RecognizeRequest{
		Audio: &speechpb.RecognitionAudio{
			AudioSource: &speechpb.RecognitionAudio_Content{
				Content: localFilePathBytes,
			},
		},
		Config: &speechpb.RecognitionConfig{
			// The number of channels in the input audio file (optional)
			AudioChannelCount: 2,
			// When set to true, each audio channel will be recognized separately.
			// The recognition result will contain a channel_tag field to state which
			// channel that result belongs to
			EnableSeparateRecognitionPerChannel: true,
			// The language of the supplied audio
			LanguageCode: "en-US",
		},
	}
	resp, err := c.Recognize(ctx, req)
	if err != nil {
		return err
	}

	for _, result := range resp.GetResults() {
		// channelTag to recognize which audio channel this result is for
		fmt.Printf("Channel tag: %v\n", result.GetChannelTag())
		// First alternative is the most probable result
		alternative := result.GetAlternatives()[0]
		fmt.Printf("Transcript: %v\n", alternative.GetTranscript())
	}
	return nil
}

// [END speech_transcribe_multichannel]

func main() {
	localFilePath := flag.String("local_file_path", "resources/multi.wav", "")
	flag.Parse()
	if err := sampleRecognize(*localFilePath); err != nil {
		log.Fatal(err)
	}
}
