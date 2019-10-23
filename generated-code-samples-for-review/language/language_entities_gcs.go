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
	"log"

	language "cloud.google.com/go/language/apiv1"
	languagepb "google.golang.org/genproto/googleapis/cloud/language/v1"
)

// [START language_entities_gcs]

// sampleAnalyzeEntities: Analyzing Entities in text file stored in Cloud Storage
//
// gcsContentUri: Google Cloud Storage URI where the file content is located.
// e.g. gs://[Your Bucket]/[Path to File]
func sampleAnalyzeEntities(gcsContentUri string) error {
	ctx := context.Background()
	c, err := language.NewClient(ctx)
	if err != nil {
		return err
	}

	// gcsContentUri := "gs://cloud-samples-data/language/entity.txt"
	req := &languagepb.AnalyzeEntitiesRequest{
		Document: &languagepb.Document{
			Source: &languagepb.Document_GcsContentUri{
				GcsContentUri: gcsContentUri,
			},
			// Available types: PLAIN_TEXT, HTML
			Type: languagepb.Document_PLAIN_TEXT,
			// Optional. If not specified, the language is automatically detected.
			// For list of supported languages:
			//   https://cloud.google.com/natural-language/docs/languages
			Language: "en",
		},
		// Available values: NONE, UTF8, UTF16, UTF32
		EncodingType: languagepb.EncodingType_UTF8,
	}
	resp, err := c.AnalyzeEntities(ctx, req)
	if err != nil {
		return err
	}

	// Loop through entitites returned from the API
	for _, entity := range resp.GetEntities() {
		fmt.Printf("Representative name for the entity: %v\n", entity.GetName())
		// Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
		fmt.Printf("Entity type: %v\n", entity.GetType())
		// Get the salience score associated with the entity in the [0, 1.0] range
		fmt.Printf("Salience score: %v\n", entity.GetSalience())
		// Loop over the metadata associated with entity. For many known entities,
		// the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
		// Some entity types may have additional metadata, e.g. ADDRESS entities
		// may have metadata for the address street_name, postal_code, et al.
		//
		for metadataName, metadataValue := range entity.GetMetadata() {
			fmt.Printf("%v: %v\n", metadataName, metadataValue)
		}
		// Loop over the mentions of this entity in the input document.
		// The API currently supports proper noun mentions.
		//
		for _, mention := range entity.GetMentions() {
			fmt.Printf("Mention text: %v\n", mention.GetText().GetContent())
			// Get the mention type, e.g. PROPER for proper noun
			fmt.Printf("Mention type: %v\n", mention.GetType())
		}
	}
	// Get the language of the text, which will be the same as
	// the language specified in the request or, if not specified,
	// the automatically-detected language.
	//
	fmt.Printf("Language of the text: %v\n", resp.GetLanguage())
	return nil
}

// [END language_entities_gcs]

func main() {
	gcsContentUri := flag.String("gcs_content_uri", "gs://cloud-samples-data/language/entity.txt", "")
	flag.Parse()
	if err := sampleAnalyzeEntities(*gcsContentUri); err != nil {
		log.Fatal(err)
	}
}
