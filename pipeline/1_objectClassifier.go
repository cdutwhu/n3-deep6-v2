package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	ds "github.com/cdutwhu/n3-deep6-v2/datastruct"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

//
// Identifies & classifies the object passed in from the
// upstream reader.
//
// Uses the config in ./config/datatype.toml for deriving the
// data model, unique id etc.
//
// ctx - context to manage the pipeline
// in - channel providing json string
//
func ObjectClassifier(ctx context.Context, filePath string, in <-chan string) (
	<-chan ds.IngestData, // emits IngestData objects with classification elements
	<-chan error, // emits errors encountered to the pipeline manager
	error) { // any error encountered when creating this component

	cOut := make(chan ds.IngestData)
	cErr := make(chan error, 1)

	// load the classifier definitions;
	// each data-model type characterized by properties of the
	// json data.
	//
	var c struct {
		Classifier []struct {
			Data_model     string   // DataMode / Type?
			Required_paths []string // Classified
			N3id           string   // N3id
			Links          []string // LinkSpecs
			Unique         []string // Unique
		}
	}
	classifierFile := fmt.Sprintf("%s/config/datatypes.toml", filePath)
	if _, err := toml.DecodeFile(classifierFile, &c); err != nil {
		return nil, nil, err
	}

	go func() {
		defer close(cOut)
		defer close(cErr)

		// I := 1

		for jsonStr := range in { // read json object (string) from upstream source

			// if I == 5 {
			// 	fmt.Println(I)
			// }
			// I++

			rawJson := []byte(jsonStr)

			jsonMap := make(map[string]interface{})
			if err := json.Unmarshal(rawJson, &jsonMap); err != nil {
				cErr <- errors.Wrap(err, "json Unmarshal error")
				return
			}

			var unique string

			// 12 fields
			igd := ds.IngestData{
				Classified:   false,
				N3id:         nuid.Next(),
				DataModel:    "JSON",
				Type:         "JSON",
				RawData:      jsonMap,
				RawBytes:     rawJson,
				UniqueValues: []string{},
				Unique:       "",
				LinkSpecs:    []string{},
			}

			//
			// check the data by comparing with the known
			// classification attributes from the config
			//
			for _, classifier := range c.Classifier {

				// extract the fields required for a synthetic unique id if specified
				if len(classifier.Unique) > 0 {
					results := gjson.GetManyBytes(rawJson, classifier.Unique...)
					for _, r := range results {
						if r.Exists() {
							igd.UniqueValues = append(igd.UniqueValues, r.String())
						}
					}
					unique = strings.Join(igd.UniqueValues, "-")
				}

				// now apply classification by 'required_paths'
				results := gjson.GetManyBytes(rawJson, classifier.Required_paths...)
				found := 0
				for _, r := range results {
					if r.Exists() {
						found++
					}
				}
				if len(classifier.Required_paths) == found {
					igd.Classified = true
				}

				if igd.Classified {
					// find the unique identifier for this object if no id available use a nuid
					result := gjson.GetBytes(rawJson, classifier.N3id)
					if result.Exists() {
						igd.N3id = result.String()
					}
					igd.DataModel = classifier.Data_model
					igd.Type = igd.DataModel
					igd.LinkSpecs = classifier.Links
					break
				}
			}

			// set the object type
			// if only 1 top level key, derive object type from it (SIF)
			// otherwise default to the datamodel as type (eg. xAPI)
			keys := []string{}
			for k := range igd.RawData {
				keys = append(keys, k)
			}
			if len(keys) == 1 {
				igd.Type = keys[0]
			}

			//
			// store metadata back into the map itself
			//
			igd.RawData["is-a"] = igd.Type
			if len(unique) > 0 {
				igd.RawData["unique"] = unique
				igd.Unique = unique
			}

			select {
			case cOut <- igd: // pass the data package on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}
	}()

	return cOut, cErr, nil
}
