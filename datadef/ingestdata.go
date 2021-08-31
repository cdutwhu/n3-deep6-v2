package datadef

import (
	"fmt"
	"strings"

	"github.com/digisan/gotk/slice/ts"
	jt "github.com/digisan/json-tool"
)

//
// structure used by the ingest pipeline to
// pass original data and derived data between
// each stage.
//
// Classification of the data, and derivation of
// the object unique id etc. are governed by the
// configuration found in the
// ./config/datatype.toml file, see comments there
// for more details.
//
type IngestData struct {
	//
	// Unique id for the object being processed
	// will be derived from the inbound json object
	// or created by the pipeline
	//
	N3id string
	//
	// Storage version for N3id in database
	//
	Version int64
	//
	// Classified indicate this ingested data instance
	// has been classified
	//
	Classified bool
	//
	// The declared type of the object
	// such as a SIF StudentPersonal
	// for data with no type system, will use
	// the object datamodel, all xAPI objects
	// for instance end up as type XAPI
	//
	Type string
	//
	// The datamodel that the object being processed
	// appears to belong to based on the
	// datatype.toml classification
	// if no model can be derived will default to JSON
	//
	DataModel string
	//
	// Original JSON Bytes
	//
	RawBytes []byte
	//
	// The unmarshaled json of the object
	// as a map[string]interface{}
	//
	RawData map[string]interface{}
	//
	// The specifications for which features of an object should
	// be surfaced as links within the graph.
	// Provided in ./config/datatype.toml.
	// Searches triple predicates for the spec, so use e.g.
	// ".RefId" to find the precise refid of a SIF object
	// but use "RefId" (no dot) to find refids of referenced objects
	// such as SchoolInfoRefId
	//
	LinkSpecs []string
	//
	// Array of  values extracted during classification
	// which will be concatenated to make
	// a unique property identifier for objects
	// that have no discriminating features, e.g.
	// a syllabus has a stage, but so do lessons
	// it has a subject, but so do lessons and subjects
	// so to avoid filtering in a traversal
	// a combination of stage and subject will link
	// to only one sylabus.
	//
	UniqueValues []string
	//
	// The resulting psuedo-unique key for this object.
	//
	Unique string
	//
	// Potential links are derived from the inbound object
	// but need to be verified and written, this is done
	// by different parts of the ingest pipeline and so
	// are carried between stages in this slice
	//
	LinkCandidates []Triple
	//
	// The slice of hexa-store triples parsed
	// from the original inbound data object
	//
	Triples []Triple
	//
	// The set of generated triples that link
	// the features requested in LinkSpecs to the
	// rest of the graph
	//
	LinkTriples []Triple
}

func (igd *IngestData) Print(msg interface{}, excl ...string) {

	const (
		format      = "\n%-30s%-5v"
		formatFirst = "%-30s%-5v"
		formatLast  = "\n%-30s%-5v"
	)

	fmt.Printf("\n\n%[1]v %[2]s %[1]v", msg, strings.Repeat("-", 120))

	fmt.Printf(format, "Classified:", igd.Classified)

	fmt.Printf(format, "N3id:", igd.N3id)

	fmt.Printf(format, "Version:", igd.Version)

	fmt.Printf(format, "Type:", igd.Type)

	fmt.Printf(format, "DataModel:", igd.DataModel)

	fmt.Printf(format, "Bytes length:", len(igd.RawBytes))
	if ts.In("RawBytes", excl...) {
		fmt.Print("	......")
	}
	if ts.NotIn("RawBytes", excl...) {
		fmt.Println(string(jt.Fmt(igd.RawBytes, "	")))
	}

	fmt.Printf(format, "The flat json map length:", len(igd.RawData))
	if ts.In("RawData", excl...) {
		fmt.Print("	......")
	}
	if ts.NotIn("RawData", excl...) {
		fmt.Println()
		for k, v := range igd.RawData {
			fmt.Println("	", k, v)
		}
	}

	fmt.Printf(format, "LinkSpecs:", igd.LinkSpecs)

	fmt.Printf(format, "Unique:", igd.Unique)

	fmt.Printf(format, "UniqueValues:", igd.UniqueValues)

	fmt.Printf(format, "Triples:", len(igd.Triples))
	if ts.In("Triples", excl...) {
		fmt.Print("	......")
	}
	if ts.NotIn("Triples", excl...) {
		fmt.Println()
		for _, t := range igd.Triples {
			fmt.Println("	", t)
		}
	}

	fmt.Printf(format, "LinkCandidates:", len(igd.LinkCandidates))
	if ts.In("LinkCandidates", excl...) {
		fmt.Print("	......")
	}
	if ts.NotIn("LinkCandidates", excl...) {
		fmt.Println()
		for _, t := range igd.LinkCandidates {
			fmt.Println("	", t)
		}
	}

	fmt.Printf(formatLast, "LinkTriples:", len(igd.LinkTriples))
	if ts.In("LinkTriples", excl...) {
		fmt.Print("	......")
	}
	if ts.NotIn("LinkTriples", excl...) {
		fmt.Println()
		for _, t := range igd.LinkTriples {
			fmt.Println("	", t)
		}
	}

	fmt.Println()
}
