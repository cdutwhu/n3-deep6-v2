package deep6

import (
	"fmt"
	"log"
	"os"

	boom "github.com/tylertreat/BoomFilters"
)

//
// loads the scalable bloom filter from disk
//
// sbf is used to capture required graph links as data
// flows through ingest - removes need for any dependency
// on ordering of data ingest, all models can loaded
// in any order or in mixed input files/streams
//
func OpenSBF(folderPath string) *boom.ScalableBloomFilter {
	sbf := boom.NewDefaultScalableBloomFilter(0.01)
	sbfFile := fmt.Sprintf("%s/sbf/featureLinks.sbf", folderPath)
	f, err := os.Open(sbfFile)
	if err != nil {
		log.Println("cannot find sbf file, creating new sbf.")
	} else {
		size, err := sbf.ReadFrom(f)
		if err != nil {
			log.Println("cannot read sbf from file, using default: ", err)
		}
		log.Printf("sbf loaded from file: %d bytes.", size)
	}
	return sbf
}

//
// saves the supplied sbf to disk
//
func SaveSBF(sbf *boom.ScalableBloomFilter, folderPath string) {
	sbfPath := fmt.Sprintf("%s/sbf", folderPath)
	err := os.MkdirAll(sbfPath, os.ModePerm)
	if err != nil {
		log.Println("cannot create sbf folder: ", sbfPath, err)
		return
	}
	sbfFile := fmt.Sprintf("%s/featureLinks.sbf", sbfPath)
	f, err := os.Create(sbfFile)
	if err != nil {
		log.Println("cannot create sbf file:", err)
		return
	}
	size, err := sbf.WriteTo(f)
	if err != nil {
		log.Println("cannot save sbf to file: ", err)
		return
	}
	log.Printf("saved sbf to file: %d bytes.", size)
}
