package deep6

import (
	"context"
	"fmt"
	"io"

	pl "github.com/cdutwhu/n3-deep6-v2/pipeline"
	jt "github.com/digisan/json-tool"
	"github.com/pkg/errors"
)

//
// Runs the ingest pipeline to consume json data.
//
// Needs db and wb as these are presumed to be in use
// by other pipelines or application features.
//
// eg. db/wb are used here for loading data
// but db can also be in use to support queries in parallel.
//
// db - instance of a badger db
// wb - badger.WriteBatch, a fast write manager provided by the db
// sbf - bloom filter used to capture required graph links as data traverses the pipeline
// r - the io.Reader (file, http body etc.) to be ingested
// auditLevel - one of: none, basic, high
//
func runIngestWithReader(r io.Reader, folderPath string) error {

	// set up a context to manage ingest pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// monitor all error channels
	var cErrList []<-chan error

	//
	// build the pipeline by connecting all stages
	//

	cJsonOut, cErr, err := jt.ScanObjectInArray(ctx, r, true)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create json-reader source component: ")
	}
	cErrList = append(cErrList, cErr)

	cClassOut, cErr, err := pl.ObjectClassifier(ctx, folderPath, cJsonOut) // 1)
	if err != nil {
		return errors.Wrap(err, "Error: cannot create object-classifier component: ")
	}
	cErrList = append(cErrList, cErr)

	go func() {
		for c := range cClassOut {
			fmt.Println(c.N3id)
		}
	}()

	// remObjOut, errc, err := objectRemover(ctx, db, wb, sbf, auditLevel, folderPath, classOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create object-remover component: ")
	// }
	// errcList = append(errcList, errc)

	// genOut, errc, err := tupleGenerator(ctx, remObjOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create tuple-generator component: ")
	// }
	// errcList = append(errcList, errc)

	// writerOut, errc, err := tripleWriter(ctx, wb, genOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create triple-writer component: ")
	// }
	// errcList = append(errcList, errc)

	// linkerOut, errc, err := linkParser(ctx, sbf, writerOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create link-parser component: ")
	// }
	// errcList = append(errcList, errc)

	// reverselinkerOut, errc, err := linkReverseChecker(ctx, db, linkerOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create reverse-link-checker component: ")
	// }
	// errcList = append(errcList, errc)

	// builderOut, errc, err := linkBuilder(ctx, db, wb, reverselinkerOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create link-builder component: ")
	// }
	// errcList = append(errcList, errc)

	// lwriterOut, errc, err := linkWriter(ctx, wb, builderOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create link-writer component: ")
	// }
	// errcList = append(errcList, errc)

	// errc, err = ingestAuditSink(ctx, auditLevel, lwriterOut)
	// if err != nil {
	// 	return errors.Wrap(err, "Error: cannot create audit-sink component: ")
	// }
	// errcList = append(errcList, errc)

	// monitor progress
	err = pl.WaitForPipeline(cErrList...)

	return err
}

//
// same behaviour as run from reader, source here is a channel
// iterator providing json objects as []bytes
//
// func runIngestWithIterator(db *badger.DB, wb *badger.WriteBatch, sbf *boom.ScalableBloomFilter, c <-chan []byte, auditLevel, folderPath string) error {

// 	// set up a context to manage ingest pipeline
// 	ctx, cancelFunc := context.WithCancel(context.Background())
// 	defer cancelFunc()

// 	// monitor all error channels
// 	var errcList []<-chan error

// 	//
// 	// build the pipleine by connecting all stages
// 	//
// 	jsonOut, errc, err := jsonIteratorSource(ctx, c)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create json-reader source component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	classOut, errc, err := objectClassifier(ctx, folderPath, jsonOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create object-classifier component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	remObjOut, errc, err := objectRemover(ctx, db, wb, sbf, auditLevel, folderPath, classOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create object-remover component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	genOut, errc, err := tupleGenerator(ctx, remObjOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create tuple-generator component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	writerOut, errc, err := tripleWriter(ctx, wb, genOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create triple-writer component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	linkerOut, errc, err := linkParser(ctx, sbf, writerOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create link-parser component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	reverselinkerOut, errc, err := linkReverseChecker(ctx, db, linkerOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create reverse-link-checker component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	builderOut, errc, err := linkBuilder(ctx, db, wb, reverselinkerOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create link-builder component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	lwriterOut, errc, err := linkWriter(ctx, wb, builderOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create link-writer component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	errc, err = ingestAuditSink(ctx, auditLevel, lwriterOut)
// 	if err != nil {
// 		return errors.Wrap(err, "Error: cannot create audit-sink component: ")
// 	}
// 	errcList = append(errcList, errc)

// 	// monitor progress
// 	err = WaitForPipeline(errcList...)

// 	return err

// }
