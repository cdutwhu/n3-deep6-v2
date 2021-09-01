package datadef

// SeisDB is a Hexastore based on this paper
// http://www.vldb.org/pvldb/1/1453965.pdf

//
// This version is taken from the original:
//
// www.github.com/dahernan/seisdb
//
// changes here are db is now managed elsewhere
// and we've added the cross-datatype link tuples
//
// meaning that data of interest is automatically linked
// within the db, rather than being linked at query time.
//
//

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/n3-deep6-v2/impl"
)

// hexa-tuples
// spo:dahernan:is-friend-of:agonzalezro
// sop:dahernan:agonzalezro:is-friend-of
// ops:agonzalezro:is-friend-of:dahernan
// osp:agonzalezro:dahernan:is-friend-of
// pso:is-friend-of:dahernan:agonzalezro
// pos:is-friend-of:agonzalezro:dahernan

const sep = "|"

type Triple struct {
	// subject
	S string
	// predicate
	P string
	// object
	O string
}

func ParseTripleData(tuple string) Triple {
	return parseTriple(tuple, "")
}

// remove prefix "lc-" on tuple string
func ParseTripleLinkCandidate(tuple string) Triple {
	return parseTriple(tuple, "lc-")
}

// remove prefix "l-" on tuple string
func ParseTripleLink(tuple string) Triple {
	return parseTriple(tuple, "l-")
}

func parseTriple(tuple, prefix string) Triple {

	tuple = tuple[len(prefix):]

	// parse this
	// spo:dahernan:is-friend-of:agonzalezro
	split := strings.SplitN(tuple, sep, 4)
	s := 1
	o := 2
	p := 3
	if len(split[0]) > 4 { // could be spo variant or spol
		return Triple{}
	}
	for index, ch := range split[0] {
		switch ch {
		case 's':
			s = index + 1
		case 'o':
			o = index + 1
		case 'p':
			p = index + 1
		}
	}
	return Triple{S: split[s], O: split[o], P: split[p]}
}

func (t Triple) hexaTuple(prefix string) []string {
	return []string{
		fmt.Sprintf("%[1]sspo%[2]s%[3]v%[2]s%[4]v%[2]s%[5]v", prefix, sep, t.S, t.P, t.O),
		fmt.Sprintf("%[1]ssop%[2]s%[3]v%[2]s%[4]v%[2]s%[5]v", prefix, sep, t.S, t.O, t.P),
		fmt.Sprintf("%[1]sops%[2]s%[3]v%[2]s%[4]v%[2]s%[5]v", prefix, sep, t.O, t.P, t.S),
		fmt.Sprintf("%[1]sosp%[2]s%[3]v%[2]s%[4]v%[2]s%[5]v", prefix, sep, t.O, t.S, t.P),
		fmt.Sprintf("%[1]spso%[2]s%[3]v%[2]s%[4]v%[2]s%[5]v", prefix, sep, t.P, t.S, t.O),
		fmt.Sprintf("%[1]spos%[2]s%[3]v%[2]s%[4]v%[2]s%[5]v", prefix, sep, t.P, t.O, t.S),
	}
}

func (t Triple) hexaTupleData() []string {
	return t.hexaTuple("")
}

func (t Triple) hexaTupleLinkCandidate() []string {
	return t.hexaTuple("lc-")
}

func (t Triple) hexaTupleLink() []string {
	return t.hexaTuple("l-")
}

// turn tuple into hexastore entries
// save triples(spo,etc.) & version into database

func (t Triple) Cache2Data(m impl.Ikv, ver int64) {
	for _, hexa := range t.hexaTupleData() {
		m.Set(hexa, ver)
	}
}

func (t Triple) Cache2LinkCandidate(m impl.Ikv, ver int64) {
	for _, hexa := range t.hexaTupleLinkCandidate() {
		m.Set(hexa, ver)
	}
}

func (t Triple) Cache2Link(m impl.Ikv, ver int64) {
	for _, hexa := range t.hexaTupleLink() {
		m.Set(hexa, ver)
	}
}
