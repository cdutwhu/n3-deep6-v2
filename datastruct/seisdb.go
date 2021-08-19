package datastruct

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

func ParseTriple(tuple string) Triple {
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

func (t Triple) HexaTuple() []string {
	return []string{
		fmt.Sprintf("spo%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.S, t.P, t.O),
		fmt.Sprintf("sop%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.S, t.O, t.P),
		fmt.Sprintf("ops%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.O, t.P, t.S),
		fmt.Sprintf("osp%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.O, t.S, t.P),
		fmt.Sprintf("pso%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.P, t.S, t.O),
		fmt.Sprintf("pos%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.P, t.O, t.S),
	}
}

func (t Triple) HexaTupleLink() []string {
	return []string{
		fmt.Sprintf("spol%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.S, t.P, t.O),
		fmt.Sprintf("sopl%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.S, t.O, t.P),
		fmt.Sprintf("opsl%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.O, t.P, t.S),
		fmt.Sprintf("ospl%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.O, t.S, t.P),
		fmt.Sprintf("psol%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.P, t.S, t.O),
		fmt.Sprintf("posl%[1]s%[2]v%[1]s%[3]v%[1]s%[4]v", sep, t.P, t.O, t.S),
	}
}
