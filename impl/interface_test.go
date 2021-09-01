package impl

import (
	"fmt"
	"testing"
)

func f(kv Ikv) {
	fmt.Println(kv)
}

func TestImpl(t *testing.T) {
	m := NewM()
	f(m)
	sm := NewSM()
	f(sm)
}
