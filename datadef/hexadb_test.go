package datadef

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParseTriple(t *testing.T) {
	type args struct {
		tuple string
		sep   string
	}
	tests := []struct {
		name string
		args args
		want Triple
	}{
		// TODO: Add test cases.
		{
			name: "ParseTriple",
			args: args{
				tuple: "spo:dahernan:is-friend-of:agonzalezro",
				sep:   ":",
			},
			want: Triple{S: "dahernan", P: "is-friend-of", O: "agonzalezro"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseTripleData(tt.args.tuple); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseTriple() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTriple_HexaTuple(t *testing.T) {
	type fields struct {
		S string
		P string
		O string
	}
	type args struct {
		sep string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		// TODO: Add test cases.
		{
			name: "HexaTuple",
			fields: fields{
				S: "dahernan",
				P: "is-friend-of",
				O: "agonzalezro",
			},
			args: args{
				sep: "|",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Triple{
				S: tt.fields.S,
				P: tt.fields.P,
				O: tt.fields.O,
			}
			fmt.Println(tr)
			fmt.Println("\ntuple data:")
			for i, t := range tr.hexaTupleData() {
				fmt.Println(i, t)
			}
			fmt.Println("\ntuple link candidates:")
			for i, t := range tr.hexaTupleLinkCandidate() {
				fmt.Println(i, t)
			}
			fmt.Println("\ntuple links:")
			for i, t := range tr.hexaTupleLink() {
				fmt.Println(i, t)
			}
		})
	}
}

func TestTripleSave(t *testing.T) {

}
