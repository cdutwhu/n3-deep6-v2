package deep6

import (
	"fmt"
	"os"
	"testing"

	boom "github.com/tylertreat/BoomFilters"
)

func Test_openSBF(t *testing.T) {
	type args struct {
		folderPath string
	}
	tests := []struct {
		name string
		args args
		want *boom.ScalableBloomFilter
	}{
		// TODO: Add test cases.
		{
			name: "openSBF",
			args: args{
				folderPath: "./sbf",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OpenSBF(tt.args.folderPath)
			file := fmt.Sprintf("%s/test.sbf", tt.args.folderPath)
			os.MkdirAll(tt.args.folderPath, os.ModePerm)
			f, e := os.OpenFile(file, os.O_CREATE|os.O_RDWR, os.ModePerm)
			if e != nil {
				panic("Cannot open or create")
			}
			got.Add([]byte("abc"))
			got.WriteTo(f)
		})
	}
}

func Test_saveSBF(t *testing.T) {
	type args struct {
		sbf        *boom.ScalableBloomFilter
		folderPath string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SaveSBF(tt.args.sbf, tt.args.folderPath)
		})
	}
}
