package function

import (
	"fmt"
	"testing"

	"github.com/cdutwhu/n3-deep6-v2/dbset"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	"github.com/dgraph-io/badger/v3"
)

func TestFindByType(t *testing.T) {

	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		byWhat string
		db     *badger.DB
		arg    string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "FindByType",
			args: args{
				byWhat: "Type",
				db:     db,
				arg:    "TeachingGroup",
				// "Syllabus",
				// "XAPI",
				// "StudentPersonal",
				// "GradingAssignment",
				// "TeachingGroup",
				// "Syllabus",
				// "JSON",
			},
			want: "",
		},
		{
			name: "FindByValue",
			args: args{
				byWhat: "Value",
				db:     db,
				arg:    "Mathematics",
			},
			want: "",
		},
		{
			name: "FindByPredicate",
			args: args{
				byWhat: "Predicate",
				db:     db,
				arg:    "StudentPersonal.LocalId",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, obj := range FindByX(tt.args.byWhat, tt.args.db, tt.args.arg) {
				fmt.Println(i)
				fmt.Println(obj)
			}			
			fmt.Println("----------------------------------------------")
		})
	}
}
