package function

import (
	"fmt"
	"testing"

	"github.com/cdutwhu/n3-deep6-v2/dbset"
	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	"github.com/dgraph-io/badger/v3"
)

func TestFindById(t *testing.T) {

	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		db *badger.DB
		id string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "FindById",
			args: args{
				db: db,
				id: "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9", // "4947ED1F-1E94-4850-8B8F-35C653F51E9C",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Println(FindById(tt.args.db, tt.args.id))
		})
	}
}
