package basic

import (
	"fmt"
	"os"
	"testing"

	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	"github.com/dgraph-io/badger/v3"
	"github.com/cdutwhu/n3-deep6-v2/dbset"
)

func TestMapAllId(t *testing.T) {

	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("\n------------------------ object id list:         ------------------------")

	mIdVer, err := MapAllId(db, true)
	if err != nil {
		panic(err)
	}
	I := 1
	mIdVer.Range(func(id string, ver int64) bool {
		fmt.Printf("%s @ %d @ %d\n", id, ver, I)
		I++
		return true
	})
}

const testID = "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9"

func TestIdStatus(t *testing.T) {
	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		id string
		db *badger.DB
	}
	tests := []struct {
		name string
		args args
		want status
	}{
		// TODO: Add test cases.
		{
			name: "IdStatus",
			args: args{
				id: testID,
				db: db,
			},
			want: None,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IdStatus(tt.args.id, nil, tt.args.db); got != tt.want {
				t.Errorf("IdStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDelObj(t *testing.T) {
	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		db  *badger.DB
		ids []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "DelObj",
			args: args{
				db:  db,
				ids: []string{testID},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := DeleteObj(nil, tt.args.db, tt.args.ids...); (err != nil) != tt.wantErr {
				t.Errorf("DelObj() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEraseObj(t *testing.T) {
	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		db  *badger.DB
		ids []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "EraseObj",
			args: args{
				db:  db,
				ids: []string{testID},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := EraseObj(nil, tt.args.db, tt.args.ids...); (err != nil) != tt.wantErr {
				t.Errorf("EraseObj() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCleanupErased(t *testing.T) {
	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	type args struct {
		db *badger.DB
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "CleanupErased",
			args: args{
				db: db,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CleanupErased(tt.args.db); (err != nil) != tt.wantErr {
				t.Errorf("CleanupErased() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBadgerDump(t *testing.T) {
	wp.SetWorkPath("../")

	db, err := dbset.NewBadgerDB(wp.DBP())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	dumpfile := "dump.txt"
	os.Remove(dumpfile)
	if dbset.BadgerDumpFile(db, dumpfile) != nil {
		panic("BadgerDumpFile Panic")
	}
	fmt.Println("raw data dumped")
}
