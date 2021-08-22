package helper

import (
	"testing"

	wp "github.com/cdutwhu/n3-deep6-v2/workpath"
	"github.com/dgraph-io/badger/v3"
	dbset "github.com/digisan/data-block/store/db"
)

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
		want sta
	}{
		// TODO: Add test cases.
		{
			name: "IdStatus",
			args: args{
				id: "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9",
				db: db,
			},
			want: Active,
		},
		{
			name: "IdStatus",
			args: args{
				id: "0054EB5F-07E6-4A26-84FA-2ADDBF5D84E8",
				db: db,
			},
			want: None,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IdStatus(tt.args.id, tt.args.db); got != tt.want {
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
				ids: []string{"0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := DelObj(tt.args.db, tt.args.ids...); (err != nil) != tt.wantErr {
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
				ids: []string{"0054EB5F-07E6-4A26-84FA-2ADDBF5D84E9"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := EraseObj(tt.args.db, tt.args.ids...); (err != nil) != tt.wantErr {
				t.Errorf("EraseObj() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
