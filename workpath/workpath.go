package workpath

import (
	"path/filepath"

	"github.com/digisan/gotk/filedir"
	"github.com/digisan/gotk/io"
)

var (
	workPath   = "./"
	dtFilePath = filepath.Join(workPath, "config/datatypes.toml")
	dbPath     = filepath.Join(workPath, "data/badger/")
)

// set root work path
func SetWorkPath(path string) {
	wp, _ := filedir.AbsPath(path, false)
	io.MustCreateDir(wp)
	workPath = wp
	dtFilePath = filepath.Join(workPath, "config/datatypes.toml")
	dbPath = filepath.Join(workPath, "data/badger/")
}

// root work path
func WP() string { return workPath }

// datatypes.toml file path
func DTP() string { return dtFilePath }

// database storage path
func DBP() string { return dbPath }
