package filemanager

import (
	"testing"
)

func TestNewFileManager(t *testing.T) {

	fm := NewFileManager("/Users/edmundmartin/SimpleDB/data", 4096)

	blk, err := fm.Append("test_file")
	if err != nil {
		t.Error(err)
	}

	pg := NewPageFromBlockSize(4096)
	pg.PutUint64(100, 0)
	pg.PutUInt32(200, 8)
	pg.PutString("Hello World", 12)

	err = fm.Write(blk, pg)
	if err != nil {
		t.Error(err)
	}

	copiedPage := NewPageFromBlockSize(4096)
	err = fm.Read(blk, copiedPage)
	if err != nil {
		t.Error(err)
	}
	val := copiedPage.GetUint64(0)
	if val != 100 {
		t.Errorf("unexpected value, got: %d", val)
	}
}
