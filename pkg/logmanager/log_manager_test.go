package logmanager

import (
	"SimpleDB/pkg/filemanager"
	"os"
	"testing"
)

func TestNewLogManager(t *testing.T) {
	defer os.Remove("/Users/edmundmartin/SimpleDB/data/logfile")
	fm := filemanager.NewFileManager("/Users/edmundmartin/SimpleDB/data", 4096)

	m, err := NewLogManager(fm, "logfile")
	if err != nil {
		t.Error(err)
	}

	_, err = m.Append([]byte("Hello World"))
	if err != nil {
		t.Error(nil)
	}
}
