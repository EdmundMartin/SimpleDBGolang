package logmanager

import (
	"SimpleDB/pkg/filemanager"
	"os"
	"testing"
)

func TestNewLogIterator(t *testing.T) {
	defer os.Remove("/Users/edmundmartin/SimpleDB/data/logfile")
	fm := filemanager.NewFileManager("/Users/edmundmartin/SimpleDB/data", 4096)

	m, err := NewLogManager(fm, "logfile")
	if err != nil {
		t.Error(err)
	}

	_, err = m.Append([]byte("Hello World"))
	if err != nil {
		t.Error(err)
	}

	_, err = m.Append([]byte("Something else"))
	if err != nil {
		t.Error(err)
	}

	iterator, err := m.Iterator()
	if err != nil {
		t.Error(err)
	}

	for iterator.HasNext() {
		contents, err := iterator.Next()
		if err != nil {
			t.Error(err)
		}
		if contents != nil {
			if len(contents) == 0 {
				t.Error("got empty contents")
			}
		}
	}
}
