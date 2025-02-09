package filemanager

import "testing"

func TestPage_GetString(t *testing.T) {
	pg := NewPageFromBlockSize(4096)
	pg.PutString("HelloWorld", 100)
	result := pg.GetString(100)
	if result != "HelloWorld" {
		t.Errorf("unexpected string, got: %s", result)
	}
}

func TestPage_GetUInt32(t *testing.T) {
	pg := NewPageFromBlockSize(4096)
	pg.PutUInt32(100, 100)
	result := pg.GetUInt32(100)

	if result != 100 {
		t.Errorf("unexpected int, got: %d", result)
	}
}