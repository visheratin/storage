package storage

import (
	"testing"
)

func TestFileService_Save(t *testing.T) {
/*	fs, err := NewFileService("test")
	if err != nil {
		t.Error(err)
	}
	err = fs.Save(bytes.NewReader([]byte("123123123")), "/test")
	if err != nil {
		t.Error(err)
	}
	if _, err = os.Stat("test/test"); os.IsNotExist(err) {
		t.Error(err)
	}
	*/
}

func TestFileService_Read(t *testing.T) {
	fs, err := NewFileService("test")
	defer fs.Dispose()
	if err != nil {
		t.Error(err)
	}
	_, err = fs.Read("/test")
	if err != nil {
		t.Error(err)
	}
}
