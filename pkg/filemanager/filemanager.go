package filemanager

import (
	"os"
	"path/filepath"
	"sync"
)

type FileManager struct {
	directory string
	blockSize int
	isNew     bool
	openFiles map[string]*os.File
	mx        *sync.Mutex
}

func NewFileManager(dbDir string, blockSize int) *FileManager {

	isNew := directoryExists(dbDir)
	return &FileManager{
		directory: dbDir,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: map[string]*os.File{},
		mx:        &sync.Mutex{},
	}
}

func (fm *FileManager) getFile(filename string) (*os.File, error) {
	val, ok := fm.openFiles[filename]
	if ok {
		return val, nil
	}
	path := filepath.Join(fm.directory, filename)
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
}

func (fm *FileManager) Read(blk *BlockId, page *Page) error {
	fm.mx.Lock()
	defer fm.mx.Unlock()
	file, err := fm.getFile(blk.Filename)
	if err != nil {
		return err
	}
	if _, err := file.Seek(int64(blk.BlockNum*fm.blockSize), 0); err != nil {
		return err
	}
	if _, err := file.Read(page.byteBuffer); err != nil {
		return err
	}
	return nil
}

func (fm *FileManager) Write(blk *BlockId, page *Page) error {
	fm.mx.Lock()
	defer fm.mx.Unlock()
	file, err := fm.getFile(blk.Filename)
	if err != nil {
		return err
	}
	if _, err := file.Seek(int64(blk.BlockNum*fm.blockSize), 0); err != nil {
		return err
	}
	if _, err := file.Write(page.byteBuffer); err != nil {
		return err
	}
	return nil
}

func (fm *FileManager) Append(filename string) (*BlockId, error) {
	fm.mx.Lock()
	defer fm.mx.Unlock()
	newBlockNum, err := fm.Size(filename)
	if err != nil {
		return nil, err
	}
	blkId := NewBlockID(filename, newBlockNum)
	contents := make([]byte, fm.blockSize)

	file, err := fm.getFile(filename)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(int64(blkId.BlockNum*fm.blockSize), 0)
	if err != nil {
		return nil, err
	}
	_, err = file.Write(contents)
	if err != nil {
		return nil, err
	}
	file.Sync()
	return blkId, nil
}

func (fm *FileManager) Size(filename string) (int, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}
	stats, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := stats.Size()

	return int(size) / fm.blockSize, nil
}

func (fm *FileManager) PageSize() int {
	return fm.blockSize
}

func directoryExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return info.IsDir()
}
