package logmanager

import (
	"SimpleDB/pkg/filemanager"
	"sync"
)

type LogManager struct {
	Fm           *filemanager.FileManager
	logFile      string
	logPage      *filemanager.Page
	currentBlock *filemanager.BlockId
	LatestLSN    int
	LastSavedLSN int
	mx           *sync.Mutex
}

func NewLogManager(fm *filemanager.FileManager, logfile string) (*LogManager, error) {
	contents := make([]byte, fm.PageSize())
	lm := &LogManager{
		Fm:      fm,
		logFile: logfile,
	}
	logPage := filemanager.NewPageFromBytes(contents)
	lm.logPage = logPage
	var currentBlock *filemanager.BlockId
	logSize, err := fm.Size(logfile)
	if err != nil {
		return nil, err
	}
	if logSize == 0 {
		currentBlock, err = lm.AppendNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		currentBlock = filemanager.NewBlockID(logfile, logSize-1)
		if err := fm.Read(currentBlock, logPage); err != nil {
			return nil, err
		}
	}
	lm.currentBlock = currentBlock

	return lm, nil
}

func (lm *LogManager) Iterator() (*LogIterator, error) {
	err := lm.flush()
	if err != nil {
		return nil, err
	}
	return NewLogIterator(lm.Fm, lm.currentBlock)
}

func (lm *LogManager) Append(record []byte) (int, error) {
	boundary := lm.logPage.GetUInt32(0)
	recordSize := len(record)
	// Uint32 is default for log operations
	bytesNeeded := 4 + recordSize
	if int(boundary)-bytesNeeded < 4 {
		lm.flush()
		var err error
		lm.currentBlock, err = lm.AppendNewBlock()
		if err != nil {
			return 0, err
		}
		boundary = lm.logPage.GetUInt32(0)
	}
	recpos := int(boundary) - bytesNeeded
	lm.logPage.PutBytes(record, recpos)
	lm.logPage.PutUInt32(0, recpos)
	lm.LatestLSN++
	return lm.LatestLSN, nil
}

func (lm *LogManager) AppendNewBlock() (*filemanager.BlockId, error) {
	blk, err := lm.Fm.Append(lm.logFile)
	if err != nil {
		return nil, err
	}

	lm.logPage.PutUInt32(uint32(lm.Fm.PageSize()), 0)
	if err := lm.Fm.Write(blk, lm.logPage); err != nil {
		return nil, err
	}
	return blk, nil
}

func (lm *LogManager) flush() error {
	if err := lm.Fm.Write(lm.currentBlock, lm.logPage); err != nil {
		return err
	}
	lm.LastSavedLSN = lm.LatestLSN
	return nil
}

func (lm *LogManager) FlushLSN(lsn int) error {

	if lsn > lm.LatestLSN {
		return lm.flush()
	}
	return nil
}
