package logmanager

import (
	"SimpleDB/pkg/filemanager"
)

type LogIterator struct {
	fm                *filemanager.FileManager
	blk               *filemanager.BlockId
	page              *filemanager.Page
	currentPos        int
	boundary          int
	finishedIteration bool
}

func NewLogIterator(manager *filemanager.FileManager, blk *filemanager.BlockId) (*LogIterator, error) {
	li := &LogIterator{
		fm:         manager,
		blk:        blk,
		page:       filemanager.NewPageFromBytes(make([]byte, manager.PageSize())),
		currentPos: 0,
		boundary:   0,
	}
	if err := li.moveToBlock(blk); err != nil {
		return nil, err
	}
	return li, nil
}

func (li *LogIterator) HasNext() bool {
	return li.currentPos < li.fm.PageSize() || li.blk.BlockNum > 0
}

func (li *LogIterator) Next() ([]byte, error) {
	if li.currentPos == li.fm.PageSize() {
		li.blk = filemanager.NewBlockID(li.blk.Filename, li.blk.BlockNum-1)
		if err := li.moveToBlock(li.blk); err != nil {
			return nil, err
		}
	}
	contents := li.page.GetBytes(li.currentPos)
	if len(contents) == 0 {
		// TODO - Verify logic
		li.blk = filemanager.NewBlockID(li.blk.Filename, li.blk.BlockNum-1)
		err := li.moveToBlock(li.blk)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	li.currentPos += 4 + len(contents)
	return contents, nil
}

func (li *LogIterator) moveToBlock(blk *filemanager.BlockId) error {
	err := li.fm.Read(blk, li.page)
	if err != nil {
		return err
	}
	li.boundary = int(li.page.GetUInt32(0))
	li.currentPos = li.boundary
	return nil
}
