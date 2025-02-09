package buffer

import (
	"SimpleDB/pkg/filemanager"
	"SimpleDB/pkg/logmanager"
)

type Buffer struct {
	fm       *filemanager.FileManager
	lm       *logmanager.LogManager
	contents *filemanager.Page
	blk      *filemanager.BlockId
	pins     int
	txNum    int
	lsn      int
}

func NewBuffer(fm *filemanager.FileManager, lm *logmanager.LogManager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: filemanager.NewPageFromBlockSize(fm.PageSize()),
		blk:      nil,
		pins:     0,
		txNum:    -1,
		lsn:      0,
	}
}

func (b *Buffer) Contents() *filemanager.Page {
	return b.contents
}

func (b *Buffer) Block() *filemanager.BlockId {
	return b.blk
}

func (b *Buffer) SetModified(txNum int, lsn int) {
	b.txNum = txNum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int {
	return b.txNum
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}

func (b *Buffer) AssignToBlock(blk *filemanager.BlockId) error {

	if err := b.flush(); err != nil {
		return err
	}
	b.blk = blk
	if err := b.fm.Read(b.blk, b.contents); err != nil {
		return err
	}
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.txNum >= 0 {
		if err := b.lm.FlushLSN(b.lsn); err != nil {
			return err
		}
		if err := b.fm.Write(b.blk, b.contents); err != nil {
			return err
		}
		b.txNum--
	}
	return nil
}