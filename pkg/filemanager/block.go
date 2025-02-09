package filemanager

import "fmt"

type BlockId struct {
	Filename string
	BlockNum int
}

func NewBlockID(filename string, blockNum int) *BlockId {
	return &BlockId{
		Filename: filename,
		BlockNum: blockNum,
	}
}

func (bl *BlockId) Equals(other *BlockId) bool {
	return bl.Filename == other.Filename && bl.BlockNum == other.BlockNum
}

func (bl *BlockId) String() string {
	return fmt.Sprintf("File: %s, Block: %d", bl.Filename, bl.BlockNum)
}
