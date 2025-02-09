package filemanager

import "encoding/binary"

type Page struct {
	byteBuffer []byte
}

func NewPageFromBytes(contents []byte) *Page {
	return &Page{
		byteBuffer: contents,
	}
}

func NewPageFromBlockSize(blockSize int) *Page {
	buffer := make([]byte, blockSize)
	return &Page{
		byteBuffer: buffer,
	}
}

func (p *Page) PutUnit16(n uint16, offset int) {
	binary.BigEndian.PutUint16(p.byteBuffer[offset:], n)
}

func (p *Page) GetUnit16(offset int) uint16 {
	return binary.BigEndian.Uint16(p.byteBuffer[offset:])
}

func (p *Page) PutUInt32(n uint32, offset int) {
	binary.BigEndian.PutUint32(p.byteBuffer[offset:], n)
}

func (p *Page) GetUInt32(offset int) uint32 {
	return binary.BigEndian.Uint32(p.byteBuffer[offset:])
}

func (p *Page) PutUint64(n uint64, offset int) {
	binary.BigEndian.PutUint64(p.byteBuffer[offset:], n)
}

func (p *Page) GetUint64(offset int) uint64 {
	return binary.BigEndian.Uint64(p.byteBuffer[offset:])
}

func (p *Page) PutBool(b bool, offset int) {
	var val uint8
	if b == true {
		val = 1
	}
	p.byteBuffer[offset] = val
}

func (p *Page) GetBool(offset int) bool {
	val := p.byteBuffer[offset]
	return val > 0
}

func (p *Page) PutString(s string, offset int) {
	asBytes := []byte(s)
	size := uint32(len(asBytes))
	p.PutUInt32(size, offset)
	offset += 4
	copy(p.byteBuffer[offset:], asBytes)
}

func (p *Page) GetString(offset int) string {
	size := p.GetUInt32(offset)
	offset += 4
	return string(p.byteBuffer[offset : offset+int(size)])
}

func (p *Page) PutBytes(contents []byte, offset int) {
	size := uint32(len(contents))
	p.PutUInt32(size, offset)
	offset += 4
	copy(p.byteBuffer[offset:], contents)
}

func (p *Page) GetBytes(offset int) []byte {
	size := p.GetUInt32(offset)
	offset += 4
	return p.byteBuffer[offset : offset+int(size)]
}
