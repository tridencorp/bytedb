package bitbox

// Simple bytes buffer that tracks it's offset
type Buffer struct {
	data []byte
	off  int
}

// Create new Buffer
func NewBuffer(data []byte) *Buffer {
	return &Buffer{data: data, off: 0}
}

// Decode data from buffer into objects
func (b *Buffer) Decode(objects ...any) {
	Decode(b)
}

// Return buffer length
func (b *Buffer) Len() int {
	return len(b.data[b.off:])
}

// Wrapper for copy()
func (b *Buffer) Copy(dst []byte) int {
	n := copy(dst, b.data[b.off:])
	b.off += n

	return n
}

// Take next N bytes from buffer.
// This will advance offset.
func (b *Buffer) Take(num int) []byte {
	off := b.off
	b.off += num

	return b.data[off:b.off]
}

// Return remaining bytes from buffer
func (b *Buffer) Data() []byte {
	return b.data[b.off:]
}

// Advance data offset
func (b *Buffer) Consume(n int) {
	b.off += n
}
