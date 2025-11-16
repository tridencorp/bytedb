package bitbox

// Simple bytes buffer that tracks it's offset
type Buffer[T any] struct {
	bytes  []T
	Offset int
}

func NewBuffer[T any](bytes []T) *Buffer[T] {
	return &Buffer[T]{bytes: bytes, Offset: 0}
}

// Copy bytes from buffer to dst. Basically it's wrapper for copy().
func (b *Buffer[T]) Copy(dst []T) int {
	n := copy(dst, b.bytes[b.Offset:])
	b.Offset += n

	return n
}

// Get next N bytes from buffer
func (b *Buffer[T]) Next(num int) []T {
	off := b.Offset
	b.Offset += num

	return b.bytes[off:b.Offset]
}
