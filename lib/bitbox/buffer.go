package bitbox

// Simple bytes buffer that tracks it's offset
type Buffer[T any] struct {
	bytes []T
	Off   int
}

func NewBuffer[T any](bytes []T) *Buffer[T] {
	return &Buffer[T]{bytes: bytes, Off: 0}
}

// Copy bytes from buffer to dst. Basically it's wrapper for copy().
func (b *Buffer[T]) Copy(dst []T) int {
	n := copy(dst, b.bytes[b.Off:])
	b.Off += n

	return n
}

// Get next N bytes from buffer.
// This will advance offset.
func (b *Buffer[T]) Take(num int) []T {
	off := b.Off
	b.Off += num

	return b.bytes[off:b.Off]
}

// Return []byte with remaining bytes
func (b *Buffer[T]) Data(num int) []T {
	return b.bytes[b.Off:]
}
