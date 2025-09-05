package index

type Header struct {
	Offset uint32
}

type Block struct {
	Header []byte
	Data   []byte
	Size   uint32
}
