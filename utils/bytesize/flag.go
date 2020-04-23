package bytesize

type Flag struct {
	size ByteSize
}

func (f *Flag) Size() ByteSize {
	return f.size
}

func (f *Flag) Default(size ByteSize) {
	f.size = size
}

func (f *Flag) String() string {
	return f.size.HumanString()
}

func (f *Flag) Set(value string) error {
	return f.size.UnmarshalText([]byte(value))
}
