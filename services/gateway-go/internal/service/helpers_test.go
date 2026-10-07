package service

import (
	"bytes"
	"io"
)

// bytesReader adapta []byte para o io.Reader que o jsonschema.UnmarshalJSON espera.
func bytesReader(data []byte) io.Reader { return bytes.NewReader(data) }
