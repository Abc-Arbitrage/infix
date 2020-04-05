package logging

import (
	"fmt"
	"io"
	"strings"
)

// Writer defines an in-memory writer that aggregates common log messages
type Writer struct {
	messages map[string]uint64
}

// NewWriter creates a new Writer
func NewWriter() *Writer {
	return &Writer{
		messages: make(map[string]uint64),
	}
}

// Write implements Writer interface
func (w *Writer) Write(p []byte) (n int, err error) {
	text := strings.TrimSuffix(string(p), "\n")
	if count, ok := w.messages[text]; ok {
		w.messages[text] = count + 1
	} else {
		w.messages[text] = 1
	}

	return len(text), nil
}

// Flush flushes the Writer
func (w *Writer) Flush(iow io.Writer) {
	for text, count := range w.messages {
		if count > 1 {
			fmt.Fprintf(iow, "%s:  #%d actions\n", text, count)
		} else {
			fmt.Fprintln(iow, text)
		}
	}
}
