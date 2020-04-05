package logging

import (
	"fmt"
	"io"
	"log"
)

var (
	instance = &manager{
		loggers: make(map[string]*log.Logger),
	}
)

// GetLogger returns an instance of logger for a given prefix
func GetLogger(prefix string) *log.Logger {
	return instance.getLogger(prefix)
}

// Flush flushes all loggers
func Flush(iow io.Writer) {
	for _, logger := range instance.loggers {
		w := logger.Writer().(*Writer)
		w.Flush(iow)
	}
}

type manager struct {
	loggers map[string]*log.Logger
}

func (m *manager) getLogger(prefix string) *log.Logger {
	if logger, ok := m.loggers[prefix]; ok {
		return logger
	}

	logger := log.New(NewWriter(), fmt.Sprintf("[%s] ", prefix), log.Lmsgprefix)
	m.loggers[prefix] = logger
	return logger
}
