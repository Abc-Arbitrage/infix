package storage

import (
	"log"
	"os"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// TSMRewriter defines a rewriter for a given TSM file
type TSMRewriter interface {
	Write(key []byte, values []tsm1.Value) error
	WriteSnapshot() error
	CompactFull() ([]string, error)
	Close() error
}

// CachedTSMRewriter defines a rewriter backed by an in-memory cache for key ordering
type CachedTSMRewriter struct {
	cache              *tsm1.Cache
	flushSizeThreshold uint64

	compactor *tsm1.Compactor
	fileStore *tsm1.FileStore

	path     string
	tsmFiles []string
}

// NewCachedTSMRewriter creates a new CacheRewriter with a maximum size in bytes
func NewCachedTSMRewriter(maxSize uint64, flushSizeThrehsold uint64, path string) *CachedTSMRewriter {
	cache := tsm1.NewCache(maxSize)

	fs := tsm1.NewFileStore(path)

	compactor := tsm1.NewCompactor()
	compactor.Dir = path
	compactor.FileStore = fs
	compactor.RateLimit = limiter.NewRate(DefaultCompactThroughput, DefaultCompactThroughputBurst)
	compactor.Open()

	return &CachedTSMRewriter{
		cache:              cache,
		flushSizeThreshold: flushSizeThrehsold,
		path:               path,
		compactor:          compactor,
		fileStore:          fs,
	}
}

// Write implements the Rewriter interface
func (w *CachedTSMRewriter) Write(key []byte, values []tsm1.Value) error {
	if err := w.cache.Write(key, values); err != nil {
		return err
	}

	sz := w.cache.Size()
	if sz > w.flushSizeThreshold {
		return w.WriteSnapshot()
	}

	return nil
}

// WriteSnapshot will snapshot the cache and write a new TSM file with its content
func (w *CachedTSMRewriter) WriteSnapshot() error {
	log.Printf("snapshoting cache")
	snapshot, err := w.cache.Snapshot()
	if err != nil {
		return err
	}

	if snapshot.Size() == 0 {
		w.cache.ClearSnapshot(true)
		return nil
	}

	// If the snapshot cache contains duplicated or unsorted date, deduplicate
	snapshot.Deduplicate()

	defer func() {
		if err != nil {
			w.cache.ClearSnapshot(false)
		}
	}()

	// write the snapshot files
	newFiles, err := w.compactor.WriteSnapshot(snapshot)
	if err != nil {
		return err
	}

	// update the file store with new TSM files from snapshot
	err = w.fileStore.ReplaceWithCallback(nil, newFiles, func(r []tsm1.TSMFile) {
		// We need to keep track of written TSM files to trigger a full compaction later
		for _, f := range r {
			log.Printf("wrote new TSM file '%s'\n", f.Path())
			w.tsmFiles = append(w.tsmFiles, f.Path())
		}
	})
	if err != nil {
		log.Println("error adding new TSM files from snapshot. Removing temporary files.")
		for _, file := range newFiles {
			if err := os.Remove(file); err != nil {
				return err
			}
		}
		return err
	}

	w.cache.ClearSnapshot(true)
	return nil
}

// CompactFull implements Rewriter interface
func (w *CachedTSMRewriter) CompactFull() ([]string, error) {
	if len(w.tsmFiles) == 0 {
		log.Println("skipping full compaction. No TSM files have been written")
		return nil, nil
	}

	files, err := w.compactor.CompactFull(w.tsmFiles)
	if err != nil {
		return nil, err
	}

	var newFiles []string

	err = w.fileStore.ReplaceWithCallback(w.tsmFiles, files, func(r []tsm1.TSMFile) {
		for _, f := range r {
			newFiles = append(newFiles, f.Path())
		}
	})

	if err != nil {
		log.Println("error adding new TSM files from full compaction. Removing temporary files.")
		for _, file := range files {
			if err := os.Remove(file); err != nil {
				return nil, err
			}
		}
		return nil, err
	}

	return newFiles, nil
}

// Close implements Rewriter interface
func (w *CachedTSMRewriter) Close() error {
	return os.RemoveAll(w.path)
}

// NoopTSMRewriter defines a rewriter that does nothing. Used in check mode
type NoopTSMRewriter struct {
}

// Write implements Rewriter interface
func (w *NoopTSMRewriter) Write(key []byte, values []tsm1.Value) error {
	return nil
}

// WriteSnapshot implemetns Rewriter interface
func (w *NoopTSMRewriter) WriteSnapshot() error {
	return nil
}

// CompactFull implements Rewriter interface
func (w *NoopTSMRewriter) CompactFull() ([]string, error) {
	return nil, nil
}

// Close implements Rewriter interface
func (w *NoopTSMRewriter) Close() error {
	return nil
}
