package storage

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

const (
	_fieldIndexFileName  = "fields.idx"
	_seriesFileDirectory = "_series"
)

// ShardInfo gives information about a shard
type ShardInfo struct {
	Path            string
	ID              uint64
	Database        string
	RetentionPolicy string

	TsmFiles    []string
	FieldsIndex *tsdb.MeasurementFieldSet
	WalFiles    []string
}

// LoadShards load all shards in a data directory
func LoadShards(dataDir string, walDir string, database string, retentionPolicy string, shardFilter string) ([]ShardInfo, error) {
	dbDirs, err := ioutil.ReadDir(dataDir)
	var shards []ShardInfo
	if err != nil {
		return nil, err
	}

	for _, db := range dbDirs {
		dbPath := filepath.Join(dataDir, db.Name())
		if !db.IsDir() {
			log.Println("Skipping database directory")
			continue
		}

		if database != "" && database != db.Name() {
			continue
		}

		rpDirs, err := ioutil.ReadDir(dbPath)
		if err != nil {
			return nil, err
		}

		for _, rp := range rpDirs {
			rpPath := filepath.Join(dataDir, db.Name(), rp.Name())
			if !rp.IsDir() {
				log.Println("Skipping retention policy directory")
			}

			if rp.Name() == _seriesFileDirectory {
				continue
			}

			if retentionPolicy != "" && retentionPolicy != rp.Name() {
				continue
			}

			shardDirs, err := ioutil.ReadDir(rpPath)
			if err != nil {
				return nil, err
			}

			for _, sh := range shardDirs {
				if sh.Name() == _seriesFileDirectory {
					continue
				}
				if shardFilter != "" && shardFilter != sh.Name() {
					continue
				}

				shPath := filepath.Join(dataDir, db.Name(), rp.Name(), sh.Name())
				walPath := filepath.Join(walDir, db.Name(), rp.Name(), sh.Name())

				shardID, err := strconv.ParseUint(sh.Name(), 10, 64)
				if err != nil {
					log.Printf("invalid shard ID found at path '%s'", shPath)
					return nil, err
				}

				log.Printf("Found shard '%s' (%d) with WAL '%s'\n", shPath, shardID, walPath)

				fieldsIndexPath := filepath.Join(shPath, _fieldIndexFileName)
				fieldsIndex, err := tsdb.NewMeasurementFieldSet(fieldsIndexPath)
				if err != nil {
					return nil, err
				}

				tsmFiles, err := filepath.Glob(filepath.Join(shPath, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
				if err != nil {
					return nil, err
				}

				walFiles, err := filepath.Glob(filepath.Join(walPath, fmt.Sprintf("%s*.%s", tsm1.WALFilePrefix, tsm1.WALFileExtension)))
				if err != nil {
					return nil, err
				}

				shardInfo := ShardInfo{
					Path:            shPath,
					ID:              shardID,
					Database:        db.Name(),
					RetentionPolicy: rp.Name(),
					TsmFiles:        tsmFiles,
					FieldsIndex:     fieldsIndex,
					WalFiles:        walFiles,
				}

				shards = append(shards, shardInfo)
			}
		}
	}

	return shards, nil
}
