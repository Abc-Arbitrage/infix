package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Abc-Arbitrage/infix/filter"
	"github.com/Abc-Arbitrage/infix/logging"
	"github.com/Abc-Arbitrage/infix/rules"
	"github.com/Abc-Arbitrage/infix/storage"
	"github.com/Abc-Arbitrage/infix/utils/bytesize"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/schollz/progressbar/v3"
)

var (
	defaultCacheMaxMemorySize      = bytesize.ByteSize(tsdb.DefaultCacheMaxMemorySize)
	defaultCacheSnapshotMemorySize = bytesize.ByteSize(tsdb.DefaultCacheSnapshotMemorySize)
)

// Command represents the program execution for "influxd dumptsm".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	config          string
	dataDir         string
	walDir          string
	database        string
	retentionPolicy string
	shardFilter     string

	maxCacheSize      bytesize.Flag
	cacheSnapshotSize bytesize.Flag

	verbose bool
	check   bool

	shards []storage.ShardInfo

	filter filter.Filter
	rules  []rules.Rule
}

// NewCommand returns a new instace of Command
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		filter: &filter.PassFilter{},
	}
}

// NewCommandWithRules returns a new instance of Command.
func NewCommandWithRules(rs rules.Set) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		rules:  rs.Rules(),
		filter: &filter.PassFilter{},
	}
}

// GlobalFilter sets the filter to apply globaly for all rules
func (cmd *Command) GlobalFilter(filter filter.Filter) {
	cmd.filter = filter
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	cmd.maxCacheSize.Default(defaultCacheMaxMemorySize)
	cmd.cacheSnapshotSize.Default(defaultCacheSnapshotMemorySize)

	fs := flag.NewFlagSet("file", flag.ExitOnError)
	fs.StringVar(&cmd.dataDir, "datadir", "/var/lib/influxdb/data", "Path to data storage")
	fs.StringVar(&cmd.walDir, "waldir", "/var/lib/influxdb/wal", "Path to WAL storage")
	fs.StringVar(&cmd.database, "database", "", "The database to enforce")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "The retention policy to enforce")
	fs.StringVar(&cmd.shardFilter, "shard", "", "The id of the shard to fix")
	fs.Var(&cmd.maxCacheSize, "max-cache-size", "The maximum in-memory cache size")
	fs.Var(&cmd.cacheSnapshotSize, "cache-snapshot-size", "The size after which the cache will be snapshotted to disk when re-writing TSM files.")
	fs.StringVar(&cmd.config, "config", "", "The configuration file for rules")
	fs.BoolVar(&cmd.verbose, "v", false, "Enable verbose logging")
	fs.BoolVar(&cmd.check, "check", false, "Run in check mode")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}

	if !cmd.verbose {
		log.SetOutput(ioutil.Discard)
	}

	if cmd.check {
		fmt.Fprintf(cmd.Stdout, "Running in check mode\n")
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	if err := checkRoot(); err != nil {
		return err
	}

	f, err := os.Open(cmd.config)
	if err != nil {
		return err
	}
	defer f.Close()

	rs, err := rules.LoadConfig(cmd.config)
	if err != nil {
		return err
	}

	for _, r := range rs {
		cmd.rules = append(cmd.rules, r)
	}

	shards, err := storage.LoadShards(cmd.dataDir, cmd.walDir, cmd.database, cmd.retentionPolicy, cmd.shardFilter)
	if err != nil {
		return err
	}

	return cmd.process(shards)
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	usage := `Apply rules to TSM and WAL files.

Usage: infix [options]

    -datadir
        Path to data storage (defaults to /var/lib/influxdb/data)
    -waldir
        Path to wal storage (defaults to /var/lib/influxdb/wal)
    -database
        The database to fix
    -retention
        The retention policy to fix
    -shard
        The id of the shard to fix
    -max-cache-size
        The maximum in-memory cache size in bytes (defaults to %s)
    -cache-snapshot-size
        The size in bytes after which the cache will be snapshotted to disk when re-writing TSM files (defaults to %s)
    -v
        Enable verbose logging
    -check
        Run in check mode (do not apply any change)
    -config
        The configuration file
`

	fmt.Fprintf(cmd.Stdout, fmt.Sprintf(usage, defaultCacheMaxMemorySize.HumanString(), defaultCacheSnapshotMemorySize.HumanString()))
}

func (cmd *Command) process(shards []storage.ShardInfo) error {
	for _, r := range cmd.rules {
		r.CheckMode(cmd.check)
		r.Start()
	}

	for _, sh := range shards {
		if err := cmd.processShard(sh); err != nil {
			return err
		}
	}

	for _, r := range cmd.rules {
		r.End()
	}

	logging.Flush(cmd.Stdout)

	return nil
}

func (cmd *Command) processShard(info storage.ShardInfo) error {
	fmt.Fprintf(cmd.Stdout, "Enforcing shard %d...\n", info.ID)

	for _, r := range cmd.rules {
		r.StartShard(info)
	}

	// we need to make sure we write the same order that the wal received the data
	tsmFiles := info.TsmFiles
	sort.Strings(tsmFiles)

	log.Printf("shard %d: enforcing %d tsm file(s)", info.ID, len(tsmFiles))

	for _, f := range tsmFiles {
		if err := cmd.processTSMFile(info, f); err != nil {
			return err
		}
	}

	walFiles := info.WalFiles
	sort.Strings(walFiles)

	log.Printf("shard %d: enforcing %d wal file(s)", info.ID, len(walFiles))
	for _, f := range walFiles {
		if err := cmd.processWALFile(info, f); err != nil {
			return err
		}
	}

	for _, r := range cmd.rules {
		r.EndShard()
	}

	if !cmd.check {
		// Write Field Index
		if err := info.FieldsIndex.Save(); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) processTSMFile(info storage.ShardInfo, tsmFilePath string) error {
	fmt.Fprintf(cmd.Stdout, "Enforcing TSM file '%s'...\n", tsmFilePath)

	rs := cmd.filterRules(cmd.rules, func(r rules.Rule) bool {
		return r.StartTSM(tsmFilePath)
	})

	if len(rs) == 0 {
		log.Printf("No candidate rule found for processing TSM file, skipping.")
		return nil
	}

	f, err := os.Open(tsmFilePath)
	if err != nil {
		return err
	}

	defer f.Close()
	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "unable to read %s, skipping: %s\n", tsmFilePath, err.Error())
		return nil
	}
	defer r.Close()

	w, err := cmd.createRewriter(tsmFilePath)

	if err != nil {
		return err
	}

	keyCount := r.KeyCount()

	log.Printf("%d total keys", keyCount)
	filtered := 0

	readRules := cmd.filterFlaggedRules(rs, rules.TSMReadOnly)
	writeRules := cmd.filterFlaggedRules(rs, rules.TSMWriteOnly)

	progress := progressbar.Default(int64(keyCount))

	for i := 0; i < keyCount; i++ {
		key, _ := r.KeyAt(i)

		progress.Add(1)

		if cmd.filter.Filter(key) {
			filtered++
			continue
		}

		readRules := cmd.filterRulesMatchingKey(readRules, key)
		writeRules := cmd.filterRulesMatchingKey(writeRules, key)

		if len(readRules) == 0 && len(writeRules) == 0 {
			filtered++
			continue
		}

		values, err := r.ReadAll(key)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
			continue
		}

		for _, r := range readRules {
			_, _, err := r.Apply(key, values)
			if err != nil {
				return err
			}
		}

		for _, r := range writeRules {
			key, values, err = r.Apply(key, values)
			if err != nil {
				return err
			}

			if key == nil {
				break
			}
		}

		if key != nil {
			if err := w.Write(key, values); err != nil {
				return err
			}
		}
	}

	if err := w.WriteSnapshot(); err != nil {
		return err
	}

	files, err := w.CompactFull()
	if err != nil {
		return err
	}

	if files != nil {
		if len(files) > 1 {
			return fmt.Errorf("Full compaction yielded more than one shard %v", files)
		}

		newFile := files[0]
		log.Printf("Fully compacted TSM file '%s'", newFile)

		log.Printf("Renaming '%s' to '%s'", newFile, tsmFilePath)
		if err := os.Rename(newFile, tsmFilePath); err != nil {
			return err
		}
	}

	log.Printf("%d (%d) total filtered keys", filtered, (filtered*100)/keyCount)

	if err := w.Close(); err != nil {
		return err
	}

	for _, r := range cmd.rules {
		r.EndTSM()
	}

	return nil
}

func (cmd *Command) processWALFile(info storage.ShardInfo, walFilePath string) error {
	fmt.Fprintf(cmd.Stdout, "Enforcing WAL file '%s'...\n", walFilePath)

	rs := cmd.filterRules(cmd.rules, func(r rules.Rule) bool {
		return r.StartWAL(walFilePath)
	})

	if len(rs) == 0 {
		log.Printf("No candidate rule found for processing WAL file, skipping.")
		return nil
	}

	f, err := os.Open(walFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	w, output, outputPath, err := cmd.createWALWriter(rs, walFilePath)
	if output != nil {
		defer output.Close()
	}

	if err != nil {
		return err
	}

	readRules := cmd.filterFlaggedRules(rs, rules.WALReadOnly)
	writeRules := cmd.filterFlaggedRules(rs, rules.WALWriteOnly)

	count := 0

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			fmt.Fprintf(cmd.Stderr, "file %s corrupt at position %d: %v", walFilePath, n, err)
			break
		}

		switch t := entry.(type) {
		case *tsm1.WriteWALEntry:
			var toDelete []string
			for key, values := range t.Values {
				for _, r := range readRules {
					_, _, err = r.Apply([]byte(key), values)
					if err != nil {
						return err
					}
				}

				newKey := []byte(key)
				for _, r := range writeRules {
					newKey, values, err = r.Apply(newKey, values)
					if err != nil {
						return err
					}
				}

				if newKey == nil {
					toDelete = append(toDelete, string(key))
				} else {
					t.Values[string(newKey)] = values
					if bytes.Compare([]byte(key), newKey) != 0 {
						toDelete = append(toDelete, string(key))
					}
				}
			}

			for _, key := range toDelete {
				delete(t.Values, key)
			}
		}

		if w != nil {
			b, err := encodeWALEntry(entry)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "Failed to encode WAL entry: %v", err)
				break
			}
			w.Write(entry.Type(), b)
		}
		count++
	}

	log.Printf("%d entries", count)

	if w != nil {
		log.Printf("Renaming '%s' to '%s'", outputPath, walFilePath)
		// Replace original file with new file.
		return os.Rename(outputPath, walFilePath)
	}

	return nil
}

func (cmd *Command) validate() error {
	if cmd.config == "" {
		return fmt.Errorf("must specify a configuration file")
	}
	if cmd.retentionPolicy != "" && cmd.database == "" {
		return fmt.Errorf("must specify a database")
	}
	return nil
}

func (cmd *Command) createRewriter(tsmFilePath string) (storage.TSMRewriter, error) {
	// If all rules are read-only, just return a NoopRewriter
	readRules := cmd.filterFlaggedRules(cmd.rules, rules.TSMReadOnly)
	readonly := len(readRules) == len(cmd.rules)

	if cmd.check || readonly {
		return &storage.NoopTSMRewriter{}, nil
	}

	// Remove previous temporary files.
	outputDir := tsmFilePath + ".rewriting"

	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.Mkdir(outputDir, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		files, err := ioutil.ReadDir(outputDir)
		if err != nil {
			return nil, err
		}

		for _, f := range files {
			path := filepath.Join(outputDir, f.Name())
			if err := os.Remove(path); err != nil {
				return nil, err
			}
		}
	}

	if err := os.RemoveAll(tsmFilePath + ".idx.tmp"); err != nil {
		return nil, err
	}

	log.Printf("Creating cached TSM rewriter to directory '%s'", outputDir)
	w := storage.NewCachedTSMRewriter(cmd.maxCacheSize.Size().UInt64(), cmd.cacheSnapshotSize.Size().UInt64(), outputDir)
	return w, nil
}

func (cmd *Command) createWALWriter(rs []rules.Rule, walFilePath string) (*tsm1.WALSegmentWriter, *os.File, string, error) {
	// If all rules are read-only, just return nil
	readRules := cmd.filterFlaggedRules(rs, rules.WALReadOnly)
	readonly := len(readRules) == len(rs)

	if cmd.check || readonly {
		return nil, nil, "", nil
	}

	// Remove previous temporary files.
	outputPath := walFilePath + ".rewriting.tmp"
	if err := os.RemoveAll(outputPath); err != nil {
		return nil, nil, "", err
	}

	// Create TSMWriter to temporary location.
	output, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, "", err
	}

	w := tsm1.NewWALSegmentWriter(output)

	return w, output, outputPath, nil
}

func (cmd *Command) filterRulesMatchingKey(rs []rules.Rule, key []byte) []rules.Rule {
	return cmd.filterRules(rs, func(r rules.Rule) bool {
		return r.FilterKey(key)
	})
}

func (cmd *Command) filterFlaggedRules(rs []rules.Rule, flags int) []rules.Rule {
	return cmd.filterRules(rs, func(r rules.Rule) bool {
		return r.Flags()&flags != 0
	})
}

func (cmd *Command) filterRules(rules []rules.Rule, filterFn func(rules.Rule) bool) (ret []rules.Rule) {
	for _, r := range rules {
		if filterFn(r) {
			ret = append(ret, r)
		}
	}
	return
}

func checkRoot() error {
	user, _ := user.Current()
	if user != nil && user.Username == "root" {
		warning := `You are currently running infix as root. This will write all your
TSM and WAL files with root ownership and will be inacessible
if you run influxd as a non-root user. You should run infix
as the same user you are running influxd (eg sudo -u influxdb infix [...])
`
		fmt.Print(warning)
		fmt.Print("Are you sure you want to continue? (yN): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("aborted by user")
		}
	}

	return nil
}

func encodeWALEntry(entry tsm1.WALEntry) ([]byte, error) {
	bytes := make([]byte, 1024<<2)

	b, err := entry.Encode(bytes)
	if err != nil {
		return nil, err
	}

	return snappy.Encode(b, b), nil
}
