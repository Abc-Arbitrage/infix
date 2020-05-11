# Infix

Infix is an open source InfluxDB disk utility to manage and apply a set of rules to TSM and WAL files.

Infix works by scanning a storage directory for TSM (`.tsm`) and WAL (`.wal`) files and then applying a set of rules
to data contained in those files.

Depending on rules being configured, Infix can rewrite TSM and WAL files to apply a set of transformations
(rename a measurement, update a field's type, rename a field, ...)

# Installation

To install Infix, run `go get`

```
go get -u github.com/Abc-Arbitrage/infix/command
```

# Usage

```
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
        The maximum in-memory cache size in bytes (defaults to 1GB)
    -cache-snapshot-size
        The size in bytes after which the cache will be snapshotted to disk when re-writing TSM files (defaults to 25MB)
    -v
        Enable verbose logging
    -check
        Run in check mode (do not apply any change)
    -config
        The configuration file
```

# Procedure

* Stop InfluxDB

Before running `infix`, stop your `influxd` process

```
sudo systemctl stop influxdb
```

* Run infix

Make sure to run `infix` with the appropriate user that owns your your TSM and WAL files.

```
sudo -u influxdb infix -datadir /var/lib/influxdb/data /var/lib/influxdb/wal -database telegraf -v -config rules.toml
```

* Optional: rebuild the TSI index

If you configured `infix` to drop or rename measurements or series, make sure to rebuild your [TSI index](https://docs.influxdata.com/influxdb/v1.8/administration/rebuild-tsi-index/#sidebar) if you are using the `tsi1` index type.

* Restart InfluxDB

Restart InfluxDB by starting the `influxd` process

```
sudo systemctl start influxdb
```

# Configuration

Rules and filters are configured in a [TOML](https://github.com/toml-lang/toml) file.

This sections lists all the available rules as well as sample configuration

## DropMeasurement Rule

This rule drops a given measurement

```
[[rules.drop-measurement]]
    [rules.drop-measurement.dropFilter.strings]
        hasprefix="linux."
```

will drop every measurement that starts with `linux.`

## DropSerie Rule

This rules drops fields from a given serie

```
[[rules.drop-serie]]
    [rules.drop-serie.dropFilter.serie]
        [rules.drop-serie.dropFilter.serie.measurement.strings]
            equal="cpu"
        [rules.drop-serie.dropFilter.serie.tag.where]
            cpu="cpu0"
        [rule.drop-serie.dropFilter.serie.field.pattern]
            pattern="^(idle|usage_idle)$"
```

will drop values from fields `idle` and `usage_idle` from serie from `cpu` measurement with tag value `cpu` matching value `cpu0`
Note that  `field` parameter can be omitted and all fields will be dropped.

## OldSerie Rule

This rule identifies series with points older than a configured timestamp

```
[[rules.old-serie]]
    time="2020-01-01T00:08:00Z"
    out="stdout"
    #out="out_file.log"
    format="text"
    #format="json"
    #timestamp=true
```

will print series older than `2020-01-01 00:08:00` to `stdout`
Output can be written to a file. Format can be either `text` or `json`. Setting `timestamp` to `true` will write
the last timestamp to the output

## RenameField Rule

This rules renames field from a given measurement

```
[[rules.rename-field]]
    to="agg_5m_${1}_${2}"
    [rules.rename-field.measurement.strings]
        hasprefix="linux."
    [rules.rename-field.field.pattern]
        pattern="(.+)_(avg|sum)"
```

will rename fields matching the pattern `(.+)_(avg_sum)` from measurements starting with `linux.` to `agg_5m_${1}_${2}`.

Note that if the field's filter is a `pattern` filter, the `to` can contains variables to replace matches of the regexp,
expanded by golang's [Regexp.ReplaceAll](https://golang.org/pkg/regexp/#Regexp.ReplaceAll) function

## UpdateFieldType Rule

This rule updates the type of a field from a given measurement

```
[[rules.update-field-type]]
    fromType="float"
    toType="integer"
    [rules.update-field.measurement.strings]
        equal="cpu"
    [rules.update-field.field.pattern]
        pattern="^(idle|active)"
```

will update the type of fields `idle` and `active` from the measurement `cpu` from `float` to `integer`

# Filters

Most rules require filters. Filters are configured by dotted keys. Given a configuration parameter `config-key` and
a filter `filter-name`, the corresponding key will be `config-key.filter-name`.

The fully qualified configuration key format for a filter applied to a given rule `rule-name` will be

```
[rules.rule-name.config-key.filter-name]
    # ... filter config
```

The section below lists all available filters and their configuration

## PatternFilter

This filter filters keys based on pattern represented by golang [Regexp](https://golang.org/pkg/regexp/) class

```
    pattern="^(cpu|disk)$"
```

## StringFilter

This filter filters keys based on golang [strings](https://golang.org/pkg/strings/) package

```
    hasprefix="linux."
    hassuffix=".gauge"
```

will filter a key if it starts with linux. or ends with .gauge

Supported functions are

* contains:    [strings.Contains](https://golang.org/pkg/strings/#Contains)
* containsAny: [strings.ContainsAny](https://golang.org/pkg/strings/#ContainsAny)
* equal:       a == b
* equalFold:   [strings.EqualFold](https://golang.org/pkg/strings/#EqualFold)
* hasprefix:   [strings.HasPrefix](https://golang.org/pkg/strings/#HasPrefix)
* hasuffix:    [strings.HasSuffix](https://golang.org/pkg/strings/#HasSuffix)

Note that functions are chained together with a `or`. The given configuration then translates to

```
strings.HasPrefix(key, "linux.") || strings.HasSuffix(key, ".gauge")
```

## SerieFilter

This filter should be used with rules that act on series like `DropSerie`. This filter builds on 3 underlying filters:

* Measurement to filter the measurement
* Tag to filter tags
* Field to filter fields

**Measurement** and **Tag** filters are mandatory. **Field** filter is optional. If none, all fields will pass the filter.

```
    [measurement.strings]
        equal="cpu"
    [tag.where]
        cpu="cpu0"
    [field.pattern]
        pattern="^(idle|usage_idle)$"
```

will filter series from measurement `cpu` (`StringFilter`) with tag `cpu` matching value `cpu0` having fields `idle` or `usage_idle`.

## WhereFilter

This filter is a special filter that can be used to filter tags and their corresponding values. It can be used in `SerieFilter` to filter tags.

```
    cpu="^(cpu0|cpu1)$"
    host="my-host"
```

will filter tag `cpu` matching pattern `^(cpu0|cpu1)$` and `host` matching `my-host`.
Note that tag values can use patterns.
