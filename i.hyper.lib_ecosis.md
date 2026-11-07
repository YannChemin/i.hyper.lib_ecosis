# i.hyper.lib_ecosis

## NAME

*i.hyper.lib_ecosis* - Harvest and curate spectra from the EcoSIS spectral
library (ecosis.org) into a shared local spectral database for use by
*i.hyper.\** modules.

## SYNOPSIS

**i.hyper.lib_ecosis**
[**output**=*string*] [**format**=*string*] [**input_dir**=*string*]
[**max_cache_file_mb**=*value*] [**stream_batch_size**=*value*]
[**dataset_id**=*string*[,*string*,...]]
[**search**=*string*] [**base_url**=*string*] [**max_datasets**=*value*]
[**block_size**=*value*] [**scale**=*string*] [**-f**] [**-k**]

## DESCRIPTION

*i.hyper.lib_ecosis* is the first of an *i.hyper.lib_\** family of
harvesters that each pull spectra from a different public spectral library
(EcoSIS, and later e.g. USGS splib07, RELAB, SPECCHIO) into **one shared,
source-tagged local spectral database**, so that any *i.hyper.\** module
(e.g. a future spectral-matching or library-lookup tool) can query across
every harvested source uniformly, without caring which one a given
spectrum came from.

Two independent, combinable input modes:

- **input_dir**: merge/curate a directory of previously-downloaded EcoSIS
  dataset JSON files (the exact format produced by the companion
  [EcoSIS_Curator](https://github.com/) desktop tool: one
  `spectra_<title>.json` file per dataset, `{"dataset_info": {...},
  "spectra": [...]}`). No network access needed -- this mirrors
  EcoSIS_Curator's own "Merge Local Spectra" tool, but writes into the
  shared database instead of another ad hoc JSON file.
- **dataset_id=**/**search=**: fetch datasets live from the EcoSIS API
  (`/api/package/search` to discover by free-text query, then
  `/api/spectra/search/<id>`, paginated, for the actual records) -- the
  same two endpoints used by EcoSIS_Curator itself.

At least one of **input_dir**, **dataset_id**, **search** is required.

### Output: a shared, partitioned database

By default the database lives at `$HOME/grassdata/hyperspeclib` -- a fixed
location alongside (not inside) your GRASS project locations, since a
spectral library is a resource shared across projects, not tied to any one
project's CRS. Override with **output**.

- **format=parquet** (default): (Geo)Parquet, Hive-partitioned by
  `source_database=ecosis/`, one file per EcoSIS dataset
  (`dataset_<id>.parquet`). This is the OGC-community-recognized modern
  columnar format: splittable at the row-group and file level for
  parallel/distributed reads, supports column and partition pushdown
  (e.g. read only `longitude`/`latitude` for a spatial query, or only the
  `source_database=ecosis` partition), and gets a proper `geometry` (WKB
  Point) column with standards-compliant
  [GeoParquet](https://geoparquet.org/) file metadata whenever
  `shapely` is importable and at least one record has coordinates.
  Requires the `pyarrow` package (`pip install pyarrow`).
- **format=sqlite**: a single `hyperspeclib.sqlite` file, table `spectra`,
  primary key `(source_database, dataset_id, record_id)` so re-ingesting
  naturally upserts rather than duplicating. Zero extra dependencies
  (Python's built-in `sqlite3`), a reasonable fallback if you don't want
  to install `pyarrow`, though it doesn't give the same columnar
  parallel-scan performance.

### Common row schema

Every record, regardless of format, has the same fields -- a small set of
promoted "core" columns plus a catch-all for everything else, because
different EcoSIS datasets (and future non-EcoSIS sources) define wildly
different extra metadata columns that cannot be unified into one fixed
relational schema:

| Column | Meaning |
|---|---|
| `source_database` | Always `"ecosis"` for this harvester -- the field that lets a downstream query span multiple sources while still tracing each record back to where it came from |
| `dataset_id` | EcoSIS package ID |
| `dataset_title` | Human-readable dataset/collection name |
| `record_id` | EcoSIS's own per-spectrum `_id` |
| `organization` | Contributing organization, when known |
| `source_url` | `https://ecosis.org/package/<dataset_id>` -- click straight through to the original online dataset |
| `source_api_url` | `https://ecosis.org/api/package/<dataset_id>` -- for programmatic re-fetching |
| `longitude`, `latitude` | Decimal degrees, when the record has a location (many leaf-level datasets don't) |
| `measurement_type` | `"reflectance"` (EcoSIS's own convention) |
| `wavelength_unit` | `"nm"` |
| `n_bands` | Convenience count |
| `wavelengths` | List of band centers (nm), ascending |
| `values` | The spectrum itself, same order as `wavelengths`, normalised to the unit interval (see *Reflectance scale* below) |
| `extra_metadata` | JSON string: every other field EcoSIS returned for this record, verbatim -- including its own `ecosis` sub-object (resource_id, dataset_link, filename, geojson, ...), so nothing is ever lost even though it isn't promoted to its own column. When the values were rescaled at ingest, a `reflectance_scale` object is added here recording what was divided out |
| `ingest_date` | When this harvester wrote the record |

### Reflectance scale

EcoSIS takes each dataset as its contributor exported it and enforces no
scale convention, so unit-interval (0--1), percent (0--100) and
scaled-integer (0--10000) reflectance all arrive through the same API. Its
`Measurement Units` metadata field is missing or wrong often enough that it
cannot be trusted. The scale is therefore read from the values themselves
and divided out here, at harvest, so that every consumer of the shared
store -- *i.hyper.speclookup*, the `openspeclib` library build, any query of
the Parquet tree -- reads one convention instead of rediscovering three.

Two faults are handled, and they need different treatment.

**Whole-dataset scaling.** Every spectrum of a dataset carries the same
multiplier. The divisor is inferred once per dataset, as the smallest of
`{1, 100, 10000}` that brings the dataset's **median value** below ~1.2, and
applied to all of its rows. The intermediate decades (0--10, 0--1000) are
deliberately not candidates: they sit too close to their neighbours for a
median to separate them.

The statistic is the median of the *values*, not of the per-spectrum maxima.
Several EcoSIS datasets are unit-scale but carry ratio noise in the
water-vapour bands at 1350--1400 nm and 1800--1900 nm, where the reference
signal is near zero: those spectra sit at 0.3 everywhere and peak at 10 or
100 in three or four bands. A median of maxima reads such a dataset as
percent and divides the whole of it by 100; a median of values does not move.

With **scale**=*auto* (the default) the divisor is inferred from the
dataset's first batch (**stream_batch_size**, 5000 records by default) and
then applied to every batch of that dataset: one dataset has one scale, and
for all but a handful of EcoSIS datasets the first batch *is* the whole
dataset. Measured over the 195 datasets of the full local cache, the
first-batch divisor agreed with the whole-dataset divisor in every case.
**scale**=*unit*, *percent* or *scaled10k* force the divisor instead, and
**scale**=*none* stores the source values untouched.

**Per-spectrum scaling.** One spectrum of an otherwise consistent dataset is
on a different scale from the rest, or one contiguous span of a spectrum
carries a multiplier the rest of it does not. Both show up as a run of bands
above the unit ceiling, and telling them apart decides what may be divided.

A run is considered at all only when it is long enough to be a spectral
region rather than a glitch: at least 16 bands **and** 2 % of the spectrum,
tolerating dips of up to 4 bands below the ceiling. Then the **step across
its edges** -- the ratio of the bands just inside to the bands just outside
-- decides between three outcomes:

| What the run looks like | Reading | What is divided |
|---|---|---|
| Covers 80 % or more of the spectrum | No reflectance spectrum is mostly above 1.0, whatever its edges do | the whole spectrum |
| A step within a factor of 3 of the run's own multiplier | Two instruments' output spliced together, or a percent-scale block pasted into a unit-scale file | the run only |
| No step, and the run covers 10 % or more | A correctly shaped spectrum whose scale is wrong throughout, showing it only where it is brightest -- typically the near-infrared plateau | the whole spectrum |
| No step, and the run is under 10 % | A detector-junction or water-vapour artefact | nothing |

The last row is the important one. Ratio noise in the water-vapour bands at
1350--1400 nm and 1800--1900 nm, and the excursions at an instrument's
detector junctions, are a few percent of the bands and climb through the
ceiling continuously rather than stepping across it. They are a data-quality
problem of the source spectrum, not a scale error, and dividing them would
hide it, so they are left for validation to report. A whole-spectrum rescale
takes its divisor from the 99th percentile rather than the maximum, so one
noise spike cannot choose a divisor ten times too large.

The module warns, per dataset, how many records needed a per-spectrum
correction.

What was divided out is recorded per record in `extra_metadata`:

```json
"reflectance_scale": {
  "normalised_to": "unit",
  "source_divisor": 100,
  "range_corrections": [{"start": 1050, "stop": 2151, "divisor": 10}]
}
```

A record with no `reflectance_scale` key was already on the unit interval.
Note that **re-harvesting is what applies this to data already in the
store**: rows written by an earlier run keep their source scale until the
dataset is ingested again with **-f**.

### Incremental re-runs

A small `_manifest.json` at the database root tracks, per dataset, its
*source* record count (`expected_total` -- EcoSIS's own `total_spectra`/API
`total`, not how many were actually written) at last ingestion. Re-running
the same command later skips any dataset whose source count hasn't grown
(saved after *every* dataset, so a long run over ~200 files does not lose
all progress to an interruption partway through) -- use **-f** to force
re-ingestion (e.g. if EcoSIS added new records to a dataset you already
have). The comparison deliberately uses the *source* count rather than the
number of rows actually written: a dataset can legitimately have a
handful of records with no parseable spectrum every time (skipped, not an
error), so comparing against the written count would make the skip check
spuriously fail and re-ingest unchanged datasets on every run.

### Streaming ingestion (memory safety at any dataset size)

EcoSIS dataset JSON files are not line-delimited, and a few real ones are
enormous (one in the wild is over 3 GB on disk). With the `ijson` package
installed (`pip install ijson`), `input_dir` files are streamed rather
than loaded whole: `dataset_info` is read via a first pass that returns as
soon as that small object is parsed (it appears near the start of the
file, well before the potentially huge `spectra` array), then a second
pass streams `spectra` one record at a time via `ijson.items(f,
"spectra.item")`. Records are batched (**stream_batch_size**, default
5000) and written incrementally -- one `ParquetWriter.write_table()` call
per batch (each becomes its own row group in that dataset's single output
file) for `format=parquet`, or one `executemany()` + commit per batch for
`format=sqlite` -- so peak memory is bounded by one batch, independent of
how large the source dataset or the whole `input_dir` cache is. The live
API path streams the same way, yielding records as they're paginated
rather than accumulating the whole dataset first.

Without `ijson` installed, `input_dir` falls back to whole-file
`json.load()` (which holds the entire parsed structure in memory at once,
several times the file's on-disk size), and **max_cache_file_mb** (default
1000) skips, with a warning, any file larger than that rather than risk
exhausting memory. This limit does not apply when `ijson` is available --
streaming has no such ceiling.

## NOTES

Use **-k** when fetching many datasets by **search=** or a long
**dataset_id=** list, so one bad/renamed dataset doesn't abort the whole
run.

**max_datasets** only bounds how many datasets a **search=** query can
discover and queue -- it does not limit **input_dir** or an explicit
**dataset_id=** list.

Wavelength keys are read as a number in nanometres. A contributor may
have exported the unit with the key (`1651 nm`, `660 (nm) wavelength`)
or used micrometres (`1.651 um`); those forms are parsed and converted.
A key that is not a wavelength at all (a trait column) is left to
*extra_metadata* rather than guessed at.

## EXAMPLES

### Merge a local EcoSIS_Curator cache (no network)

```sh
i.hyper.lib_ecosis input_dir=$HOME/DBDATA/EcoSISData format=sqlite \
    output=/tmp/test_hyperspeclib
```

```text
Scanning local cache: /home/yann/DBDATA/EcoSISData…
Found 2 cached dataset file(s).
Done: 2 dataset(s) ingested (288 spectra), 0 already up to date, library at
/tmp/test_hyperspeclib (format=sqlite).
```

Re-running the exact same command skips both (already fully ingested):

```text
Done: 0 dataset(s) ingested (0 spectra), 2 already up to date, library at
/tmp/test_hyperspeclib (format=sqlite).
```

### Fetch one dataset live by ID, GeoParquet output

```sh
i.hyper.lib_ecosis dataset_id=ef43f755-9be1-409f-a0cb-375dabcb2b69 \
    format=parquet output=/tmp/test_hyperspeclib_pq
```

```text
Wrote 122 record(s) for 'Range Creek Utah species spectra' →
/tmp/test_hyperspeclib_pq/source_database=ecosis/dataset_ef43f755-9be1-409f-a0cb-375dabcb2b69.parquet
Done: 1 dataset(s) ingested (122 spectra), 0 already up to date, library at
/tmp/test_hyperspeclib_pq (format=parquet).
```

### Discover datasets by free-text search, capped

```sh
i.hyper.lib_ecosis search=sagebrush max_datasets=2 format=sqlite \
    output=/tmp/test_hyperspeclib
```

```text
Searching EcoSIS for 'sagebrush'…
Found 2 matching dataset(s) (capped at max_datasets=2).
Wrote 503 record(s) for 'California vegetation species image spectra' → ...
Wrote 101 record(s) for 'Missoula Montana lodgepole pine & big sagebrush time series' → ...
Done: 2 dataset(s) ingested (604 spectra), 0 already up to date, library at
/tmp/test_hyperspeclib (format=sqlite).
```

### Reading the GeoParquet library back (e.g. from a future i.hyper.* consumer)

```python
import pyarrow.dataset as ds

dataset = ds.dataset("/tmp/test_hyperspeclib_pq", format="parquet", partitioning="hive")
table = dataset.to_table(
    columns=["dataset_title", "record_id", "longitude", "latitude", "n_bands"],
    filter=(ds.field("source_database") == "ecosis") & (ds.field("longitude") < -100),
)
```

This reads only the requested columns, only the matching partition/rows,
without ever loading the whole library into memory -- the same pattern a
future `i.hyper.lib_usgs`/`i.hyper.lib_relab`/etc. sibling would let you
combine transparently, since they would all write into the same root
under their own `source_database=<name>/` partition.

### Real-world scale: the full local EcoSIS cache

Ingesting an entire ~11 GB local EcoSIS_Curator cache (197 dataset JSON
files, with `ijson` installed, into the default
`$HOME/grassdata/hyperspeclib`, format=parquet):

```sh
i.hyper.lib_ecosis input_dir=$HOME/DBDATA/EcoSISData
```

```text
Scanning local cache: /home/yann/DBDATA/EcoSISData…
Found 197 cached dataset file(s).
...
WARNING: Dataset '2009 to 2016 Cedar Creek Enemy Removal Experiment Canopy
         Reflectance (MSR)' yielded no usable spectra; skipped.
WARNING: Dataset 'Leaf reflectance spectra and nitrogen concentration of
         oilseed rape' yielded no usable spectra; skipped.
...
Done: 195 dataset(s) ingested (229893 spectra), 0 already up to date,
library at /home/yann/grassdata/hyperspeclib (format=parquet).
```

195 of 197 files became one Parquet file each (2 excluded: both had no
records with a parseable spectrum -- including the one ~3.3 GB file, which
streaming now handles like any other, in constant memory, rather than
skipping outright), totaling **2.1 GB on disk** (down from ~11 GB of
source JSON -- Parquet's columnar encoding plus zstd compression). A
cross-dataset geographic query over the entire library (195 files) via
`pyarrow.dataset`, reading only the columns and rows needed:

```python
import pyarrow.dataset as ds
dataset = ds.dataset("/home/yann/grassdata/hyperspeclib", format="parquet", partitioning="hive")
table = dataset.to_table(
    columns=["dataset_title", "longitude", "latitude"],
    filter=(ds.field("longitude") > -100) & (ds.field("longitude") < 0) & (ds.field("latitude") > 30),
)
# -> 24248 matching records, read directly from the relevant row groups
#    across the partitioned tree, no full-library load required.
```

Re-running the exact same command afterward is now fully idempotent:

```text
Done: 0 dataset(s) ingested (0 spectra), 195 already up to date, library at
/home/yann/grassdata/hyperspeclib (format=parquet).
```

## SEE ALSO

*[i.hyper.spectroscopy](i.hyper.spectroscopy.md),
[i.hyper.endmembers](i.hyper.endmembers.md),
[i.hyper.import](i.hyper.import.md)*

EcoSIS API: [https://ecosis.org](https://ecosis.org) --
reference client: `$HOME/dev/EcoSIS_Curator/ecosys_curator.py`

GeoParquet specification: [https://geoparquet.org](https://geoparquet.org)

## AUTHOR

Spectral Feature Extraction and Interpretation Engine
