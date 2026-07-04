#!/usr/bin/env python
##############################################################################
# MODULE:    i.hyper.lib_ecosis
# AUTHOR(S): Spectral Feature Extraction and Interpretation Engine
# PURPOSE:   Harvest and curate spectra from the EcoSIS spectral library
#            (ecosis.org), or from a local EcoSIS_Curator JSON cache, into a
#            shared, source-tagged, partitioned local spectral database
#            (GeoParquet by default, SQLite fallback) usable by any
#            i.hyper.* module and by future i.hyper.lib_* harvesters.
# COPYRIGHT: (C) 2026 by the GRASS Development Team
# SPDX-License-Identifier: GPL-2.0-or-later
##############################################################################

# %module
# % description: Harvest EcoSIS spectra (live API or local JSON cache) into a shared local spectral database for use by i.hyper.* modules
# % keyword: imagery
# % keyword: hyperspectral
# % keyword: spectral library
# % keyword: EcoSIS
# %end

# %option
# % key: output
# % type: string
# % required: no
# % description: Shared spectral library root directory (default: $HOME/grassdata/hyperspeclib -- a fixed location shared by all i.hyper.lib_* harvesters, not tied to any one GRASS project)
# % guisection: Output
# %end

# %option
# % key: format
# % type: string
# % required: no
# % options: parquet,sqlite
# % answer: parquet
# % description: Storage backend. parquet (GeoParquet when geometry is available) supports fast partitioned/parallel columnar access; sqlite is a zero-extra-dependency fallback
# % guisection: Output
# %end

# %option
# % key: input_dir
# % type: string
# % required: no
# % description: Directory of previously-downloaded EcoSIS dataset JSON files to merge/ingest (EcoSIS_Curator's own "spectra_*.json" format: {dataset_info, spectra}) -- no network access needed
# % guisection: Input
# %end

# %option
# % key: max_cache_file_mb
# % type: double
# % required: no
# % answer: 1000
# % description: Fallback-only safety cap (MB) on input_dir cache files, used solely when the 'ijson' package is not installed (whole-file JSON parsing is then used instead of streaming, which can expand several times the file's size in memory). Ignored when ijson is available -- streaming has no such limit.
# % guisection: Input
# %end

# %option
# % key: stream_batch_size
# % type: integer
# % required: no
# % answer: 5000
# % description: Number of records buffered in memory per incremental write batch during streaming ingestion (bounds peak memory regardless of dataset size)
# % guisection: Input
# %end

# %option
# % key: dataset_id
# % type: string
# % required: no
# % multiple: yes
# % description: EcoSIS package/dataset ID(s) to fetch live from the API
# % guisection: Input
# %end

# %option
# % key: search
# % type: string
# % required: no
# % description: Free-text query against EcoSIS's package search API; matching datasets (up to max_datasets) are fetched live
# % guisection: Input
# %end

# %option
# % key: base_url
# % type: string
# % required: no
# % answer: https://ecosis.org
# % description: EcoSIS API base URL
# % guisection: Input
# %end

# %option
# % key: max_datasets
# % type: integer
# % required: no
# % answer: 20
# % description: Safety cap on the number of datasets fetched in one run via search= (does not limit input_dir or explicit dataset_id=)
# % guisection: Input
# %end

# %option
# % key: block_size
# % type: integer
# % required: no
# % answer: 100
# % description: Pagination block size for the EcoSIS spectra/search API
# % guisection: Input
# %end

# %flag
# % key: f
# % description: Force re-ingestion of datasets already present in the library (default: skip datasets whose source record count hasn't grown since last ingestion)
# % guisection: Input
# %end

# %flag
# % key: k
# % description: Keep going on a per-dataset fetch/parse error instead of aborting the whole run
# %end

# %rules
# % required: input_dir,dataset_id,search
# %end

from __future__ import annotations

import os
import sys
import json
import math
import sqlite3
import datetime
from typing import Optional

import grass.script as gs

SOURCE_DATABASE = "ecosis"
DEFAULT_LIBRARY_ROOT = os.path.join(os.path.expanduser("~"), "grassdata", "hyperspeclib")
MANIFEST_NAME = "_manifest.json"

# Fields already promoted to dedicated columns; everything else in a record
# goes into extra_metadata (verbatim, including EcoSIS's own "ecosis" block --
# resource_id, dataset_link, filename, organization, geojson, ... -- so the
# online record is always traceable even for fields we don't promote).
_CORE_RECORD_KEYS = {"_id", "datapoints", "Longitude", "Latitude"}

# ---------------------------------------------------------------------------
# EcoSIS API (see $HOME/dev/EcoSIS_Curator/ecosys_curator.py for the reference
# client this mirrors: /api/package/search for dataset discovery, then
# /api/spectra/search/<dataset_id> paginated in blocks for the actual records)
# ---------------------------------------------------------------------------


def _http_get(url: str, params: dict, timeout: int = 60) -> dict:
    import requests
    resp = requests.get(url, params=params, timeout=timeout)
    if resp.status_code != 200:
        gs.fatal(f"EcoSIS API request failed ({resp.status_code}): {url}")
    return resp.json()


def search_datasets(base_url: str, text: str, max_datasets: int, block_size: int) -> list[dict]:
    """Query /api/package/search, paginated, capped at max_datasets."""
    url = f"{base_url.rstrip('/')}/api/package/search"
    datasets: list[dict] = []
    start = 0
    while len(datasets) < max_datasets:
        requested = min(block_size, max_datasets - len(datasets))
        stop = start + requested
        data = _http_get(url, {"text": text, "filters": "[]", "start": start, "stop": stop})
        items = data.get("items", [])
        if not items:
            break
        datasets.extend(items)
        start += len(items)
        if len(items) < requested:
            break  # server returned fewer than asked: no more results
    return datasets[:max_datasets]


def peek_dataset_total(base_url: str, dataset_id: str) -> int:
    """Cheap: /api/spectra/search/<id> reports a 'total' record count even
    for a single-item page, letting the caller decide whether to skip an
    already-fully-ingested dataset before streaming anything."""
    url = f"{base_url.rstrip('/')}/api/spectra/search/{dataset_id}"
    data = _http_get(url, {"start": 0, "stop": 1, "filters": "[]"})
    return int(data.get("total", 0))


def iter_dataset_spectra(base_url: str, dataset_id: str, block_size: int):
    """Paginate /api/spectra/search/<dataset_id>, yielding one record at a
    time (not accumulated into a list) until exhausted."""
    url = f"{base_url.rstrip('/')}/api/spectra/search/{dataset_id}"
    start = 0
    while True:
        data = _http_get(url, {"start": start, "stop": start + block_size, "filters": "[]"})
        batch = data.get("items", [])
        if not batch:
            break
        yield from batch
        start += len(batch)
        if len(batch) < block_size:
            break


def fetch_dataset_info(base_url: str, dataset_id: str) -> dict:
    """/api/package/<id> returns the dataset's own flat metadata dict; the
    human title/organization live under its "ecosis" sub-object as
    package_title/organization (same field names used per-record)."""
    url = f"{base_url.rstrip('/')}/api/package/{dataset_id}"
    data = _http_get(url, {})
    ecosis_block = (data.get("ecosis") or {}) if isinstance(data, dict) else {}
    return {
        "id": dataset_id,
        "title": ecosis_block.get("package_title") or dataset_id,
        "download_date": datetime.datetime.now().isoformat(),
        "source": "EcoSIS API",
    }

# ---------------------------------------------------------------------------
# Local EcoSIS_Curator JSON cache ("spectra_*.json": {dataset_info, spectra})
# ---------------------------------------------------------------------------


def _has_ijson() -> bool:
    try:
        import ijson  # noqa: F401
        return True
    except ImportError:
        return False


def _list_cache_files(input_dir: str, max_file_mb: float = 1000.0) -> list[str]:
    """List spectra_*.json file paths in input_dir.

    With ijson installed, every file is streamed (see _stream_cache_dataset)
    regardless of size -- no size filtering happens here in that case. Only
    the fallback path (ijson not installed, whole-file json.load()) applies
    the max_file_mb safety cap, since that path holds the whole parsed
    structure in memory at once and a multi-GB file is a real OOM risk.
    """
    streaming = _has_ijson()
    paths = []
    for name in sorted(os.listdir(input_dir)):
        if not (name.startswith("spectra_") and name.endswith(".json")):
            continue
        path = os.path.join(input_dir, name)
        size_mb = os.path.getsize(path) / 1e6
        if size_mb < 0.00005:
            continue
        if not streaming and size_mb > max_file_mb:
            gs.warning(
                f"Skipping {name}: {size_mb:.0f} MB exceeds max_cache_file_mb="
                f"{max_file_mb:.0f} (ijson not installed, so whole-file JSON "
                "parsing is used instead of streaming; install ijson to "
                "remove this limit, or raise max_cache_file_mb if you have "
                "enough memory)."
            )
            continue
        paths.append(path)
    return paths


def _load_cache_file(path: str) -> Optional[tuple[dict, list[dict]]]:
    """Whole-file fallback (no ijson): parse a spectra_*.json file entirely
    into memory. Returns None (with a warning) if it's unreadable or
    missing dataset_info/spectra."""
    name = os.path.basename(path)
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        gs.warning(f"Skipping unreadable cache file {name}: {exc}")
        return None
    dataset_info = data.get("dataset_info") or {}
    spectra = data.get("spectra") or []
    if not dataset_info or not spectra:
        gs.warning(f"Skipping {name}: missing dataset_info/spectra")
        return None
    return dataset_info, spectra


def stream_cache_dataset_info(path: str) -> Optional[dict]:
    """ijson pass 1: dataset_info is a small object near the start of the
    file, so this returns as soon as it's parsed without scanning the
    (potentially huge) "spectra" array that follows it."""
    import ijson
    with open(path, "rb") as f:
        return next(ijson.items(f, "dataset_info"), None)


def stream_cache_dataset_spectra(path: str):
    """ijson pass 2: yields one spectrum record at a time from the
    "spectra" array without ever holding the whole array in memory. Reopens
    the file (pass 1 only reads its small prefix, so this is not wasted
    work -- pass 2 is the one real full read, done exactly once)."""
    import ijson
    with open(path, "rb") as f:
        yield from ijson.items(f, "spectra.item")

# ---------------------------------------------------------------------------
# Record -> unified row schema
# ---------------------------------------------------------------------------


def _parse_spectrum(datapoints: dict) -> tuple[list[float], list[float]]:
    pairs = []
    for k, v in datapoints.items():
        try:
            fk, fv = float(k), float(v)
        except (TypeError, ValueError):
            continue
        # float() parses the literal strings "nan"/"inf" without raising
        # (unlike a genuinely unparseable value), so a datapoint recorded
        # as such would otherwise slip through as if it were a normal
        # measurement and silently corrupt any downstream computation
        # (e.g. spectral similarity matching) that assumes finite values.
        if not (math.isfinite(fk) and math.isfinite(fv)):
            continue
        pairs.append((fk, fv))
    pairs.sort(key=lambda p: p[0])
    wavelengths = [p[0] for p in pairs]
    values = [p[1] for p in pairs]
    return wavelengths, values


def _extract_lonlat(rec: dict) -> tuple[Optional[float], Optional[float]]:
    lon = rec.get("Longitude")
    lat = rec.get("Latitude")
    if lon in (None, "") or lat in (None, ""):
        geojson = (rec.get("ecosis") or {}).get("geojson") or {}
        coords = geojson.get("coordinates")
        if coords and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    try:
        lon = float(lon) if lon not in (None, "") else None
        lat = float(lat) if lat not in (None, "") else None
    except (TypeError, ValueError):
        lon = lat = None
    return lon, lat


def build_row(dataset_info: dict, rec: dict, base_url: str) -> Optional[dict]:
    """Transform one EcoSIS spectrum record into the common i.hyper.lib_*
    row schema. Returns None if the record has no usable spectrum."""
    datapoints = rec.get("datapoints") or {}
    wavelengths, values = _parse_spectrum(datapoints)
    if not wavelengths:
        return None

    dataset_id = str(dataset_info.get("id", ""))
    lon, lat = _extract_lonlat(rec)
    ecosis_block = rec.get("ecosis") or {}
    extra = {k: v for k, v in rec.items() if k not in _CORE_RECORD_KEYS}

    return {
        "source_database": SOURCE_DATABASE,
        "dataset_id": dataset_id,
        "dataset_title": str(dataset_info.get("title", "")),
        "record_id": str(rec.get("_id", "")),
        "organization": ecosis_block.get("organization"),
        "source_url": f"{base_url.rstrip('/')}/package/{dataset_id}",
        "source_api_url": f"{base_url.rstrip('/')}/api/package/{dataset_id}",
        "longitude": lon,
        "latitude": lat,
        "measurement_type": "reflectance",
        "wavelength_unit": "nm",
        "n_bands": len(wavelengths),
        "wavelengths": wavelengths,
        "values": values,
        "extra_metadata": json.dumps(extra, default=str),
        "ingest_date": datetime.datetime.now().isoformat(),
    }


def iter_row_batches(dataset_info: dict, record_iter, base_url: str, batch_size: int):
    """Consume a (possibly streaming) iterator of raw records and yield
    lists of built rows, batch_size at a time -- the unit both writer
    backends below write incrementally, so peak memory is one batch, not
    the whole dataset, regardless of whether record_iter is a streaming
    generator (ijson / live API pagination) or a plain in-memory list
    (the no-ijson fallback)."""
    batch = []
    for rec in record_iter:
        row = build_row(dataset_info, rec, base_url)
        if row is not None:
            batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# ---------------------------------------------------------------------------
# Manifest (per-dataset ingest bookkeeping, for incremental/skip-if-complete)
# ---------------------------------------------------------------------------


def load_manifest(root: str) -> dict:
    path = os.path.join(root, MANIFEST_NAME)
    if os.path.isfile(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_manifest(root: str, manifest: dict) -> None:
    path = os.path.join(root, MANIFEST_NAME)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def manifest_key(dataset_id: str) -> str:
    return f"{SOURCE_DATABASE}:{dataset_id}"

# ---------------------------------------------------------------------------
# Parquet (GeoParquet when shapely is available) backend
# ---------------------------------------------------------------------------

_PARQUET_SCHEMA_FIELDS = [
    ("source_database", "string"),
    ("dataset_id", "string"),
    ("dataset_title", "string"),
    ("record_id", "string"),
    ("organization", "string"),
    ("source_url", "string"),
    ("source_api_url", "string"),
    ("longitude", "double"),
    ("latitude", "double"),
    ("measurement_type", "string"),
    ("wavelength_unit", "string"),
    ("n_bands", "int32"),
    ("wavelengths", "list<double>"),
    ("values", "list<double>"),
    ("extra_metadata", "string"),
    ("ingest_date", "string"),
]


def _require_pyarrow():
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        gs.fatal(
            "format=parquet requires the 'pyarrow' package "
            "(pip install pyarrow). Use format=sqlite for a "
            "zero-extra-dependency alternative."
        )


def _has_shapely() -> bool:
    try:
        import shapely  # noqa: F401
        return True
    except ImportError:
        return False


def _geoparquet_metadata() -> bytes:
    return json.dumps({
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": None,  # OGC:CRS84 (lon/lat WGS84), GeoParquet default
            }
        },
    }).encode()


def _rows_to_arrow_table(rows: list[dict], include_geometry: bool):
    """One batch of rows -> a pyarrow Table with a fixed schema (whether or
    not this particular batch has any geolocated rows -- include_geometry
    is decided once per dataset/run, not per batch, so every batch written
    to the same incremental ParquetWriter has an identical schema)."""
    import pyarrow as pa

    columns = {name: [] for name, _ in _PARQUET_SCHEMA_FIELDS}
    for r in rows:
        for name, _ in _PARQUET_SCHEMA_FIELDS:
            columns[name].append(r.get(name))

    arrays = {
        "source_database": pa.array(columns["source_database"], type=pa.string()),
        "dataset_id": pa.array(columns["dataset_id"], type=pa.string()),
        "dataset_title": pa.array(columns["dataset_title"], type=pa.string()),
        "record_id": pa.array(columns["record_id"], type=pa.string()),
        "organization": pa.array(columns["organization"], type=pa.string()),
        "source_url": pa.array(columns["source_url"], type=pa.string()),
        "source_api_url": pa.array(columns["source_api_url"], type=pa.string()),
        "longitude": pa.array(columns["longitude"], type=pa.float64()),
        "latitude": pa.array(columns["latitude"], type=pa.float64()),
        "measurement_type": pa.array(columns["measurement_type"], type=pa.string()),
        "wavelength_unit": pa.array(columns["wavelength_unit"], type=pa.string()),
        "n_bands": pa.array(columns["n_bands"], type=pa.int32()),
        "wavelengths": pa.array(columns["wavelengths"], type=pa.list_(pa.float64())),
        "values": pa.array(columns["values"], type=pa.list_(pa.float64())),
        "extra_metadata": pa.array(columns["extra_metadata"], type=pa.string()),
        "ingest_date": pa.array(columns["ingest_date"], type=pa.string()),
    }

    if include_geometry:
        from shapely.geometry import Point
        wkb = [
            Point(lon, lat).wkb if lon is not None and lat is not None else None
            for lon, lat in zip(columns["longitude"], columns["latitude"])
        ]
        arrays["geometry"] = pa.array(wkb, type=pa.binary())

    return pa.table(arrays)


def write_parquet_dataset_streaming(root: str, dataset_id: str, batch_iter,
                                     include_geometry: bool) -> tuple[Optional[str], int]:
    """One Parquet file per dataset, under a Hive-style
    source_database=<name>/ partition -- any consumer can read the whole
    tree (pyarrow.dataset.dataset(root, partitioning='hive')) with
    predicate/column pushdown, or open a single dataset's file directly.

    batch_iter yields row-batches (lists of row dicts, from
    iter_row_batches()); each is written as its own row group via a single
    ParquetWriter kept open across all batches, so peak memory is one
    batch, not the whole dataset. Returns (path, n_rows) -- path is None if
    there were no rows at all.
    """
    import pyarrow.parquet as pq

    part_dir = os.path.join(root, f"source_database={SOURCE_DATABASE}")
    os.makedirs(part_dir, exist_ok=True)
    out_path = os.path.join(part_dir, f"dataset_{dataset_id}.parquet")

    writer = None
    n_total = 0
    try:
        for batch_rows in batch_iter:
            if not batch_rows:
                continue
            table = _rows_to_arrow_table(batch_rows, include_geometry)
            if writer is None:
                schema = table.schema
                if include_geometry:
                    schema = schema.with_metadata(
                        {**(schema.metadata or {}), b"geo": _geoparquet_metadata()})
                writer = pq.ParquetWriter(out_path, schema, compression="zstd")
            writer.write_table(table)
            n_total += len(batch_rows)
    finally:
        if writer is not None:
            writer.close()

    if n_total == 0:
        return None, 0
    return out_path, n_total

# ---------------------------------------------------------------------------
# SQLite backend (zero extra dependency)
# ---------------------------------------------------------------------------

_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS spectra (
    source_database TEXT NOT NULL,
    dataset_id      TEXT NOT NULL,
    dataset_title   TEXT,
    record_id       TEXT NOT NULL,
    organization    TEXT,
    source_url      TEXT,
    source_api_url  TEXT,
    longitude       REAL,
    latitude        REAL,
    measurement_type TEXT,
    wavelength_unit TEXT,
    n_bands         INTEGER,
    wavelengths     TEXT,
    values_json     TEXT,
    extra_metadata  TEXT,
    ingest_date     TEXT,
    PRIMARY KEY (source_database, dataset_id, record_id)
);
CREATE INDEX IF NOT EXISTS idx_spectra_dataset ON spectra (source_database, dataset_id);
CREATE INDEX IF NOT EXISTS idx_spectra_geo ON spectra (longitude, latitude);
"""


_SQLITE_INSERT = (
    "INSERT OR REPLACE INTO spectra VALUES "
    "(:source_database,:dataset_id,:dataset_title,:record_id,:organization,"
    ":source_url,:source_api_url,:longitude,:latitude,:measurement_type,"
    ":wavelength_unit,:n_bands,:wavelengths,:values_json,:extra_metadata,:ingest_date)"
)


def write_sqlite_dataset_streaming(root: str, batch_iter) -> tuple[Optional[str], int]:
    """batch_iter yields row-batches (lists of row dicts, from
    iter_row_batches()); each batch is inserted and committed before the
    next is requested, so peak memory is one batch, not the whole dataset.
    INSERT OR REPLACE on the (source_database, dataset_id, record_id)
    primary key means re-ingesting naturally upserts. Returns (path,
    n_rows) -- path is None if there were no rows at all.
    """
    db_path = os.path.join(root, "hyperspeclib.sqlite")
    con = sqlite3.connect(db_path)
    n_total = 0
    try:
        con.executescript(_SQLITE_DDL)
        for batch_rows in batch_iter:
            if not batch_rows:
                continue
            con.executemany(_SQLITE_INSERT, [
                {**r, "wavelengths": json.dumps(r["wavelengths"]),
                 "values_json": json.dumps(r["values"])}
                for r in batch_rows
            ])
            con.commit()
            n_total += len(batch_rows)
    finally:
        con.close()

    if n_total == 0:
        return None, 0
    return db_path, n_total

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(options, flags):
    output_root = options.get("output") or DEFAULT_LIBRARY_ROOT
    fmt = options.get("format", "parquet") or "parquet"
    input_dir = options.get("input_dir", "")
    dataset_ids = [d for d in (options.get("dataset_id", "") or "").split(",") if d]
    search_text = options.get("search", "")
    base_url = options.get("base_url", "https://ecosis.org") or "https://ecosis.org"
    max_datasets = int(options.get("max_datasets", "20") or "20")
    block_size = int(options.get("block_size", "100") or "100")
    max_cache_file_mb = float(options.get("max_cache_file_mb", "1000") or "1000")
    stream_batch_size = int(options.get("stream_batch_size", "5000") or "5000")
    force = flags["f"]
    keep_going = flags["k"]

    if fmt == "parquet":
        _require_pyarrow()
    streaming = _has_ijson()
    if input_dir and not streaming:
        gs.warning(
            "ijson not installed: falling back to whole-file JSON parsing "
            "for input_dir (pip install ijson to stream arbitrarily large "
            "cache files with bounded memory)."
        )
    # Decided once, applies to every batch of every dataset this run writes,
    # so all batches feeding one dataset's ParquetWriter share one schema
    # (see write_parquet_dataset_streaming).
    include_geometry = fmt == "parquet" and _has_shapely()

    os.makedirs(output_root, exist_ok=True)
    manifest = load_manifest(output_root)

    # --- Assemble lightweight job descriptors only (no parsed/fetched data
    # yet) -- each dataset is streamed, written in batches and discarded one
    # at a time in the loop below, so peak memory is one batch, not the
    # whole dataset, let alone the whole ~200-file, tens-of-GB cache.
    jobs: list[tuple[str, str]] = []  # (kind, ref); kind: "local" | "live"

    if input_dir:
        gs.message(f"Scanning local cache: {input_dir}…")
        cache_files = _list_cache_files(input_dir, max_cache_file_mb)
        jobs.extend(("local", path) for path in cache_files)
        gs.message(f"Found {len(cache_files)} cached dataset file(s).")

    if search_text:
        gs.message(f"Searching EcoSIS for '{search_text}'…")
        found = search_datasets(base_url, search_text, max_datasets, block_size)
        gs.message(f"Found {len(found)} matching dataset(s) (capped at max_datasets={max_datasets}).")
        for d in found:
            dataset_ids.append(str(d.get("_id")))

    jobs.extend(("live", did) for did in dict.fromkeys(dataset_ids))  # de-dup, preserve order

    if not jobs:
        gs.fatal("Nothing to ingest: no local cache files and no datasets found/given.")

    # --- Ingest each dataset, streaming one batch at a time -----------------
    n_datasets_written = 0
    n_records_written = 0
    n_datasets_skipped = 0

    for i, (kind, ref) in enumerate(jobs):
        gs.percent(i, len(jobs), 2)

        try:
            if kind == "local" and streaming:
                dataset_info = stream_cache_dataset_info(ref)
                if not dataset_info:
                    gs.warning(f"Skipping {os.path.basename(ref)}: no dataset_info found.")
                    continue
                expected = int(dataset_info.get("total_spectra") or 0)
                record_source = lambda ref=ref: stream_cache_dataset_spectra(ref)
            elif kind == "local":
                parsed = _load_cache_file(ref)
                if parsed is None:
                    continue
                dataset_info, spectra_list = parsed
                expected = len(spectra_list)
                record_source = lambda spectra_list=spectra_list: iter(spectra_list)
            else:
                dataset_info = fetch_dataset_info(base_url, ref)
                expected = peek_dataset_total(base_url, ref)
                record_source = lambda ref=ref: iter_dataset_spectra(base_url, ref, block_size)
        except Exception as exc:
            msg = f"Could not load dataset ({kind}={ref}): {exc}"
            if keep_going:
                gs.warning(msg)
                continue
            gs.fatal(msg)

        dataset_id = str(dataset_info.get("id", ""))
        title = dataset_info.get("title", dataset_id)

        key = manifest_key(dataset_id)
        # Compare against the *raw source* record count from last time
        # (expected_total), not n_records written -- a source dataset can
        # legitimately have a few records with no parseable spectrum every
        # time (build_row returns None for them), so n_records is
        # consistently a bit lower than total_spectra/API total even when
        # nothing has changed; comparing against n_records would make the
        # skip check spuriously fail and re-ingest unchanged datasets every
        # run. Falls back to n_records for manifest entries written before
        # this field existed.
        prev_entry = manifest.get(key, {})
        prev_expected = prev_entry.get("expected_total", prev_entry.get("n_records", 0))
        if not force and prev_expected and expected and prev_expected >= expected:
            gs.verbose(f"Skipping '{title}' ({dataset_id}): already ingested "
                      f"({prev_entry.get('n_records', 0)} of {prev_expected} "
                      "records). Use -f to force re-ingestion.")
            n_datasets_skipped += 1
            continue

        try:
            batches = iter_row_batches(dataset_info, record_source(), base_url, stream_batch_size)
            if fmt == "parquet":
                out_path, n_rows = write_parquet_dataset_streaming(
                    output_root, dataset_id, batches, include_geometry)
            else:
                out_path, n_rows = write_sqlite_dataset_streaming(output_root, batches)
        except Exception as exc:
            msg = f"Could not stream/write dataset ({kind}={ref}, '{title}'): {exc}"
            if keep_going:
                gs.warning(msg)
                continue
            gs.fatal(msg)

        if n_rows == 0:
            gs.warning(f"Dataset '{title}' ({dataset_id}) yielded no usable spectra; skipped.")
            continue

        manifest[key] = {
            "dataset_title": title,
            "n_records": n_rows,
            "expected_total": expected,
            "last_updated": datetime.datetime.now().isoformat(),
            "path": out_path,
        }
        save_manifest(output_root, manifest)  # after every dataset: a long
        # run over ~200 datasets should not lose all progress tracking to
        # an interruption partway through.
        n_datasets_written += 1
        n_records_written += n_rows
        gs.verbose(f"Wrote {n_rows} record(s) for '{title}' → {out_path}")

    gs.percent(len(jobs), len(jobs), 2)

    gs.message(
        f"Done: {n_datasets_written} dataset(s) ingested ({n_records_written} spectra), "
        f"{n_datasets_skipped} already up to date, "
        f"library at {output_root} (format={fmt})."
    )


if __name__ == "__main__":
    options, flags = gs.parser()
    sys.exit(main(options, flags))
