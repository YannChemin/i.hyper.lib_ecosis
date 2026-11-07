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

# %option
# % key: scale
# % type: string
# % required: no
# % options: auto,none,unit,percent,scaled10k
# % answer: auto
# % description: Reflectance scale of the source data. EcoSIS does not enforce one, so contributors upload unit (0-1), percent (0-100) and scaled-integer (0-10000) data alike; values are divided onto the unit interval at ingest. auto infers the divisor per dataset from its median value and also divides out a multiplier left on only part of a spectrum; unit/percent/scaled10k force the dataset divisor; none stores the source values untouched
# % guisection: Output
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
import re
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


# Wavelength keys are usually a bare number ("1651"), but a contributor may
# have exported the unit with it ("1651 nm", "660 (nm) wavelength"). Those
# keys fail float() and, when every key in the dataset carries a unit, the
# whole dataset used to be dropped. Parse the number and the unit instead,
# and convert to nanometres, which is what the row schema declares.
_WAVELENGTH_KEY = re.compile(
    r"^\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*(.*)$"
)
_WAVELENGTH_NOISE = re.compile(r"[()\[\]{}_,;:]|\bwave\s*lengths?\b|\bwl\b|\bband\b")
_WAVELENGTH_UNIT_TO_NM = {
    "": 1.0,
    "nm": 1.0,
    "nanometer": 1.0,
    "nanometers": 1.0,
    "nanometre": 1.0,
    "nanometres": 1.0,
    "um": 1000.0,
    "\u00b5m": 1000.0,
    "\u03bcm": 1000.0,
    "micron": 1000.0,
    "microns": 1000.0,
    "micrometer": 1000.0,
    "micrometers": 1000.0,
    "micrometre": 1000.0,
    "micrometres": 1000.0,
    "a": 0.1,
    "\u00e5": 0.1,
    "angstrom": 0.1,
    "angstroms": 0.1,
}


def _parse_wavelength_key(key) -> Optional[float]:
    """Wavelength in nanometres for a datapoint key, or None if the key is
    not a wavelength. Accepts a bare number and a number followed by a
    recognised length unit, with or without the word "wavelength"; anything
    else (a trait column, a free-text label) is rejected rather than
    guessed at."""
    if isinstance(key, (int, float)):
        return float(key) if math.isfinite(float(key)) else None
    if not isinstance(key, str):
        return None
    match = _WAVELENGTH_KEY.match(key)
    if match is None:
        return None
    try:
        number = float(match.group(1))
    except ValueError:
        return None
    if not math.isfinite(number):
        return None
    unit = _WAVELENGTH_NOISE.sub(" ", match.group(2).lower())
    unit = "".join(unit.split())
    scale = _WAVELENGTH_UNIT_TO_NM.get(unit)
    if scale is None:
        return None
    return number * scale


def _parse_spectrum(datapoints: dict) -> tuple[list[float], list[float]]:
    pairs = []
    for k, v in datapoints.items():
        fk = _parse_wavelength_key(k)
        if fk is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        # float() parses the literal strings "nan"/"inf" without raising
        # (unlike a genuinely unparseable value), so a datapoint recorded
        # as such would otherwise slip through as if it were a normal
        # measurement and silently corrupt any downstream computation
        # (e.g. spectral similarity matching) that assumes finite values.
        if not math.isfinite(fv):
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


# ---------------------------------------------------------------------------
# Reflectance scale normalisation
#
# EcoSIS takes each dataset as its contributor exported it and enforces no
# scale convention, so unit-interval (0-1), percent (0-100) and scaled-integer
# (0-10000) reflectance all arrive through the same API. The "Measurement
# Units" metadata field is missing or wrong often enough that it cannot be
# trusted, so the scale is inferred from the values themselves and divided out
# here, at harvest, rather than being left for every consumer of the shared
# store to rediscover.
#
# Two different faults are handled, and they need different treatment.
#
# Whole-dataset scaling: every spectrum carries the same multiplier because
# the contributor exported percent or scaled integers throughout. The divisor
# is inferred once per dataset and applied to all of its rows.
#
# Range-contained scaling: one contiguous span of a spectrum carries a
# multiplier the rest of it does not, because two instruments' output was
# spliced together or a percent-scale block was pasted into a unit-scale file.
# Each such span is divided by its own power of ten.
#
# The range case has to be conservative, because a noise spike looks like a
# scaled range with a sample size of one. Two tests separate them: the span
# must be long enough to be a spectral region rather than a glitch, and
# dividing it must make it join up with the unscaled bands either side. Ratio
# noise in the water-vapour bands at 1350-1400 nm and 1800-1900 nm, where the
# reference signal is near zero, fails both -- it is a few bands wide and no
# power of ten brings it level with its neighbours. Those spans are left as
# they are: they are a data-quality problem of the source spectrum, not a
# scale error, and dividing them would hide it.
# ---------------------------------------------------------------------------

# Candidate whole-dataset divisors, in increasing order. The intermediate
# decades (0-10, 0-1000) are deliberately not candidates at dataset level:
# they sit too close to their neighbours for a median to separate them, so
# offering them would turn a dark percent-scale dataset into a plausible but
# wrong 0-10 classification.
_DATASET_DIVISORS = (1, 100, 10000)

# What the scale= option forces, where it forces anything.
_FORCED_DIVISORS = {"unit": 1, "percent": 100, "scaled10k": 10000}

# Soft upper bound on a unit-scale dataset's median value. Reflectance
# medians sit near 0.2-0.4; the headroom is for measurements that
# legitimately approach 1.
_UNIT_MEDIAN_CEILING = 1.2

# Values sampled per dataset to estimate its median. The estimate only has to
# pick a decade, so a few thousand values are ample.
_MEDIAN_SAMPLE_CAP = 20000

# A band above this counts as out of the unit interval.
_UNIT_CEILING = 1.5

# Shortest span of consecutive out-of-range bands that may be treated as a
# scaled range rather than noise, as a band count and as a fraction of the
# spectrum. Both must be met.
_MIN_RUN_BANDS = 16
_MIN_RUN_FRACTION = 0.02

# Bands below the ceiling tolerated inside a scaled range without ending it: a
# scaled range still dips under 1.5 wherever the true reflectance is low.
_MAX_RUN_GAP = 4

# Bands averaged each side of a range boundary when measuring the step
# across it, and how near the span's own multiplier that step must be,
# either way, for the span to count as a block spliced in from another
# scale. Much smaller and the signal runs continuously through the
# ceiling, which is what a correctly shaped but wrongly scaled spectrum
# looks like; much larger and dividing would not join the two sides.
_EDGE_BANDS = 3
_SPLICE_TOLERANCE = 3.0

# A span this much of the spectrum settles the question on its own: no
# reflectance spectrum is mostly above the unit interval, whatever happens
# at the span's edges.
_DOMINANT_FRACTION = 0.80

# Below that, a span covering at least this much of the spectrum and
# showing no step at its edges is read as a whole-spectrum scale error
# rather than a local artefact. Detector-junction and water-vapour
# artefacts are a few percent of the bands; a mis-scaled near-infrared
# plateau is ten percent or more.
_RESCALE_FRACTION = 0.10

# Quantile used to pick the divisor when a whole spectrum is rescaled. Not
# the maximum: one noise spike an order of magnitude above the signal would
# otherwise choose a divisor ten times too large.
_RESCALE_QUANTILE = 0.99

# Largest power-of-ten multiplier a single range may carry.
_MAX_RANGE_EXPONENT = 6

# Measurement types whose values belong on the unit interval. Every EcoSIS row
# is reflectance today; the guard is here so that adding another type later
# cannot silently rescale a quantity that is not bounded by 1.
_UNIT_INTERVAL_TYPES = frozenset({"reflectance", "transmittance", "emissivity"})


def _median(values: list[float]) -> float:
    """Return the lower median of an unsorted, non-empty list."""
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def _sample_values(rows: list[dict], cap: int = _MEDIAN_SAMPLE_CAP) -> list[float]:
    """Collect up to cap finite values spread across a batch of rows.

    The stride doubles as the sample fills, so the kept values stay spread
    over everything seen rather than over the first few spectra.
    """
    sample: list[float] = []
    stride = 1
    seen = 0
    for row in rows:
        for value in row["values"]:
            if not math.isfinite(value):
                continue
            if seen % stride == 0:
                sample.append(value)
                if len(sample) >= cap:
                    sample = sample[::2]
                    stride *= 2
            seen += 1
    return sample


def _divisor_for_median(median_value: float) -> int:
    """Smallest whole-dataset divisor that brings a median into the unit
    interval, or the largest candidate when none does."""
    for divisor in _DATASET_DIVISORS:
        if median_value / divisor <= _UNIT_MEDIAN_CEILING:
            return divisor
    return _DATASET_DIVISORS[-1]


def infer_dataset_divisor(rows: list[dict]) -> int:
    """Infer a dataset's whole-dataset scale divisor from its rows.

    The statistic is the median of the values, not of the per-spectrum
    maxima. Several EcoSIS datasets are unit-scale but carry ratio noise in
    the water-vapour bands, where the reference signal is near zero: those
    spectra sit at 0.3 everywhere and peak at 10 or 100 in three or four
    bands. A median of maxima reads such a dataset as percent and divides all
    of it by 100; a median of values does not move.
    """
    sample = _sample_values(rows)
    if not sample:
        return 1
    return _divisor_for_median(_median(sample))


def _scaled_runs(values: list[float]) -> list[tuple[int, int]]:
    """Find spans of bands above the unit ceiling, as (start, stop) index
    pairs with stop exclusive.

    A span survives a dip below the ceiling of up to _MAX_RUN_GAP bands,
    since a scaled range still goes under 1.5 wherever the true reflectance
    is low. Non-finite bands never end a span either.
    """
    runs: list[tuple[int, int]] = []
    start = None
    last_over = -1
    gap = 0
    for index, value in enumerate(values):
        if math.isfinite(value) and value > _UNIT_CEILING:
            if start is None:
                start = index
            last_over = index
            gap = 0
            continue
        if start is None:
            continue
        if math.isfinite(value):
            gap += 1
            if gap > _MAX_RUN_GAP:
                runs.append((start, last_over + 1))
                start = None
                gap = 0
    if start is not None:
        runs.append((start, last_over + 1))
    return runs


def _edge_median(values: list[float], indices) -> float | None:
    """Median of the finite values at the given band indices, or None."""
    finite = [values[i] for i in indices if math.isfinite(values[i])]
    if not finite:
        return None
    return _median(finite)


def _range_divisor(median_value: float) -> int:
    """Smallest power of ten bringing a span into the unit interval.

    Unlike the whole-dataset ladder this walks every decade: a span is
    checked against its own neighbours afterwards, which resolves the
    ambiguity between adjacent decades that a dataset median cannot.
    Returns 1 when no power of ten up to _MAX_RANGE_EXPONENT normalises it.
    """
    divisor = 10
    for _ in range(_MAX_RANGE_EXPONENT):
        if median_value / divisor <= 1.0:
            return divisor
        divisor *= 10
    return 1


def _boundary_step(values: list[float], start: int, stop: int) -> float | None:
    """How far the signal jumps across the edges of a span.

    Returns the larger of the two edge ratios, inside over outside, so a
    span spliced in at one end only is still recognised. A ratio near the
    span's own multiplier is a real discontinuity -- two instruments' output
    pasted together. A ratio near 1 means the signal runs continuously
    through the ceiling, which is what a correctly shaped but wrongly scaled
    spectrum looks like. None when the span has no usable neighbour.
    """
    pairs = (
        (_edge_median(values, range(max(0, start - _EDGE_BANDS), start)),
         _edge_median(values, range(start, min(stop, start + _EDGE_BANDS)))),
        (_edge_median(values, range(stop, min(len(values), stop + _EDGE_BANDS))),
         _edge_median(values, range(max(start, stop - _EDGE_BANDS), stop))),
    )
    steps = [inside / outside for outside, inside in pairs
             if outside is not None and inside is not None and outside > 0]
    return max(steps) if steps else None


def _whole_spectrum_divisor(values: list[float]) -> int:
    """The power of ten that brings a whole spectrum into the unit interval.

    Returns 1 when the spectrum is already in range, or when no power of ten
    up to _MAX_RANGE_EXPONENT brings it there.
    """
    finite = sorted(v for v in values if math.isfinite(v))
    if not finite:
        return 1
    high = finite[min(len(finite) - 1, int(_RESCALE_QUANTILE * len(finite)))]
    if high <= 1.0:
        return 1
    return _range_divisor(high)


def correct_scaled_ranges(values: list[float]) -> tuple[list[float], list[dict]]:
    """Divide out a multiplier left on part of a spectrum.

    Takes values already divided by any whole-dataset divisor, and returns
    a new list with the corrections applied, plus one {start, stop, divisor}
    record per corrected span for provenance. The input is left untouched.
    """
    values = list(values)
    if not values:
        return values, []

    peak = max((v for v in values if math.isfinite(v)), default=0.0)
    if peak <= _UNIT_CEILING:
        return values, []

    n_finite = sum(1 for v in values if math.isfinite(v))
    if not n_finite:
        return values, []
    minimum_run = max(_MIN_RUN_BANDS, math.ceil(_MIN_RUN_FRACTION * len(values)))
    corrections: list[dict] = []
    for start, stop in _scaled_runs(values):
        length = stop - start
        if length < minimum_run:
            continue                       # A glitch, not a spectral region.
        span = [v for v in values[start:stop] if math.isfinite(v)]
        if not span:
            continue
        divisor = _range_divisor(_median(span))
        if divisor == 1:
            continue
        step = _boundary_step(values, start, stop)
        fraction = length / n_finite

        # A span covering most of the spectrum settles it on its own: no
        # reflectance spectrum is mostly above the unit interval, so the
        # multiplier belongs to the whole of it and dividing only the span
        # would put a step where the data has none.
        if fraction < _DOMINANT_FRACTION and step is not None:
            if divisor / _SPLICE_TOLERANCE <= step <= divisor * _SPLICE_TOLERANCE:
                # A real discontinuity about the size of the multiplier: a
                # block pasted in from another scale. Divide the block.
                for index in range(start, stop):
                    if math.isfinite(values[index]):
                        values[index] = values[index] / divisor
                corrections.append(
                    {"start": start, "stop": stop, "divisor": divisor})
                continue
            if fraction < _RESCALE_FRACTION:
                # No step, and only a sliver of the spectrum: a detector
                # junction or a water-vapour artefact rather than a scale
                # error. Left as it is, so validation still reports it.
                continue

        whole = _whole_spectrum_divisor(values)
        if whole == 1:
            continue
        for index, value in enumerate(values):
            if math.isfinite(value):
                values[index] = value / whole
        return values, [{"start": 0, "stop": len(values), "divisor": whole}]
    return values, corrections


def _record_scale(row: dict, divisor: int, corrections: list[dict]) -> None:
    """Note on the row what was divided out of it.

    The store has no column for this, so it goes into extra_metadata, which
    is where everything the row's own source said about itself already lives.
    """
    if divisor == 1 and not corrections:
        return
    try:
        extra = json.loads(row["extra_metadata"])
    except (TypeError, ValueError):
        extra = {}
    scale = {"normalised_to": "unit"}
    if divisor != 1:
        scale["source_divisor"] = divisor
    if corrections:
        scale["range_corrections"] = corrections
    extra["reflectance_scale"] = scale
    row["extra_metadata"] = json.dumps(extra, default=str)


def normalise_batch(rows: list[dict], divisor: int) -> int:
    """Put one batch of rows onto the unit interval, in place.

    Applies the dataset divisor, then divides out any multiplier left on a
    contiguous part of an individual spectrum. Returns how many rows needed
    the second, per-range correction.
    """
    n_ranged = 0
    for row in rows:
        if row.get("measurement_type") not in _UNIT_INTERVAL_TYPES:
            continue
        values = row["values"]
        if divisor != 1:
            values = [v / divisor for v in values]
        values, corrections = correct_scaled_ranges(values)
        if corrections:
            n_ranged += 1
        row["values"] = values
        _record_scale(row, divisor, corrections)
    return n_ranged


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


def iter_row_batches(
    dataset_info: dict, record_iter, base_url: str, batch_size: int, scale: str = "auto"
):
    """Consume a (possibly streaming) iterator of raw records and yield
    lists of built rows, batch_size at a time -- the unit both writer
    backends below write incrementally, so peak memory is one batch, not
    the whole dataset, regardless of whether record_iter is a streaming
    generator (ijson / live API pagination) or a plain in-memory list
    (the no-ijson fallback).

    Each batch is put onto the unit interval before it is yielded, as
    scale= asks. With scale=auto the dataset's divisor is inferred from its
    first batch and then applied to every batch of that dataset: a whole
    dataset shares one scale, so deciding it twice could only disagree with
    itself, and the first batch is the whole dataset for all but a handful
    of EcoSIS datasets (the default batch is 5000 spectra). The decision
    needs a complete batch, which is why it happens here rather than in
    build_row."""
    divisor = _FORCED_DIVISORS.get(scale, 1)
    decide = scale == "auto"
    n_ranged = 0
    title = dataset_info.get("title", dataset_info.get("id", ""))

    def finish(rows):
        nonlocal divisor, decide, n_ranged
        if scale == "none":
            return rows
        if decide:
            divisor = infer_dataset_divisor(rows)
            decide = False
            if divisor != 1:
                gs.verbose(
                    "Dataset '{}': source values are on a 0-{} scale; "
                    "dividing by {} to reach the unit interval.".format(
                        title, divisor, divisor
                    )
                )
        n_ranged += normalise_batch(rows, divisor)
        return rows

    batch = []
    for rec in record_iter:
        row = build_row(dataset_info, rec, base_url)
        if row is not None:
            batch.append(row)
        if len(batch) >= batch_size:
            yield finish(batch)
            batch = []
    if batch:
        yield finish(batch)
    if n_ranged:
        gs.warning(
            "Dataset '{}': {} record(s) carried a multiplier of their own, on "
            "top of the dataset's; see extra_metadata.reflectance_scale.".format(
                title, n_ranged
            )
        )

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
    scale = options.get("scale", "auto") or "auto"
    force = flags["f"]
    keep_going = flags["k"]

    if fmt == "parquet":
        _require_pyarrow()
    streaming = _has_ijson()
    if scale == "none":
        gs.warning(
            "scale=none: source values are stored as they came, so the "
            "library will mix unit, percent and 0-10000 reflectance and "
            "every consumer of it will have to sort that out for itself."
        )
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
            batches = iter_row_batches(
                dataset_info, record_source(), base_url, stream_batch_size, scale
            )
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
