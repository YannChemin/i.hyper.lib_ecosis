"""Tests for i.hyper.lib_ecosis's reflectance scale normalisation.

The module is a GRASS script, not an importable package, so it is executed
here with a stub for ``grass.script``: the scale functions are pure and need
no GRASS session, and loading the file this way keeps the tests honest about
testing the shipped code rather than a copy of it.
"""

import json
import sys
import types
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parent.parent / "i.hyper.lib_ecosis.py"


def _load_module():
    """Execute the addon with a stubbed grass.script and return it."""
    stub = types.ModuleType("grass.script")
    for name in ("message", "warning", "verbose", "percent", "fatal", "debug"):
        setattr(stub, name, lambda *args, **kwargs: None)
    grass = types.ModuleType("grass")
    grass.script = stub
    sys.modules.setdefault("grass", grass)
    sys.modules.setdefault("grass.script", stub)
    module = types.ModuleType("i_hyper_lib_ecosis")
    module.__file__ = str(MODULE_PATH)
    exec(compile(MODULE_PATH.read_text(), str(MODULE_PATH), "exec"), module.__dict__)
    return module


@pytest.fixture(scope="module")
def mod():
    return _load_module()


def _rows(module, *value_lists):
    """Build minimal harvest rows carrying the given spectra."""
    return [
        {
            "values": list(values),
            "measurement_type": "reflectance",
            "extra_metadata": "{}",
        }
        for values in value_lists
    ]


class TestDatasetDivisor:
    def test_unit_dataset(self, mod):
        rows = _rows(mod, [0.05, 0.1, 0.2, 0.5], [0.04, 0.08, 0.18, 0.45])
        assert mod.infer_dataset_divisor(rows) == 1

    def test_percent_dataset(self, mod):
        rows = _rows(mod, [5.0, 10.0, 25.0, 60.0], [3.0, 8.0, 22.0, 55.0])
        assert mod.infer_dataset_divisor(rows) == 100

    def test_scaled_integer_dataset(self, mod):
        rows = _rows(mod, [500, 1500, 3000, 5500], [400, 1400, 2800, 5300])
        assert mod.infer_dataset_divisor(rows) == 10000

    def test_water_band_noise_does_not_look_like_percent(self, mod):
        """Every spectrum peaks at 98 in three bands and sits at 0.3.

        Classifying on per-spectrum maxima would divide a unit-scale
        dataset by 100; the median of the values does not move.
        """
        spectrum = [0.3] * 200
        spectrum[150:153] = [12.0, 98.0, 7.0]
        assert mod.infer_dataset_divisor(_rows(mod, spectrum, spectrum)) == 1

    def test_empty_dataset(self, mod):
        assert mod.infer_dataset_divisor([]) == 1
        assert mod.infer_dataset_divisor(_rows(mod, [])) == 1


class TestRangeCorrection:
    def test_unit_spectrum_untouched(self, mod):
        values = [0.1 + 0.001 * i for i in range(200)]
        out, corrections = mod.correct_scaled_ranges(list(values))
        assert corrections == []
        assert out == values

    def test_percent_block_spliced_into_a_unit_spectrum(self, mod):
        vnir = [0.2 + 0.001 * i for i in range(100)]
        swir = [(0.3 + 0.001 * i) * 100 for i in range(100)]
        out, corrections = mod.correct_scaled_ranges(vnir + swir)
        assert corrections == [{"start": 100, "stop": 200, "divisor": 100}]
        assert out[100] == pytest.approx(0.3)
        assert max(out) <= 1.0

    def test_factor_of_ten_range(self, mod):
        out, corrections = mod.correct_scaled_ranges([0.4] * 100 + [4.0] * 100)
        assert corrections == [{"start": 100, "stop": 200, "divisor": 10}]
        assert out[150] == pytest.approx(0.4)

    def test_water_vapour_spike_left_alone(self, mod):
        values = [0.3] * 2000
        values[1500:1505] = [3.0, 9.0, 30.0, 8.0, 2.0]
        out, corrections = mod.correct_scaled_ranges(values)
        assert corrections == []
        assert out[1502] == 30.0

    def test_detector_junction_artefact_is_left_alone(self, mod):
        # A 20-band excursion at an instrument's detector junction, the
        # shape the real data shows: the signal climbs through the ceiling
        # and back rather than stepping across it, and it covers 3 % of the
        # spectrum. An artefact of the measurement, not a scale error.
        values = [0.3] * 300
        values += [0.3 + 1.5 * i / 10.0 for i in range(10)]
        values += [1.8] * 20
        values += [1.8 - 1.5 * i / 10.0 for i in range(10)]
        values += [0.3] * 660
        out, corrections = mod.correct_scaled_ranges(values)
        assert corrections == []
        assert out[310] == pytest.approx(1.8)

    def test_no_power_of_ten_joins_the_two_sides(self, mod):
        # A step of 3000 is not a scale splice: dividing the block by 10
        # would leave it 300 times above its surroundings, and the span is
        # too short for anything else to apply.
        values = [0.001] * 950 + [3.0] * 50
        out, corrections = mod.correct_scaled_ranges(values)
        assert corrections == []

    def test_a_spectrum_mostly_above_the_ceiling_is_rescaled_whole(self, mod):
        # 93 % of the bands above the ceiling: no reflectance spectrum looks
        # like that, so the multiplier belongs to all of it, including the
        # bands that were already under 1.5.
        values = [2.0] * 930 + [1.2] * 70
        out, corrections = mod.correct_scaled_ranges(values)
        assert corrections == [{"start": 0, "stop": 1000, "divisor": 10}]
        assert out[0] == pytest.approx(0.2)
        assert out[999] == pytest.approx(0.12)

    def test_a_mis_scaled_near_infrared_plateau_rescales_the_spectrum(self, mod):
        # The real shape of this fault: a vegetation spectrum whose whole
        # scale is wrong shows it only where the spectrum is brightest, so
        # the run covers the NIR plateau and the signal rises through the
        # ceiling continuously rather than stepping across it.
        values = [0.1] * 300
        values += [0.1 + 2.8 * i / 60.0 for i in range(60)]
        values += [2.9] * 120
        values += [2.9 - 2.5 * i / 120.0 for i in range(120)]
        values += [0.4] * 400
        out, corrections = mod.correct_scaled_ranges(values)
        assert 0.10 < sum(v > 1.5 for v in values) / len(values) < 0.80
        assert corrections == [{"start": 0, "stop": 1000, "divisor": 10}]
        assert max(out) <= 1.0

    def test_a_spliced_block_is_divided_on_its_own(self, mod):
        # A clean factor-of-100 step at the boundary: two instruments'
        # output pasted together, and only the pasted block may be touched.
        values = [0.3] * 300 + [30.0] * 200 + [0.3] * 500
        out, corrections = mod.correct_scaled_ranges(values)
        assert corrections == [{"start": 300, "stop": 500, "divisor": 100}]
        assert out[0] == 0.3
        assert out[400] == pytest.approx(0.3)

    def test_whole_spectrum_has_no_neighbours_to_check(self, mod):
        out, corrections = mod.correct_scaled_ranges(
            [30.0 + 0.1 * i for i in range(200)]
        )
        assert corrections == [{"start": 0, "stop": 200, "divisor": 100}]
        assert max(out) <= 1.0


class TestNormaliseBatch:
    def test_divisor_and_provenance_are_applied(self, mod):
        rows = _rows(mod, [20.0 + 0.1 * i for i in range(200)])
        assert mod.normalise_batch(rows, 100) == 0
        assert max(rows[0]["values"]) <= 1.0
        scale = json.loads(rows[0]["extra_metadata"])["reflectance_scale"]
        assert scale == {"normalised_to": "unit", "source_divisor": 100}

    def test_range_correction_is_counted_and_recorded(self, mod):
        spliced = [0.2] * 100 + [25.0] * 100
        rows = _rows(mod, spliced)
        assert mod.normalise_batch(rows, 1) == 1
        scale = json.loads(rows[0]["extra_metadata"])["reflectance_scale"]
        assert scale["range_corrections"] == [
            {"start": 100, "stop": 200, "divisor": 100}
        ]
        assert "source_divisor" not in scale

    def test_unit_rows_keep_their_metadata_untouched(self, mod):
        rows = _rows(mod, [0.25] * 200)
        assert mod.normalise_batch(rows, 1) == 0
        assert rows[0]["extra_metadata"] == "{}"

    def test_a_non_unit_quantity_is_never_rescaled(self, mod):
        rows = _rows(mod, [3.0] * 200)
        rows[0]["measurement_type"] = "absorbance"
        assert mod.normalise_batch(rows, 100) == 0
        assert rows[0]["values"] == [3.0] * 200


class TestWavelengthKeys:
    """Datapoint keys that carry their unit must still parse.

    Two EcoSIS datasets export every key with a unit ("1651 nm", and
    "660 (nm) wavelength"); before this was handled they parsed to no
    bands at all and the whole dataset was silently dropped.
    """

    def test_bare_number(self, mod):
        assert mod._parse_wavelength_key("1651") == 1651.0
        assert mod._parse_wavelength_key("450.5") == 450.5
        assert mod._parse_wavelength_key(" 2400 ") == 2400.0

    def test_numeric_key(self, mod):
        assert mod._parse_wavelength_key(1651) == 1651.0
        assert mod._parse_wavelength_key(1651.5) == 1651.5

    def test_nanometre_suffix(self, mod):
        assert mod._parse_wavelength_key("1651 nm") == 1651.0
        assert mod._parse_wavelength_key("1651nm") == 1651.0
        assert mod._parse_wavelength_key("1651 NM") == 1651.0

    def test_parenthesised_unit_and_word(self, mod):
        assert mod._parse_wavelength_key("660 (nm) wavelength") == 660.0
        assert mod._parse_wavelength_key("1650 (nm) Wavelength") == 1650.0

    def test_micrometres_are_converted(self, mod):
        assert mod._parse_wavelength_key("1.651 um") == pytest.approx(1651.0)
        assert mod._parse_wavelength_key("0.45 µm") == pytest.approx(450.0)

    def test_a_trait_column_is_rejected(self, mod):
        for key in ("SLA", "Nitrogen (%)", "LMA g/m2", "", "plot 3 north"):
            assert mod._parse_wavelength_key(key) is None

    def test_non_finite_is_rejected(self, mod):
        assert mod._parse_wavelength_key("nan") is None
        assert mod._parse_wavelength_key("inf") is None
        assert mod._parse_wavelength_key(float("nan")) is None

    def test_spectrum_with_unit_suffixed_keys(self, mod):
        datapoints = {"1651 nm": "0.42", "492 nm": "0.07", "688 nm": "0.09"}
        wavelengths, values = mod._parse_spectrum(datapoints)
        assert wavelengths == [492.0, 688.0, 1651.0]
        assert values == [0.07, 0.09, 0.42]

    def test_trait_columns_do_not_enter_the_spectrum(self, mod):
        datapoints = {"660 (nm) wavelength": "0.05", "SLA": "12.3"}
        wavelengths, values = mod._parse_spectrum(datapoints)
        assert wavelengths == [660.0]
        assert values == [0.05]
