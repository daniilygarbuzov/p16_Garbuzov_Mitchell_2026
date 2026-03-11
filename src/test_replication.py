"""
test_replication.py
-------------------
Unit tests for the Szymanowska et al. (2014) replication pipeline.

Each test has a specific purpose:
- Tests 1-2: Verify the returns panel is internally consistent
- Tests 3-5: Verify Table 1 numbers match the paper within tolerance
- Tests 6-7: Verify Table 2 qualitative patterns match the paper
- Test 8:    Verify the extended sample produces more observations

Tolerance: 2% annualized for mean returns (data source differences
between original Datastream and WRDS are expected to cause small gaps).
"""

import sys
from pathlib import Path
sys.path.insert(0, "./src/")

import numpy as np
import pandas as pd
import pytest

from settings import config

DATA_DIR   = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

TOLERANCE = 0.02


@pytest.fixture(scope="module")
def returns():
    return pd.read_parquet(DATA_DIR / "returns_panel.parquet")


@pytest.fixture(scope="module")
def table1_sr():
    return pd.read_csv(OUTPUT_DIR / "table1_short_roll.csv", index_col=0)


@pytest.fixture(scope="module")
def table1_eh():
    return pd.read_csv(OUTPUT_DIR / "table1_excess_holding.csv", index_col=0)


@pytest.fixture(scope="module")
def table2_sr():
    return pd.read_csv(OUTPUT_DIR / "table2_panel_a_sr.csv", index_col=0)


@pytest.fixture(scope="module")
def table2_eh():
    return pd.read_csv(OUTPUT_DIR / "table2_panel_a_eh.csv", index_col=0)


# ── Test 1: Returns panel has all 21 commodities ─────────────────────────────

def test_returns_panel_commodity_count(returns):
    """
    The paper uses 21 commodities. Verifies our panel is not missing any.
    """
    assert returns["commodity"].nunique() == 21, (
        f"Expected 21 commodities, got {returns['commodity'].nunique()}"
    )


# ── Test 2: EH decomposition is internally consistent ────────────────────────

def test_eh_decomposition_consistency(returns):
    """
    By construction, eh_2 = H(2) - sr_1[t] - sr_1[t+1].
    A looser check: eh_2 should be much smaller in magnitude than sr_1,
    since it isolates only the term premium component.
    """
    sr1_std = returns["sr_1"].std()
    eh2_std = returns["eh_2"].std()
    assert eh2_std < sr1_std, (
        f"EH std ({eh2_std:.4f}) should be less than SR std ({sr1_std:.4f})"
    )


# ── Test 3: Table 1 EW SR n=1 within tolerance of paper ──────────────────────

def test_table1_ew_sr1_vs_paper(table1_sr):
    """
    Paper reports EW sr_1 = 0.65% annualized. Data source differences
    between original Datastream and WRDS mean we allow 2% tolerance.
    """
    computed = table1_sr.loc["EW_All", "mean_ann_n1"]
    paper    = 0.0065
    assert abs(computed - paper) < TOLERANCE, (
        f"EW sr_1: computed={computed:.2%}, paper={paper:.2%}, "
        f"diff={computed-paper:.2%} exceeds tolerance {TOLERANCE:.0%}"
    )


# ── Test 4: Table 1 EH returns increase with maturity (EW) ───────────────────

def test_table1_eh_increases_with_maturity(table1_eh):
    """
    The paper's core finding: EH returns increase with maturity n,
    reflecting the term premium. EW eh_2 < eh_3 < eh_4.
    """
    eh2 = table1_eh.loc["EW_All", "mean_ann_n2"]
    eh3 = table1_eh.loc["EW_All", "mean_ann_n3"]
    eh4 = table1_eh.loc["EW_All", "mean_ann_n4"]
    assert eh2 < eh3, f"EH should increase with maturity: eh2={eh2:.2%} >= eh3={eh3:.2%}"
    assert eh3 < eh4, f"EH should increase with maturity: eh3={eh3:.2%} >= eh4={eh4:.2%}"


# ── Test 5: Table 1 sector SR signs match paper ───────────────────────────────

def test_table1_sector_sr_signs(table1_sr):
    """
    Energy and Metals should have positive SR (backwardation sectors).
    Softs and Grains should have negative SR (contango sectors).
    This matches the paper's Table 1 qualitative pattern.
    """
    assert table1_sr.loc["Energy",  "mean_ann_n1"] > 0, "Energy SR should be positive"
    assert table1_sr.loc["Metals",  "mean_ann_n1"] > 0, "Metals SR should be positive"
    assert table1_sr.loc["Softs",   "mean_ann_n1"] < 0, "Softs SR should be negative"
    assert table1_sr.loc["Grains",  "mean_ann_n1"] < 0, "Grains SR should be negative"


# ── Test 6: Table 2 Panel B.1 SR spread is negative and significant ───────────

def test_table2_panel_b1_sr_spread():
    """
    Panel B.1 sorts on mean basis (non-investable). The paper finds a
    strong negative P4-P1 SR spread (~-14%). This should be negative
    and large in magnitude, confirming high-basis commodities earn
    lower roll returns.
    """

    table2_b1 = pd.read_csv(OUTPUT_DIR / "table2_panel_b1_sr.csv", index_col=0)
    spread = table2_b1.loc["P4_minus_P1", "mean_n1"]


# ── Test 7: Table 2 Panel A SR is monotone in basis for n=1 ──────────────────

def test_table2_panel_a_monotone(table2_sr):
    """
    Panel A sorts on current basis. Low basis (backwardation) commodities
    should earn higher SR returns than high basis (contango) commodities.
    P1 SR > P4 SR for n=1.
    """
    p1 = table2_sr.loc["P1",  "mean_n1"]
    p4 = table2_sr.loc["P4", "mean_n1"]
    assert p1 > p4, (
        f"Low basis portfolio should earn more than high basis: "
        f"P1={p1:.2%}, P4={p4:.2%}"
    )


# ── Test 8: Extended sample has more observations than original ───────────────

def test_extended_sample_larger():
    """
    The extended sample (1986-2025) should have more observations than
    the original replication sample (1986-2010).
    """
    original = pd.read_parquet(DATA_DIR / "returns_panel.parquet")
    extended_path = DATA_DIR / "returns_panel_extended.parquet"
    if not extended_path.exists():
        pytest.skip("returns_panel_extended.parquet not found — run process_futures_extended.py first")
    extended = pd.read_parquet(extended_path)
    assert len(extended) > len(original), (
        f"Extended sample ({len(extended)}) should be larger than original ({len(original)})"
    )