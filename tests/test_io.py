from pathlib import Path

import pandas as pd
import pytest

from redline.core import io as rio


def test_schema_validation(tmp_path: Path):
    sample = "Realm,Data_Usage_MB,SIM_Subscription\nZA,10,123\n"
    p = tmp_path / "sup.csv"
    p.write_text(sample)
    df, meta = rio.read_file(p, "supplier")
    assert meta["rows"] == 1
    assert set(df.columns) == {"realm", "data_mb", "sim_subscription"}
    with pytest.raises(rio.SchemaError):
        rio.read_file(p, "billing")  # wrong schema 