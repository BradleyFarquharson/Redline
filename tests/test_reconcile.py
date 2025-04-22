import pandas as pd
import yaml

from redline.core import reconcile


CFG = yaml.safe_load(open("config/settings.yaml"))


def sample():
    sup = pd.DataFrame({"realm": ["ZA"], "sim_subscription": ["S1"], "data_mb": [100]})
    raw = pd.DataFrame(
        {"customer_code": ["C1"], "realm": ["ZA"], "sim_subscription": ["S1"], "data_mb": [100]}
    )
    bill = pd.DataFrame(
        {
            "customer_code": ["C1"],
            "realm": ["ZA"],
            "sim_subscription": ["S1"],
            "bundle_mb": [80],
            "excess_mb": [20],
        }
    )
    return sup, raw, bill


def test_reconcile_ok():
    res = reconcile.run(*sample(), CFG)
    assert all((df["status"] == "OK").all() for df in res.values()) 