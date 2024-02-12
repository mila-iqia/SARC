import json

import pandas as pd

from sarc.config import MyMilaConfig


def query_mymila(cfg: MyMilaConfig):
    if cfg is None:
        return pd.DataFrame()

    # NOTE: Using json loads on open instead of pd.read_json
    #       because the mocked config is on `open`. It stinks, but we
    #       will replace this part of the code in favor of direct calls to
    #       MyMila API anyway.
    return pd.DataFrame(
        json.loads(open(cfg.tmp_json_path, "r", encoding="uft-8").read())
    )
