import logging
from pathlib import Path
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def DEV_load_new_DEV_value_dfs() -> Dict[str, pd.DataFrame]:
    """Load the latest available locally saved validation
    datafames into a dictionary of df_name, df value pairs.
    """
    logger.warning("WORKING WITH DEV DATA FOR NEW DFs!")
    df_dict_new = {}
    dev_values_path = Path.cwd() / "src" / "dev" / "values"
    print(dev_values_path)
    for file in dev_values_path.iterdir():
        file_name = file.name[:-11]  # truncate the timestamp
        df_dict_new[file_name] = pd.read_parquet(file)

    logging.debug(f"{len(list(df_dict_new.items()))} new DEV dataframes loaded.")
    return df_dict_new
