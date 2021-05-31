import datetime as dt
import dateutil.relativedelta as rd
import logging
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import sqlalchemy

logger = logging.getLogger(__name__)


def get_start_and_end_date_strings(n_months: int) -> Tuple[str, str]:
    """Return strings for the start and end date of the validation
    queries (where applicable). The end date is the last day of the
    previous month relative to the run date. The start day is n
    months back. n_months can be configured in the `config.yaml`.
    """
    end_date = dt.datetime.now().date().replace(day=1) - dt.timedelta(days=1)
    start_date = end_date + dt.timedelta(days=1) - rd.relativedelta(months=n_months)
    start_date = dt.datetime.strftime(start_date, format="%Y%m%d")
    end_date = dt.datetime.strftime(end_date, format="%Y%m%d")
    return start_date, end_date


def load_old_value_dfs(latest_data_path: str) -> Dict[str, pd.DataFrame]:
    """Load the latest available locally saved validation
    datafames into a dictionary of df_name, df value pairs.
    """
    df_dict_old = {}
    latest_values_path = Path(latest_data_path) / "values"
    for file in latest_values_path.iterdir():
        file_name = file.name[:-11]  # truncate the timestamp
        df_dict_old[file_name] = pd.read_parquet(file)

    logger.debug(f"{len(list(df_dict_old.items()))} previous dataframes loaded.")
    return df_dict_old


def load_new_value_dfs(
    connection: sqlalchemy.engine.Connection,
    query_dict: Dict[str, str],
    start_date: str,
    end_date: str
) -> Dict[str, pd.DataFrame]:
    """Return a dict of df_name : df pairs by iterating over all
    the queries in the query dict of the `sql_queries.py` module.
    """
    df_dict_new = {}
    for n, item in enumerate(list(query_dict.items())):
        q_name, query = item[0], item[1]
        query = query.replace('start_date', start_date)
        query = query.replace('end_date', end_date)
        result = connection.execute(query).fetchall()
        result_df = pd.DataFrame(result, columns=result[0].keys())

        # Fix dtypes
        for col in result_df:
            if col in ["total_value", "n_trx", "n_members"]:
                result_df[col] = pd.to_numeric(result_df[col], errors="raise")
            else:
                result_df[col] = result_df[col].astype(str)

        df_dict_new[q_name] = result_df
        logger.debug(
            f"{q_name} appended to dict. "
            f"({n+1}/{len(list(query_dict.items()))})"
        )

    return df_dict_new


def save_new_value_dfs(df_dict: Dict[str, pd.DataFrame], actual_data_path: Path):
    """Save the new dataframes, timestamped, to parquet files in
    the 'values' subfolder of the actual data folder.
    """
    date_today_str = dt.datetime.strftime(dt.date.today(), "%Y-%m-%d")
    for q_name, df in df_dict.items():
        filename = f"{q_name}_{date_today_str}"
        df.to_parquet(f"{actual_data_path / 'values' / filename}", index=False)
    logger.debug(f"{len(list(df_dict.items()))} new dataframes saved to disc.\n")


def grab_and_truncate_df_names_for_vendor(
    vendor_name: str,
    df_dict_new: Dict[str, pd.DataFrame],
    df_dict_old: Dict[str, pd.DataFrame]
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """Return the df_dicts with the dfs for the relevant vendor only.
    The df_names have the vendor_prefix removed.
    """
    dict_list = []
    for df_dict in [df_dict_new, df_dict_old]:
        df_dict = {k: v for k, v in df_dict.items() if k.startswith(vendor_name)}.copy()

        new_keys = []
        for k in df_dict.keys():
            try:
                k = k.split("_", 1)[1]
                new_keys.append(k)
            except IndexError:
                print(
                    "Every key in the sql_queries dict "
                    "has to start with a vendor name prefix.")
                raise
        if len(new_keys) != len(list(df_dict.values())):
            raise AssertionError(
                "Problems with vendor name prefixes in "
                "the dataframe dictionary keys."
            )
        df_dict = dict(zip(new_keys, list(df_dict.values())))
        dict_list.append(df_dict)

    df_dict_new, df_dict_old = dict_list[0], dict_list[1]
    return df_dict_new, df_dict_old


# Transaction validation


def return_subtraction_df(
    df_1: pd.DataFrame,
    df_2: pd.DataFrame,
    index_col="yearmon"
) -> pd.DataFrame:
    """Return a dataframe with the values of the numeric cols
    from df_2 subtracted from the numeric cols of df_1. You can pass
    a column that exists in both df as index, default is `yearmon`.
    Only the values with overlapping index values will be compared!
    Important: Make sure the numeric cols have the same names in
    both dataframes, else the function will break.
    """
    df_1 = df_1.set_index(index_col).copy()
    df_2 = df_2.set_index(index_col).copy()

    overlapping_index_values = sorted(list(set(df_1.index.intersection(df_2.index))))
    num_cols = df_1.select_dtypes(include=np.number).columns.to_list()

    df_1_num_values = df_1.loc[overlapping_index_values, num_cols].to_numpy()
    df_2_num_values = df_2.loc[overlapping_index_values, num_cols].to_numpy()
    df_diff_values = df_1_num_values - df_2_num_values
    df_diff = pd.DataFrame(
        df_diff_values,
        columns=num_cols,
        index=sorted(overlapping_index_values)
    )
    return df_diff


# Member validation


def return_diff_member_stuff(df_dict_new, df_dict_old) -> Tuple[int, int]:
    """Return the difference in unique Member AK and in the number
    of birthdates with the default value Jan 1st 1900. The latter is
    probably only relevant for Loeb.
    """
    n_MemberAK_new = df_dict_new["DM_DimMember_AK"].loc[0, "n_dates_1Jan1900"]
    n_MemberAK_old = df_dict_old["DM_DimMember_AK"].loc[0, "n_dates_1Jan1900"]
    n_defaultDates_new = df_dict_new["DM_DimMember_AK"].loc[0, "n_dates_1Jan1900"]
    n_defaultDates_old = df_dict_old["DM_DimMember_AK"].loc[0, "n_dates_1Jan1900"]

    diff_n_MemberAK = int(n_MemberAK_new) - int(n_MemberAK_old)
    diff_defaultDates = int(n_defaultDates_new) - int(n_defaultDates_old)
    return diff_n_MemberAK, diff_defaultDates


def compare_three_members(df_dict_new, df_dict_old) -> bool:
    """Return a boolean indicating of the 2019 summary values of 3 members
    have changed or not. If they have, the MemberAK may have changed.
    """
    return df_dict_new["DM_three_members"].iloc[:, :-1].equals(
        df_dict_old["DM_three_members"].iloc[:, :-1]
    )


# Product validation


def compare_three_products(df_dict_new, df_dict_old) -> bool:
    """Return a boolean indicating of the 2019 summary values of 3 products
    have changed or not. If they have, the ProductAK (in the case of Loeb)
    or the TransactionItemCode (in the case of PKZ) may have changed.
    """
    return df_dict_new["DM_three_products"].iloc[:, :-1].equals(
        df_dict_old["DM_three_products"].iloc[:, :-1]
    )


def check_for_duplicate_TISK(df_dict_new) -> int:
    """Return the number of duplicate TransactionItemSK in the
    FactTransItem table. For the moment this check is only implemented
    for PKZ.
    """
    return int(df_dict_new["DM_duplicate_TISK"].iloc[0, 0])
