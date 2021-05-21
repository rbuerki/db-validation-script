""" Note: This approach uses the lower level Inspector class / inspect()
method and not the Metadata class / reflect() method, because of some invalid
column names like "epos User$" that cause the latter to break.
"""

import datetime as dt
import logging
import pickle
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

logger = logging.getLogger(__name__)


def inspect_db(
    connection: sqlalchemy.engine.Connection
) -> sqlalchemy.engine.reflection.Inspector:
    """Get and return MetaData of DB using SQLAlchemy's
    inspect() method.
    """
    insp = sqlalchemy.inspect(connection)
    return insp


def load_latest_tables_and_views_dict(db_name: str, latest_data_path: str):
    """Load the latest available locally saved dictionary containing
    all the views and tables with their columns.
    """
    name_pattern = f"{db_name}_tables_and_views"
    latest_structure_path = Path(latest_data_path) / "structure"
    file_list = [
        file.name for file in latest_structure_path.iterdir()
        if file.name.startswith(name_pattern)
    ]
    try:
        latest_file = sorted(file_list)[-1]
    except IndexError:
        print(f"\nError: No file '{name_pattern}' at path {latest_structure_path}.")
        raise
    with open(latest_structure_path / latest_file, "rb") as f:
        dict_old = pickle.load(f)
    return dict_old


def create_new_tables_and_views_dict(
    db_name: str,
    insp: sqlalchemy.engine.reflection.Inspector
) -> Dict[str, List[str]]:
    """Create and return a new dict containing tables and views of
    the DB as keys and their respective column names as values.
    """
    tables_views_new = {}
    for table in insp.get_table_names():
        try:
            tables_views_new[table] = sorted(
                [col["name"] for col in insp.get_columns(table)]
            )
        except ProgrammingError:
            logger.warning(
                f"Table '{table}' NOT PARSED! It is not included in analysis."
            )
            pass
    for view in insp.get_view_names():
        try:
            tables_views_new[view] = sorted(
                [col["name"] for col in insp.get_columns(view)]
            )
        except ProgrammingError:
            logger.warning(
                f"View '{view}' NOT PARSED! It is not included in analysis."
            )
            pass

    return tables_views_new


def compare_tables_and_views_dicts(
    dict_new: Dict[str, List[str]],
    dict_old: Dict[str, List[str]],
    db_name: str
):
    """Compare new and old dicts and output the differences."""
    if dict_new == dict_old:
        logger.info("No changes detected in tables and / or views since last run.\n")
        added = removed = modified = 0
    else:
        new_keys = set(dict_new.keys())
        old_keys = set(dict_old.keys())
        intersect_keys = new_keys.intersection(old_keys)
        added = new_keys - old_keys
        removed = old_keys - new_keys
        modified = {
            o : (dict_new[o], dict_old[o]) for o in intersect_keys
            if dict_new[o] != dict_old[o]
        }
        # Pretify the output if len of object is 0
        # Prettify the output
        if len(added) == 0:
            added = "-"
        else:
            added = ", ".join([table for table in added])
        if len(removed) == 0:
            removed = "-"
        else:
            removed = ", ".join([table for table in removed])
        if len(modified) == 0:
            modified = "-"
            modified_dict = ""
        else:
            modified_dict = "\n".join(
                [
                    f"{key}:\n - columns now: {value[0]}\n - columns previous: {value[1]}\n"
                    for key, value in modified.items()
                ]
            )
            modified = ", ".join([table for table in modified])

        logger.warning(
            "[dark_red]CHANGES DETECTED in tables and / or views since last run[/]:\n"
            f"- Tables / views that have been newly added with this run: {added}\n"
            f"- Tables / views that have been removed with this run: {removed}\n"
            f"- Tables / views whose columns have changed:{modified}\n{modified_dict}"
        )


def save_new_tables_and_views_dict(
    dict_new: Dict[str, List[str]],
    db_name: str,
    actual_data_path: str
) -> None:
    """Save the new dict, timestamped, to a pickle object."""
    dt_now_str = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d-%H-%M-%S")
    filename = f"{db_name}_tables_and_views_{dt_now_str}"
    actual_structure_path = Path(actual_data_path / "structure")
    with open(actual_structure_path / filename, "wb") as savepath:
        pickle.dump(dict_new, savepath)


def load_latest_empty_cols_dict(db_name: str, latest_data_path: str) -> None:
    """Load the latest available locally saved dictionary containing
    tables and views with empty columns listed.
    (Note: Only the name_patterns differs from function above.)
    """
    name_pattern = f"{db_name}_empty_cols"
    latest_structure_path = Path(latest_data_path) / "structure"
    file_list = [
        file.name for file in latest_structure_path.iterdir()
        if file.name.startswith(name_pattern)
    ]
    latest_file = sorted(file_list)[-1]
    with open(latest_structure_path / latest_file, "rb") as f:
        dict_old = pickle.load(f)
    return dict_old


def create_new_empty_cols_dict(
    db_name: str,
    tables_views_new: Dict[str, List[str]],
    connection: sqlalchemy.engine.Connection ,
    n_rows: int = 50
) -> Dict[str, List[str]]:
    """Use the new dict to check for columns with empty values in
    the first n rows. Output the result. Note: Empty strings are handled
    as NaN - that was a mean one ;-).
    """
    empty_cols_new = {}
    for table, columns in list(tables_views_new.items()):
        try:
            query = f"SELECT TOP {n_rows} * FROM [{table}]"
            result = connection.execute(query).fetchall()
            result_df = pd.DataFrame(result, columns=columns)
            result_df.replace("", np.NaN, inplace=True)
            empty_cols = [
                col for col in result_df.columns
                if result_df[col].isnull().all() == True  # noqa: E712, does not work with 'is'!
            ]
            if len(empty_cols) > 0:
                empty_cols_new[table] = empty_cols
        except ProgrammingError:
            logger.warning(
                f"Table / view '{table}' NOT PARSED! It is not included in analysis.\n"
            )
            pass

    # This is for output summary only
    count_total = len(list(tables_views_new.keys()))
    count_empty = len(list(empty_cols_new.keys()))
    count_empty_cols = sum([len(v) for v in empty_cols_new.values()])
    logger.info(
        f"{count_empty} out of total {count_total} tables have "
        f"a total of {count_empty_cols} empty columns.\n"
    )
    return empty_cols_new


def compare_empty_cols_dicts(
    dict_new: Dict[str, List[str]],
    dict_old: Dict[str, List[str]],
    db_name: str
):
    """Compare new and old dicts and output the differences."""
    if dict_new == dict_old:
        logger.info("No changes detected in empty columns since last run.")
        added = removed = modified = 0
    else:
        new_keys = set(dict_new.keys())
        old_keys = set(dict_old.keys())
        intersect_keys = new_keys.intersection(old_keys)
        added = {o : dict_new[o] for o in new_keys - old_keys}
        removed = old_keys - new_keys
        modified = {
            o : (dict_new[o], dict_old[o]) for o in intersect_keys
            if dict_new[o] != dict_old[o]
        }
        # Prettify the output
        if len(added) == 0:
            added = "-"
        else:
            added = ", ".join([table for table in added])
        if len(removed) == 0:
            removed = "-"
        else:
            removed = ", ".join([table for table in removed])
        if len(modified) == 0:
            modified = "-"
            modified_dict = ""
        else:
            modified_dict = "\n".join(
                [
                    f"{key}:\n - empty now: {value[0]}\n - empty previous: {value[1]}\n"
                    for key, value in modified.items()
                ]
            )
            modified = ", ".join([table for table in modified])

        logger.warning(
            "[dark_red]CHANGES DETECTED in empty columns since last run.[/]\n"
            f"- Tables / views that got empty columns only with this run: {added}\n"
            f"- Tables / views that have no more empty columns with this run: {removed}\n"
            f"- Tables / views whose empty columns have changed: {modified}\n{modified_dict}"
        )


def save_new_empty_cols_dict(
    dict_new: Dict[str, List[str]],
    db_name: str,
    actual_data_path: str
):
    """Save the new dict, timestamped, to a pickle object.
    (Note: Only the name_patterns differs from the function above.)
    """
    dt_now_str = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d-%H-%M-%S")
    filename = f"{db_name}_empty_cols_{dt_now_str}"
    actual_structure_path = Path(actual_data_path / "structure")
    with open(actual_structure_path / filename, "wb") as savepath:
        pickle.dump(dict_new, savepath)
