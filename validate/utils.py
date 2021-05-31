import datetime as dt
import logging
import yaml
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import sqlalchemy

logger = logging.getLogger(__name__)


def read_yaml(file_path: Union[str, Path], section: Optional[str]) -> Dict:
    """Return the key-value-pairs from a YAML file, or, if the
    optional `section` parameter is passed, only from a specific
    section of that file.
    """
    with open(file_path, "r") as f:
        yaml_content = yaml.safe_load(f)
    if not section:
        return yaml_content
    else:
        try:
            return yaml_content[section]
        except KeyError:
            logging.error(f"Section {section} not found in config file. Please check.")
            raise


def connect_to_db(
    server: str,
    db_name: str
) -> Tuple[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection]:
    """Connect to DB and open a persistent connection. The param
    `fast_exectuemany` is active for bulk operations. Return engine
    and connection objects.
    """
    con_string = (
        f"mssql+pyodbc://{server}/{db_name}?driver=ODBC Driver 13 for SQL Server"
    )

    engine = sqlalchemy.create_engine(con_string, fast_executemany=True)
    connection = engine.connect()
    return engine, connection


def get_latest_previous_validation_data_path(data_path: str) -> Path:
    """Return the path to the latest available validation data from
    previous runs. Has to be from before the actual date. This data
    will be used for the comparison with the results of the actual run.
    """
    date_today_str = dt.datetime.strftime(dt.date.today(), "%Y-%m-%d")
    data_path = Path(data_path)
    data_dirs = [d.name for d in data_path.iterdir() if d.is_dir()]
    previous_data_dirs = [d for d in data_dirs if d[:10] != date_today_str]
    try:
        latest_data_path = data_path / sorted(previous_data_dirs)[-1]
    except IndexError:
        logging.error("No previous validation data found in {data_path}!")
        raise
    return latest_data_path


def create_actual_validation_data_path(data_path: str) -> Path:
    """Create a new data directory that is named with today's date
    string. If there is already such a directory name, output a warning
    and use the existing directory.
    """
    date_today_str = dt.datetime.strftime(dt.date.today(), "%Y-%m-%d")
    actual_data_path = Path(data_path) / f"{date_today_str}_catalyst_validation_data"
    if actual_data_path.exists():
        logging.warning(
            "Data path for actual date already exists, "
            "existing data will be overwritten."
        )
    actual_data_path.mkdir(exist_ok=True)
    # Create the subdirectories too
    actual_structure_path = actual_data_path / "structure"
    actual_structure_path.mkdir(exist_ok=True)
    actual_values_path = actual_data_path / "values"
    actual_values_path.mkdir(exist_ok=True)
    return actual_data_path


# def close(cur, conn):
#     """Close the communication with the database."""
#     try:
#         cur.close()

#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)

#     finally:
#         if conn is not None:
#             conn.close()
#             print("Database connection closed.")
