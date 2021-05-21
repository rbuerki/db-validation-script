import datetime as dt
import logging
from pathlib import Path
from time import sleep
from typing import Tuple

from rich.console import Console
from rich.logging import RichHandler

# from dev import dev_functions as DEVEL  # TODO Dev stand in
import utils
import validate_structure as struct
import validate_values as val
from sql_queries import query_dict

console = Console()

CONFIG_PATH = "config.yaml"
DATA_PATH = "data/"


def initialize_logger():
    """Initialize logging, to console using rich for formatting
    and to file.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # Create a formatter (same for file and console)
    fformatter = logging.Formatter(
        fmt="%(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    cformatter = logging.Formatter(
        fmt="%(message)s",
        datefmt="[%X]"
    )
    # Create console handler
    sh = RichHandler(show_time=False, show_path=False, markup=True)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(cformatter)
    logger.addHandler(sh)
    # Create file handler
    filename = (
        f"cat_val_{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d-%H-%M-%S')}.log"
    )
    fh = logging.FileHandler(
        Path.cwd() / "logs" / filename, "w", encoding=None, delay="true"
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fformatter)
    logger.addHandler(fh)

    return logger


def run_set_up(logger: logging.Logger) -> Tuple[Path, Path]:
    """Get the path to the validation data for the latest date
    available and if it is not the actual date, create a new
    folder for saving the data from the actual run.
    """
    console.print("")
    logger.info("[bold DARK_MAGENTA]Set-up CATALYST VALIDATION[/]",)
    latest_data_path = utils.get_latest_previous_validation_data_path(DATA_PATH)
    actual_data_path = utils.create_actual_validation_data_path(DATA_PATH)
    logger.info(f"Actual run date: {dt.datetime.now().date()}")
    logger.info(
        f"Comparing to previous validation data from: {latest_data_path.name[:10]}\n"
    )
    return latest_data_path, actual_data_path


def run_structure_validation(
    logger: logging.Logger, latest_data_path: Path, actual_data_path: Path
) -> None:
    """Run the structure validation part (schema checks and empty
    columns checks).
    """
    logger.info("[bold DARK_MAGENTA]STARTING STRUCTURE CHECKS ...[/]\n",)

    server = utils.read_yaml(CONFIG_PATH, "SERVER")
    db_list = utils.read_yaml(CONFIG_PATH, "DB_LIST")

    # Structure checks for all DBs in config list
    console.rule("[bold dark_yellow] Schema Checks for DataMarts and BCL")
    console.print("")

    for db_name in db_list:
        engine, connection = utils.connect_to_db(server, db_name)
        with connection:
            insp = struct.inspect_db(connection)
            logger.info(f"[bold DARK_MAGENTA]Schema Check[/] {db_name.upper()}")
            tables_views_old = struct.load_latest_tables_and_views_dict(
                db_name,
                latest_data_path
            )
            tables_views_new = struct.create_new_tables_and_views_dict(
                db_name,
                insp
            )
            struct.compare_tables_and_views_dicts(
                tables_views_new,
                tables_views_old,
                db_name
            )
            struct.save_new_tables_and_views_dict(
                tables_views_new,
                db_name,
                actual_data_path
            )

    # Empty cols check for the DM DBs only
    console.rule("[bold dark_yellow] Empty Columns Checks for DataMarts")
    console.print("")
    for db_name in [db_name for db_name in db_list if db_name.startswith("Snipp")]:
        engine, connection = utils.connect_to_db(server, db_name)
        with connection:
            insp = struct.inspect_db(connection)
            tables_views_new = struct.create_new_tables_and_views_dict(
                db_name,
                insp
            )
            logger.info(f"[bold DARK_MAGENTA]Empty Columns-Check[/] {db_name.upper()}")
            empty_cols_old = struct.load_latest_empty_cols_dict(
                db_name,
                latest_data_path
            )
            empty_cols_new = struct.create_new_empty_cols_dict(
                db_name,
                tables_views_new,
                connection
            )
            struct.compare_empty_cols_dicts(
                empty_cols_new,
                empty_cols_old,
                db_name
            )
            struct.save_new_empty_cols_dict(
                empty_cols_new,
                db_name,
                actual_data_path
            )


def run_value_validation(
    logger: logging.Logger, latest_data_path: Path, actual_data_path: Path
) -> None:
    """Run the values validation part (consistency between DBs
    and consistency over time).
    """
    logger.info("[bold DARK_MAGENTA]STARTING DATA VALUE CHECKS ...[/]\n",)

    server = utils.read_yaml(CONFIG_PATH, "SERVER")
    db_list = utils.read_yaml(CONFIG_PATH, "DB_LIST")
    vendor_list = utils.read_yaml(CONFIG_PATH, "VENDOR_LIST")

    engine, connection = utils.connect_to_db(server, db_list[0])
    with connection:
        n_months = utils.read_yaml(CONFIG_PATH, "QUERY_N_MONTHS_BACK")
        start_date, end_date = val.get_start_and_end_date_strings(n_months)
        df_full_old = val.load_old_value_dfs(latest_data_path)
        # df_full_new = DEVEL.DEV_load_new_DEV_value_dfs()  # TODO DEV stand in
        df_full_new = val.load_new_value_dfs(
            connection, query_dict, start_date, end_date
        )
        val.save_new_value_dfs(df_full_new, actual_data_path)

        for vendor in [vendor.lower() for vendor in vendor_list]:
            df_vendor_new, df_vendor_old = val.grab_and_truncate_df_names_for_vendor(
                vendor, df_full_new, df_full_old
            )

            # Run transaction checks
            console.rule(f"[bold dark_yellow] Fact table checks {vendor.upper()}")
            console.print("")
            sleep(0.5)
            logger.info(
                f"{vendor.upper()} - Summary of DM_FactTrans:\n"
                f"{df_vendor_new['DM_FactTrans']}\n"
            )
            sleep(0.5)
            df_diff = val.return_subtraction_df(
                df_vendor_new["DM_FactTrans"],
                df_vendor_new["bcl_EtlTransaction"]
            )
            logger.info(
                f"{vendor.upper()} - Difference DM_FactTrans to bcl_EtlTransactions:\n"
                f"{df_diff}\n")
            sleep(0.5)
            df_diff = val.return_subtraction_df(
                df_vendor_new["DM_FactTrans"],
                df_vendor_new["DM_FactTransItem"]
            )
            logger.info(
                f"{vendor.upper()} - Difference DM_FactTrans to DM_FactTransItem:\n"
                f"{df_diff}\n")
            sleep(0.5)
            df_diff = val.return_subtraction_df(
                df_vendor_new["DM_FactTrans"],
                df_vendor_old["DM_FactTrans"]
            )
            logger.info(
                f"{vendor.upper()} - Difference DM_FactTrans new to previous:\n"
                f"{df_diff}\n")
            sleep(0.5)
            df_diff = val.return_subtraction_df(
                df_vendor_new["DM_FactTransItem"],
                df_vendor_old["DM_FactTransItem"]
            )
            logger.info(
                f"{vendor.upper()} - Difference DM_FactTransItem new to previous:\n"
                f"{df_diff}\n")
            sleep(0.5)
            if vendor == "pkz":
                n_dup_TISK = val.check_for_duplicate_TISK(df_vendor_new)
                if n_dup_TISK == 0:
                    logger.info(
                        f"{vendor.upper()}, extra check - No duplicate "
                        f"TransactionItemSK in FactTransItem.\n"
                    )
                else:
                    logger.error(
                        f"{vendor.upper()}, extra check - {n_dup_TISK} "
                        f"duplicate TransactionItemSK in FactTransItem!\n")
            sleep(0.5)
            # Run member checks
            console.rule(f"[bold dark_yellow] Member Checks {vendor.upper()}")
            console.print("")
            sleep(0.5)
            logger.info(
                f"{vendor.upper()} - Summary of MemberAK:\n"
                f"{df_vendor_new['DM_DimMember_AK']}\n"
            )
            sleep(0.5)
            diff_n_MemberAK, diff_defaultDates = val.return_diff_member_stuff(
                df_vendor_new, df_vendor_old
            )
            logger.info(
                f"{vendor.upper()} - Change in n MemberAK: {diff_n_MemberAK}\n"
                f"{vendor.upper()} - Change in n Birthdates '1Jan1900': {diff_defaultDates}\n"
            )
            sleep(0.5)
            logger.info(
                f"{vendor.upper()} - 2019 Summary for 3 random customers:\n"
                f"{df_vendor_new['DM_three_members']}\n")
            three_members_equal = val.compare_three_members(
                df_vendor_new,
                df_vendor_old
            )
            if three_members_equal:
                logger.info("Consistency Check for 3 MemberAK ok.\n")
            else:
                logger.error(
                    f"Consistency Check for 3 MemberAK failed!\n"
                    f"Check the previous 'DM_Three_Members' data:\n"
                    f"{df_vendor_old['DM_Three_Members']}\n"
                )
            sleep(0.5)
            # Run product checks
            console.rule(f"[bold dark_yellow] Product Checks {vendor.upper()}")
            console.print("")
            sleep(0.5)
            logger.info(
                f"{vendor.upper()} - 2019 Summary for 3 random products:\n"
                f"{df_vendor_new['DM_three_products']}\n")
            three_members_equal = val.compare_three_products(
                df_vendor_new,
                df_vendor_old
            )
            if three_members_equal and vendor == "pkz":
                logger.info("Consistency Check for 3 TransactionItemCode ok.\n")
            elif three_members_equal and vendor == "loeb":
                logger.info("Consistency Check for 3 ProductAK ok.\n")
            else:
                logger.error(
                    f"Consistency Check for 3 Products failed!\n"
                    f"Check the previous 'DM_Three_Products' data:\n"
                    f"{df_vendor_old['DM_Three_Members']}\n"
                )


def main(logger):
    latest_data_path, actual_data_path = run_set_up(logger)
    run_structure_validation(logger, latest_data_path, actual_data_path)
    run_value_validation(logger, latest_data_path, actual_data_path)


if __name__ == "__main__":
    logger = initialize_logger()
    main(logger)
