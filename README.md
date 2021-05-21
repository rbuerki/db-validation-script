
# Catalyst Validations

May 2021, Version 0.2

## Introduction

Command line tool for validation of the Loeb and PKZ DB dumps in the CAE. It has two components:

1. check for changes in structure since last run (schema / empty columns)
2. check of some data summaries for consistency compared to last run and between different tables.

It outputs the result of the checks to the console and to a timestamped logfile in the `logs` folder.

## Run

After activating an env with the necessary dependencies (see section "build"), the app can be run from the main folder (containing this README file) by typing the following command in the CLI:

```python
python src
```

## Build

This project runs with **Python 3.6 or higher**. There is no need for a separate env, but make sure you have installed the following dependencies:

- The usual suspects: numpy, pandas, sqlalchemy
- Rich (`pip install rich`)
- Pyarrow (`conda install pyarrow -c conda-forge`)

## What has to be true?

For automated loads and saves, the package heavily relies on the exact naming of folders and files in the `data` directory. Do not change the logic of file or folder names in that directory and also don't save manually created files in there (e.g. an XLSX file with some manual checks).  

While you should not delete individual files, you can delete entire subfolders in the `data` dir, but to work properly, the package needs at least one complete `data` subfolder that starts with a date string prior to the actual date.

## FAQ

**How can I change the "previous" data / date the data from the actual run is compared to?**

For data comparisons the package will always look for the most recent date string in the subfolders of the `data` directory that is prior to the actual date. Let's look at an example:

Today is May, 1st and we have two subfolders in the `data` directory:

- `2021-01-01_catalyst_validation_data`
- `2021-02-28_catalyst_validation_data`

For some reason we would like to compare the actual run not to the data from Feb 28th, but from Jan, 1st. How can we do that? Either we make a copy of the Jan subfolder and change the date string to be larger than 2021-02-28, or we simply delete the Feb subfolder or rename it with a date string < 2021-01-01.

Special case: You want to run the package twice on the same date and compare the output of the first run to the output of the second run. Then, after the first run you will have to rename it with a date string that is < than the actual date. Now you can re-run the package and compare the two runs.

**How can I add a new data check?**

1) Write an sql query und copy it into the `sql_queries.py` module.
2) Add the query-variable to the `query_dict` at the end of the `sql_queries.py` module (make sure you use a {vendor}_{db}_{anynameyouwant} pattern for the dict key).
3) Insert a piece of code (use copy paste of existing code) to display your check in the `run_value_validation` function in the `__main__` script.
