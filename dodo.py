"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based.
"""

import sys
sys.path.insert(1, "./src/")

import shutil
from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init
from doit.reporter import ConsoleReporter
from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)


if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}

init(autoreset=True)

DATA_DIR        = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR      = config("OUTPUT_DIR")
OS_TYPE         = config("OS_TYPE")

environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"

# fmt: off
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    from_path = Path(from_path)
    to_path   = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        return f"mv {from_path} {to_path}"
    else:
        return f"move {from_path} {to_path}"


def copy_file(origin_path, destination_path, mkdir=True):
    def _copy_file():
        origin = Path(origin_path)
        dest   = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)
    return _copy_file


##################################
## Pipeline tasks
##################################

def task_config():
    """Create empty directories for data and output if they don't exist."""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull_wrds():
    """Pull raw futures settlement data from WRDS."""
    return {
        "actions": ["ipython ./src/pull_wrds_clean.py"],
        "targets": [DATA_DIR / "wrds_futures.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_wrds_clean.py"],
        "clean": [],
    }


def task_build_clean_data():
    """Build bimonthly commodity panel (original sample 1986-2010)."""
    return {
        "actions": ["ipython ./src/build_clean_data.py"],
        "targets": [DATA_DIR / "commodity_panel.parquet"],
        "file_dep": [
            "./src/build_clean_data.py",
            DATA_DIR / "wrds_futures.parquet",
        ],
        "clean": True,
    }


def task_build_clean_data_extended():
    """Build bimonthly commodity panel (extended sample 1986-2025)."""
    return {
        "actions": ["ipython ./src/build_clean_data_extended.py"],
        "targets": [DATA_DIR / "commodity_panel_extended.parquet"],
        "file_dep": [
            "./src/build_clean_data_extended.py",
            DATA_DIR / "wrds_futures.parquet",
        ],
        "clean": True,
    }


def task_build_returns():
    """Compute SR and EH returns panel (original sample)."""
    return {
        "actions": ["ipython ./src/process_futures.py"],
        "targets": [DATA_DIR / "returns_panel.parquet"],
        "file_dep": [
            "./src/process_futures.py",
            DATA_DIR / "commodity_panel.parquet",
        ],
        "clean": True,
    }


def task_build_returns_extended():
    """Compute SR and EH returns panel (extended sample)."""
    return {
        "actions": ["ipython ./src/process_futures_extended.py"],
        "targets": [DATA_DIR / "returns_panel_extended.parquet"],
        "file_dep": [
            "./src/process_futures_extended.py",
            DATA_DIR / "commodity_panel_extended.parquet",
        ],
        "clean": True,
    }


def task_create_table_1():
    """Replicate Table 1 (original sample)."""
    return {
        "actions": ["ipython ./src/create_table_1.py"],
        "targets": [
            OUTPUT_DIR / "table1_short_roll.csv",
            OUTPUT_DIR / "table1_excess_holding.csv",
            OUTPUT_DIR / "table1.tex",
        ],
        "file_dep": [
            "./src/create_table_1.py",
            DATA_DIR / "returns_panel.parquet",
        ],
        "clean": True,
    }


def task_create_table_1_extended():
    """Replicate Table 1 (extended sample)."""
    return {
        "actions": ["ipython ./src/create_table_1_extended.py"],
        "targets": [
            OUTPUT_DIR / "table1_extended_short_roll.csv",
            OUTPUT_DIR / "table1_extended_excess_holding.csv",
            OUTPUT_DIR / "table1_extended.tex",
        ],
        "file_dep": [
            "./src/create_table_1_extended.py",
            DATA_DIR / "returns_panel_extended.parquet",
        ],
        "clean": True,
    }


def task_create_table_2():
    """Replicate Table 2 (original sample)."""
    return {
        "actions": ["ipython ./src/create_table_2.py"],
        "targets": [
            OUTPUT_DIR / "table2_panel_a_sr.csv",
            OUTPUT_DIR / "table2_panel_a_eh.csv",
            OUTPUT_DIR / "table2.tex",
        ],
        "file_dep": [
            "./src/create_table_2.py",
            DATA_DIR / "returns_panel.parquet",
        ],
        "clean": True,
    }


def task_create_table_2_extended():
    """Replicate Table 2 (extended sample)."""
    return {
        "actions": ["ipython ./src/create_table_2_extended.py"],
        "targets": [
            OUTPUT_DIR / "table2_extended_panel_a_sr.csv",
            OUTPUT_DIR / "table2_extended_panel_a_eh.csv",
            OUTPUT_DIR / "table2_extended.tex",
        ],
        "file_dep": [
            "./src/create_table_2_extended.py",
            DATA_DIR / "returns_panel_extended.parquet",
        ],
        "clean": True,
    }


def task_exploratory_charts():
    """Generate exploratory charts of futures settlement prices."""
    return {
        "actions": ["ipython ./src/exploratory_charts.py"],
        "targets": [OUTPUT_DIR / "futures_settlements.png"],
        "file_dep": [
            "./src/exploratory_charts.py",
            DATA_DIR / "wrds_futures.parquet",
        ],
        "clean": True,
    }


def task_test():
    """Run replication unit tests."""
    return {
        "actions": ["pytest src/test_replication.py -v"],
        "file_dep": [
            "./src/test_replication.py",
            DATA_DIR / "returns_panel.parquet",
            OUTPUT_DIR / "table1_short_roll.csv",
            OUTPUT_DIR / "table1_excess_holding.csv",
            OUTPUT_DIR / "table2_panel_a_sr.csv",
            OUTPUT_DIR / "table2_panel_b1_sr.csv",
        ],
        "verbosity": 2,
    }


##################################
## Notebook tasks
##################################

notebook_tasks = {
    "example_notebook_interactive_ipynb": {
        "path": "./src/example_notebook_interactive_ipynb.py",
        "file_dep": [],
        "targets": [],
    },
}

# fmt: off
def task_run_notebooks():
    """Execute notebooks and export to HTML."""
    for notebook, meta in notebook_tasks.items():
        pyfile_path   = Path(meta["path"])
        notebook_path = pyfile_path.with_suffix(".ipynb")
        yield {
            "name": notebook,
            "actions": [
                f"jupytext --to notebook --output {notebook_path} {pyfile_path}",
                jupyter_execute_notebook(notebook_path),
                jupyter_to_html(notebook_path),
                mv(notebook_path, OUTPUT_DIR),
            ],
            "file_dep": [pyfile_path, *meta["file_dep"]],
            "targets": [OUTPUT_DIR / f"{notebook}.html", *meta["targets"]],
            "clean": True,
        }
# fmt: on


##################################
## LaTeX compilation
##################################
def task_make_summary_stats():
    """Generate LaTeX snippets from panels and returns."""
    return {
        "actions": ["ipython ./src/make_summary_stats.py"],
        "file_dep": [
            "./src/make_summary_stats.py",
            DATA_DIR / "returns_panel.parquet",
            DATA_DIR / "commodity_panel.parquet",
        ],
        "targets": [
            OUTPUT_DIR / "summary_stats.tex",
            OUTPUT_DIR / "commodity_panel_head.tex",
            OUTPUT_DIR / "sectors_list.tex",
            OUTPUT_DIR / "commodities_list.tex",
        ],
        "clean": True,
    }

def task_compile_latex_docs():
    """Compile replication_summary.tex to PDF."""
    return {
        "actions": [
            "cd reports && latexmk -xelatex -gg -halt-on-error -interaction=nonstopmode replication_summary.tex"
        ],
        "targets": ["reports/replication_summary.pdf"],
        "file_dep": [
            "reports/replication_summary.tex",
            OUTPUT_DIR / "summary_stats.tex",
            OUTPUT_DIR / "commodity_panel_head.tex",
            OUTPUT_DIR / "sectors_list.tex",
            OUTPUT_DIR / "commodities_list.tex",
            OUTPUT_DIR / "table1.tex",
            OUTPUT_DIR / "table1_extended.tex",
            OUTPUT_DIR / "table2.tex",
            OUTPUT_DIR / "table2_extended.tex",
        ],
        "clean": True,
    }

