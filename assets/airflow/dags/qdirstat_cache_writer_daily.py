"""
QDirStat Cache Writer Daily DAG

This DAG runs the qdirstat-cache-writer script equivalenton specified directories.
It runs on a daily schedule with default paths, but allows users to specify 
custom paths dynamically when triggering manually from the Airflow UI.
"""

import logging
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator
from airflow.decorators import task

# Default arguments for the DAG
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}

# Define the DAG
dag = DAG(
    "qdirstat_cache_writer_daily",
    default_args=default_args,
    description="Daily run of qdirstat-cache-writer script equivalent on specified paths",
    schedule="0 0 * * 6",  # Runs weekly (Saturday) at midnight
    start_date=datetime(2026, 3, 23),  # start_date is required for scheduled DAGs
    catchup=False,
    tags=["qdirstat", "storage", "tracking"],
    params=ParamsDict(
        {
            "paths_to_scan": Param(
                default=["dir_to_scan_1", "dir_to_scan_2"],
                type="array",
                items={"type": "string"},
                title="Paths to Scan",
                description="List of directory paths to scan. You can add or remove paths here during a manual run.",
                section="Scan Configuration",
            ),
            "bin_path": Param(
                "path_to_crawler",
                type="string",
                title="Binary Path",
                description="Absolute path to the executable binary",
                section="Command Configuration",
            )
        }
    ),
)


@task(task_id="generate_scan_commands", dag=dag)
def generate_scan_commands(**kwargs):
    """
    Extracts parameters at runtime and generates a list of bash commands.
    This enables Airflow to dynamically map a task for each path.
    """
    params = kwargs["params"]
    bin_path = params["bin_path"]
    paths_to_scan = params["paths_to_scan"]

    commands = []
    for path in paths_to_scan:
        # Wrap the command with a simple echo for better Airflow logging
        cmd = f'echo "Scanning directory: {path}" && {bin_path}  --data-root {path}  --output-cache-file {path}/.qdirstat.cache.gz --skip-fingerprint'
        commands.append(cmd)
        logging.info(f"Generated command for path: {path}")

    return commands


# 1. First task: generate the list of commands based on UI/Schedule parameters
commands_list = generate_scan_commands()

# 2. Second task: Dynamic Task Mapping
# This automatically expands into N tasks depending on how many paths are in `paths_to_scan`
scan_directories = BashOperator.partial(
    task_id="scan_directories",
    dag=dag,
    max_active_tis_per_dag=1,
).expand(bash_command=commands_list)
