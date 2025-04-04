from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
from dagster_snowflake_pandas import SnowflakePandasIOManager
from dagster_azure.adls2 import ADLS2Resource, ADLS2SASToken

from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    resources={
        "io_manager": SnowflakePandasIOManager(
            # Read about using environment variables and secrets in Dagster:
            # https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets
            account=EnvVar("SNOWFLAKE_ACCOUNT"),
            user=EnvVar("SNOWFLAKE_USER"),
            password=EnvVar("SNOWFLAKE_PASSWORD"),
            warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
            database=EnvVar("SNOWFLAKE_DATABASE"),
            schema=EnvVar("SNOWFLAKE_SCHEMA"),
        ),
        "adls2": ADLS2Resource(
            storage_account=EnvVar("my_storage_account"),
            credential=ADLS2SASToken(token=EnvVar("my_sas_token")),
        )
    },
    schedules=[daily_refresh_schedule],
)
