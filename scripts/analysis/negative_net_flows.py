import pandas as pd

from scripts.analysis.common import (
    convert_to_net_flows,
    summarise_by_country,
    create_groupings,
    reorder_countries,
)
from scripts.analysis.net_flows import get_all_flows, exclude_outlier_countries
from scripts.analysis.population_tools import add_population_under18
from scripts.config import Paths


def check_inflows_and_outflows_present(data: pd.DataFrame):
    """Check if inflows and outflows are present in the data"""

    data = (
        data.groupby(["year", "country", "indicator_type"], dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )
    data = data.pivot(
        index=[c for c in data.columns if c not in ["value", "indicator_type"]],
        columns="indicator_type",
        values="value",
    ).reset_index()

    return data


def count_negative_flows_by_year(data: pd.DataFrame) -> pd.DataFrame:
    """Count the number of negative flows by year"""
    data = data.assign(negative_flows=data["value"] < 0)

    data = (
        data.groupby(["year", "negative_flows"], dropna=False, observed=True)["value"]
        .count()
        .reset_index()
    )

    data = data.pivot(
        index="year",
        columns="negative_flows",
        values="value",
    ).reset_index()

    return data


def negative_flows_only(data: pd.DataFrame) -> pd.DataFrame:
    """produce a list of countries with negative flows"""

    return data.query("value < 0")


def output_pipeline(constant: bool = False, limit_to_2022: bool = True) -> None:
    df = (
        get_all_flows(constant=constant, limit_to_2022=limit_to_2022)
        .pipe(exclude_outlier_countries)
        .pipe(convert_to_net_flows)
        .pipe(summarise_by_country)
        .pipe(negative_flows_only)
    )

    df_grouped = create_groupings(df).pipe(reorder_countries)

    # Save data
    df.reset_index(drop=True).to_parquet(
        Paths.output / "net_negative_flows_country.parquet"
    )

    df_grouped.reset_index(drop=True).to_parquet(
        Paths.output / "net_negative_flows_group.parquet"
    )


if __name__ == "__main__":
    output_pipeline()
