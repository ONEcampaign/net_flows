import pandas as pd

from scripts.analysis.common import create_groupings, reorder_countries
from scripts.analysis.net_flows import (
    get_all_flows,
    prep_flows,
    rename_indicators,
    exclude_outlier_countries,
)
from scripts.config import Paths
from scripts.data.outflows import get_debt_service_data


def calculate_average_inflows(data: pd.DataFrame, years: int = 2) -> pd.DataFrame:
    """Calculate average inflows over the last n years"""

    data = data.loc[lambda d: d.indicator_type == "inflow"].copy()

    data = (
        data.groupby(["year", "country"], dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Add the rolling sum
    data["rolling_inflow"] = (
        data.fillna({"value": 0})
        .groupby(
            [c for c in data.columns if c not in ["year", "value"]],
            observed=True,
            dropna=False,
            group_keys=False,
            as_index=False,
        )["value"]
        .rolling(
            years, min_periods=1
        )  # min periods are 1 to avoid Nans in sparse data (when very granular)
        .mean()["value"]
    )

    return data.drop(columns="value")


def outflows_projections(constant: bool = False) -> pd.DataFrame:
    outflows = (
        get_debt_service_data(constant=constant)
        .pipe(prep_flows)
        .assign(value=lambda d: -d.value)
        .pipe(rename_indicators, suffix="")
        .groupby(
            ["year", "country", "continent", "income_level"],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )

    return outflows.query("year >= 2023")


def projected_netflows(inflows: pd.DataFrame, outflows: pd.DataFrame) -> pd.DataFrame:
    """project net flows based on average inflows and outflows"""

    data = outflows.merge(inflows, on=["country"], how="left", suffixes=("", "_inflow"))

    data["net_flows"] = data["rolling_inflow"] + data["value"]

    return data


def projections_pipline(constant: bool = False, limit_to_2022: bool = True) -> None:

    # Get full dataset
    data = get_all_flows(limit_to_2022=limit_to_2022, constant=constant).pipe(
        exclude_outlier_countries
    )

    # Calculate average inflows (2 years) and keep only latest
    avg_inflows = calculate_average_inflows(data).query("year == 2022")

    # Get outflows projections
    outflows = outflows_projections(constant=constant).pipe(exclude_outlier_countries)

    # Get projected net flows
    projections = (
        projected_netflows(inflows=avg_inflows, outflows=outflows)
        .drop(columns=["year_inflow", "rolling_inflow", "value"])
        .rename({"net_flows": "value"}, axis=1)
    )

    projections_grouped = create_groupings(projections).pipe(reorder_countries)

    # Save
    projections_grouped.reset_index(drop=True).to_parquet(
        Paths.output / "net_flow_projections_group.parquet"
    )

    projections.reset_index(drop=True).to_parquet(
        Paths.output / "net_flow_projections_country.parquet"
    )


if __name__ == "__main__":
    projections_pipline()
