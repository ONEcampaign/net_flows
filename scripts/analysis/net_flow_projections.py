import pandas as pd

from scripts.analysis.net_flows import (
    get_all_flows,
    prep_flows,
    rename_indicators,
    exclude_outlier_countries,
)
from scripts.analysis.population_tools import add_population_under18
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


def projected_negative_list(net_negative_df: pd.DataFrame) -> pd.DataFrame:
    """List of countries with negative net flows"""

    data = net_negative_df  # .query("net_flows < 0")

    return data.filter(["year", "country", "continent", "income_level", "net_flows"])


def projections_pipline() -> pd.DataFrame: ...


if __name__ == "__main__":
    df = get_all_flows().pipe(exclude_outlier_countries)
    inflows_df = calculate_average_inflows(df).query("year == 2022")
    outflows_df = outflows_projections()

    net_flows = projected_netflows(inflows_df, outflows_df)
    net_negative = (
        projected_negative_list(net_flows)
        .pipe(add_population_under18, country_col="country")
        .rename(columns={"population": "population_under_18"})
    )

    count = net_negative.groupby(["year"])[["country"]].count().reset_index()

    nn23 = net_negative.query("year == 2023")
    nn24 = net_negative.query("year == 2024")
    nn25 = net_negative.query("year == 2025")

    nn24.to_clipboard(index=False)
