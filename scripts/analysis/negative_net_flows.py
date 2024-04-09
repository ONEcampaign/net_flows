import pandas as pd

from scripts.analysis.net_flows import get_all_flows
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


def convert_to_net_flows(data: pd.DataFrame) -> pd.DataFrame:
    """Group data summing the inflows and outflows to calculate the net flows"""
    data = (
        data.groupby(
            [c for c in data.columns if c not in ["value", "indicator_type"]],
            observed=True,
            dropna=False,
        )[["value"]]
        .sum()
        .reset_index()
    )

    return data


def summarise_by_year_debtor(data: pd.DataFrame) -> pd.DataFrame:
    """Summarise data by year and debtor country"""

    data = (
        data.groupby(
            ["year", "country", "income_level", "continent"],
            dropna=False,
            observed=True,
        )["value"]
        .sum()
        .reset_index()
    )

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


def negative_flows_list(data: pd.DataFrame, latest_only: bool = True) -> pd.DataFrame:
    """produce a list of countries with negative flows"""

    data = data.query("value < 0")

    if latest_only:
        data = data.query("year == year.max()")

    data = data.drop_duplicates(subset=["year", "country"]).reset_index(drop=True)

    return data.sort_values(
        ["year", "income_level", "continent", "value"],
        ascending=[False, True, True, True],
    )


def calculate_close_negative(threshold_gdp: float = 0.5) -> pd.DataFrame:
    data = pd.read_csv(Paths.output / "scatter_totals.csv")

    data = (
        data.groupby(
            ["year", "country", "continent", "income_level"],
            dropna=False,
            observed=True,
        )[["inflow", "outflow"]]
        .sum()
        .reset_index()
    )

    data["net"] = data["inflow"] - data["outflow"]

    # keep countries that are above zero but less than 0.5
    data = data.query(f"net > 0 and net < {threshold_gdp}")

    return data


if __name__ == "__main__":
    # df = get_all_flows()
    # dfp = check_inflows_and_outflows_present(df).query("year == 2022")
    # net = convert_to_net_flows(df).pipe(summarise_by_year_debtor)
    # negative_count = count_negative_flows_by_year(net)
    # negative_list = negative_flows_list(net)

    df = calculate_close_negative().query("year == 2022")
