"""Debt service analysis to show historical trends and projections"""

import pandas as pd

from scripts.analysis.common import (
    exclude_outlier_countries,
    create_grouping_totals,
    create_world_total,
)
from scripts.config import Paths
from scripts.data.outflows import get_debt_service_data


def remove_world(df: pd.DataFrame) -> pd.DataFrame:
    """Remove 'World' (totals) from counterpart area data"""
    return df.loc[lambda d: d.counterpart_area != "World"]


def add_china_as_counterpart_type(df: pd.DataFrame) -> pd.DataFrame:
    """Adds China as counterpart type"""

    # Get china as counterpart
    china = df.loc[lambda d: d.counterpart_area == "China"].copy()

    # Add counterpart type, by type
    china["counterpart_type"] = "China"

    # Remove China from the original data
    df = df.loc[lambda d: d.counterpart_area != "China"]

    # Concatenate the data
    return pd.concat([df, china], ignore_index=True)


def groupby_counterpart_type(df: pd.DataFrame) -> pd.DataFrame:
    """Group data by counterpart type"""
    return (
        df.groupby(
            ["year", "country", "continent", "income_level", "counterpart_type"],
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
    )


def add_africa_total(df: pd.DataFrame) -> pd.DataFrame:
    """Add a total for Africa"""
    africa = df.loc[lambda d: d.continent == "Africa"].copy()
    africa["country"] = "Africa"
    africa = (
        africa.groupby(
            [c for c in africa.columns if c not in ["value", "income_level"]],
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
    )
    return pd.concat([df, africa], ignore_index=True)


def reorder_countries(df: pd.DataFrame, counterpart_type: bool = False) -> pd.DataFrame:
    """Reorder countries by continent and income level"""
    top = {
        "Developing countries": 1,
        "Low income": 2,
        "Lower middle income": 3,
        "Upper middle income": 4,
        "Africa": 7,
        "Europe": 8,
        "Asia": 9,
        "America": 10,
        "Oceania": 11,
    }

    df["order"] = df["country"].map(top).fillna(99)

    counterpart_order = {
        "Bilateral": 1,
        "Multilateral": 2,
        "Private": 3,
        "China": 4,
    }

    if counterpart_type:
        df["order_counterpart"] = (
            df["counterpart_type"].map(counterpart_order).fillna(99)
        )

    df = (
        df.sort_values(
            ["order", "country", "year", "order_counterpart"]
            if counterpart_type
            else ["order", "country", "year"]
        )
        .drop(columns=["order", "order_counterpart"] if counterpart_type else ["order"])
        .reset_index(drop=True)
    )

    return df


def pivot_flourish_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot the DataFrame to have the counterpart_type as columns"""
    return df.pivot(
        index=[c for c in df.columns if c not in ["value", "counterpart_type"]],
        columns="counterpart_type",
        values="value",
    ).reset_index(drop=False)


def calculate_mean(df: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    df = df.query(f"year >= {start} & year <= {end}").assign(year=f"{start}-{end}")

    return (
        df.groupby(
            [c for c in df.columns if c not in ["value"]],
            dropna=False,
            observed=True,
        )["value"]
        .mean()
        .reset_index()
    )


def group_by_avg_payments(df: pd.DataFrame, groups: list[tuple[int]]) -> pd.DataFrame:
    """Group in groups of 3 years"""

    dfs = []

    for start, end in groups:
        dfs.append(df.pipe(calculate_mean, start=start, end=end))

    return pd.concat(dfs, ignore_index=True)


def remove_default_groupings(data: pd.DataFrame) -> pd.DataFrame:
    """Remove default groupings"""

    default_groupings = [
        "Low & middle income",
        "Low income",
        "Lower middle income",
        "Upper middle income",
        "Middle income",
        "Sub-Saharan Africa (excluding high income)",
        "Africa",
        "Least developed countries: UN classification",
        "Middle East & North Africa (excluding high income)",
        "Latin America & Caribbean (excluding high income)",
        "Europe & Central Asia (excluding high income)",
        "East Asia & Pacific (excluding high income)",
        "IDA only",
        "IDA total",
    ]

    return data.loc[lambda d: ~d.country.isin(default_groupings)]


def avg_repayments_charts() -> None:
    """Export data for average repayment charts for flourish"""

    data = (
        get_debt_service_data(constant=False)
        .pipe(exclude_outlier_countries)
        .pipe(remove_default_groupings)
        .pipe(create_world_total, "Developing countries")
        .pipe(
            create_grouping_totals,
            group_column="continent",
            exclude_cols=["income_level"],
        )
        .pipe(
            create_grouping_totals,
            group_column="income_level",
            exclude_cols=["continent"],
        )
        .pipe(remove_world)
        .pipe(groupby_counterpart_type)
        .pipe(group_by_avg_payments, [(2010, 2014), (2018, 2022), (2023, 2025)])
        .pipe(add_africa_total)
        .pipe(reorder_countries, True)
        .drop(columns=["income_level", "continent"])
        .replace({"year": {"2023-2025": "2023-2025 (projected)"}})
    )

    data_china = (
        get_debt_service_data(constant=False)
        .pipe(exclude_outlier_countries)
        .pipe(remove_world)
        .pipe(add_china_as_counterpart_type)
        .pipe(remove_default_groupings)
        .pipe(create_world_total, "Developing countries")
        .pipe(
            create_grouping_totals,
            group_column="continent",
            exclude_cols=["income_level"],
        )
        .pipe(
            create_grouping_totals,
            group_column="income_level",
            exclude_cols=["continent"],
        )
        .pipe(groupby_counterpart_type)
        .pipe(group_by_avg_payments, [(2010, 2014), (2018, 2022), (2023, 2025)])
        .pipe(add_africa_total)
        .pipe(reorder_countries, True)
        .drop(columns=["income_level", "continent"])
        .replace({"year": {"2023-2025": "2023-2025 (projected)"}})
    )

    data.to_csv(Paths.output / "avg_repayments.csv", index=False)
    data_china.to_csv(Paths.output / "avg_repayments_china.csv", index=False)


if __name__ == "__main__":
    avg_repayments_charts()
