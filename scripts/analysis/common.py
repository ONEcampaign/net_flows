import json
import os

import pandas as pd


GROUPS = {
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


def create_grouping_totals(
    data: pd.DataFrame, group_column: str, exclude_cols: list[str]
) -> pd.DataFrame:
    """Create group totals as 'country'"""

    dfs = []

    for group in data[group_column].unique():
        df_ = data.loc[lambda d: d[group_column] == group].copy()
        df_["country"] = group
        df_ = (
            df_.groupby(
                [c for c in df_.columns if c not in ["value"] + exclude_cols],
                observed=True,
                dropna=False,
            )["value"]
            .sum()
            .reset_index()
        )

        dfs.append(df_)

    groups = pd.concat(dfs, ignore_index=True)

    return pd.concat([data, groups], ignore_index=True)


def exclude_outlier_countries(data: pd.DataFrame) -> pd.DataFrame:
    data = data.loc[lambda d: ~d.country.isin(["China", "Ukraine", "Russia"])]

    return data


def create_world_total(data: pd.DataFrame, name: str = "World") -> pd.DataFrame:
    """Create a world total for the data"""

    df = data.copy(deep=True)
    df["country"] = name
    df = (
        df.groupby(
            [c for c in df.columns if c not in ["income_level", "continent", "value"]],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )

    return pd.concat([data, df], ignore_index=True)


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


def convert_to_net_flows(data: pd.DataFrame) -> pd.DataFrame:
    """Group the indicator type to get net flows"""

    data = (
        data.groupby(
            [c for c in data.columns if c not in ["value", "indicator_type"]],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )

    data["indicator_type"] = "net_flow"

    return data


def summarise_by_country(data: pd.DataFrame) -> pd.DataFrame:
    """Summarise the data by country"""

    data = (
        data.groupby(
            [
                c
                for c in data.columns
                if c
                not in [
                    "value",
                    "counterpart_area",
                    "counterpart_type",
                    "indicator",
                ]
            ],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )

    return data


def create_groupings(data: pd.DataFrame) -> pd.DataFrame:
    # Create world totals
    data_grouped = create_world_total(data, "Developing countries")

    # Create continent totals
    data_grouped = create_grouping_totals(
        data_grouped, group_column="continent", exclude_cols=["income_level"]
    )

    # Create income_level totals
    data_grouped = create_grouping_totals(
        data_grouped, group_column="income_level", exclude_cols=["continent"]
    )

    # remove individual country data
    data_grouped = data_grouped.loc[lambda d: d.country.isin(GROUPS)]

    return data_grouped


def reorder_countries(df: pd.DataFrame, counterpart_type: bool = False) -> pd.DataFrame:
    """Reorder countries by continent and income level"""

    df["order"] = df["country"].map(GROUPS).fillna(99)

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


def update_key_number(path: str, new_dict: dict) -> None:
    """Update a key number json by updating it with a new dictionary"""

    # Check if the file exists, if not create
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump({}, f)

    with open(path, "r") as f:
        data = json.load(f)

    for k in new_dict.keys():
        data[k] = new_dict[k]

    with open(path, "w") as f:
        json.dump(data, f, indent=4)
