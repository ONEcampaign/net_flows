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
