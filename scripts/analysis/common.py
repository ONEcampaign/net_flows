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
    df = df.groupby(
        [c for c in df.columns if c not in ["income_level", "continent"]],
        observed=True,
        dropna=False,
        as_index=False,
    )["value"].sum()

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
