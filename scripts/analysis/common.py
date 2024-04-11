import pandas as pd


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
