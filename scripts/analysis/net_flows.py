import pandas as pd
from bblocks import set_bblocks_data_path
from bblocks.dataframe_tools.add import add_gdp_column

from scripts.analysis.common import (
    create_grouping_totals,
    exclude_outlier_countries,
    create_world_total,
    GROUPS,
)
from scripts.config import Paths
from scripts.data.inflows import get_total_inflows
from scripts.data.outflows import get_debt_service_data

set_bblocks_data_path(Paths.raw_data)


def prep_flows(inflows: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the inflow data for further processing.

    This function drops rows with NaN in 'iso_code', zero in 'value', or 'World' in
    'counterpart_area'. Then, it groups the DataFrame by all columns except
    'value', and sums up 'value' within each group.

    Args:
        inflows (pd.DataFrame): The input DataFrame containing inflow data. It is expected to
         have columns including 'iso_code', 'value', and 'counterpart_area'.

    Returns:
        pd.DataFrame: The processed DataFrame.

    """
    # Drop rows with NaN in 'iso_code'
    df = inflows.dropna(subset=["iso_code"])

    # Further drop rows with zero 'value' or 'World' in 'counterpart_area'
    df = df.loc[lambda d: d.value != 0].loc[lambda d: d.counterpart_area != "World"]

    # Group by all columns except 'value', and sum up 'value' within each group
    df = (
        df.groupby(
            [c for c in df.columns if c != "value"], observed=True, dropna=False
        )[["value"]]
        .sum()
        .reset_index()
    )

    return df


def rename_indicators(df: pd.DataFrame, suffix: str = "") -> pd.DataFrame:
    """
    Rename the indicators in the DataFrame.

    Maps the original indicator names to new ones based on a predefined dictionary.
    The new names are constructed by appending a suffix to the base name of each indicator.
    If an indicator does not exist in the dictionary, its original name is preserved.

    Args:
        df (pd.DataFrame): The input DataFrame with an 'indicator' column that needs to be renamed.
        suffix (str, optional): The suffix to append to the base name of each indicator.
        Defaults to an empty string.

    Returns:
        pd.DataFrame: The DataFrame with renamed indicators.
    """

    indicators = {
        "grants": f"Grants{suffix}",
        "grants_bilateral": f"Bilateral Grants{suffix}",
        "grants_multilateral": f"Multilateral Grants{suffix}",
        "bilateral": f"All bilateral{suffix}",
        "bilateral_non_concessional": f"Bilateral Non-Concessional Debt{suffix}",
        "bilateral_concessional": f"Bilateral Concessional Debt{suffix}",
        "multilateral_non_concessional": f"Multilateral Non-Concessional Debt{suffix}",
        "multilateral_concessional": f"Multilateral Concessional Debt{suffix}",
        "multilateral": f"All multilateral{suffix}",
        "bonds": f"Private - bonds{suffix}",
        "banks": f"Private  - banks{suffix}",
        "other_private": f"Private - other{suffix}",
        "other": f"Private - other{suffix}",
    }
    return df.assign(
        indicator=lambda d: d.indicator.map(indicators).fillna(d.indicator)
    )


def get_all_flows(constant: bool = False, limit_to_2022: bool = True) -> pd.DataFrame:
    """
    Retrieve all inflow and outflow data, process them, and combine into a single DataFrame.

    Args:
        constant (bool, optional): A flag to indicate whether to retrieve constant inflow
        and debt service data. Defaults to False.

    Returns:
        pd.DataFrame: The combined DataFrame of processed inflow and outflow data.
    """

    # Get inflow and outflow data
    inflows = (
        get_total_inflows(constant=constant)
        .pipe(prep_flows)
        .pipe(rename_indicators, suffix="")
    )

    # Get outflow data. NOTE: the value of outflow is negative
    outflows = (
        get_debt_service_data(constant=constant)
        .pipe(prep_flows)
        .assign(value=lambda d: -d.value)
        .pipe(rename_indicators, suffix="")
    )

    # Combine inflow and outflow data
    data = (
        pd.concat([inflows, outflows], ignore_index=True)
        .drop(columns=["counterpart_iso_code", "iso_code"])
        .loc[lambda d: d.value != 0]
    )
    if limit_to_2022:
        data = data.loc[lambda d: d.year <= 2022]

    return data


def pivot_by_indicator(data: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the DataFrame based on the 'indicator_type' column.

    This function pivots the DataFrame such that each unique value
    in the 'indicator_type' column becomes a new column.

    Args:
        data (pd.DataFrame): The input DataFrame to be pivoted.
        It is expected to have columns including 'value' and 'indicator_type'.

    Returns:
        pd.DataFrame: The pivoted DataFrame.
    """

    return data.pivot(
        index=[c for c in data.columns if c not in ["value", "indicator_type"]],
        columns="indicator_type",
        values="value",
    ).reset_index()


def flip_outflow_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flip the sign of the outflow values in the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame with an 'outflow' column.

    Returns:
        pd.DataFrame: The DataFrame with the sign of the 'outflow' column flipped.
    """
    df.loc[lambda d: d.indicator_type == "outflow", "value"] *= -1
    return df


def calculate_flows_as_percent_of_gdp(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the inflow and outflow as a percentage of GDP.

    Args:
        data (pd.DataFrame): The input DataFrame with 'inflow' and 'outflow' columns.

    Returns:
        pd.DataFrame: The DataFrame with 'inflow' and 'outflow' columns converted to
        percentages of GDP.
    """

    # Use bblokcs to add GDP data
    data = add_gdp_column(
        data,
        id_column="country",
        id_type="regex",
        date_column="year",
        include_estimates=True,
    )

    # Drop rows with NaN in 'gdp' or 'outflow'
    data = data.dropna(subset=["gdp", "outflow"], how="any")

    # Calculate inflow and outflow as a percentage of GDP
    data = data.assign(
        inflow=lambda d: 100 * d.inflow / d.gdp,
        outflow=lambda d: 100 * d.outflow / d.gdp,
    ).drop(columns=["gdp"])

    return data


def create_scatter_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Create a dataset to visualise as a scatter plot. This function aggregates the
    inflow and outflow data by year, country, continent, income level, and indicator.
    The value columns become 'inflows' and 'outflows', as a share of GDP.

    Args:
        data (pd.DataFrame): The input DataFrame with the inflow and outflow data.

    Returns:
        pd.DataFrame: The aggregated and pivoted DataFrame.

    """

    # Aggregate the data by year, country, continent, income level, and indicator
    df = (
        data.loc[lambda d: d.prices == "current"]
        .groupby(
            [
                "year",
                "country",
                "continent",
                "income_level",
                "indicator_type",
                "counterpart_type",
            ],
            dropna=False,
            observed=True,
        )["value"]
        .sum()
        .reset_index()
        .fillna({"income_level": "Not assessed"})
    )

    # Flip the sign of the outflow values
    df = flip_outflow_values(df)

    # Pivot the DataFrame to have the indicators as columns
    df = pivot_by_indicator(df)

    # Calculate inflow and outflow as a percentage of GDP
    df = calculate_flows_as_percent_of_gdp(data=df)

    # Save the data
    df.to_csv(Paths.output / "scatter_totals.csv", index=False)

    return df


def all_flows_pipeline(exclude_countries: bool = True) -> pd.DataFrame:
    """Create a dataset with all flows for visualisation. It is saved as a CSV in the
    output folder. It includes both constant and current prices.

    The data is also returned as a DataFrame.

    """

    # get constant and current data
    df_const = get_all_flows(constant=False, limit_to_2022=True)
    df_current = get_all_flows(constant=True, limit_to_2022=True)

    # Combine and make sure it is grouped at the right level
    data = (
        pd.concat([df_const, df_current], ignore_index=True)
        .groupby(
            [c for c in df_const.columns if c != "value"], observed=True, dropna=False
        )[["value"]]
        .sum()
        .reset_index()
    )

    if exclude_countries:
        data = exclude_outlier_countries(data)

    # Create world totals
    data_grouped = create_world_total(data)

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

    # Save as parquet
    data.reset_index(drop=True).to_parquet(Paths.output / "full_flows_country.parquet")

    # Saved grouped data
    data_grouped.reset_index(drop=True).to_parquet(
        Paths.output / "full_flows_grouping.parquet"
    )

    return data


if __name__ == "__main__":
    full_data = all_flows_pipeline()
    scatter = create_scatter_data(full_data)
