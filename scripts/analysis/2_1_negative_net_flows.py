import pandas as pd
from bblocks import set_bblocks_data_path
from bblocks.dataframe_tools.add import add_iso_codes_column
from bblocks.import_tools.imf_weo import WEO
import numpy as np

from scripts.config import Paths

set_bblocks_data_path(Paths.raw_data)


def get_parquet(file_name: str) -> pd.DataFrame:
    """
    Loads parquet file from output folder based.

    Args:
        doc (str): Name of parquet file.
    """
    return pd.read_parquet(Paths.output / file_name)


def calculate_net_transfers(df: pd.DataFrame, as_billion: bool = True) -> pd.DataFrame:
    """
    Creates net_flow column, which is the sum of inflows (+ve) and outflows (-ve).
    Removes inflow and outflow columns for output.

    Args:
        df (pd.DataFrame)

    Returns: pd.DataFrame with additional column for 'net_flow' and without inflow and
    outflow columns. net_flow is presented in billions.
    """
    df["net_flow"] = df["inflow"] + df["outflow"]

    # If units required in billions, convert to billions
    if as_billion:
        df["net_flow"] = df["net_flow"] / 1e9

    return df.filter(
        items=["year", "country", "continent", "income_level", "net_flow"], axis=1
    )


def add_projections_data(df: pd.DataFrame, as_billion: bool = True) -> pd.DataFrame:
    """
    Adds projections data from the net_flow_projections_country.parquet file.
    Projections are represented in billions to match actual data.

    Args:
        df (pd.DataFrame)

    Returns: pd.DataFrame with additional rows for 2023-2025 data.
    """
    # Load projections data
    projection = get_parquet(file_name="net_flow_projections_country.parquet").rename(
        {"value": "net_flow"}, axis=1
    )

    # if billions required, put projections into billions
    if as_billion:
        projection["net_flow"] = projection["net_flow"] / 1e9

    # Merge projections data into actual data.
    df_merged = pd.concat([df, projection])

    return df_merged


def calculate_net_flow_as_share_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds new column for net flows as a share of gdp.

    Args:
        df (pd.DataFrame)

    Returns: pd.DataFrame with additional column called 'net_flows_over_gdp_percent'.
    """

    # add gdp data
    df = merge_gdp_data(df)

    # calculate net transfers share of gdp
    df = calculate_share_gdp(df)

    return df


def merge_gdp_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges IMF WEO gdp data (in current prices, US$), scraped using bblocks (see
    download_gdp_data function. Merges data on year and iso_code.

    Args:
        df (pd.DataFrame)

    Returns: pd.DataFrame with additional column for gdp.
    """

    gdp = _download_gdp_data()

    df_merged = pd.merge(df, gdp, on=["year", "iso_3"], how="left")

    df_filled_empty = _add_latest_year_for_missing_gdp_data(df=df_merged, gdp=gdp)

    return df_filled_empty


def _download_gdp_data() -> pd.DataFrame:
    """
    Download GDP data from IMF using bblocks. Takes latest version of IMF WEO database.

    returns: pd.DataFrame containing IMF WEO GDP data by country and year from 1990 to
    2029.
    """

    weo = WEO(version="latest")

    weo = weo.load_data(indicators="NGDPD")

    gdp = weo.get_data()

    return (
        gdp.pipe(
            add_iso_codes_column,
            id_column="ref_area",
            id_type="regex",
            target_column="iso_3",
        )
        .filter(items=["time_period", "obs_value", "iso_3"], axis=1)
        .rename(columns={"time_period": "year", "obs_value": "gdp"})
    )


def _add_latest_year_for_missing_gdp_data(
    df: pd.DataFrame, gdp: pd.DataFrame
) -> pd.DataFrame:
    """
    Fills gaps in GDP data where the year does not currently have data. Fills gaps with
    latest available year.

    Args:
        df (pd.DataFrame) with missing GDP data
        gdp (pd.DataFrame) with all gdp data

    Returns: pd.DataFrame with gdp data for the latest year available, where neccessary.
    """

    # Make sure data is sorted by year
    df = df.sort_values(by=["iso_3", "year"])

    # Forward fill the missing gdp values
    df["gdp"] = df.groupby(
        ["country", "continent", "income_level", "iso_3"], dropna=False
    )["gdp"].ffill()

    return df


def calculate_share_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates net_flow_over_gdp_percent column, which divides net_flow by gdp.

    Args:
        df (pd.DataFrame)

    Returns: pd.DataFrame with additional column for 'net_flow_over_gdp_percent'
    """

    df["net_flows_over_gdp_percent"] = df["net_flow"] * 100 / df["gdp"]

    return df


def add_nnt_column(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    Adds new column nnt which is filled with 'net negative transfers' when net_transfers
    value is below 0.

    Args:
        df (pd.DataFrame)

    Returns: pd.DataFrame with additional column 'nnt'.
    """
    df["nnt"] = np.where(df[target_col] < 0, "net negative transfers", "")
    return df


def filter_for_specified_years(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    """
    Filters the dataframe to keep only the rows for specified years.

    Args:
        df (pd.DataFrame)
        years (list[int]): list of required years for filtered DataFrame.
    """
    return df.loc[lambda d: d.year.isin(years)]


def flourish_1_beeswarm_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline function to produce flourish chart 2.1. Output takes data from
    'full_flows_country.parquet', 'net_flow_projections_country.parquet' and
    'weo_data.csv' files to calculate net_flows as a share of gdp for 2022 and 2025.

    Args:
        df (pd.DataFrame): DataFrame containing the 'full_flows_country.parquet' data

    Returns: pd.DataFrame of 2022 and 2025 data ready for export as CSV for Flourish.
    """

    # filter for current prices
    df = df.loc[lambda d: d.prices == "current"]

    # Groupby country
    df = (
        df.groupby(
            by=["year", "country", "continent", "income_level", "indicator_type"]
        )["value"]
        .sum()
        .reset_index()
    )

    # Pivot to have inflow and outflow columns
    df = df.pivot_table(
        index=["year", "country", "continent", "income_level"],
        columns="indicator_type",
        values="value",
        fill_value=0,
    ).reset_index()

    # Calculate net transfers
    df = calculate_net_transfers(df)

    # add projections data
    df = add_projections_data(df, as_billion=True)

    # add iso columns (needed to merge in GDP data in next step)
    df = add_iso_codes_column(
        df=df, id_column="country", id_type="regex", target_column="iso_3"
    )

    # Add new column for net flows as a share of GDP (using WEO GDP data)
    df = calculate_net_flow_as_share_gdp(df)

    # Add negative net transfers column (states 'nnt' when outflow > inflow)
    df = add_nnt_column(df, target_col="net_flow")

    # filter for specified years
    df = filter_for_specified_years(df=df, years=[2022, 2025])

    return df


if __name__ == "__main__":

    data = get_parquet(file_name="full_flows_country.parquet")

    flourish_chart_1 = flourish_1_beeswarm_pipeline(df=data)
    flourish_chart_1.to_csv(Paths.output / "chart_2_1.csv", index=False)
