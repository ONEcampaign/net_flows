""" DEBT INFLOWS FROM IDS AND GRANTS INFLOWS FROM ODA DATA"""
import pandas as pd
from bblocks import set_bblocks_data_path, DebtIDS, add_income_level_column
from oda_data import ODAData, set_data_path
from pydeflate import deflate, set_pydeflate_path

from scripts import config
from scripts.data.common import (
    clean_debtors,
    clean_creditors,
    add_oecd_names,
    remove_counterpart_totals,
    remove_groupings_and_totals_from_recipients,
    remove_non_official_counterparts,
    filter_and_assign_indicator,
    get_concessional_non_concessional,
)

# set the path for the raw data
set_bblocks_data_path(config.Paths.raw_data)
set_data_path(config.Paths.raw_data)
set_pydeflate_path(config.Paths.raw_data)

# this dictionary contains the IDS codes. When a tuple, it's the total and concessional
disbursements_indicators: dict = {
    "total": "DT.DIS.DPPG.CD",
    "bilateral": ("DT.DIS.BLAT.CD", "DT.DIS.BLTC.CD"),
    "multilateral": ("DT.DIS.MLAT.CD", "DT.DIS.MLTC.CD"),
    "bonds": "DT.DIS.PBND.CD",
    "banks": "DT.DIS.PCBK.CD",
    "other_private": "DT.DIS.PROP.CD",
}


def to_constant_prices(data: pd.DataFrame, base_year: int) -> pd.DataFrame:
    """
    This method takes in a pandas DataFrame 'data' and an integer 'base_year' as input parameters.
    It returns a new pandas DataFrame with constant prices.

    Parameters:
    - data (pd.DataFrame): The  DataFrame containing the data to be converted to constant prices.
    - base_year (int): The base year against which the prices will be deflated.

    Returns:
    - pd.DataFrame: A new DataFrame with constant prices.

    """

    # Pass the data to the deflate function and assign a prices column
    data = deflate(
        df=data,
        base_year=base_year,
        deflator_source=config.PRICES_SOURCE,
        deflator_method="gdp",
        exchange_source=config.PRICES_SOURCE,
        date_column="year",
    ).assign(prices="constant")

    return data


def clean_debt_output(data: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the output data frame by replacing bad characters and
    cleaning debtors and creditors.

    Args:
        data (pd.DataFrame): The input data frame containing the data to be cleaned.


    """
    # replace bad characters
    data["counterpart_area"] = data["counterpart_area"].str.replace(" ", "")

    # clean debtors
    data = clean_debtors(data, "country")

    # clean creditors
    data = clean_creditors(data, "counterpart_area")

    # Convert the year to an integer
    data.year = data.year.dt.year

    # add income level
    data = add_income_level_column(data, id_column="iso_code", id_type="ISO3")

    # drop missing values and values which are zero
    data = data.dropna(subset=["value"]).loc[lambda d: d.value != 0]

    return data


def clean_grants_inflows_output(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Grants Inflows Output

    Cleans the given input DataFrame by performing the following operations:
        - Adds OECD names to the data.
        - Removes non-official counterparts from the data.
        - Removes groupings and totals from the recipients.
        - Cleans debtor values for recipients.
        - Cleans creditor values for donors.
        - Filters the columns to only include "year", "iso_code", "recipient",
          "continent", "donor", "counterpart_iso_code", "prices", and "value".
        - Renames the columns "donor" to "counterpart_area" and "recipient" to "country".
        - Removes counterpart totals from the data.

    Args:
       - data : pd.DataFrame
    """

    # Pipeline
    data = (
        data.pipe(add_oecd_names)
        .pipe(remove_non_official_counterparts)
        .pipe(remove_groupings_and_totals_from_recipients)
        .pipe(clean_debtors, column="recipient")
        .pipe(clean_creditors, column="donor")
        .filter(
            [
                "year",
                "iso_code",
                "recipient",
                "continent",
                "donor",
                "counterpart_iso_code",
                "prices",
                "value",
            ]
        )
        .rename(columns={"donor": "counterpart_area", "recipient": "country"})
        .pipe(remove_counterpart_totals)
        .assign(value=lambda d: d.value * 1e6)  # to units
        .pipe(add_income_level_column, id_column="iso_code", id_type="ISO3")
    )

    return data


def get_debt_inflows(constant: bool = False) -> pd.DataFrame:
    """
    Retrieve debt inflows data to bilateral, multilateral,
    bonds, banks, and other private entities.

    Note: this is disbursements data, not debt stocks or new commitments.
    """
    # get bilateral data, split by concessional and non-concessional
    bilateral = get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1],
        total_indicator=disbursements_indicators["bilateral"][0],
        concessional_indicator=disbursements_indicators["bilateral"][1],
        indicator_prefix="bilateral",
    )

    # get multilateral data, split by concessional and non-concessional
    multilateral = get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1],
        total_indicator=disbursements_indicators["multilateral"][0],
        concessional_indicator=disbursements_indicators["multilateral"][1],
        indicator_prefix="multilateral",
    )

    # Load bonds, banks, and other private
    ids = DebtIDS().load_data(
        indicators=[
            disbursements_indicators["bonds"],
            disbursements_indicators["banks"],
            disbursements_indicators["other_private"],
        ],
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1],
    )

    # Get bonds data
    bonds = ids.get_data(disbursements_indicators["bonds"]).pipe(
        filter_and_assign_indicator, "bonds"
    )

    # Get banks data
    banks = ids.get_data(disbursements_indicators["banks"]).pipe(
        filter_and_assign_indicator, "banks"
    )

    # Get other private data
    other_private = ids.get_data(disbursements_indicators["other_private"]).pipe(
        filter_and_assign_indicator, "other_private"
    )

    # combine
    data = pd.concat(
        [bilateral, multilateral, bonds, banks, other_private], ignore_index=True
    ).pipe(clean_debt_output)

    if constant:
        data = to_constant_prices(data, config.CONSTANT_BASE_YEAR)
    else:
        data = data.assign(prices="current")

    return data


def get_grants_inflows(constant: bool = False) -> pd.DataFrame:
    """
    Retrieve grants inflows from OECD ODA data.

    Args:
        - constant (bool): Whether to retrieve the data in constant or current prices.

    Returns:
        pd.DataFrame: DataFrame containing grants inflows data.
    """

    # Create an object with the basic settings
    oda = ODAData(
        years=range(config.ANALYSIS_YEARS[0], config.ANALYSIS_YEARS[1] + 1),
        include_names=False,
        base_year=config.CONSTANT_BASE_YEAR if constant else None,
        prices="constant" if constant else "current",
    )

    # Load the data
    oda.load_indicator("recipient_grants_flow")

    # Retrieve and clean the data
    data = oda.get_data().pipe(clean_grants_inflows_output)

    # assign indicator
    data = data.assign(indicator="grants")

    return data


def get_total_inflows(constant: bool = False) -> pd.DataFrame:
    """
    Get total inflows data.

    This method calculates the total inflows by combining the grants inflows and
    debt inflows data. The resulting data will have an additional column indicating
    the type of indicator as "inflow".

    Parameters:
        constant (bool, optional): Flag to convert to constant values. Default is False.

    Returns:
        pd.DataFrame: Combined inflows data with an additional column indicating the indicator type.

    """
    # Get grants data
    grants = get_grants_inflows(constant)

    # Get debt data
    debt = get_debt_inflows(constant)

    # Combine the data and assign indicator type
    data = pd.concat([grants, debt], ignore_index=True).assign(indicator_type="inflow")

    return data


if __name__ == "__main__":
    net_flows = get_total_inflows(constant=False)
