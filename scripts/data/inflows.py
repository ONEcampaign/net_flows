""" DEBT INFLOWS FROM IDS AND GRANTS INFLOWS FROM ODA DATA"""
import pandas as pd
from bblocks import set_bblocks_data_path, DebtIDS
from oda_data import ODAData, set_data_path

from scripts import config
from scripts.data.common import (
    clean_debtors,
    clean_creditors,
    add_oecd_names,
    remove_counterpart_totals,
    remove_groupings_and_totals_from_recipients,
    remove_non_official_counterparts,
    filter_and_assign_indicator,
)

# set the path for the raw data
set_bblocks_data_path(config.Paths.raw_data)
set_data_path(config.Paths.raw_data)

disbursements_indicators: dict = {
    "total": "DT.DIS.DPPG.CD",  # 'DT.DIS.DPPG.CD'
    "bilateral": ("DT.DIS.BLAT.CD", "DT.DIS.BLTC.CD"),
    "multilateral": ("DT.DIS.MLAT.CD", "DT.DIS.MLTC.CD"),
    "bonds": "DT.DIS.PBND.CD",
    "banks": "DT.DIS.PCBK.CD",
    "other_private": "DT.DIS.PROP.CD",
}


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
        .pipe(clean_debtors, "recipient")
        .pipe(clean_creditors, "donor")
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
    )

    return data


def _get_concessional_non_concessional(
    start_year: int,
    end_year: int,
    total_indicator: str,
    concessional_indicator: str,
    indicator_prefix: str,
) -> pd.DataFrame:
    """
    Get the concessional and non-concessional data for a given range of years,
    using the specified indicators and indicator prefix.

    Args:
        - start_year (int): The start year of the data range.
        - end_year (int): The end year of the data range.
        - total_indicator (str): The indicator for total data.
        - concessional_indicator (str): The indicator for concessional data.
        - indicator_prefix (str): The prefix to use for the indicator columns.

    """
    # Load indicators
    ids = DebtIDS().load_data(
        indicators=[total_indicator, concessional_indicator],
        start_year=start_year,
        end_year=end_year,
    )

    # Get total data and rename column
    total = ids.get_data(total_indicator).rename(
        columns={"value": f"{indicator_prefix}_total"}
    )

    # Get concessional data and rename column
    concessional = ids.get_data(concessional_indicator).rename(
        columns={"value": f"{indicator_prefix}_concessional"}
    )

    # Merge data
    data = pd.merge(
        total,
        concessional,
        on=["year", "country", "counterpart_area"],
        how="left",
        suffixes=("_total", "_concessional"),
    )

    # Calculate non concessional
    data[f"{indicator_prefix}_non_concessional"] = data[
        f"{indicator_prefix}_total"
    ].fillna(0) - data[f"{indicator_prefix}_concessional"].fillna(0)

    # Melt data
    data = data.filter(
        [
            "year",
            "country",
            "counterpart_area",
            f"{indicator_prefix}_concessional",
            f"{indicator_prefix}_non_concessional",
        ]
    ).melt(
        id_vars=["year", "country", "counterpart_area"],
        var_name="indicator",
        value_name="value",
    )

    return data


def get_debt_inflows() -> pd.DataFrame:
    """
    Retrieve debt inflows data to bilateral, multilateral,
    bonds, banks, and other private entities.

    Note: this is disbursements data, not debt stocks or new commitments.
    """
    # get bilateral data, split by concessional and non-concessional
    bilateral = _get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1],
        total_indicator=disbursements_indicators["bilateral"][0],
        concessional_indicator=disbursements_indicators["bilateral"][1],
        indicator_prefix="bilateral",
    )

    # get multilateral data, split by concessional and non-concessional
    multilateral = _get_concessional_non_concessional(
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

    return data


def get_grants_inflows(constant: bool = False) -> pd.DataFrame:
    """
    Retrieve grants inflows from OECD ODA data.

    Args:
        - constant (bool): Whether to retrieve the data in constant or current prices.

    Returns:
        pd.DataFrame: DataFrame containing grants inflows data.
    """

    # Create an object with the basic settiongs
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

    return data


if __name__ == "__main__":
    grants = get_grants_inflows()
    debt_inflows = get_debt_inflows()
