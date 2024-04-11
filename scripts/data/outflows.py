"""DEBT SERVICE OUTFLOWS FROM IDS"""

import pandas as pd
from bblocks import set_bblocks_data_path, DebtIDS

from scripts import config
from scripts.data.common import (
    filter_and_assign_indicator,
    get_concessional_non_concessional,
)
from scripts.data.inflows import clean_debt_output, to_constant_prices

# set the path for the raw data
set_bblocks_data_path(config.Paths.raw_data)

outflow_indicators: dict = {
    "total_amt": "DT.AMT.DPPG.CD",
    "total_int": "DT.INT.DPPG.CD",
    "bilateral_amt": ("DT.AMT.BLAT.CD", "DT.AMT.BLTC.CD"),
    "bilateral_int": ("DT.INT.BLAT.CD", "DT.INT.BLTC.CD"),
    "multilateral_amt": ("DT.AMT.MLAT.CD", "DT.AMT.MLTC.CD"),
    "multilateral_int": ("DT.INT.MLAT.CD", "DT.INT.MLTC.CD"),
    "bonds_amt": "DT.AMT.PBND.CD",
    "bonds_int": "DT.INT.PBND.CD",
    "banks_amt": "DT.AMT.PCBK.CD",
    "banks_int": "DT.INT.PCBK.CD",
    "other_private_amt": "DT.AMT.PROP.CD",
    "other_private_int": "DT.INT.PROP.CD",
}


def get_debt_service_data(constant: bool = False) -> pd.DataFrame:
    """
    Retrieve debt service data to bilateral, multilateral,
    bonds, banks, and other private entities.

    Note: debt service combines principal and interest payments.

    Returns:
        pd.DataFrame: DataFrame containing debt service data.

    """
    # get bilateral amt data, split by concessional and non-concessional
    bilateral_amt = get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1] + 3,
        total_indicator=outflow_indicators["bilateral_amt"][0],
        concessional_indicator=outflow_indicators["bilateral_amt"][1],
        indicator_prefix="bilateral",
    )
    # get bilateral int data, split by concessional and non-concessional
    bilateral_int = get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1] + 3,
        total_indicator=outflow_indicators["bilateral_int"][0],
        concessional_indicator=outflow_indicators["bilateral_int"][1],
        indicator_prefix="bilateral",
    )

    # get multilateral amt data, split by concessional and non-concessional
    multilateral_amt = get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1] + 3,
        total_indicator=outflow_indicators["multilateral_amt"][0],
        concessional_indicator=outflow_indicators["multilateral_amt"][1],
        indicator_prefix="multilateral",
    )
    # get multilateral int data, split by concessional and non-concessional
    multilateral_int = get_concessional_non_concessional(
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1] + 3,
        total_indicator=outflow_indicators["multilateral_int"][0],
        concessional_indicator=outflow_indicators["multilateral_int"][1],
        indicator_prefix="multilateral",
    )

    # Load bonds, banks, and other private
    ids = DebtIDS().load_data(
        indicators=[
            outflow_indicators["bonds_amt"],
            outflow_indicators["banks_amt"],
            outflow_indicators["other_private_amt"],
            outflow_indicators["bonds_int"],
            outflow_indicators["banks_int"],
            outflow_indicators["other_private_int"],
        ],
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1] + 3,
    )

    # Get bonds data
    bonds = ids.get_data(
        [outflow_indicators["bonds_amt"], outflow_indicators["bonds_int"]]
    ).pipe(filter_and_assign_indicator, "bonds")

    # Get banks data
    banks = ids.get_data(
        [outflow_indicators["banks_amt"], outflow_indicators["banks_int"]]
    ).pipe(filter_and_assign_indicator, "banks")

    # Get other private data
    other_private = ids.get_data(
        [
            outflow_indicators["other_private_amt"],
            outflow_indicators["other_private_int"],
        ]
    ).pipe(filter_and_assign_indicator, "other_private")

    # combine
    data = (
        pd.concat(
            [
                bilateral_amt,
                bilateral_int,
                multilateral_amt,
                multilateral_int,
                bonds,
                banks,
                other_private,
            ],
            ignore_index=True,
        )
        .pipe(clean_debt_output)
        .assign(indicator_type="outflow")
    )

    if constant:
        data = to_constant_prices(data, config.CONSTANT_BASE_YEAR)
    else:
        data = data.assign(prices="current")

    return data


if __name__ == "__main__":
    debt_service = get_debt_service_data()
