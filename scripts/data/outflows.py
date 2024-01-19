"""DEBT SERVICE OUTFLOWS FROM IDS"""
import pandas as pd
from bblocks import set_bblocks_data_path, DebtIDS
from scripts import config
from scripts.data.common import filter_and_assign_indicator
from scripts.data.inflows import clean_debt_output

# set the path for the raw data
set_bblocks_data_path(config.Paths.raw_data)


def get_debt_service_data() -> pd.DataFrame:
    """
    Retrieve debt service data to bilateral, multilateral,
    bonds, banks, and other private entities.

    Note: debt service combines principal and interest payments.

    Returns:
        pd.DataFrame: DataFrame containing debt service data.

    """
    # get bonds, banks, and other private
    ids = DebtIDS()

    # debt service indicators
    ds_indicators = ids.debt_service_indicators()
    ids.load_data(
        indicators=list(ds_indicators),
        start_year=config.ANALYSIS_YEARS[0],
        end_year=config.ANALYSIS_YEARS[1],
    )

    # Get bilateral data
    bilateral = ids.get_data(["DT.AMT.BLAT.CD", "DT.INT.BLAT.CD"]).pipe(
        filter_and_assign_indicator, "bilateral"
    )

    # Get multilateral data
    multilateral = ids.get_data(["DT.AMT.MLAT.CD", "DT.INT.MLAT.CD"]).pipe(
        filter_and_assign_indicator, "multilateral"
    )

    # Get bonds data
    bonds = ids.get_data(["DT.AMT.PBND.CD", "DT.INT.PBND.CD"]).pipe(
        filter_and_assign_indicator, "bonds"
    )

    # Get banks data
    banks = ids.get_data(["DT.AMT.PCBK.CD", "DT.INT.PCBK.CD"]).pipe(
        filter_and_assign_indicator, "banks"
    )

    # Get other private data
    other_private = ids.get_data(["DT.AMT.PROP.CD", "DT.INT.PROP.CD"]).pipe(
        filter_and_assign_indicator, "other"
    )

    # combine
    data = pd.concat(
        [bilateral, multilateral, bonds, banks, other_private], ignore_index=True
    ).pipe(clean_debt_output)

    return data


if __name__ == "__main__":
    debt_service = get_debt_service_data()
