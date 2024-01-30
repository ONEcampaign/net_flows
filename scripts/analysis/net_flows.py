import pandas as pd
from bblocks import set_bblocks_data_path
from bblocks.dataframe_tools.add import add_gdp_column

from scripts.config import Paths
from scripts.data.inflows import get_total_inflows
from scripts.data.outflows import get_debt_service_data

set_bblocks_data_path(Paths.raw_data)


def prep_flows(inflows: pd.DataFrame) -> pd.DataFrame:
    df = (
        inflows.dropna(subset=["iso_code"])
        .loc[lambda d: d.value != 0]
        .loc[lambda d: d.counterpart_area != "World"]
    )

    df = (
        df.groupby(
            [c for c in df.columns if c != "value"], observed=True, dropna=False
        )[["value"]]
        .sum()
        .reset_index()
    )

    return df


def rename_indicators(df: pd.DataFrame, suffix: str = "") -> pd.DataFrame:
    indicators = {
        "grants": f"Grants{suffix}",
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


def get_all_flows(constant: bool = False) -> pd.DataFrame:
    inflows = (
        get_total_inflows(constant=constant)
        .pipe(prep_flows)
        .pipe(rename_indicators, suffix="")
    )

    outflows = (
        get_debt_service_data(constant=constant)
        .pipe(prep_flows)
        .assign(value=lambda d: -d.value)
        .pipe(rename_indicators, suffix="")
    )

    data = (
        pd.concat([inflows, outflows], ignore_index=True)
        .drop(columns=["counterpart_iso_code", "iso_code"])
        .loc[lambda d: d.value != 0]
    )

    return data


def pivot_by_indicator(data: pd.DataFrame) -> pd.DataFrame:
    return data.pivot(
        index=[c for c in data.columns if c not in ["value", "indicator_type"]],
        columns="indicator_type",
        values="value",
    ).reset_index()


def create_scatter_data(data: pd.DataFrame) -> pd.DataFrame:
    scatter = (
        data.groupby(
            [
                "year",
                "country",
                "continent",
                "income_level",
                "prices",
                "indicator_type",
            ],
            dropna=False,
            observed=True,
        )["value"]
        .sum()
        .reset_index()
    )

    scatter.loc[lambda d: d.indicator_type == "outflow", "value"] *= -1
    scatter = scatter.fillna({"income_level": "Not assessed"})

    scatter = pivot_by_indicator(scatter)

    scatter = add_gdp_column(
        scatter,
        id_column="country",
        id_type="regex",
        date_column="year",
        include_estimates=True,
    )

    scatter = scatter.dropna(subset=["gdp", "outflow"], how="any")

    scatter = scatter.assign(
        inflow=lambda d: 100 * d.inflow / d.gdp,
        outflow=lambda d: 100 * d.outflow / d.gdp,
    )

    scatter = scatter.loc[lambda d: d.prices == "current"].drop(
        columns=["prices", "gdp"]
    )

    return scatter


if __name__ == "__main__":
    ...
    df_const = get_all_flows(constant=False)
    df_current = get_all_flows(constant=True)

    data = (
        pd.concat([df_const, df_current], ignore_index=True)
        .groupby(
            [c for c in df_const.columns if c != "value"], observed=True, dropna=False
        )[["value"]]
        .sum()
        .reset_index()
    )

    data.to_csv(Paths.output / "net_flows_full.csv", index=False)

    possibilities = data.filter(
        ["country", "continent", "income_level", "counterpart_area"]
    ).drop_duplicates()

    possibilities.to_csv(Paths.output / "combinations.csv", index=False)

    # Scatter data
    scatter = create_scatter_data(data)
    scatter.to_csv(Paths.output / "scatter_totals.csv", index=False)

    check = (
        data.groupby(
            [
                "year",
                "country",
                "continent",
                "income_level",
                "prices",
                "indicator_type",
            ],
            dropna=False,
            observed=True,
        )["value"]
        .sum()
        .reset_index()
    )

    check.loc[lambda d: d.indicator_type == "outflow", "value"] *= -1
    check = check.fillna({"income_level": "Not assessed"})

    check = pivot_by_indicator(check)

    check = check.loc[lambda d: d.outflow.isna()]

    checkp = check.pivot(
        index=[c for c in check.columns if c not in ["year", "outflow", "inflow"]],
        columns="year",
        values="inflow",
    ).reset_index()
