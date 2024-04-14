import pandas as pd
from bblocks import add_iso_codes_column

from scripts.analysis.common import update_key_number
from scripts.analysis.population_tools import population_for_countries
from scripts.config import Paths

KEY_NUMBERS = Paths.output / "key_numbers.json"


def net_flows_grouping_per_year(
    grouping: str = "Developing countries",
    prices: str = "current",
    as_billion: bool = True,
) -> pd.DataFrame:
    """Get net flows for all countries per year"""
    df = pd.read_parquet(Paths.output / "net_flows_grouping.parquet")

    df = df.loc[lambda d: d.country == grouping].loc[lambda d: d.prices == prices]

    df = (
        df.groupby(["year", "country"], observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
    )

    if as_billion:
        df["value"] = df["value"] / 1e9

    return df


def net_flows_grouping_projections_per_year(
    grouping: str = "Developing countries", as_billion: bool = True
) -> pd.DataFrame:
    """Get net flows for all countries per year"""
    df = pd.read_parquet(Paths.output / "net_flow_projections_group.parquet")

    df = df.loc[lambda d: d.country == grouping]

    if as_billion:
        df["value"] = df["value"] / 1e9

    return df


def negative_net_flows_counts(year: int) -> dict:

    if year > 2022:
        data = pd.read_parquet(Paths.output / "net_flow_projections_country.parquet")
    else:
        data = (
            pd.read_parquet(Paths.output / "net_flows_country.parquet")
            .query("prices == 'current'")
            .groupby(["year", "country"], observed=True, dropna=False)[["value"]]
            .sum()
            .reset_index()
        )

    # Filter for year
    data = data.loc[lambda d: d.year == year]

    # get full country count
    full_country_count = data["country"].nunique()

    # get negative net flows count
    negative_net_flows_count = data.loc[lambda d: d.value < 0]["country"].nunique()

    return {
        "negative_net_flows_count": negative_net_flows_count,
        "full_country_count": full_country_count,
    }


def highest_flow(df: pd.DataFrame) -> dict:
    """Get the highest flow (year: value)"""
    return (
        df.loc[lambda d: d.value == d.value.max()]
        .round(2)
        .set_index("year")["value"]
        .to_dict()
    )


def latest_flow(df: pd.DataFrame) -> dict:
    """Get the latest flow (year: value)"""
    return (
        df.loc[lambda d: d.year == d.year.max()]
        .round(2)
        .set_index("year")["value"]
        .to_dict()
    )


def change_between_years(
    df: pd.DataFrame, latest_year: int = 2022, previous_year: int = 2021
) -> dict:
    """Get the change between two years"""

    # keep only latest and previous
    df = df.loc[lambda d: d.year.isin([latest_year, previous_year])]

    # Calculate change
    change = df.pivot(index="country", columns="year", values="value")
    change["change"] = round(change[latest_year] - change[previous_year], 3)

    # As percentage
    change["change_percentage"] = round(
        change["change"] / change[previous_year] * 100, 1
    )

    # return as a single dictionary (indicator: value)
    return {
        "change": change["change"].item(),
        "change_percentage": change["change_percentage"].item(),
    }


def income_grouping_country_list(income_level: str) -> list[str]:
    # Get list of income level with data
    data = (
        pd.read_parquet(Paths.output / "full_flows_country.parquet")
        .query(f"income_level == '{income_level}'")
        .pipe(add_iso_codes_column, id_column="country", id_type="regex")["iso_code"]
        .unique()
    )

    return data


# ----------------------------

# Produce key numbers

# ----------------------------


def net_flows_dev_countries_summary():

    # Get the data for all countries per year
    data = net_flows_grouping_per_year(
        grouping="Developing countries", prices="current", as_billion=True
    )

    # Peak year and value
    peak_year, peak_value = highest_flow(data).popitem()

    # Latest year and value
    latest_year, latest_value = latest_flow(data).popitem()

    # Change 2021 to 2022
    change = change_between_years(data, latest_year, latest_year - 1)

    # 2024 projection
    nt_2024 = (
        net_flows_grouping_projections_per_year(
            grouping="Developing countries", as_billion=True
        )
        .query("year == 2024")["value"]
        .round(3)
        .item()
    )

    # Net transfers change from 2022 to 2024
    nt_change = round(nt_2024 - latest_value, 2)

    # numbers
    numbers = {
        "dev_countries_nt_peak_year": int(peak_year),
        "dev_countries_nt_peak_value": f"${peak_value} bn",
        "dev_countries_nt_latest_year": int(latest_year),
        "dev_countries_nt_latest_value": f"${latest_value} bn",
        f"dev_countries_nt_change_{latest_year - 1}_{latest_year}_value": f"${change['change']} bn",
        f"dev_countries_nt_change_{latest_year - 1}_{latest_year}_percentage": (
            f"{change['change_percentage']}%"
        ),
        f"dev_countries_nt_2024": f"${nt_2024} bn",
        f"dev_countries_nt_2022_2024_change": f"${nt_change} bn",
    }

    # update dictionary
    update_key_number(KEY_NUMBERS, numbers)


def upper_middle_income_nt_numbers() -> None:
    # Get the data for upper middle income per year
    data = net_flows_grouping_per_year(
        grouping="Upper middle income", prices="current", as_billion=True
    )

    umics = income_grouping_country_list("Upper middle income")

    # UMIC population. In millions
    umic_population = int(round(population_for_countries(umics) / 1e6, 0))

    numbers = {
        "umic_nt_2021_value": f"${data.query('year == 2021').round(2)['value'].item()} bn",
        "umic_nt_population": f"{umic_population} million",
    }

    update_key_number(KEY_NUMBERS, numbers)


def lower_middle_income_nt_projection_numbers() -> None:
    # Get the data for upper middle income per year

    data = net_flows_grouping_projections_per_year(
        grouping="Lower middle income", as_billion=True
    )

    # 2024 projection
    nt_2024 = data.query("year == 2024")["value"].round(2).item()

    # lmics with data
    lmics = income_grouping_country_list("Lower middle income")

    # lmic population. In billion
    lmic_population = round(population_for_countries(lmics) / 1e9, 1)

    # numbers
    numbers = {
        "lmic_nt_2024_value": f"${nt_2024} bn",
        "lmic_nt_population": f"{lmic_population} billion",
    }

    update_key_number(KEY_NUMBERS, numbers)


def negative_nt_counts_numbers() -> None:
    # Get 2022 negative net flows count
    nnt2022 = negative_net_flows_counts(year=2022)

    # Get 2023 projected negative net flows count
    nnt2023 = negative_net_flows_counts(year=2024)

    # Get 2024 projected negative net flows count
    nnt2024 = negative_net_flows_counts(year=2024)

    # Get 2025 projected negative net flows count
    nnt2025 = negative_net_flows_counts(year=2025)

    # numbers
    numbers = {
        "nnt_count_2022": (
            f"{nnt2022['negative_net_flows_count']} out"
            f" of {nnt2022['full_country_count']} countries"
        ),
        "nnt_count_2023": (
            f"{nnt2023['negative_net_flows_count']} out"
            f" of {nnt2023['full_country_count']} countries"
        ),
        "nnt_count_2024": (
            f"{nnt2024['negative_net_flows_count']} out"
            f" of {nnt2024['full_country_count']} countries"
        ),
        "nnt_count_2025": (
            f"{nnt2025['negative_net_flows_count']} out"
            f" of {nnt2025['full_country_count']} countries"
        ),
    }

    update_key_number(KEY_NUMBERS, numbers)


if __name__ == "__main__":
    net_flows_dev_countries_summary()
    upper_middle_income_nt_numbers()
    lower_middle_income_nt_projection_numbers()
    negative_nt_counts_numbers()
