import pandas as pd
from bblocks import add_iso_codes_column

from scripts.analysis.common import update_key_number, exclude_outlier_countries
from scripts.analysis.net_flows import prep_flows, rename_indicators
from scripts.analysis.population_tools import population_for_countries
from scripts.config import Paths
from scripts.data.inflows import get_debt_inflows

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


def negative_net_flows_counts_totals(year: int, as_billion: bool = True) -> dict:

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

    if as_billion:
        data["value"] = data["value"] / 1e9

    # Filter for year
    data = data.loc[lambda d: d.year == year]

    # get full country count
    full_country_count = data["country"].nunique()

    # get negative net flows count
    negative_net_flows_count = data.loc[lambda d: d.value < 0]["country"].nunique()

    # Negative net countries
    negative_net_countries = (
        data.loc[lambda d: d.value < 0]
        .copy()
        .pipe(add_iso_codes_column, id_column="country", id_type="regex")["iso_code"]
        .unique()
    )

    # Negative net countries population
    nnt_population = int(
        round(population_for_countries(negative_net_countries) / 1e6, 0)
    )

    # get net flows total
    net_flows_total = data.loc[lambda d: d.value < 0]["value"].sum().round(2)

    return {
        "negative_net_flows_count": negative_net_flows_count,
        "full_country_count": full_country_count,
        "net_flows_total": f"${net_flows_total} bn",
        "nnt_population": f"{nnt_population} million",
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
        (change[previous_year] - change["change"]) / change[previous_year] * 100, 1
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


def loan_inflows() -> pd.DataFrame:
    return (
        pd.read_parquet(Paths.output / "debt_inflows_country.parquet")
        .pipe(prep_flows)
        .pipe(rename_indicators, suffix="")
        .pipe(exclude_outlier_countries)
    )


def private_lending_to(to="all") -> pd.DataFrame:
    data = loan_inflows()

    if to not in ["all", "country", "income_level", "continent"]:
        raise ValueError(
            "to must be either 'all', 'country', 'income_level', or 'continent'"
        )

    if to == "all":
        data = (
            data.groupby(["year", "counterpart_type"], observed=True, dropna=False)[
                "value"
            ]
            .sum()
            .reset_index()
            .assign(country="Developing countries")
        )

    else:
        data = (
            data.groupby(["year", to, "counterpart_type"], observed=True, dropna=False)[
                "value"
            ]
            .sum()
            .reset_index()
            .rename(columns={to: "country"})
        )

    data = data.loc[lambda d: d.counterpart_type == "Private"]

    return data


def china_lending_to(to="all") -> pd.DataFrame:
    data = loan_inflows()

    if to not in ["all", "country", "income_level", "continent"]:
        raise ValueError(
            "to must be either 'all', 'country', 'income_level', or 'continent'"
        )

    if to == "all":
        data = (
            data.groupby(["year", "counterpart_area"], observed=True, dropna=False)[
                "value"
            ]
            .sum()
            .reset_index()
            .assign(country="Developing countries")
        )

    else:
        data = (
            data.groupby(["year", to, "counterpart_area"], observed=True, dropna=False)[
                "value"
            ]
            .sum()
            .reset_index()
            .rename(columns={to: "country"})
        )

    data = data.loc[lambda d: d.counterpart_area == "China"]

    return data


def outflows_historical_and_projections(
    year: int | None, as_billion
) -> pd.DataFrame | float:
    data = pd.read_parquet(Paths.output / "inflows_outflows_projected_country.parquet")

    data = (
        data.groupby(["year"], observed=True, dropna=False)["outflow"]
        .sum()
        .reset_index()
    )

    if as_billion:
        data["outflow"] = data["outflow"] / 1e9

    data = data.rename(columns={"outflow": "value"})

    if year is not None:
        data = data.loc[lambda d: d.year == year].round(2).value.item()

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
    nnt2022 = negative_net_flows_counts_totals(year=2022)

    # Get 2023 projected negative net flows count
    nnt2023 = negative_net_flows_counts_totals(year=2023)

    # Get 2024 projected negative net flows count
    nnt2024 = negative_net_flows_counts_totals(year=2024)

    # Get 2025 projected negative net flows count
    nnt2025 = negative_net_flows_counts_totals(year=2025)

    # 2022 negative net flows population

    # numbers
    numbers = {
        "nnt_count_2022": (
            f"{nnt2022['negative_net_flows_count']} out"
            f" of {nnt2022['full_country_count']} countries"
        ),
        "nnt_countries_total_value_2022": nnt2022["net_flows_total"],
        "nnt_2022_countries_population": nnt2022["nnt_population"],
        "nnt_count_2023": (
            f"{nnt2023['negative_net_flows_count']} out"
            f" of {nnt2023['full_country_count']} countries"
        ),
        "nnt_countries_total_value_2023": nnt2023["net_flows_total"],
        "nnt_count_2024": (
            f"{nnt2024['negative_net_flows_count']} out"
            f" of {nnt2024['full_country_count']} countries"
        ),
        "nnt_countries_total_value_2024": nnt2024["net_flows_total"],
        "nnt_count_2025": (
            f"{nnt2025['negative_net_flows_count']} out"
            f" of {nnt2025['full_country_count']} countries"
        ),
        "nnt_countries_total_value_2025": nnt2025["net_flows_total"],
    }

    update_key_number(KEY_NUMBERS, numbers)


def debt_service_numbers() -> None:

    # 2022 debt service
    ds2022 = outflows_historical_and_projections(year=2022, as_billion=True)
    ds2023 = outflows_historical_and_projections(year=2023, as_billion=True)
    ds2024 = outflows_historical_and_projections(year=2024, as_billion=True)

    # numbers
    numbers = {
        "dev_countries_debt_service_2022": f"${ds2022} bn",
        "dev_countries_debt_service_projected_2023": f"${ds2023} bn",
        "dev_countries_debt_service_projected_2024": f"${ds2024} bn",
    }

    update_key_number(KEY_NUMBERS, numbers)


def china_lending_numbers() -> None:

    lending_to_continents = china_lending_to(to="continent")
    lending_to_income = china_lending_to(to="income_level")

    africa_average_2008_2021 = (
        lending_to_continents.query("country == 'Africa' and year.between(2008,2021)")
        .groupby("country")["value"]
        .mean()
        .div(1e9)
        .round(1)
        .item()
    )

    africa_lending_2022 = (
        lending_to_continents.query("country == 'Africa' and year == 2022")["value"]
        .div(1e9)
        .round(1)
        .item()
    )

    low_income_2008_2021_total_china = (
        lending_to_income.query("country == 'Low income' and year.between(2008,2021)")
        .groupby("country")["value"]
        .sum()
        .div(1e9)
        .round(2)
        .item()
    )

    low_income_total = (
        loan_inflows()
        .query("income_level == 'Low income'")
        .groupby(["year", "income_level"])["value"]
        .sum()
        .div(1e9)
        .round(2)
        .reset_index()
    )

    low_income_2008_2021_total_all = (
        low_income_total.query("year.between(2008,2021)")
        .groupby("income_level")["value"]
        .sum()
        .item()
    )

    low_income_2022_total = (
        low_income_total.query("year == 2022")
        .groupby("income_level")["value"]
        .sum()
        .item()
    )

    low_income_2022_total_china = (
        lending_to_income.query("country == 'Low income' and year==2022")
        .groupby("country")["value"]
        .sum()
        .div(1e9)
        .round(4)
        .item()
    )

    period_share_lic_2008_2021 = round(
        low_income_2008_2021_total_china / low_income_2008_2021_total_all * 100, 1
    )

    period_share_lic_2022 = round(
        low_income_2022_total_china / low_income_2022_total * 100, 1
    )

    numbers = {
        "china_lending_africa_average_2008_2021": f"${africa_average_2008_2021} bn",
        "china_lending_africa_2022": f"${africa_lending_2022} bn",
        "low_income_china_share_2008_2021": f"{period_share_lic_2008_2021}%",
        "low_income_period_share_2022": f"{period_share_lic_2022}%",
        "china_lending_low_income_2022": f"${low_income_2022_total_china} bn",
    }

    update_key_number(KEY_NUMBERS, numbers)


def private_lending_numbers(as_billion=True) -> None:

    # Private to all countries
    private_to_all = private_lending_to(to="all")

    if as_billion:
        private_to_all["value"] = private_to_all["value"] / 1e9

    # Max private lending
    max_year = private_to_all.loc[lambda d: d.value == d.value.max()].year.item()
    max_year_value = (
        private_to_all.loc[lambda d: d.year == max_year].round(1).value.item()
    )

    # 2022 private lending
    private_2022 = private_to_all.query("year == 2022").round(1).value.item()

    ratio_2022_to_max = round(private_2022 / max_year_value, 1)

    numbers = {
        "private_lending_max_year": int(max_year),
        "private_lending_max_year_value": f"${max_year_value} bn",
        "private_lending_2022": f"${private_2022} bn",
        "private_lending_2022_to_max_ratio": ratio_2022_to_max,
    }

    update_key_number(KEY_NUMBERS, numbers)


if __name__ == "__main__":
    net_flows_dev_countries_summary()
    upper_middle_income_nt_numbers()
    lower_middle_income_nt_projection_numbers()
    negative_nt_counts_numbers()
    debt_service_numbers()
    china_lending_numbers()
    private_lending_numbers()
