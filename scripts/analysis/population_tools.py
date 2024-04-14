import json
import time

import pandas as pd
import requests
from bblocks import add_iso_codes_column, add_income_level_column
from bs4 import BeautifulSoup

from scripts import config

from oda_data.get_data.common import get_url_selenium

from scripts.config import logger, Paths

INDICATORS = {49: "Total Population"}
UN_POPULATION_URL: str = "https://population.un.org/dataportalapi/api/v1/"
UN_POPULATION_YEARS: dict = {"start_year": 2023, "end_year": 2023}


def _get_un_locations() -> pd.DataFrame:
    """Download all UN locations using API"""

    locations_url = UN_POPULATION_URL + "locations/"

    logger.info(f"Downloading UN locations from {locations_url}")

    return download_un_population_data(locations_url)


def un_population_url(
    indicator: int, start_year: int, end_year: int, locations: int | list[int]
) -> str:
    """Create a URL to download UN population data from the API"""

    if isinstance(locations, list):
        locations = ",".join(map(str, locations))

    logger.debug(f"Downloading UN population data using API")

    return (
        f"{UN_POPULATION_URL}data/indicators/{indicator}/"
        f"locations/{locations}?startYear={start_year}&endYear={end_year}"
        f"&startAge=0&endAge=18&sexes=3&variants=4&pageSize=100"
    )


def get_un_url(url: str) -> json:
    logger.debug(f"Downloading UN population data from {url}")

    page_source = get_url_selenium(url).page_source
    soup = BeautifulSoup(page_source, "html.parser")
    pre_tag = soup.find("pre")  # Assuming the JSON is within a <pre> tag

    if pre_tag:
        json_string = pre_tag.text
        return json.loads(json_string)
    else:
        print("population data not found")


def download_un_population_data(url: str) -> pd.DataFrame:
    response = get_un_url(url)

    df = pd.json_normalize(response["data"])

    while response["nextPage"] is not None:
        new_url = response["nextPage"]
        try:
            response = get_un_url(new_url)
        except:
            logger.error(f"Error downloading data from {new_url}")
            time.sleep(10)
            try:
                response = get_un_url(new_url)
            except:
                logger.error(f"2 Error downloading data from {new_url}")
                time.sleep(10)
                response = get_un_url(new_url)

        df = pd.concat([df, pd.json_normalize(response["data"])])

    return df


def split_list(input_list, n):
    """Splits a list into n approximately equal parts"""
    k, m = divmod(len(input_list), n)
    return (
        input_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)
    )


def get_data_for_ids(ids, indicator: int):
    """Retrieve data for a list of IDs and return as a DataFrame"""
    url = un_population_url(
        indicator=indicator,
        start_year=UN_POPULATION_YEARS["start_year"],
        end_year=UN_POPULATION_YEARS["end_year"],
        locations=ids,
    )
    return download_un_population_data(url)


def download_all_population(indicator: int = 49) -> None:
    """Update the raw UN population data"""

    locations = _get_un_locations()
    ids = locations["id"].to_list()

    # Split the IDs into three parts
    split_ids = list(split_list(ids, 3))

    # Create DataFrames for each part and collect them in a list
    dfs = [get_data_for_ids(part, indicator=indicator) for part in split_ids]

    df = pd.concat(dfs, ignore_index=True)

    file_path = Paths.raw_data / f"un_population_raw_{indicator}.csv"

    df.to_csv(file_path, index=False)

    logger.info(f"Downloaded UN population data to {file_path}")


def raw_un_population_data(indicator: int = 47) -> pd.DataFrame:
    """Read the raw UN population data"""

    file_path = Paths.raw_data / f"un_population_raw_{indicator}.csv"

    logger.debug(f"Read UN population data from {file_path}")

    return pd.read_csv(file_path)


def filter_total_population(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.loc[lambda d: d.sex == "Both sexes"]
        .reset_index(drop=True)
        .drop("sex", axis=1)
    )


def filter_median_variant(data: pd.DataFrame) -> pd.DataFrame:
    return data.loc[lambda d: d.variantLabel == "Median"].reset_index(drop=True)


def filter_key_columns(data: pd.DataFrame) -> pd.DataFrame:
    return data.filter(
        ["location", "iso3", "indicator", "timeLabel", "sex", "value"], axis=1
    )


def clean_population_data(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.pipe(filter_total_population)
        .pipe(filter_median_variant)
        .assign(indicator=lambda d: d.indicatorId.map(INDICATORS))
        .pipe(filter_key_columns)
        .rename(
            columns={
                "iso3": "iso_code",
                "timeLabel": "year",
                "location": "location_name",
            }
        )
        .astype({"year": int, "value": int})
    )


def un_population_data() -> pd.DataFrame:
    """Clean dataset containing the median estimates for all available countries"""
    return raw_un_population_data().pipe(clean_population_data)


def add_population_under18(data: pd.DataFrame, country_col: str = None) -> pd.DataFrame:

    population = (
        raw_un_population_data()
        .astype({"ageStart": "int32[pyarrow]"})
        .loc[lambda d: d.ageStart < 18]
        .groupby(["iso3"], dropna=False, observed=True)["value"]
        .sum()
        .reset_index()
        .rename(columns={"iso3": "iso_code", "value": "population"})
    )

    if country_col is not None:
        data = data.pipe(add_iso_codes_column, id_column=country_col, id_type="regex")

    data = data.merge(population, on=["iso_code"], how="left")

    return data


def get_population() -> pd.DataFrame:
    # get population
    return (
        raw_un_population_data(indicator=49)
        .query("sex == 'Both sexes' and variant=='Median'")
        .filter(["iso3", "value"])
        .pipe(add_income_level_column, id_column="iso3", id_type="ISO3")
    )


def population_for_income(income_level: str | list[str]) -> pd.DataFrame:
    if isinstance(income_level, str):
        income_level = [income_level]

    population = get_population()

    return population.loc[lambda d: d.income_level.isin(income_level)].value.sum()


def population_for_countries(countries: list[str]) -> pd.DataFrame:
    population = get_population()

    return population.loc[lambda d: d.iso3.isin(countries)].value.sum()


def population_as_share_for_countries(countries: list[str]) -> float:
    # Get population
    population = get_population()

    # Total population
    total_population = population["value"].sum()

    # income level population
    country_population = population_for_countries(countries)

    return round(country_population / total_population * 100, 1)


def population_as_share(income_level: str | list[str]) -> float:

    if isinstance(income_level, str):
        income_level = [income_level]

    # Get population
    population = get_population()

    # Total population
    total_population = population["value"].sum()

    # income level population
    income_level_population = population_for_income(income_level)

    return round(income_level_population / total_population * 100, 1)


if __name__ == "__main__":
    download_all_population(indicator=47)
