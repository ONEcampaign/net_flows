import asana
import pandas as pd
from bblocks import set_bblocks_data_path
from bblocks.dataframe_tools.add import add_gdp_share_column
from bblocks.import_tools.imf_weo import WEO
from bblocks.dataframe_tools.add import add_iso_codes_column
from bblocks import clean_numeric_series

from scripts.config import Paths


set_bblocks_data_path(Paths.raw_data)


def get_csv(doc: str) -> pd.DataFrame:
    return pd.read_csv(Paths.output / doc)


def get_parquet(doc: str) -> pd.DataFrame:
    return pd.read_parquet(Paths.output / doc)


def remove_special_case_countries(df: pd.DataFrame, iso_to_remove) -> pd.DataFrame:
    return df.loc[lambda d: ~d.iso_3.isin(iso_to_remove)]


def filter_net_negative_only(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[lambda d: d.net_flow <= 0]


def filter_for_specified_years(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    return df.loc[lambda d: d.year.isin(years)]


def calculate_net_transfers(df: pd.DataFrame) -> pd.DataFrame:
    df["net_flow"] = df["inflow"] + df["outflow"]

    return df


def calculate_share_gdp(df: pd.DataFrame) -> pd.DataFrame:
    # df['negative_net_flows'] = df['net_flows']*-1

    df["net_flows_over_gdp_percent"] = df["net_flows"] * 100 / df["gdp"]

    return df


def add_gdp_data(df: pd.DataFrame) -> pd.DataFrame:

    gdp = get_csv(doc="weo_data.csv").rename(
        {"entity_code": "iso_3", "value": "gdp"}, axis=1
    )

    df_merged = pd.merge(df, gdp, on=["year", "iso_3"], how="inner").filter(
        items=[
            "year",
            "country",
            "income_level",
            "continent",
            "net_flows",
            "iso_3",
            "gdp",
        ],
        axis="columns",
    )

    return df_merged


def create_histogram_bins(df: pd.DataFrame, grouping: str) -> pd.DataFrame:
    """Create a histogram from a dataframe with 1% increment bins from -5% to 20%
    and a single bin for values above 20%.

    Args:
        df: dataframe with values to bin in the 'net_flows_over_gdp_%' column.
        grouping: column to group by.

    Returns:
        dataframe with binned values and counts.
    """

    # Bins manually defined
    bins = list(range(-5, 21)) + [float("inf")]  # Up to 20, then everything above 20
    # Labels manually defined for each bin, including a label for >20%
    labels = {
        "-5 to -4": -4.5,
        "-4 to -3": -3.5,
        "-3 to -2": -2.5,
        "-2 to -1": -1.5,
        "-1 to 0": -0.5,
        "0 to 1": 0.5,
        "1 to 2": 1.5,
        "2 to 3": 2.5,
        "3 to 4": 3.5,
        "4 to 5": 4.5,
        "5 to 6": 5.5,
        "6 to 7": 6.5,
        "7 to 8": 7.5,
        "8 to 9": 8.5,
        "9 to 10": 9.5,
        "10 to 11": 10.5,
        "11 to 12": 11.5,
        "12 to 13": 12.5,
        "13 to 14": 13.5,
        "14 to 15": 14.5,
        "15 to 16": 15.5,
        "16 to 17": 16.5,
        "17 to 18": 17.5,
        "18 to 19": 18.5,
        "19 to 20": 19.5,
        "20+": ">20",
    }

    data = df.assign(
        binned=pd.cut(
            df.net_flows_over_gdp_percent,
            bins=bins,
            labels=list(labels.keys()),
            include_lowest=True,
        )
    )

    unique_countries_per_bin = (
        data.groupby(by=["year", "binned"])["country"]
        .nunique()
        .reset_index()
        .rename(columns={"country": "count"})
    )

    # Assign bins with labels
    return unique_countries_per_bin.assign(x_values=lambda d: d.binned.map(labels))


def pivot_by_income_group_chart_4(df: pd.DataFrame) -> pd.DataFrame:

    pivot_by_income_group = df.filter(
        items=["year", "income_level", "net_flow"], axis=1
    )

    pivot_by_income_group["net_flow"] = pivot_by_income_group["net_flow"] * -1

    pivot_by_income_group = pivot_by_income_group.pivot_table(
        index=["year"],  # Rows
        columns="income_level",  # Columns to pivot on
        values="net_flow",  # Values to fill in the table
        fill_value=0,  # Fill missing values with 0
    ).reset_index()

    return pivot_by_income_group


def pivot_by_continent_chart_4(df: pd.DataFrame) -> pd.DataFrame:

    pivot_by_continent = df.filter(items=["year", "continent", "net_flow"], axis=1)

    pivot_by_continent["net_flow"] = pivot_by_continent["net_flow"] * -1

    pivot_by_continent = pivot_by_continent.pivot_table(
        index=["year"],  # Rows
        columns="continent",  # Columns to pivot on
        values="net_flow",  # Values to fill in the table
        fill_value=0,  # Fill missing values with 0
    ).reset_index()

    return pivot_by_continent

def aggregate_by_year_chart_4(df: pd.DataFrame) -> pd.DataFrame:
    df["net_flow"] = df["net_flow"] * -1

    return df.groupby(["year"])[["inflow", "outflow", "net_flow"]].sum().reset_index()


def flourish_1_beeswarm() -> pd.DataFrame:
    df = get_csv(doc="final_stacked_net_flows.csv").rename(
        {"value": "net_flows"}, axis=1
    )
    # df['net_flows'] = clean_numeric_series(data=df['net_flows'], to=float)
    df["net_flows"] = df["net_flows"] / 1000000000

    df = add_iso_codes_column(
        df=df, id_column="country", id_type="regex", target_column="iso_3"
    )

    # add gdp data
    data = add_gdp_data(df)

    # calculate net transfers share of gdp
    data = calculate_share_gdp(data)

    # remove Russia, China and Ukraine
    special_case_countries = ["CHN", "RUS", "UKR"]
    data = remove_special_case_countries(df=data, iso_to_remove=special_case_countries)

    # filter for specified years
    years = [2010, 2022]
    data = filter_for_specified_years(df=data, years=years)

    return data


def flourish_2_histogram(df: pd.DataFrame) -> pd.DataFrame:
    # bin data
    data = create_histogram_bins(df=df, grouping="net_flows_over_gdp_percent")

    # specify bins required for analysis
    bins = [
        "-5 to -4",
        "-4 to -3",
        "-3 to -2",
        "-2 to -1",
        "-1 to 0",
        "0 to 1",
        "1 to 2",
        "2 to 3",
        "3 to 4",
        "4 to 5",
    ]

    return data.loc[lambda d: d.binned.isin(bins)]


def flourish_3_scatter() -> pd.DataFrame:
    data = get_csv(doc="scatter_totals.csv")

    # Filter for 2022
    data = data.loc[lambda d: d.year == 2022]

    # Groupby country
    data = (
        data.groupby(by=["year", "country", "continent", "income_level"])[
            ["inflow", "outflow"]
        ]
        .sum()
        .reset_index()
    )

    return data


def flourish_4_steamgraph() -> pd.DataFrame:
    data = get_parquet(doc="net_flows_full.parquet")

    # filter for current prices
    data = data.loc[lambda d: d.prices == "constant"]

    # Groupby country
    data = (
        data.groupby(
            by=["year", "country", "continent", "income_level", "indicator_type"]
        )["value"]
        .sum()
        .reset_index()
    )

    # Groupby counterpart area
    # data_counterpart = (
    #     data.groupby(
    #         by=["year", "indicator_type", "counterpart_area"]
    #     )["value"]
    #     .sum()
    #     .reset_index()
    # ).pivot_table(index=["year", "counterpart_area"],  # Rows
    #               columns="indicator_type",  # Columns to pivot on
    #               values="value",  # Values to fill in the table
    #               fill_value=0,  # Fill missing values with 0
    #               ).reset_index()

    # Pivot for inflows and outflows
    data = data.pivot_table(
        index=["year", "country", "continent", "income_level"],  # Rows
        columns="indicator_type",  # Columns to pivot on
        values="value",  # Values to fill in the table
        fill_value=0,  # Fill missing values with 0
    ).reset_index()

    data = calculate_net_transfers(data)

    data = filter_net_negative_only(data)

    pivot_by_income_group = pivot_by_income_group_chart_4(df=data)

    pivot_by_continent = pivot_by_continent_chart_4(df=data)

    aggregate_by_year = aggregate_by_year_chart_4(data)

    return data.loc[lambda d: d.year >= 2010]

def filter_for_countries_with_negative_net_flow_at_any_point(df: pd.DataFrame) -> pd.DataFrame:
    # Identify countries with any negative net_flow
    has_negative_value = df.loc[df['net_flow'] < 0, 'country'].unique()

    # Filter the original DataFrame to keep only those countries
    filtered_df = df.loc[lambda d: d.country.isin(has_negative_value)]

    return filtered_df

def add_nnt_column(df: pd.DataFrame) -> pd.DataFrame:
    df['nnt'] = df['net_flow'].apply(lambda x: 'nnt' if x < 0 else '')

    return df




def flourish_5_connected_scatterplot() -> pd.DataFrame:
    data = get_parquet(doc="net_flows_full.parquet")

    # filter for current prices
    data = data.loc[lambda d: d.prices == "constant"]

    # Groupby country
    data = (
        data.groupby(
            by=["year", "country", "continent", "income_level", "indicator_type"]
        )["value"]
        .sum()
        .reset_index()
    )

    data = data.pivot_table(
        index=["year", "country", "continent", "income_level"],  # Rows
        columns="indicator_type",  # Columns to pivot on
        values="value",  # Values to fill in the table
        fill_value=0,  # Fill missing values with 0
    ).reset_index()

    data = calculate_net_transfers(data)

    negative_countries = filter_for_countries_with_negative_net_flow_at_any_point(data)

    negative_countries['outflow'] = negative_countries['outflow']*-1

    negative_countries = add_nnt_column(negative_countries)

    return negative_countries

if __name__ == "__main__":
    # full_data = get_csv(doc="net_flows_full.csv")

    # flourish_chart_1 = flourish_1_beeswarm()
    # flourish_chart_1.to_csv(Paths.output / "flourish_chart_1.csv", index=False)

    # flourish_chart_2 = flourish_2_histogram(df=flourish_chart_1)
    # flourish_chart_2.to_csv(Paths.output / "flourish_chart_2.csv", index=False)

    # flourish_chart_3 = flourish_3_scatter()
    # flourish_chart_3.to_csv(Paths.output / "flourish_chart_3.csv", index=False)

    # flourish_chart_4 = flourish_4_steamgraph()
    # flourish_chart_4.to_csv(Paths.output / "flourish_chart_4.csv", index=False)

    flourish_chart_5 = flourish_5_connected_scatterplot()
    flourish_chart_5.to_csv(Paths.output / "flourish_chart_5.csv", index=False)

    # inflows_outflows_ratio = net_inflows_pipeline(df=full_data, prices="constant")

    # inflows_outflows_ratio.to_csv(Paths.output / "inflows_outflows_ratio.csv", index=False)
