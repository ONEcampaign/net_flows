import logging

import pandas as pd
from bblocks import convert_id, DebtIDS

from scripts import config

logging.getLogger("country_converter").setLevel(logging.ERROR)


def multilateral_mapping() -> dict:
    """
    Returns a dictionary of multilateral institutions and their
    corresponding names.

    This is done to ensure consistent naming across the data sets.
    """
    return {
        "Adaptation Fund": "Adaptation Fund",
        "African Dev. Bank": "African Development Bank",
        "African Development Bank [AfDB]": "African Development Bank",
        "African Development Fund [AfDF]": "African Development Fund",
        "African Export-Import Bank": "African Export Import Bank",
        "Arab African International Bank": "Arab African International Bank",
        "Arab Bank for Economic Dev. in Africa (BADEA)": (
            "Arab Bank for Economic Development in Africa"
        ),
        "Asian Forest Cooperation Organisation [AFoCO]": "Asian Forest Cooperation Organisation",
        "Arab Bank for Economic Development in Africa [BADEA]": (
            "Arab Bank for Economic Development in Africa"
        ),
        "Arab Fund for Tech. Assist. to African Countries": (
            "Arab Fund for Technical Assistance to African Countries"
        ),
        "Arab International Bank": "Arab International Bank",
        "Arab League": "Arab League",
        "Arab Monetary Fund": "Arab Monetary Fund",
        "Arab Towns Organization (ATO)": "Arab Towns Organization",
        "Arab Fund (AFESD)": "Arab Fund for Economic and Social Development",
        "Asian Dev. Bank": "Asian Development Bank",
        "Asian Development Bank [AsDB]": "Asian Development Bank",
        "Asian Infrastructure Investment Bank": "Asian Infrastructure Investment Bank",
        "Bank for International Settlements (BIS)": "Bank for International Settlements",
        "Bolivarian Alliance for the Americas (ALBA)": (
            "Bolivarian Alliance for the Americas"
        ),
        "Caribbean Community (CARICOM)": "Caribbean Community",
        "Caribbean Dev. Bank": "Caribbean Development Bank",
        "Caribbean Development Bank [CarDB]": "Caribbean Development Bank",
        "Center for Latin American Monetary Studies (CEMLA)": (
            "Center for Latin American Monetary Studies"
        ),
        "Central American Bank for Econ. Integ. (CABEI)": (
            "Central American Bank for Economic Integration"
        ),
        "Central American Bank for Economic Integration [CABEI]": (
            "Central American Bank for Economic Integration"
        ),
        "Central American Bank for Econ. Integration (BCIE)": (
            "Central American Bank for Economic Integration"
        ),
        "Central Bank of West African States (BCEAO)": (
            "Central Bank of West African States"
        ),
        "CGIAR": "CGIAR",
        "Corporacion Andina de Fomento": "Corporacion Andina de Fomento",
        "Council of Europe": "Council of Europe",
        "Council of Europe Development Bank [CEB]": "Council of Europe",
        "Dev. Bank of the Central African States (BDEAC)": (
            "Development Bank of the Central African States"
        ),
        "East African Community": "East African Community",
        "Eastern & Southern African Trade & Dev. Bank (TDB)": (
            "Eastern and Southern African Trade and Development Bank"
        ),
        "ECO Trade and Dev. Bank": "ECO Trade and Development Bank",
        "Econ. Comm. of the Great Lakes Countries (ECGLC)": (
            "Economic Community of the Great Lakes Countries"
        ),
        "Economic Community of West African States (ECOWAS)": (
            "Economic Community of West African States"
        ),
        "Eurasian Development Bank": "Eurasian Development Bank",
        "EUROFIMA": "EUROFIMA",
        "European Bank for Reconstruction and Dev. (EBRD)": (
            "European Bank for Reconstruction and Development"
        ),
        "European Coal and Steel Community (ECSC)": (
            "European Coal and Steel Community"
        ),
        "European Development Fund (EDF)": "European Development Fund",
        "EU Institutions": "EU Institutions",
        "European Economic Community (EEC)": "European Economic Community",
        "European Free Trade Association (EFTA)": "European Free Trade Association",
        "European Investment Bank": "European Investment Bank",
        "European Relief Fund": "European Relief Fund",
        "European Social Fund (ESF)": "European Social Fund",
        "European Union": "European Union",
        "Fondo Latinoamericano de Reservas (FLAR)": "Fondo Latinoamericano de Reservas",
        "Food and Agriculture Organization (FAO)": "Food and Agriculture Organization",
        "Food and Agriculture Organisation [FAO]": "Food and Agriculture Organization",
        "IFAD": "International Fund for Agricultural Development",
        "Foreign Trade Bank of Latin America (BLADEX)": (
            "Foreign Trade Bank of Latin America"
        ),
        "Global Environment Facility": "Global Environment Facility",
        "Global Environment Facility [GEF]": "Global Environment Facility",
        "Global Alliance for Vaccines and Immunization [GAVI]": (
            "Global Alliance for Vaccines and Immunization"
        ),
        "Global Fund": "Global Fund to Fight AIDS, Tuberculosis and Malaria",
        "International Atomic Energy Agency [IAEA]": "International Atomic Energy Agency",
        "Inter-American Dev. Bank": "Inter-American Development Bank",
        "Inter-American Development Bank [IDB]": "Inter-American Development Bank",
        "International Bank for Economic Cooperation (IBEC)": (
            "International Bank for Economic Cooperation"
        ),
        "International Coffee Organization (ICO)": "International Coffee Organization",
        "International Finance Corporation": "International Finance Corporation",
        "International Fund for Agricultural Dev.": (
            "International Fund for Agricultural Development"
        ),
        "International Investment Bank (IIB)": "International Investment Bank",
        "International Labour Organization (ILO)": "International Labour Organization",
        "International Labour Organisation [ILO]": "International Labour Organization",
        "International Monetary Fund": "International Monetary Fund",
        "IMF (Concessional Trust Funds)": "International Monetary Fund",
        "Islamic Dev. Bank": "Islamic Development Bank",
        "Islamic Development Bank [IsDB]": "Islamic Development Bank",
        "Islamic Solidarity Fund for Dev. (ISFD)": "Islamic Solidarity Fund for Development",
        "Latin Amer. Conf. of Saving & Credit Coop. (COLAC)": (
            "Latin American Conference of Saving and Credit Cooperation"
        ),
        "Latin American Agribusiness Dev. Corp. (LAAD)": (
            "Latin American Agribusiness Development Corporation"
        ),
        "Montreal Protocol Fund": "Montreal Protocol Fund",
        "Montreal Protocol": "Montreal Protocol Fund",
        "Nordic Development Fund": "Nordic Development Fund",
        "Nordic Development Fund [NDF]": "Nordic Development Fund",
        "Nordic Environment Finance Corporation (NEFCO)": "Nordic Environment Finance Corporation",
        "Nordic Investment Bank": "Nordic Investment Bank",
        "OPEC Fund for International Dev.": "OPEC Fund for International Development",
        "OPEC Fund for International Development [OPEC Fund]": (
            "OPEC Fund for International Development"
        ),
        "Org. of Arab Petroleum Exporting Countries (OAPEC)": (
            "Organization of Arab Petroleum Exporting Countries"
        ),
        "Plata Basin Financial Dev. Fund": "Plata Basin Financial Development Fund",
        "South Asian Development Fund (SADF)": "South Asian Development Fund",
        "UN-Children's Fund (UNICEF)": "UNICEF",
        "UNICEF": "UNICEF",
        "World Health Organisation [WHO]": "WHO",
        "UN-Development Fund for Women (UNIFEM)": "UN Development Fund for Women",
        "UN Women": "UN Women",
        "COVID-19 Response and Recovery Multi-Partner Trust Fund [UN COVID-19 MPTF]": (
            "UN COVID-19 Response and Recovery Multi-Partner Trust Fund"
        ),
        "Joint Sustainable Development Goals Fund [Joint SDG Fund]": (
            "Joint Sustainable Development Goals Fund"
        ),
        "International Commission on Missing Persons [ICMP]": (
            "International Commission on Missing Persons"
        ),
        "WHO-Strategic Preparedness and Response Plan [SPRP]": (
            "WHO Strategic Preparedness and Response Plan"
        ),
        "International Centre for Genetic Engineering and Biotechnology [ICGEB]": (
            "International Centre for Genetic Engineering and Biotechnology"
        ),
        "World Organisation for Animal Health [WOAH]": "World Organisation for Animal Health",
        "UN-Development Programme (UNDP)": "UN Development Programme",
        "UNDP": "UN Development Programme",
        "UN-Educ., Scientific and Cultural Org. (UNESCO)": "UNESCO",
        "UNECE": "UNECE",
        "UN-Environment Programme (UNEP)": "UN Environment Programme",
        "UNEP": "UN Environment Programme",
        "UN-Fund for Drug Abuse Control (UNFDAC)": "UN Fund for Drug Abuse Control",
        "UN-Fund for Human Rights": "UN Fund for Human Rights",
        "UN-General Assembly (UNGA)": "UN General Assembly",
        "UN-High Commissioner for Refugees (UNHCR)": "UN High Commissioner for Refugees",
        "UNHCR": "UN High Commissioner for Refugees",
        "UNAIDS": "UNAIDS",
        "UN-Industrial Development Organization (UNIDO)": (
            "UN Industrial Development Organization"
        ),
        "United Nations Industrial Development Organization [UNIDO]": (
            "UN Industrial Development Organization"
        ),
        "UN Institute for Disarmament Research [UNIDIR]": (
            "UN Institute for Disarmament Research"
        ),
        "UN-INSTRAW": (
            "UN International Research and Training Institute for the Advancement of Women"
        ),
        "UN-Office on Drugs and Crime (UNDCP)": "UN Office on Drugs and Crime",
        "UN-Population Fund (UNFPA)": "UN Population Fund",
        "UNFPA": "UN Population Fund",
        "UN Peacebuilding Fund [UNPBF]": "UN Peacebuilding Fund",
        "UN-Regular Programme of Technical Assistance": (
            "UN Regular Programme of Technical Assistance"
        ),
        "UN-Regular Programme of Technical Coop. (RPTC)": (
            "UN Regular Programme of Technical Assistance"
        ),
        "UN-Relief and Works Agency (UNRWA)": "UN Relief and Works Agency",
        "UNRWA": "UN Relief and Works Agency",
        "UN-UNETPSA": "UN UNETPSA",
        "UN-World Food Programme (WFP)": "UN World Food Programme",
        "WFP": "UN World Food Programme",
        "UN-World Intellectual Property Organization": "UN World Intellectual Property Organization",
        "UN-World Meteorological Organization": "UN World Meteorological Organization",
        "United Nations Conference on Trade and Development [UNCTAD]": (
            "UN Conference on Trade and Development"
        ),
        "North American Development Bank [NADB]": "North American Development Bank",
        "WTO - International Trade Centre [ITC]": "International Trade Centre",
        "UN Capital Development Fund [UNCDF]": "UN Capital Development Fund",
        "OSCE": "Organization for Security and Co-operation in Europe",
        "West African Development Bank - BOAD": "West African Development Bank",
        "West African Monetary Union (UMOA)": "West African Monetary Union",
        "World Bank-IBRD": "WB International Bank for Reconstruction and Development",
        "World Bank-IDA": "WB International Development Association",
        "International Development Association [IDA]": "WB International Development Association",
        "World Bank-MIGA": "WB Multilateral Investment Guarantee Agency",
        "World Trade Organization": "World Trade Organization",
        "Climate Investment Funds [CIF]": "Climate Investment Funds",
        "Global Green Growth Institute [GGGI]": "Global Green Growth Institute",
        "Green Climate Fund [GCF]": "Green Climate Fund",
        "World Tourism Organisation [UNWTO]": "World Tourism Organization",
        "Center of Excellence in Finance [CEF]": "Center of Excellence in Finance",
        "Central Emergency Response Fund [CERF]": "Central Emergency Response Fund",
    }


def clean_debtors(df: pd.DataFrame, column) -> pd.DataFrame:
    """
    Clean debtors names by converting to ISO3 and continent, and by
    creating a new column with the short name (from bblocks)
    """
    df[column] = df[column].astype("string[pyarrow]")

    df["iso_code"] = convert_id(
        df[column],
        from_type="regex",
        to_type="ISO3",
        not_found=pd.NA,
        additional_mapping={"Macau (China)": "MAC"},
    )
    df["continent"] = convert_id(
        df[column],
        from_type="regex",
        to_type="continent",
        additional_mapping={"Macau (China)": "Asia"},
    )
    df[f"{column}"] = convert_id(
        df[column],
        from_type="regex",
        to_type="name_short",
        additional_mapping={"Macau (China)": "Macau"},
    )

    return df.set_index(["iso_code", f"{column}", "continent"]).reset_index()


def clean_creditors(df: pd.DataFrame, column) -> pd.DataFrame:
    """
    Clean creditors names by converting to ISO3 and by creating a new column
    with the short name (from bblocks)
    """
    additional_iso = {
        "Korea, D.P.R. of": "PRK",
        "German Dem. Rep.": "DEU",
        "Neth. Antilles": "ANT",
        "Yugoslavia": "YUG",
    }
    additional_names = {
        "Korea, D.P.R. of": "North Korea",
        "German Dem. Rep.": "Germany",
        "Neth. Antilles": "Netherlands Antilles",
    }
    df[column] = df[column].astype("string[pyarrow]")

    df["counterpart_iso_code"] = convert_id(
        df[column],
        from_type="regex",
        to_type="ISO3",
        additional_mapping=multilateral_mapping() | additional_iso,
    )
    df[column] = convert_id(
        df[column],
        from_type="regex",
        to_type="name_short",
        additional_mapping=multilateral_mapping() | additional_names,
    )

    return df


def add_oecd_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add the OECD names for the donor and recipient. This is done by
    merging on the donor and recipient codes from the DAC2a data set.

    Args:
        - df (pd.DataFrame): The data frame to add the names to.
    """
    # import the required functions
    from oda_data import set_data_path, read_dac2a

    # set a path to the raw data
    set_data_path(config.Paths.raw_data)

    # read the DAC2a data set
    dac2a = read_dac2a(years=range(2010, 2023))

    # Create two dataframes, one with donors and one with recipients
    donors = dac2a.filter(["donor_code", "donor"]).drop_duplicates()
    recipients = dac2a.filter(["recipient_code", "recipient"]).drop_duplicates()

    # Merge donors (by codes to get the names), and then merge recipients
    df = df.merge(donors, on=["donor_code"], how="left")
    df = df.merge(recipients, on=["recipient_code"], how="left")

    return df


def remove_counterpart_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove counterpart totals from the data.
    """
    return df[lambda d: ~d["counterpart_area"].astype("string[pyarrow]").str.contains(", Total")]


def remove_recipient_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove recipient totals from the data.
    """
    return df[~df["country"].str.contains(", Total")]


def remove_groupings_and_totals_from_recipients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove regional groupings and totals from the data.
    """
    # import the required functions
    from oda_data import recipient_groupings

    # Select only the codes for the developing countries and regions
    groupings = recipient_groupings()["all_developing_countries_regions"]

    # Keep only the rows that are not in the groupings
    return df[df["recipient_code"].isin(groupings)]


def remove_non_official_counterparts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove non-official counterparts from the data.

    Args:
        - df (pd.DataFrame): The data frame to remove the non-official counterparts from.

    Returns:
        pd.DataFrame: The data frame with the non-official counterparts removed.
    """
    # import the required functions
    from oda_data import donor_groupings

    # Select only the codes for the official counterparts
    official = donor_groupings()["all_official"]

    # Add other official counterparts
    other_official = {
        1038: "UN Institute for Disarmament Research",
        962: "UN Conference on Trade and Development",
        1039: "UN Capital Development Fund",
        1045: "North American Development Bank",
        1401: "International Trade Centre",
        1406: "UN Industrial Development Organization",
        910: "Central American Bank for Economic Integration",
        1046: "UN Women",
        1047: "UN COVID-19 Response and Recovery Multi-Partner Trust Fund",
        1048: "Joint Sustainable Development Goals Fund",
        1049: "International Commission on Missing Persons",
        1050: "WHO Strategic Preparedness and Response Plan",
        1054: "World Organisation for Animal Health",
        915: "Asian Forest Cooperation Organisation",
        1055: "CGIAR",
    }

    # Combine the official counterparts
    official = official | other_official

    # Keep only the rows that are in the official counterparts
    return df[df["donor_code"].isin(official)]


def filter_and_assign_indicator(df: pd.DataFrame, indicator: str) -> pd.DataFrame:
    """
    Filter for key columns and assign the requested indicator name.

    """
    return df.filter(["year", "country", "counterpart_area", "value"]).assign(
        indicator=indicator
    )


def get_concessional_non_concessional(
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
