import logging

import pandas as pd
from bblocks import convert_id

logging.getLogger("country_converter").setLevel(logging.ERROR)

def multilateral_mapping() -> dict:
    return {
        "African Dev. Bank": "African Development Bank",
        "African Export-Import Bank": "African Export Import Bank",
        "Arab African International Bank": "Arab African International Bank",
        "Arab Bank for Economic Dev. in Africa (BADEA)": (
            "Arab Bank for Economic Development in Africa"
        ),
        "Arab Fund for Tech. Assist. to African Countries": (
            "Arab Fund for Technical Assistance to African Countries"
        ),
        "Arab International Bank": "Arab International Bank",
        "Arab League": "Arab League",
        "Arab Monetary Fund": "Arab Monetary Fund",
        "Arab Towns Organization (ATO)": "Arab Towns Organization",
        "Asian Dev. Bank": "Asian Development Bank",
        "Asian Infrastructure Investment Bank": "Asian Infrastructure Investment Bank",
        "Bank for International Settlements (BIS)": "Bank for International Settlements",
        "Bolivarian Alliance for the Americas (ALBA)": (
            "Bolivarian Alliance for the Americas"
        ),
        "Caribbean Community (CARICOM)": "Caribbean Community",
        "Caribbean Dev. Bank": "Caribbean Development Bank",
        "Center for Latin American Monetary Studies (CEMLA)": (
            "Center for Latin American Monetary Studies"
        ),
        "Central American Bank for Econ. Integ. (CABEI)": (
            "Central American Bank for Economic Integration"
        ),
        "Central American Bank for Econ. Integration (BCIE)": (
            "Central American Bank for Economic Integration"
        ),
        "Central Bank of West African States (BCEAO)": (
            "Central Bank of West African States"
        ),
        "Corporacion Andina de Fomento": "Corporacion Andina de Fomento",
        "Council of Europe": "Council of Europe",
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
        "European Economic Community (EEC)": "European Economic Community",
        "European Free Trade Association (EFTA)": "European Free Trade Association",
        "European Investment Bank": "European Investment Bank",
        "European Relief Fund": "European Relief Fund",
        "European Social Fund (ESF)": "European Social Fund",
        "European Union": "European Union",
        "Fondo Latinoamericano de Reservas (FLAR)": "Fondo Latinoamericano de Reservas",
        "Food and Agriculture Organization (FAO)": "Food and Agriculture Organization",
        "Foreign Trade Bank of Latin America (BLADEX)": (
            "Foreign Trade Bank of Latin America"
        ),
        "Global Environment Facility": "Global Environment Facility",
        "Inter-American Dev. Bank": "Inter-American Development Bank",
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
        "International Monetary Fund": "International Monetary Fund",
        "Islamic Dev. Bank": "Islamic Development Bank",
        "Islamic Solidarity Fund for Dev. (ISFD)": "Islamic Solidarity Fund for Development",
        "Latin Amer. Conf. of Saving & Credit Coop. (COLAC)": (
            "Latin American Conference of Saving and Credit Cooperation"
        ),
        "Latin American Agribusiness Dev. Corp. (LAAD)": (
            "Latin American Agribusiness Development Corporation"
        ),
        "Montreal Protocol Fund": "Montreal Protocol Fund",
        "Nordic Development Fund": "Nordic Development Fund",
        "Nordic Environment Finance Corporation (NEFCO)": "Nordic Environment Finance Corporation",
        "Nordic Investment Bank": "Nordic Investment Bank",
        "OPEC Fund for International Dev.": "OPEC Fund for International Development",
        "Org. of Arab Petroleum Exporting Countries (OAPEC)": (
            "Organization of Arab Petroleum Exporting Countries"
        ),
        "Plata Basin Financial Dev. Fund": "Plata Basin Financial Development Fund",
        "South Asian Development Fund (SADF)": "South Asian Development Fund",
        "UN-Children's Fund (UNICEF)": "UNICEF",
        "UN-Development Fund for Women (UNIFEM)": "UN Development Fund for Women",
        "UN-Development Programme (UNDP)": "UN Development Programme",
        "UN-Educ., Scientific and Cultural Org. (UNESCO)": "UNESCO",
        "UN-Environment Programme (UNEP)": "UN Environment Programme",
        "UN-Fund for Drug Abuse Control (UNFDAC)": "UN Fund for Drug Abuse Control",
        "UN-Fund for Human Rights": "UN Fund for Human Rights",
        "UN-General Assembly (UNGA)": "UN General Assembly",
        "UN-High Commissioner for Refugees (UNHCR)": "UN High Commissioner for Refugees",
        "UN-Industrial Development Organization (UNIDO)": (
            "UN Industrial Development Organization"
        ),
        "UN-INSTRAW": (
            "UN International Research and Training Institute for the Advancement of Women"
        ),
        "UN-Office on Drugs and Crime (UNDCP)": "UN Office on Drugs and Crime",
        "UN-Population Fund (UNFPA)": "UN Population Fund",
        "UN-Regular Programme of Technical Assistance": (
            "UN Regular Programme of Technical Assistance"
        ),
        "UN-Regular Programme of Technical Coop. (RPTC)": (
            "UN Regular Programme of Technical Assistance"
        ),
        "UN-Relief and Works Agency (UNRWA)": "UN Relief and Works Agency",
        "UN-UNETPSA": "UN UNETPSA",
        "UN-World Food Programme (WFP)": "UN World Food Programme",
        "UN-World Intellectual Property Organization": "UN World Intellectual Property Organization",
        "UN-World Meteorological Organization": "UN World Meteorological Organization",
        "West African Development Bank - BOAD": "West African Development Bank",
        "West African Monetary Union (UMOA)": "West African Monetary Union",
        "World Bank-IBRD": "WB International Bank for Reconstruction and Development",
        "World Bank-IDA": "WB International Development Association",
        "World Bank-MIGA": "WB Multilateral Investment Guarantee Agency",
        "World Trade Organization": "World Trade Organization",
    }


def clean_debtors(df: pd.DataFrame, column) -> pd.DataFrame:
    df["iso_code"] = convert_id(df[column], from_type="regex", to_type="ISO3")
    df["continent"] = convert_id(df[column], from_type="regex", to_type="continent")
    df[column] = convert_id(df[column], from_type="regex", to_type="name_short")

    return df.set_index(["iso_code", column, "continent"]).reset_index()


def clean_creditors(df: pd.DataFrame, column) -> pd.DataFrame:
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
