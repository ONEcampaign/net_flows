import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from scripts.analysis.common import (
    create_groupings,
    reorder_countries,
    add_china_as_counterpart_type,
    exclude_countries_without_outflows,
)
from scripts.analysis.net_flows import (
    get_all_flows,
    prep_flows,
    rename_indicators,
    exclude_outlier_countries,
)
from scripts.config import Paths
from scripts.data.outflows import get_debt_service_data


def calculate_linear_trend_and_predict(
    data: pd.DataFrame,
    base_year: int,
    years_back: int = 5,
    years_forward: int = 3,
    creditors_grouping: str = "counterpart_area",
) -> pd.DataFrame:
    """Calculate linear trend over the last n years and predict for the next m years"""
    if creditors_grouping is None:
        creditors_grouping = ["counterpart_area"]

    if isinstance(creditors_grouping, str):
        creditors_grouping = [creditors_grouping]

    # Filter to last n years
    full = data.copy()
    data = data.loc[lambda d: d.year >= base_year - years_back + 1]

    # group
    group = ["year", "country", "continent", "income_level"] + creditors_grouping

    data = (
        data.groupby(group, dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    full = (
        full.groupby(group, dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Prepare data for regression
    regress_data = data[group + ["value"]].dropna()

    # Initialize the linear regression model
    model = LinearRegression()

    # Prediction years
    future_years = np.array(
        [base_year - years_back + 1 + years_back + i for i in range(years_forward)]
    ).reshape(-1, 1)

    # Convert to df
    future_years_df = pd.DataFrame(future_years, columns=["year"])

    # Predict for each country
    predictions = []
    for (
        country,
        continent,
        income_level,
        counterpart,
    ), group in regress_data.groupby(
        ["country", "continent", "income_level"] + creditors_grouping,
        observed=True,
        dropna=False,
    ):
        # Need at least two points to fit a line
        if len(group) < 2:
            continue

        X = group[["year"]].reset_index(drop=True)
        y = group["value"].reset_index(drop=True)
        model.fit(X, y)
        future_values = model.predict(future_years_df)
        for year, value in zip(future_years_df["year"], future_values):
            predictions.append(
                {
                    "year": year,
                    "country": country,
                    "continent": continent,
                    "income_level": income_level,
                    creditors_grouping[0]: counterpart,
                    "value": value,
                }
            )

    prediction_df = pd.DataFrame(predictions)

    # Limit predictions to positive values (inflows cannot be negative)
    prediction_df.loc[lambda d: d.value < 0, "value"] = 0

    return pd.concat([full, prediction_df], ignore_index=True).assign(
        indicator_type="inflow"
    )


def calculate_average_inflows(data: pd.DataFrame, years: int = 2) -> pd.DataFrame:
    """Calculate average inflows over the last n years"""

    data = data.loc[lambda d: d.indicator_type == "inflow"].copy()

    data = (
        data.groupby(["year", "country"], dropna=False, observed=True)
        .sum(numeric_only=True)
        .reset_index()
    )

    # Add the rolling sum
    data["rolling_inflow"] = (
        data.fillna({"value": 0})
        .groupby(
            [c for c in data.columns if c not in ["year", "value"]],
            observed=True,
            dropna=False,
            group_keys=False,
            as_index=False,
        )["value"]
        .rolling(
            years, min_periods=1
        )  # min periods are 1 to avoid Nans in sparse data (when very granular)
        .mean()["value"]
    )

    return data.drop(columns="value")


def outflows_projections(
    constant: bool = False, creditors_grouping: str = "counterpart_area"
) -> pd.DataFrame:
    outflows = (
        get_debt_service_data(constant=constant)
        .pipe(prep_flows)
        .assign(value=lambda d: -d.value)
        .pipe(rename_indicators, suffix="")
        .pipe(add_china_as_counterpart_type)
        .groupby(
            ["year", "country", "continent", "income_level", creditors_grouping],
            observed=True,
            dropna=False,
        )["value"]
        .sum()
        .reset_index()
    )

    return outflows


def projected_netflows(inflows: pd.DataFrame, outflows: pd.DataFrame) -> pd.DataFrame:
    """project net flows based on average inflows and outflows"""

    data = pd.concat([inflows, outflows], ignore_index=True)

    # pivot the data
    data = (
        data.pivot(
            index=[c for c in data.columns if c not in ["value", "indicator_type"]],
            columns="indicator_type",
            values="value",
        )
        .reset_index()
        .fillna(0)
    )

    # Group by country
    data = (
        data.groupby(
            ["year", "country", "continent", "income_level"],
            dropna=False,
            observed=True,
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    data["net_flows"] = data["inflow"] + data["outflow"]

    return data


def projections_pipline(
    years_back: int = 3,
    years_forward: int = 3,
    constant: bool = False,
    limit_to_2022: bool = True,
    projected_only: bool = True,
) -> None:

    # Get full dataset
    data = (
        get_all_flows(limit_to_2022=limit_to_2022, constant=constant)
        .pipe(exclude_outlier_countries)
        .pipe(exclude_countries_without_outflows)
        .loc[lambda d: d.indicator_type == "inflow"]
        .reset_index(drop=True)
        .copy(deep=True)
        .pipe(add_china_as_counterpart_type)
    )

    # Calculate projected inflows based on 5 year trend
    inflows = calculate_linear_trend_and_predict(
        data=data,
        base_year=2022,
        years_back=years_back,
        years_forward=years_forward,
        creditors_grouping="counterpart_type",
    )

    # Get outflows projections
    outflows = (
        outflows_projections(constant=constant, creditors_grouping="counterpart_type")
        .pipe(exclude_outlier_countries)
        .assign(indicator_type="outflow")
    )

    # Get projected net flows
    projections = (
        projected_netflows(inflows=inflows, outflows=outflows)
        .drop(columns=["inflow", "outflow"])
        .rename({"net_flows": "value"}, axis=1)
    )

    if projected_only:
        projections = projections.loc[lambda d: d.year > 2022]

    projections_grouped = create_groupings(projections).pipe(reorder_countries)

    # Save
    projections_grouped.reset_index(drop=True).to_parquet(
        Paths.output / "net_flow_projections_group.parquet"
    )

    projections.reset_index(drop=True).to_parquet(
        Paths.output / "net_flow_projections_country.parquet"
    )


if __name__ == "__main__":
    projections_pipline()
