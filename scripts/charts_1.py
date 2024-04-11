"""Charts 1.1 and 1.2"""

import pandas as pd
import numpy as np

from scripts.config import Paths


def chart_1_1():
    """Chart 1.1"""

    orig_reg_df = (
        pd.read_parquet(Paths.output / "net_flows_grouping.parquet", engine="pyarrow")
        .query("prices=='current'")[["year", "country", "value"]]
        .groupby(["country", "year"])
        .agg({"value": "sum"})
        .reset_index()
    )

    orig_country_df = (
        pd.read_parquet(Paths.output / "net_flows_country.parquet", engine="pyarrow")
        .query("prices=='current'")[["year", "country", "value"]]
        .groupby(["country", "year"])
        .agg({"value": "sum"})
        .reset_index()
    )

    orig_df = pd.concat([orig_country_df, orig_reg_df]).assign(value_type="data")

    proj_df = pd.concat(
        [
            (
                pd.read_parquet(
                    Paths.output / "net_flow_projections_group.parquet",
                    engine="pyarrow",
                )[["year", "country", "value"]]
            ),
            (
                pd.read_parquet(
                    Paths.output / "net_flow_projections_country.parquet",
                    engine="pyarrow",
                )[["year", "country", "value"]]
            ),
        ]
    ).assign(value_type="projection")

    # join last year of data with first year of projection
    add_df = (
        orig_df.loc[orig_df["year"] == 2022]
        .reset_index(drop=True)
        .assign(value_type="projection")
    )

    order = [
        "Developing countries",
        "Low income",
        "Lower middle income",
        "Upper middle income",
        "Africa",
        "America",
        "Asia",
        "Europe" "Oceania",
    ]

    final_df = (
        pd.concat([proj_df, add_df, orig_df])
        .pivot(index=["country", "year"], columns="value_type", values="value")
        .reset_index()
        .assign(
            sort_col=lambda d: d.country.apply(
                lambda x: order.index(x) if x in order else len(order) + ord(x[0])
            )
        )
        .sort_values(by=["sort_col", "country", "year"])
        .drop(columns="sort_col")
    )

    # save the output to a file
    final_df.to_csv(Paths.output / "chart_1_1.csv", index=False)


def chart_1_2():
    """Chart 1.2"""

    inflow_group = (
        pd.read_parquet(Paths.output / "full_flows_grouping.parquet", engine="pyarrow")
        .query("prices=='current' and indicator_type == 'inflow'")
        .loc[:, ["year", "country", "counterpart_area", "counterpart_type", "value"]]
        .assign(
            counterpart_type=lambda d: np.where(
                d.counterpart_area == "China", "China", d.counterpart_type
            )
        )
        .groupby(["year", "country", "counterpart_type"])
        .agg({"value": "sum"})
        .reset_index()
    )

    inflow_country = (
        pd.read_parquet(Paths.output / "full_flows_country.parquet", engine="pyarrow")
        .query("prices=='current' and indicator_type == 'inflow'")
        .loc[:, ["year", "country", "counterpart_area", "counterpart_type", "value"]]
        .assign(
            counterpart_type=lambda d: np.where(
                d.counterpart_area == "China", "China", d.counterpart_type
            )
        )
        .groupby(["year", "country", "counterpart_type"])
        .agg({"value": "sum"})
        .reset_index()
    )

    order = [
        "Developing countries",
        "Low income",
        "Lower middle income",
        "Upper middle income",
        "Africa",
        "America",
        "Asia",
        "Europe" "Oceania",
    ]

    final_df = (
        pd.concat([inflow_group, inflow_country])
        .pivot(index=["year", "country"], columns="counterpart_type", values="value")
        .reset_index()
        .assign(
            sort_col=lambda d: d.country.apply(
                lambda x: order.index(x) if x in order else len(order) + ord(x[0])
            )
        )
        .sort_values(by=["sort_col", "country", "year"])
        .drop(columns="sort_col")
        .rename(
            columns={
                "Bilateral": "Bilateral (excl. China)",
                "Private": "Private (excl. China)",
            }
        )
    )

    # save the output to a file
    final_df.to_csv(Paths.output / "chart_1_2.csv", index=False)


if __name__ == "__main__":
    chart_1_1()
    chart_1_2()
