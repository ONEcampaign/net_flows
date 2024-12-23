# Outputs

This directory contains the outputs of the analysis.


## Research outputs

### Inflows, outflows and net flows

- [`full_flows_country.parquet`](full_flows_country.parquet): Data on inflows and outflows by country and
counterpart. Presented yearly, including continent and income level.
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`full_flows_grouping.parquet`](full_flows_country.parquet): Data on inflows and outflows by country groupings and
counterpart. Presented yearly.
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`net_flows_country.parquet`](full_flows_country.parquet): Data on net flows (inflows - outflows) by country and
counterpart. Presented yearly, including continent and income level.
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`net_flows_grouping.parquet`](full_flows_country.parquet): Data on net flows (inflows - outflows) by country groupings and
counterpart. Presented yearly.
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`summary_flows_country.parquet`](summary_flows_country.parquet): Data broken down by country, for all counterpart_areas (total). 
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`summary_flows_grouping.parquet`](summary_flows_grouping.parquet): Data broken down by country groupings, 
for all counterpart_areas (total). 
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`summary_net_flows_country.parquet`](summary_net_flows_country.parquet): Data broken down by country, for all counterpart_areas, 
presented as net flows (inflows - outflows). It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`summary_net_flows_grouping.parquet`](summary_net_flows_grouping.parquet): Data broken down by country groupings,
for all counterpart_areas, presented as net flows (inflows - outflows). 
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).

All of the files above have an additional version ending in `_china_as_counterpart_type`, which
separates inflows and outflows from China as a counterpart type, from the rest of the data.

### Net negative flows
- [`net_negative_flows_country.parquet`](net_negative_flows_country.parquet): Data on net
negative flows (inflows - outflows < 0) by country (for all counterparts total). Presented yearly, including continent and income level.
It is created by the script [negative_net_flows.py](../scripts/analysis/negative_net_flows.py).
- [`net_negative_flows_group.parquet`](net_negative_flows_country.parquet): Data on net
  negative flows (inflows - outflows < 0) by country groupings (for all counterparts total). Presented yearly. 
  It is created by the script [negative_net_flows.py](../scripts/analysis/negative_net_flows.py).

### Projections
- [`net_flow_projections_country.parquet`](net_flow_projections_country.parquet): Data on projected
net flows, by country.
It is created by the script [net_flow_projections.py](../scripts/analysis/net_flow_projections.py).
- [`net_flow_projections_group.parquet`](net_flow_projections_group.parquet): ata on projected
  net flows, by country grouping. Presented yearly.
  It is created by the script [net_flow_projections.py](../scripts/analysis/net_flow_projections.py).


## Data viz outputs
- [`scatter_totals.csv`](scatter_totals.csv): Total inflows and outflows by country, as a share of GDP.
  Structured for an Observable scatter plot. It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`avg_repayments.csv`](avg_repayments.csv): Average yearly repayments. Structured for a flourish
  bar chart. It is created by the script [debt_service.py](../scripts/analysis/debt_service.py).
- [`avg_repayments_china.csv`](avg_repayments.csv): Average yearly repayments, highlighting China.
  Structured for a flourish bar chart. It is created by the script [debt_service.py](../scripts/analysis/debt_service.py).
- [`chart_1_1](chart_1_1.csv): Data for chart 1.1. Net flows for countries and groups
- [`chart_1_2](chart_1_2.csv): Data for chart 1.2. Inflows for countries and groups
- [`chart_2_1`](chart_2_1.csv): Data for chart 2.1. Net negative transfer countries (2022 and 2025). 
