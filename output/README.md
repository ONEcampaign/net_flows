# Outputs

This directory contains the outputs of the analysis.


## Research outputs

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



## Data viz outputs
- [`scatter_totals.csv`](scatter_totals.csv): Total inflows and outflows by country, as a share of GDP.
  Structured for an Observable scatter plot. It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`avg_repayments.csv`](avg_repayments.csv): Average yearly repayments. Structured for a flourish
  bar chart. It is created by the script [debt_service.py](../scripts/analysis/debt_service.py).
- [`avg_repayments_china.csv`](avg_repayments.csv): Average yearly repayments, highlighting China.
  Structured for a flourish bar chart. It is created by the script [debt_service.py](../scripts/analysis/debt_service.py).