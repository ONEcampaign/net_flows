# Outputs

This directory contains the outputs of the analysis.


## Research outputs

- [`full_flows_country.parquet`](full_flows_country.parquet): Data on inflows and outflows by country and
counterpart. Presented yearly, including continent and income level.
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).

- [`full_flows_grouping.parquet`](full_flows_country.parquet): Data on inflows and outflows by country groupings and
counterpart. Presented yearly.
It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).



## Data viz outputs
- [`scatter_totals.csv`](scatter_totals.csv): Total inflows and outflows by country, as a share of GDP.
  Structured for an Observable scatter plot. It is created by the script [net_flows.py](../scripts/analysis/net_flows.py).
- [`avg_repayments.csv`](avg_repayments.csv): Average yearly repayments. Structured for a flourish
  bar chart. It is created by the script [debt_service.py](../scripts/analysis/debt_service.py).
- [`avg_repayments_china.csv`](avg_repayments.csv): Average yearly repayments, highlighting China.
  Structured for a flourish bar chart. It is created by the script [debt_service.py](../scripts/analysis/debt_service.py).