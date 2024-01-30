# Net Flows Research

_Note: this project is still in progress. Data, methodology and outputs may change without warning._

This repository contains the code and data for ONE's research on the net flows (concessional and non-concessional)
received by developing countries.

You can explore the data and methodology in this [interactive notebook](https://observablehq.com/@one-campaign/net-flows)

## Requirements
This project requires python 3.10 or higher. Other dependencies are listed in `pyproject.toml`.

## Usage

- Use [inflows.py](scripts/data/inflows.py) to prepare the inflows data.
- Use [outflows.py](scripts/data/outflows.py) to prepare the outflows data.
- Use [net_flows.py](scripts/analysis/net_flows.py) to perform the analysis and generate the output files
`net_flows_full.csv` and `scatter_totals.csv`.

## Contributing
If you are interested in learning more about this project or contributing, please reach out via GitHub or email.

## Sources 
For this research, we use data from the [World Bank's International Debt Statistics (IDS)](https://databank.worldbank.org/source/international-debt-statistics) database and the OECD DAC [Creditor Reporting System (CRS)](https://stats.oecd.org/Index.aspx?DataSetCode=crs1) database.

