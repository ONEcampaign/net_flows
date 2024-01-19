# Data
This module contains scripts to get and process the data used in the analysis. 

## inflows.py
This script downloads and processes:
- Debt inflows from the World Bank's [International Debt Statistics](https://datacatalog.worldbank.org/dataset/international-debt-statistics) database. 
- Grant inflows from the OECD DAC Table2a database.

The data is saved in the `raw_data` folder. The processed data is saved in the `output` folder.

## outflows.py
This script downloads and processes:
- Debt service outflows from the World Bank's [International Debt Statistics](https://datacatalog.worldbank.org/dataset/international-debt-statistics) database.

The data is saved in the `raw_data` folder. The processed data is saved in the `output` folder.