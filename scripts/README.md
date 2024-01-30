# Scripts

This directory contains scripts that are used to get and analyse the data.
- [data](./data/) contains scripts to get and prepare the data
- [analysis](./analysis/) contains scripts to analyse the data and generate the outputs

## Config
The [config.py](config.py) file contains the configuration for the project. 
It is used by the scripts in the `data` and `analysis` directories.

- Use `CONSTANT_BASE_YEAR` to change the base year for the analysis (when using constant prices).
- Use `ANALYSIS_YEARS` to change the years that are used for the analysis.
- Use `PRICES_SOURCE` to change the source of the prices data (to deflate to constant prices).

The logger can also be configured here.