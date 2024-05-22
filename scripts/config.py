import logging
from pathlib import Path


class Paths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    raw_data = project / "raw_data"
    output = project / "output"
    scripts = project / "scripts"
    dashboard = project / "dashboard" / "src" / "data"


CONSTANT_BASE_YEAR: int = 2022
PRICES_SOURCE: str = "imf"
ANALYSIS_YEARS: tuple = (2000, 2022)

# Create a root logger
logger = logging.getLogger(__name__)

# Create the terminal handler
shell_handler = logging.StreamHandler()

# Set levels for the logger, shell and file
logger.setLevel(logging.DEBUG)
shell_handler.setLevel(logging.DEBUG)

# Format the outputs
fmt_shell = "%(levelname)s %(message)s"

# Create formatters
shell_formatter = logging.Formatter(fmt_shell)

# Add formatters to handlers
shell_handler.setFormatter(shell_formatter)

# Add handlers to the logger
logger.addHandler(shell_handler)
