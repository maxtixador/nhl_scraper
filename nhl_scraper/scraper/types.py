"""
NHL Scraper Type Definitions.

This module contains type definitions and aliases used throughout the NHL Scraper
package to ensure type safety and improve code maintainability.

Types:
    XPathResult: Type for lxml xpath query results
    ElementText: Type for optional element text content
    EventDict: Type for event processing results
"""

from typing import Callable, Dict, List, Optional, Tuple, TypeVar, Union

from lxml.etree import _Element
from pandas import DataFrame

# Type variables for generic types
T = TypeVar("T")

# Custom type aliases
XPathResult = Union[List[_Element], List[str], List[float], bool]
ElementText = Optional[str]
EventDict = Dict[str, Union[str, List[str]]]
GameId = Union[str, int]
DataFrameOutput = DataFrame

# Function types
PlayByPlayFunc = Callable[[GameId], DataFrameOutput]
GameCompleteFunc = Callable[[GameId], Tuple[DataFrameOutput, DataFrameOutput, DataFrameOutput]]
