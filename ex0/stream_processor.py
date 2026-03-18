#!/usr/bin/env python3
from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    """An abstract base class defining the common processing interface."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data and return result string"""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """alidate if data is appropriate for this processor"""
        pass
    
    def format_output(self, result: str) -> str:
        """Format the output string"""
        return result
    

class NumericProcessor(DataProcessor):


class TextProcessor(DataProcessor):


class LogProcessor(DataProcessor):