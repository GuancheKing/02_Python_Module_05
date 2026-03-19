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
        """Validate if data is appropriate for this processor"""
        pass
    
    def format_output(self, result: str) -> str:
        """Format the output string"""
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:

        print(f"Processing data: {data}")

        if not self.validate(data):
            return "Error, numeric data expected"

        print("Validation: Numeric data verified")

        count = len(data)
        total = sum(data)
        average = total / count

        result = (f"Processed {count} numeric values,"
        f" sum={total}, avg={average}")

        return result

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        if not data:
            return False
        return all(isinstance(each, (int, float)) for each in data)


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:

        print(f'Processing data: "{data}"')

        if not self.validate(data):
            return "Error, text data expected"

        print("Validation: Text data verified")

        count = len(data)
        words = len(data.split())

        result = (f"Processed text: {count} characters,"
        f" {words} words")

        return result
    
    def validate(self, data: Any) -> bool:

        return isinstance(data, str)


# class LogProcessor(DataProcessor):


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    print("\nInitializing Numeric Processor...")
    num_data = [1, 2, 3, 4, 5]
    numeric = NumericProcessor()
    num_result = numeric.process(num_data)
    print(numeric.format_output(num_result))

    print("\nInitializing Text Processor...")
    text_data = "Hello Nexus World"
    text = TextProcessor()
    text_result = text.process(text_data)
    print(text.format_output(text_result))

if __name__ == "__main__":
    main()
