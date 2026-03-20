#!/usr/bin/env python3
from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    """An abstract base class defining the common processing interface."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the input data and return a result string."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if data is appropriate for this processor."""
        pass

    def format_output(self, result: str) -> str:
        """Format the output string."""
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """Process numeric lists."""

    def process(self, data: Any) -> str:
        """Process numeric data and return a summary string."""
        if not self.validate(data):
            return "Error, numeric data expected"

        count = len(data)
        total = sum(data)
        average = total / count

        result = (
            f"Processed {count} numeric values,"
            f" sum={total}, avg={average}"
            )

        return result

    def validate(self, data: Any) -> bool:
        """Validate that the input is a non-empty numeric list."""
        if not isinstance(data, list):
            return False
        if not data:
            return False
        return all(isinstance(each, (int, float)) for each in data)


class TextProcessor(DataProcessor):
    """Process text strings."""

    def process(self, data: Any) -> str:
        """Process text data and return character and word counts."""
        if not self.validate(data):
            return "Error, text data expected"

        count = len(data)
        words = len(data.split())

        result = (
            f"Processed text: {count} characters,"
            f" {words} words"
            )

        return result

    def validate(self, data: Any) -> bool:
        """Validate that the input is a string."""

        return isinstance(data, str)


class LogProcessor(DataProcessor):
    """Process formatted log entries."""

    def process(self, data: Any) -> str:
        """Process log data and detect its level."""
        if not self.validate(data):
            return "Error, log entry expected"

        parts = data.split(":", 1)
        level = parts[0]
        message = parts[1]
        if level == 'ERROR':
            result = (
                f"[ALERT] {level} level detected:"
                f"{message}"
                )
        elif level == 'INFO':
            result = (
                f"[INFO] {level} level detected:"
                f"{message}"
                )
        return result

    def validate(self, data: Any) -> bool:
        """Validate that the input is a supported log entry."""
        if not isinstance(data, str):
            return False
        return 'ERROR: ' in data or 'INFO: ' in data


def main() -> None:
    """Run the mandatory processor demonstrations."""

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    print("\nInitializing Numeric Processor...")
    num_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_data}")
    numeric = NumericProcessor()
    print("Validation: Numeric data verified")
    num_result = numeric.process(num_data)
    print(numeric.format_output(num_result))

    print("\nInitializing Text Processor...")
    text_data = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    text = TextProcessor()
    print("Validation: Text data verified")
    text_result = text.process(text_data)
    print(text.format_output(text_result))

    print("\nInitializing Log Processor...")
    log_data = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')
    log = LogProcessor()
    print("Validation: Log entry verified")
    log_result = log.process(log_data)
    print(log.format_output(log_result))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    processors = [NumericProcessor(), TextProcessor(), LogProcessor()]
    inputs = [[1, 2, 3], "Hello World!", "INFO: System ready"]
    for processor, data in zip(processors, inputs):
        result = processor.process(data)
        print(processor.format_output(result))

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
