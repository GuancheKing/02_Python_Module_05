#!/usr/bin/env python3
from typing import Any, List, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    """Define the common interface for all data stream handlers."""

    def __init__(self, stream_id: str, stream_type: str) -> None:
        """Initialize a stream with its identifier and domain type."""
        self.stream_id = stream_id
        self.stream_type: str = stream_type
        self.count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return a summary string."""
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter a batch using a simple text-based criteria."""
        if not criteria:
            return data_batch
        return [item for item in data_batch if str(criteria) in str(item)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return basic statistics for the current stream."""
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "processed_items": self.count
        }


class SensorStream(DataStream):
    """Handle environmental sensor readings and temperature alerts."""

    def __init__(self, stream_id: str) -> None:
        """Initialize a sensor stream with its stream identifier."""
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor readings and report temperature statistics."""
        if not isinstance(data_batch, list):
            raise ValueError("Error: Sensor data must be a list")
        try:
            readings = []
            for reading in data_batch:
                if ":" in str(reading):
                    value_part = str(reading).split(":", 1)[1]
                    readings.append(float(value_part))
                else:
                    readings.append(float(reading))
            temps = []
            for reading in data_batch:
                if "temp:" in str(reading):
                    temp_value = str(reading).split(":", 1)[1]
                    temps.append(float(temp_value))
            if readings:
                self.count = len(readings)
                if temps:
                    avg_temp = sum(temps) / len(temps)
                    if avg_temp > 39:
                        return (f"Sensor analysis: {self.count} readings "
                                f"processed, avg temp: {avg_temp}°C [ALERT:"
                                " high temperature]")
                    else:
                        return (f"Sensor analysis: {self.count} readings "
                                f"processed, avg temp: {avg_temp}°C")
                else:
                    return (f"Sensor analysis: {self.count} readings processed"
                            f", no temperature data found")
            else:
                return "Sensor analysis: 0 readings processed"
        except (ValueError, TypeError, IndexError):
            return "Error: Sensor batch contains invalid data"


class TransactionStream(DataStream):
    """Handle financial transactions and calculate net flow."""

    def __init__(self, stream_id: str) -> None:
        """Initialize a transaction stream with its stream identifier."""
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process buy and sell operations and return a net flow summary."""
        if not isinstance(data_batch, list):
            raise ValueError("Error: Transaction data must be a list")
        try:
            buying = []
            selling = []
            for reading in data_batch:
                if "buy:" in str(reading):
                    buy_value = str(reading).split(":", 1)[1]
                    buying.append(int(buy_value))
                elif "sell:" in str(reading):
                    sell_value = str(reading).split(":", 1)[1]
                    selling.append(int(sell_value))
            if not buying and not selling:
                return "Transaction analysis: 0 operations processed"
            else:
                self.count = len(buying) + len(selling)
                net = sum(buying) - sum(selling)
                if sum(buying) > sum(selling):
                    return (f"Transaction analysis: {self.count} operations,"
                            f" net flow: +{net} units")
                elif sum(buying) == sum(selling):
                    return (f"Transaction analysis: {self.count} operations,"
                            f" net flow: 0 units")
                else:
                    return (f"Transaction analysis: {self.count} operations,"
                            f" net flow: {net} units")
        except (ValueError, TypeError, IndexError):
            return "Error: Transaction batch contains invalid data"


class EventStream(DataStream):
    """Handle system events and count relevant event categories."""

    def __init__(self, stream_id: str) -> None:
        """Initialize an event stream with counters for event categories."""
        super().__init__(stream_id, "System Events")
        self.error_count: int = 0
        self.login_count: int = 0
        self.logout_count: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process system events and report detected errors."""
        if not isinstance(data_batch, list):
            raise ValueError("Error: Event data must be a list")
        try:
            self.error_count = 0
            self.login_count = 0
            self.logout_count = 0
            for reading in data_batch:
                if "error" in str(reading):
                    self.error_count += 1
                elif "login" in str(reading):
                    self.login_count += 1
                elif "logout" in str(reading):
                    self.logout_count += 1
            self.count = len(data_batch)
            if self.error_count == 1:
                return (f"Event analysis: {self.count} events,"
                        f" {self.error_count} error detected")
            else:
                return (f"Event analysis: {self.count} events,"
                        f" {self.error_count} errors detected")
        except (ValueError, TypeError, IndexError):
            return "Error: Events batch contains invalid data"


class StreamProcessor:
    """Manage multiple data streams through a shared interface."""

    def __init__(self):
        """Initialize an empty registry of data streams."""
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Register a new stream in the processor."""
        self.streams.append(stream)

    def process_all(self, batches: List[List[Any]]) -> List[str]:
        """Process one batch per registered stream and collect the results."""
        results: List[str] = []
        if len(self.streams) != len(batches):
            raise ValueError("Number of streams and batches must match")
        for stream, batch in zip(self.streams, batches):
            print(f"\nProcessing {stream.stream_type.lower()}"
                  f" batch: {batch}")
            result = stream.process_batch(batch)
            results.append(result)
            print(result)
        return results


def main() -> None:
    """Run a demonstration of the polymorphic stream system."""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_data = [
        "temp:22.5", "humidity:65", "pressure:1013"
        ]
    sensor_result = sensor.process_batch(sensor_data)
    print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
    print(sensor_result)

    print("\nInitializing Transaction Stream...")
    trans = TransactionStream("TRANS_001")
    print(f"Stream ID: {trans.stream_id}, Type: {trans.stream_type}")
    trans_data = ["buy:100", "sell:150", "buy:75"]
    trans_result = trans.process_batch(trans_data)
    print("Processing transaction batch: [buy:100, sell:150, buy:75]")
    print(trans_result)

    print("\nInitializing Event Stream...")
    events = EventStream("EVENT_001")
    print(f"Stream ID: {events.stream_id}, Type: {events.stream_type}")
    event_data = ["login", "error", "logout"]
    event_result = events.process_batch(event_data)
    print("Processing event batch: [login, error, logout]")
    print(event_result)

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(trans)
    processor.add_stream(events)
    processor.process_all([sensor_data, trans_data, event_data])

    print("\nBatch 1 Results:")
    print(f"- Sensor data: {sensor.count} readings processed")
    print(f"- Transaction data: {trans.count} operations processed")
    print(f"- Event data: {events.count} events processed")

    print("\n=== Stream Filtering Demo ===")

    filtered_sensor = sensor.filter_data(sensor_data, "temp")
    print(f"Filtered sensor data: {filtered_sensor}")

    filtered_trans = trans.filter_data(trans_data, "buy")
    print(f"Filtered transaction data: {filtered_trans}")

    filtered_events = events.filter_data(event_data, "error")
    print(f"Filtered event data: {filtered_events}")

    print("\n=== Stream Statistics ===")
    for stream in processor.streams:
        print(stream.get_stats())

    print("\nAll streams processed successfully.")


if __name__ == "__main__":
    main()
