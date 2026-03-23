#!/usr/bin/env python3
from typing import Any, List, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    """Abstract base class with core streaming functionality."""

    def __init__(self, stream_id: str, stream_type: str) -> None:
        self.stream_id = stream_id
        self.stream_type: str = stream_type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data"""
        pass
    
    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
        ) -> List[Any]:
        """Filter data based on criteria."""
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        return {"status": "no stats"}


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
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
                count = len(readings)
                if temps:
                    avg_temp = sum(temps) / len(temps)
                    return (f"Sensor Analysis: {count} readings processed,"
                            f" avg temp: {avg_temp}°C")
                else:
                    return (f"Sensor Analysis: {count} readings processed,"
                            f" no temperature data found")  
            else:
                return "Sensor analysis: 0 readings processed"
        except (ValueError, TypeError, IndexError):
            return "Error: Sensor batch contains invalid data"


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")

    def process_batch(self, data_batch: List[Any]) -> str:
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
                count = len(buying) + len(selling)
                net = sum(buying) - sum(selling)
                if sum(buying) > sum(selling):
                    return (f"Transaction analysis: {count} operations,"
                            f" net flow: +{net} units")
                elif sum(buying) == sum(selling):
                    return (f"Transaction analysis: {count} operations,"
                            f" net flow: 0 units")
                else:
                    return (f"Transaction analysis: {count} operations,"
                            f" net flow: {net} units")
        except (ValueError, TypeError, IndexError):
            return "Error: Transaction batch contains invalid data"


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("Error: Event data must be a list")
        try:
            error_count = 0
            for reading in data_batch:
                if "error" in str(reading):
                    error_count += 1
            count = len(data_batch)
            if error_count == 1:
                return (f"Event analysis: {count} events,"
                        f" {error_count} error detected")
            else:
                return (f"Event analysis: {count} events,"
                        f" {error_count} errors detected")
        except (ValueError, TypeError, IndexError):
            return "Error: Events batch contains invalid data"


class StreamProcessor:
    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)
    
    def process_all(self, batch: list[Any]) -> None:
        criteria

def main() -> None:
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


if __name__ == "__main__":
    main()
