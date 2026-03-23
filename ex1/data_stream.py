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
            raise ValueError("Error data must be a list")
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

#    def process_batch(self, data_batch: List[Any]) -> str:


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

#    def process_batch(self, data_batch: List[Any]) -> str:

class TestStream(DataStream):
    pass


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_data = [
        "temp:22.5", "humidity:65", "pressure:1013"
        ]
    sensor_result = sensor.process_batch(sensor_data)
    print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
    print(sensor_result)

    print("\nInitializing Transaction Stream...")


if __name__ == "__main__":
    main()
