#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol


class ProcessingStage(Protocol):

    def process(self, data: Any) -> Any:
        ...


class InputStage:

    def process(self, data: Any) -> Any:
        if not data or data == "":
            raise ValueError("Empty data")
        return data


class TransformStage:

    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("TransformStage expects a dictionary")
        
        kind = data.get("kind")

        # TEMPERATURE
        if kind == "temperature":
            value = data.get("value")
            unit = data.get("unit")
            if value is None or unit is None:
                raise ValueError("Missing temperature data")
            try:
                temp_value = float(value)
            except (ValueError, TypeError):
                raise ValueError("Temperature value must be numeric")
            if 10 < temp_value < 39:
                data["range"] = "Normal range"
            else:
                data["range"] = "Temp is out of range"
            return data
        
        # USER ACTIVITY
        if kind == "activity":
            user = data.get("user")
            action = data.get("action")
            if not user or not action:
                raise ValueError("Invalid user activity data")
            data["actions_count"] = 1  # simple caso base
            return data

        # STREAM
        if kind == "stream":
            readings = data.get("readings")
            if not isinstance(readings, list) or not readings:
                raise ValueError("Invalid stream data")
            try:
                avg = sum(readings) / len(readings)
            except Exception:
                raise ValueError("Invalid readings for stream")
            data["average"] = round(avg, 2)
            data["count"] = len(readings)
            return data
        
        # UNKNOWN
        raise ValueError("Unsupported data kind")


class OutputStage:

    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("OutputStage expects a dictionary")
        
        kind = data.get("kind")
    
        # TEMPERATURE
        if kind == "temperature":
            value = data.get("value")
            unit = data.get("unit")
            range_status = data.get("range")

            if value is None or unit is None or range_status is None:
                raise ValueError("Incomplete temperature data for output")

            return f"Processed temperature reading: {value}°{unit} ({range_status})"
        
        # USER ACTIVITY
        if kind == "activity":
            user = data.get("user")
            action = data.get("action")
            count = data.get("actions_count")

            if not user or not action or count is None:
                raise ValueError("Incomplete activity data for output")

            return f"User activity logged: {count} actions processed"
        
        # STREAM
        if kind == "stream":
            avg = data.get("average")
            count = data.get("count")

            if avg is None or count is None:
                raise ValueError("Incomplete stream data for output")

            return f"Stream summary: {count} readings, avg: {avg}°C"

        # UNKNOWN
        raise ValueError("Unsupported data kind")


class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: list[ProcessingStage] = []
        self.processed_count: int = 0
        self.error_count: int = 0

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        current_data = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data
    
    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)
    
    def process(self, data: Any) -> Any:

        
        # if isinstance(data, dict) and data.get("sensor") == "temp":
        #     try:
        #         temp_value = float(data.get("value"))
        #     except ValueError:
        #         raise ValueError (
        #             "Temperature value must be of type float/int"
        #         )
        #     new_data = data
        #     if 10 < temp_value < 39:
        #         new_data.update({"range": "Normal range"})
        #     else:
        #         new_data.update({"range": "Temp is out of range"})
        #     return new_data
        if isinstance(data, str) and "action" in data and "user" in data:

        # print("Transform: Enriched with metadata and validation")
        # # elif isinstance(data, str) and "temp" in data:
        # #     print("Transform: Parsed CSV sensor values")
        # elif "user" in str(data) and "action" in str(data):
        #     print("Transform: Parsed and structured data")
        # else:
        #     print("Transform: Aggregated and filtered")
        return data

