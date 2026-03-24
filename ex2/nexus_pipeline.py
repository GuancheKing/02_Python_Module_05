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
        if isinstance(data, dict) and data.get("sensor") == "temp":
            try:
                temp_value = float(data.get("value"))
            except ValueError:
                raise ValueError (
                    "Temperature value must be of type float/int"
                )
            unit = data.get("unit")
            if 10 < temp_value < 39:
                data.update({"range": "Normal range"})
            else:
                data.update({"range": "Temp is out of range"})
            return data
        if isinstance(data, str) and "action" in data and "user" in data:

        # print("Transform: Enriched with metadata and validation")
        # # elif isinstance(data, str) and "temp" in data:
        # #     print("Transform: Parsed CSV sensor values")
        # elif "user" in str(data) and "action" in str(data):
        #     print("Transform: Parsed and structured data")
        # else:
        #     print("Transform: Aggregated and filtered")
        return data


class OutputStage:

    def process(self, data: Any) -> Any:
        return ("placeholder str")
        


class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: list[ProcessingStage] = []
        self.processed_count: int = 0
        self.error_count: int = 0

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

        # labels = {
        #     "InputStage": "Input validation and parsing",
        #     "TransformStage": "Data transformation and enrichment",
        #     "OutputStage": "Output formatting and delivery"
        # }
        # print(
        #     f"Stage {len(self.stages)}: {labels.get(stage.__class__.__name__)}"
        # )

    @abstractmethod
    def process(self, data: Any) -> Any:
        current_data = data
        try:
            for stage in self.stages:
                current_data = stage.process(current_data)
            return current_data
        except Exception as e:
            return self.handle_recovery(e, data)

    def handle_recovery(self, error: Exception, data: Any) -> Any:
        print(f"Error detected in Stage xx : {error}")
        print("Recovery initiated: Switching to backup processor")
        print("Recovery successful: Pipeline restored, processing resumed")
        return data


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
    
    def proccess(self, data: Any) -> Any:
