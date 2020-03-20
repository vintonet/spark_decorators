import spark_decorators.selector as S
import json
from typing import Dict
from pyspark.sql import DataFrame

class Stage(object):
    name: str 
    arguments: dict
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments


class Plan(object):
    name: str
    stages: Dict[int, Stage] = {}
    dataframe_cache: Dict[int, DataFrame] = {}

    def __init__(self, name: str, stages: Dict[int, Stage]):
        self.name = name
        self.stages = stages
        
    def __repr__(self):
        return json.dumps({
            "name": self.name,
            "stages": self.stages
        })

    def execute(self, **kwargs) -> DataFrame:
        stage_numbers = []

        if kwargs["stages"]: 
            stage_numbers = kwargs["stages"]
        else:
            stage_numbers = range(1,len(self.stages)+1)

        for idx, stage_number in enumerate(stage_numbers):
            stage = self.stages[stage_number]
            stage_selector = S.selector_registry[stage.name]

            if stage and S.selector_registry[stage.name]:
                if stage_selector.conf.in_type == S.SelectorInput.NONE:
                    self.dataframe_cache[stage_number] = S._execute_selector(stage_selector)
                else:
                    last_stage_df = self.dataframe_cache[stage_numbers[idx-1]]
                    self.dataframe_cache[stage_number] = S._execute_selector(stage_selector, in_df = last_stage_df)
                    
        return self.dataframe_cache[stage_numbers[-1]]