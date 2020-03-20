import spark_decorators.selector as S
import json
from typing import Dict

class Stage(object):
    name: str 
    arguments: dict
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments


class Plan(object):
    name: str
    stages: Dict[int, Stage] = {}
    def __init__(self, name: str):
        self.name = name
        
    def __repr__(self):
        return json.dumps({
            "name": self.name,
            "stages": self.stages
        })

    def add_stage(self, name: str, arguments: dict):
        stage = Stage(name, arguments)
        self.stages[len(self.stages)+1] = stage

    def execute(self, **kwargs) -> DataFrame:
        stage_numbers = []
        if kwargs["stages"]: 
            stage_numbers = kwargs["stages"]
        else:
            stage_numbers = range(1,len(self.stages)+1)
        stage_dataframes = {}
        for idx, stage_number in enumerate(stage_numbers):
            stage = self.stages[stage_number]
            stage_selector = S.selector_registry[stage.name]
            if stage and S.selector_registry[stage.name]:
                if stage_selector.conf.in_type == S.SelectorInput.NONE:
                    stage_dataframes[stage_number] = S._execute_selector(stage_selector)
                else:
                    last_stage_df = stage_dataframes[stage_numbers[idx-1]]
                    stage_dataframes[stage_number] = S._execute_selector(stage_selector, in_df = last_stage_df)
        return stage_dataframes[stage_numbers[-1]]
        


