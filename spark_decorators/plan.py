import spark_decorators.selector as S
import json

class Plan(object):
    name: str
    stages: dict = {}
    def __init__(self, name: str):
        self.name = name
        
    def __repr__(self):
        print_dict = {}
        for key, value in self.stages.items():
            print_dict[key] = str(value)
        return json.dumps(print_dict)

    def add_stage(self, s: S.Selector, arg_dict: dict):
        s.arg_dict = arg_dict 
        self.stages[len(self.stages)+1] = s

    def execute(self, **kwargs) -> DataFrame:
        stage_numbers = []
        if kwargs["stages"]: 
            stage_numbers = kwargs["stages"]
        else:
            stage_numbers = range(1,len(self.stages)+1)
        stage_dataframes = {}
        for idx, stage_number in enumerate(stage_numbers):
            stage_selector: S.Selector = self.stages[stage_number]
            if stage_selector:
                if stage_selector.conf.in_type == S.SelectorInput.NONE:
                    stage_dataframes[stage_number] = S._execute_selector(stage_selector)
                else:
                    in_df = stage_dataframes[stage_numbers[idx-1]]
                    stage_dataframes[stage_number] = S._execute_selector(stage_selector, in_df)
        return stage_dataframes[stage_numbers[-1]]
        


