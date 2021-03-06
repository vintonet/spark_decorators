from pyspark.sql import DataFrame
from enum import Enum
from typing import Callable, Dict, List

class SelectorInput(Enum):
    NONE = 1
    TABLE = 2
    DATAFRAME = 3
    

class SelectorOutput(Enum):
    NONE = 1
    TEMP_TABLE = 2
    PERSISTENT_TABLE = 3

class SelectorConf(object):
    name: str
    in_type: SelectorInput
    out_type: SelectorOutput
    arg_names: List[str] = []
    def __init__(self, *args, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)

class Selector(object):
    name: str
    conf: SelectorConf
    arg_dict: Dict[str, object] = {}
    func: Callable[[], DataFrame]

    def __init__(self, conf: SelectorConf, func: Callable[[], DataFrame]):
        self.name = conf.name
        self.conf = conf
        self.func = func
        
    def __repr(self):
        print_dict = {
            name: self.name,
            conf: self.conf,
            arg_dict: self.arg_dict,
            func: self.func.__name__
        }
        return json.dumps(print_dict)