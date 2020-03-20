from pyspark import SparkContext
from pyspark.sql import DataFrame
from enum import Enum
from typing import Callable

class SelectorInput(Enum):
    NONE = 1
    TABLE = 2

class SelectorOutput(Enum):
    NONE = 1
    TEMP_TABLE = 2
    PERSISTENT_TABLE = 3

class SelectorConf(object):
    name: str
    in_type: SelectorInput
    out_type: SelectorOutput
    arg_names: list = []
    def __init__(self, *args, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)

class Selector(object):
    name: str
    conf: SelectorConf
    arg_dict: dict
    spark_context: SparkContext    
    func: Callable[[], DataFrame]
    def __init__(self, conf: SelectorConf, func: Callable[[], DataFrame], sc: SparkContext):
        self.name = conf.name
        self.conf = conf
        self.func = func
        self.spark_context = sc
    def __repr(self):
        print_dict = {
            name: self.name,
            conf: self.conf,
            arg_dict: self.arg_dict,
            func: self.func.__name__
        }
        return json.dumps(print_dict)
