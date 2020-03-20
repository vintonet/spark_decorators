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
    args: list = []
    def __init__(self, *args, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)


class Selector(object):
    name: str
    conf: SelectorConf
    spark_context: SparkContext    
    func: Callable[[], DataFrame]
    def __init__(self, conf: SelectorConf, func, sc: SparkContext):
        self.name = conf.name
        self.conf = conf
        self.func = func
        self.spark_context = sc

selector_registry: dict = {}

in_args = {
    SelectorInput.NONE.value: [],
    SelectorInput.TABLE.value: ["input_table"]
}
out_args = {
    SelectorOutput.NONE.value: [],
    SelectorOutput.TEMP_TABLE.value: ["output_table"],
    SelectorOutput.PERSISTENT_TABLE.value: ["output_table", "output_path"]
}
in_resolvers = {
    SelectorInput.NONE.value: lambda s: None,
    SelectorInput.TABLE.value: lambda s: s.spark_context.sql(f"SELECT * FROM {s.conf.properties['input_table']}")
}
out_resolvers = {
    SelectorOutput.NONE.value: lambda s, df: None,
    SelectorOutput.TEMP_TABLE.value: lambda s, df: df.createOrReplaceTempView(s.conf.properties['output_table']),
    SelectorOutput.PERSISTENT_TABLE.value: lambda s, df: df.saveAsTable(s.conf.properties['output_table'], mode="overwrite", path=s.conf.properties['output_path'])
}

spark_context = None

def selector(c: SelectorConf): 
    def selector_decorator(func):
        if spark_context:
            #business logic to allow
            selector_registry[c.name] = Selector(c, func, spark_context)
        else:
            raise Exception("Spark context must be registered with register_spark_context")
    return selector_decorator

def execute_selector(name: str, **kwargs):
    s: Selector = selector_registry[name]
    s.conf.args.extend(in_args[s.conf.in_type.value])
    s.conf.args.extend(out_args[s.conf.out_type.value])
    missing_kwargs = [key for key, value in kwargs if key not in s.conf.arguments]
    if len(missing_kwargs) > 0:
        raise Exception(f"Missing required arguments: {' '.join(missing_kwargs)}")
    in_df = in_resolvers[s.conf.in_type.value](s)
    out_df = s.func(in_df, kwargs)
    out_resolvers[s.conf.out_type.value](s, out_df)
    return out_df

def register_spark_context(sc):
    spark_context = sc