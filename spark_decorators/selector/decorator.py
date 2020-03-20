from spark_decorators.selector.classes import SelectorConf, Selector, SelectorInput
from spark_decorators.selector.config import in_args, out_args, in_resolvers, out_resolvers
from pyspark.sql import DataFrame

selector_registry: dict = {}
spark_context = None

def selector(c: SelectorConf): 
    def selector_decorator(func):
        if spark_context:
            selector_registry[c.name] = Selector(c, func, spark_context)
        else:
            raise Exception("Spark context must be registered with register_spark_context")
    return selector_decorator

def execute_selector(name: str, **kwargs) -> DataFrame:
    s: Selector = selector_registry[name]
    return _execute_selector(s, **kwargs)

def _execute_selector(s: Selector, **kwargs) -> DataFrame:
    #if executed against an input dataframe and we want to curry
    if kwargs["in_df"]:
        s.conf.in_type = SelectorInput.DATAFRAME

    s.conf.arg_names.extend(in_args[s.conf.in_type.name])
    s.conf.arg_names.extend(out_args[s.conf.out_type.name])
    s.arg_dict = {**s.arg_dict, **kwargs}
    missing_args = [key for key, value in s.arg_dict if key not in s.conf.arg_names]

    if len(missing_args) > 0:
        raise Exception(f"Missing required arguments: {' '.join(missing_args)}")

    out_df: DataFrame

    if s.conf.in_type.NONE:
        out_df = s.func(**s.arg_dict)

    else:
        in_df: DataFrame
        if kwargs["in_df"]:
            in_df = kwargs["in_df"]
        else:
            in_df = in_resolvers[s.conf.in_type.name](s)
        out_df = s.func(in_df, **s.arg_dict)
        
    out_resolvers[s.conf.out_type.name](s, out_df)
    return out_df

def register_spark_context(sc):
    spark_context = sc

def display_selector(name):
    return str(selector_registry[name])