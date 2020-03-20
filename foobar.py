from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from spark_decorators import *

import os
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3.7"
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3.7"

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

#define mapping with inputs and outputs using @selector decorator

@selector(SelectorConf(name = "foo", in_type = SelectorInput.NONE, out_type = SelectorOutput.TEMP_TABLE))
def foo(args_dict):
    data = [
        Row(foo="1"),
        Row(foo="2"),
        Row(foo="3"),
    ]
    return spark.createDataFrame(data)

@selector(SelectorConf(name = "bar", in_type = SelectorInput.TABLE, out_type = SelectorOutput.TEMP_TABLE))
def bar(in_df, args_dict):
    return in_df.withColumn('bar', F.col('foo')*2)

#print a manifest of selectors
print(foo)
print(bar)

#define a plan relating executors
plan = Plan(name="foobar", stages={
    1: Stage("foo", {
        "output_table": "tmp_foo"
     }),
    2: Stage("bar", {
        "input_table": "tmp_foo",
        "output_table": "tmp_bar",
        "spark_session": spark
     }),
})

#print plan
print(plan)
#execute plan
plan.execute().show()

#intemidiary tables are opt-in for plans
plan2 = Plan(name="foobar", stages={
    1: Stage("foo", {
     }),
    2: Stage("bar", {
     }),
})

print(plan2)
plan2.execute().show()

#execute individual selectors
execute_selector("foo", output_table="tmp_foo")
spark.sql("SELECT * FROM tmp_foo").show()

execute_selector("bar", input_table = "tmp_foo", output_table="tmp_bar", spark_session=spark)
spark.sql("SELECT * FROM tmp_bar").show()
