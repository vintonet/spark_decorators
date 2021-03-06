from .selector_classes import SelectorInput, SelectorOutput

in_args = {
    SelectorInput.NONE.name: [],
    SelectorInput.TABLE.name: ["input_table", "spark_session"],
    SelectorInput.DATAFRAME.name: ["in_df"]
}
out_args = {
    SelectorOutput.NONE.name: [],
    SelectorOutput.TEMP_TABLE.name: ["output_table"],
    SelectorOutput.PERSISTENT_TABLE.name: ["output_table", "output_path"]
}
in_resolvers = {
    SelectorInput.NONE.name: lambda s: None,
    SelectorInput.TABLE.name: lambda s: s.arg_dict['spark_session'].sql(f"SELECT * FROM {s.arg_dict['input_table']}"),
    SelectorInput.DATAFRAME.name: lambda s: None,
}
out_resolvers = {
    SelectorOutput.NONE.name: lambda s, df: None,
    SelectorOutput.TEMP_TABLE.name: lambda s, df: df.createOrReplaceTempView(s.arg_dict['output_table']),
    SelectorOutput.PERSISTENT_TABLE.name: lambda s, df: df.saveAsTable(s.arg_dict['output_table'], mode="overwrite", path=s.arg_dict['output_path'])
}