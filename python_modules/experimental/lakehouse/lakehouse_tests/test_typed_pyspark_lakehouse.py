from dagster import ModeDefinition, resource, file_relative_path
from dagster_pyspark import spark_session_resource

from pyspark.sql import Row, DataFrame as SparkDF, types as spark_types

from lakehouse import (
    InMemTableHandle,
    pyspark_mem_lakehouse_resource,
    lakehouse_table,
    lakehouse_table_input_def,
    PySparkMemLakeHouse,
    construct_lakehouse_pipeline,
)

# required in order for both dagit load to work and test case
import sys, os

sys.path.insert(0, os.path.abspath(file_relative_path(__file__, '.')))

from common import execute_spark_lakehouse_build


NUMBER_TABLE_STRUCT_TYPE = spark_types.StructType(
    [
        spark_types.StructField('id', spark_types.IntegerType()),
        spark_types.StructField('number', spark_types.IntegerType()),
    ]
)


def create_column_descriptions(spark_type):
    buildme = 'Columns:\n\n    '
    parts = []
    for spark_field in spark_type.fields:
        parts.append(
            '{name}: {type_name}'.format(
                name=spark_field.name, type_name=spark_field.dataType.typeName()
            )
        )
    return buildme + '\n    '.join(parts)


@lakehouse_table(
    metadata={'spark_type': NUMBER_TABLE_STRUCT_TYPE},
    description=create_column_descriptions(NUMBER_TABLE_STRUCT_TYPE),
)
def NumberTable(context) -> SparkDF:
    return context.resources.spark.createDataFrame([Row(id=1, number=2)], NUMBER_TABLE_STRUCT_TYPE)


STRING_TABLE_STRUCT_TYPE = spark_types.StructType(
    [
        spark_types.StructField('id', spark_types.IntegerType()),
        spark_types.StructField('string', spark_types.StringType()),
    ]
)


@lakehouse_table(
    metadata={'spark_type': STRING_TABLE_STRUCT_TYPE},
    description=create_column_descriptions(STRING_TABLE_STRUCT_TYPE),
)
def StringTable(context) -> SparkDF:
    return context.resources.spark.createDataFrame(
        [Row(id=1, string='23')], STRING_TABLE_STRUCT_TYPE
    )


JOIN_TABLE_STRUCT_TYPE = spark_types.StructType(
    [
        spark_types.StructField('id', spark_types.IntegerType()),
        spark_types.StructField('number', spark_types.IntegerType()),
        spark_types.StructField('string', spark_types.StringType()),
    ]
)


@lakehouse_table(
    input_defs=[
        lakehouse_table_input_def('number_df', NumberTable),
        lakehouse_table_input_def('string_df', StringTable),
    ],
    metadata={'spark_type': JOIN_TABLE_STRUCT_TYPE},
    description=create_column_descriptions(JOIN_TABLE_STRUCT_TYPE),
)
def JoinTable(_context, number_df: SparkDF, string_df: SparkDF) -> SparkDF:
    return number_df.join(string_df, number_df.id == string_df.id, 'inner').drop(string_df.id)


class TypedPySparkMemLakeHouse:
    def __init__(self):
        self.collected_tables = {}

    def hydrate(self, _context, _table_type, _table_metadata, table_handle):
        return table_handle.value

    def materialize(self, _context, table_type, _table_metadata, value):
        self.collected_tables[table_type.name] = value.collect()
        return None, InMemTableHandle(value=value)


@resource
def typed_pyspark_mem_lakehouse(_):
    return TypedPySparkMemLakeHouse()


def test_execute_typed_in_mem_lakehouse():
    lakehouse = TypedPySparkMemLakeHouse()
    pipeline_result = execute_spark_lakehouse_build(
        tables=[NumberTable, StringTable, JoinTable], lakehouse=lakehouse
    )

    assert pipeline_result.success
    assert lakehouse.collected_tables['JoinTable'] == [Row(id=1, number=2, string='23')]


typed_lakehouse_pipeline = construct_lakehouse_pipeline(
    name='typed_lakehouse_pipeline',
    lakehouse_tables=[NumberTable, StringTable, JoinTable],
    mode_defs=[
        ModeDefinition(
            resource_defs={
                'lakehouse': typed_pyspark_mem_lakehouse,
                'spark': spark_session_resource,
            }
        )
    ],
)


def define_typed_lakehouse_pipeline():
    return typed_lakehouse_pipeline
