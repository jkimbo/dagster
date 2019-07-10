from dagster import RepositoryDefinition
from test_typed_pyspark_lakehouse import define_typed_lakehouse_pipeline


def lakehouse_test_repo():
    return RepositoryDefinition(
        name='lakehouse_test_repo', pipeline_defs=[define_typed_lakehouse_pipeline()]
    )

