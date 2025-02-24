from dagster_examples.intro_tutorial.composition import (
    compute_two,
    compute_three,
    multiple_outputs,
    complex_pipeline,
    composed_pipeline,
)
from dagster import execute_pipeline


def test_pipelines_execute():

    assert execute_pipeline(compute_two).success
    assert execute_pipeline(compute_three).success
    assert execute_pipeline(multiple_outputs).success
    assert execute_pipeline(complex_pipeline).success
    assert execute_pipeline(composed_pipeline).success
