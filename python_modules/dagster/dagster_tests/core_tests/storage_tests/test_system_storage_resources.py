import pytest

from dagster import (
    DagsterInvalidDefinitionError,
    ModeDefinition,
    ResourceDefinition,
    execute_pipeline,
    pipeline,
    system_storage,
)
from dagster.core.definitions.system_storage import create_mem_system_storage_data


def test_resource_requirements_pass():
    called = {}

    @system_storage(required_resource_keys={'yup'})
    def storage_with_req(init_context):
        assert hasattr(init_context.resources, 'yup')
        assert not hasattr(init_context.resources, 'not_required')
        assert not hasattr(init_context.resources, 'kjdkfjdkfje')
        called['called'] = True
        return create_mem_system_storage_data(init_context)

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    'yup': ResourceDefinition.none_resource(),
                    'not_required': ResourceDefinition.none_resource(),
                },
                system_storage_defs=[storage_with_req],
            )
        ]
    )
    def resource_req_pass_pipeline():
        pass

    assert execute_pipeline(
        resource_req_pass_pipeline, environment_dict={'storage': {'storage_with_req': None}}
    ).success

    assert called['called']


def test_resource_requirements_fail():
    @system_storage(required_resource_keys={'yup'})
    def storage_with_req(init_context):
        return create_mem_system_storage_data(init_context)

    with pytest.raises(DagsterInvalidDefinitionError) as exc_info:

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    resource_defs={'nope': ResourceDefinition.none_resource()},
                    system_storage_defs=[storage_with_req],
                )
            ]
        )
        def _resource_req_pass_pipeline():
            pass

    assert str(exc_info.value) == (
        'Resource "yup" is required by system storagestorage_with_req, but '
        'is not provided by mode "default".'
    )
