import sys

import pandas as pd

from dagster import execute_pipeline, pipeline, solid, ModeDefinition

from dagster_snowflake import snowflake_resource

if sys.version_info.major >= 3:
    import unittest.mock as mock
else:
    import mock


def create_mock_connector(*_args, **_kwargs):
    return connect_with_fetchall_returning(pd.DataFrame())


def connect_with_fetchall_returning(value):
    cursor_mock = mock.MagicMock()
    cursor_mock.fetchall.return_value = value
    snowflake_connect = mock.MagicMock()
    snowflake_connect.cursor.return_value = cursor_mock
    m = mock.Mock()
    m.return_value = snowflake_connect
    return m


@mock.patch('snowflake.connector.connect', new_callable=create_mock_connector)
def test_snowflake_resource(snowflake_connect):
    @solid(required_resource_keys={'snowflake'})
    def snowflake_solid(context):
        assert context.resources.snowflake
        with context.resources.snowflake.get_connection(context.log) as _:
            pass

    @pipeline(mode_defs=[ModeDefinition(resource_defs={'snowflake': snowflake_resource})])
    def test_pipeline():
        snowflake_solid()  # pylint: disable=no-value-for-parameter

    result = execute_pipeline(
        test_pipeline,
        {
            'resources': {
                'snowflake': {
                    'config': {
                        'account': 'foo',
                        'user': 'bar',
                        'password': 'baz',
                        'database': 'TESTDB',
                        'schema': 'TESTSCHEMA',
                        'warehouse': 'TINY_WAREHOUSE',
                    }
                }
            }
        },
    )
    assert result.success
    snowflake_connect.assert_called_once_with(
        account='foo',
        user='bar',
        password='baz',
        database='TESTDB',
        schema='TESTSCHEMA',
        warehouse='TINY_WAREHOUSE',
    )
