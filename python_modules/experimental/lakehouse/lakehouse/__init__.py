from collections import defaultdict

from dagster import (
    Any,
    DependencyDefinition,
    InputDefinition,
    Output,
    OutputDefinition,
    PipelineDefinition,
    SolidDefinition,
    check,
    define_python_dagster_type,
    execute_solid,
    resource,
)


class ITableHandle:
    pass


class TableHandle(ITableHandle):
    pass


class InMemTableHandle(ITableHandle):
    def __init__(self, value):
        self.value = value


def table_def_of_type(pipeline_def, type_name):
    for solid_def in pipeline_def.solid_defs:
        if (
            isinstance(solid_def, LakehouseTableDefinition)
            and solid_def.table_type.name == type_name
        ):
            return solid_def


class LakehouseTableDefinition(SolidDefinition):
    def __init__(self, lakehouse_fn, **kwargs):
        self.lakehouse_fn = lakehouse_fn
        super(LakehouseTableDefinition, self).__init__(**kwargs)

    @property
    def table_type(self):
        return self.output_defs[0].runtime_type


def _create_lakehouse_table_def(name, lakehouse_fn, input_defs, metadata=None, description=None):
    metadata = check.opt_dict_param(metadata, 'metadata')

    table_type = define_python_dagster_type(
        python_type=ITableHandle, name=name, description=description
    )

    table_type_inst = table_type.inst()

    input_def_dict = {input_def.name: input_def for input_def in input_defs}

    def _compute(context, inputs):
        # hydrate all things

        # TODO support non lakehouse table inputs
        hydrated_tables = {}
        for input_name, table_handle in inputs.items():
            context.log.info(
                'About to hydrate table {input_name} for use in {name}'.format(
                    input_name=input_name, name=name
                )
            )
            input_type = input_def_dict[input_name].runtime_type
            hydrated_tables[input_name] = context.resources.lakehouse.hydrate(
                context,
                input_type,
                table_def_of_type(context.pipeline_def, input_type.name).metadata,
                table_handle,
            )

        # call user-provided business logic which operates on the hydrated values
        # (as opposed to the handles)
        computed_output = lakehouse_fn(context, **hydrated_tables)

        materialization, output_table_handle = context.resources.lakehouse.materialize(
            context, table_type_inst, metadata, computed_output
        )

        if materialization:
            yield materialization

        # just pass in a dummy handle for now if the materialize function
        # does not return one
        yield Output(output_table_handle if output_table_handle else TableHandle())

    return LakehouseTableDefinition(
        lakehouse_fn=lakehouse_fn,
        name=name,
        input_defs=input_defs,
        output_defs=[OutputDefinition(table_type)],
        compute_fn=_compute,
        metadata=metadata,
        description=description,
    )


def lakehouse_table(name=None, input_defs=None, metadata=None, description=None):
    if callable(name):
        fn = name
        return _create_lakehouse_table_def(name=fn.__name__, lakehouse_fn=fn, input_defs=[])

    def _wrap(fn):
        return _create_lakehouse_table_def(
            name=name if name is not None else fn.__name__,
            lakehouse_fn=fn,
            input_defs=input_defs or [],
            metadata=metadata,
            description=description,
        )

    return _wrap


def lakehouse_table_input_def(name, lakehouse_table_def):
    check.str_param(name, 'name')
    check.inst_param(lakehouse_table_def, 'lakehouse_table_def', LakehouseTableDefinition)
    return InputDefinition(name, type(lakehouse_table_def.table_type))


def construct_lakehouse_pipeline(name, lakehouse_tables, mode_defs=None):
    solid_defs = lakehouse_tables
    type_to_solid = {}
    for solid_def in solid_defs:
        check.invariant(len(solid_def.output_defs) == 1)
        output_type_name = solid_def.output_defs[0].runtime_type.name
        check.invariant(output_type_name not in type_to_solid)
        type_to_solid[output_type_name] = solid_def

    dependencies = defaultdict(dict)

    for solid_def in solid_defs:
        for input_def in solid_def.input_defs:
            input_type_name = input_def.runtime_type.name
            check.invariant(input_type_name in type_to_solid)
            dependencies[solid_def.name][input_def.name] = DependencyDefinition(
                type_to_solid[input_type_name].name
            )

    return PipelineDefinition(
        name=name, mode_defs=mode_defs, solid_defs=solid_defs, dependencies=dependencies
    )


class PySparkMemLakeHouse:
    def __init__(self):
        self.collected_tables = {}

    def hydrate(self, _context, _table_type, _table_metadata, table_handle):
        return table_handle.value

    def materialize(self, _context, table_type, _table_metadata, value):
        self.collected_tables[table_type.name] = value.collect()
        return None, InMemTableHandle(value=value)


@resource
def pyspark_mem_lakehouse_resource(_):
    return PySparkMemLakeHouse()


def invoke_compute(table_def, inputs, mode_def=None):
    '''
    Invoke the core computation defined on a table directly.
    '''

    def _compute_fn(context, _):
        yield Output(table_def.lakehouse_fn(context, **inputs))

    return execute_solid(
        SolidDefinition(
            name='wrap_lakehouse_fn_solid',
            input_defs=[],
            output_defs=[OutputDefinition(Any)],
            compute_fn=_compute_fn,
        ),
        mode_def=mode_def,
    )
