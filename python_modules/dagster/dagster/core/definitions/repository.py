from dagster import check
from dagster.core.errors import DagsterInvalidDefinitionError, DagsterInvariantViolationError

from .pipeline import PipelineDefinition


class RepositoryDefinition(object):
    '''Define a repository that contains a collection of pipelines.

    Args:
        name (str): The name of the pipeline.
        pipeline_dict (Dict[str, callable]):
            An dictionary of pipelines. The value of the dictionary is a function that takes
            no parameters and returns a PipelineDefiniton.

            We pass callables instead of the PipelineDefinitions itself so that they can be
            created on demand when accessed by name.

            As the pipelines are retrieved it ensures that the keys of the dictionary and the
            name of the pipeline are the same.
        pipeline_defs (List[PipelineDefinition]):
            A list of instantiated pipeline definitions.
    '''

    def __init__(self, name, pipeline_dict=None, pipeline_defs=None):
        self.name = check.str_param(name, 'name')

        pipeline_dict = check.opt_dict_param(pipeline_dict, 'pipeline_dict', key_type=str)
        pipeline_defs = check.opt_list_param(pipeline_defs, 'pipeline_defs', PipelineDefinition)

        for val in pipeline_dict.values():
            check.is_callable(val, 'Value in pipeline_dict must be function')

        self._lazy_pipeline_dict = pipeline_dict

        self._pipeline_cache = {}
        self._pipeline_names = set(pipeline_dict.keys())
        for defn in pipeline_defs:
            check.invariant(
                defn.name not in self._pipeline_names,
                'Duplicate pipelines named {name}'.format(name=defn.name),
            )
            self._pipeline_names.add(defn.name)
            self._pipeline_cache[defn.name] = defn

        self._all_pipelines = None
        self._solid_defs = None

    @property
    def pipeline_names(self):
        return self._pipeline_names

    def has_pipeline(self, name):
        check.str_param(name, 'name')
        return name in self._pipeline_names

    def get_pipeline(self, name):
        '''Get a pipeline by name. Only constructs that pipeline and caches it.

        Args:
            name (str): Name of the pipeline to retriever

        Returns:
            PipelineDefinition: Instance of PipelineDefinition with that name.
        '''
        check.str_param(name, 'name')

        if name in self._pipeline_cache:
            return self._pipeline_cache[name]

        try:
            pipeline = self._lazy_pipeline_dict[name]()
        except KeyError:
            raise DagsterInvariantViolationError(
                'Could not find pipeline "{name}". Found: {pipeline_names}.'.format(
                    name=name,
                    pipeline_names=', '.join(
                        [
                            '"{pipeline_name}"'.format(pipeline_name=pipeline_name)
                            for pipeline_name in self._pipeline_names
                        ]
                    ),
                )
            )
        check.invariant(
            pipeline.name == name,
            'Name does not match. Name in dict {name}. Name in pipeline {pipeline.name}'.format(
                name=name, pipeline=pipeline
            ),
        )

        self._pipeline_cache[name] = check.inst(
            pipeline,
            PipelineDefinition,
            (
                'Function passed into pipeline_dict with key {key} must return a '
                'PipelineDefinition'
            ).format(key=name),
        )

        return pipeline

    def get_all_pipelines(self):
        '''Return all pipelines as a list

        Returns:
            List[PipelineDefinition]:

        '''
        if self._all_pipelines:
            return self._all_pipelines

        self._all_pipelines = list(map(self.get_pipeline, self._pipeline_names))
        # This does uniqueness check
        self.get_all_solid_defs()
        return self._all_pipelines

    def get_all_solid_defs(self):
        if self._solid_defs:
            return self._solid_defs

        self._solid_defs = self._construct_solid_defs()
        return list(self._solid_defs.values())

    def _construct_solid_defs(self):
        solid_defs = {}
        solid_to_pipeline = {}
        # This looks like it should infinitely loop but the
        # memoization of all_pipelines and _solids_defs short
        # circuits that
        for pipeline in self.get_all_pipelines():
            for solid_def in pipeline.solid_defs:
                if solid_def.name not in solid_defs:
                    solid_defs[solid_def.name] = solid_def
                    solid_to_pipeline[solid_def.name] = pipeline.name

                if not solid_defs[solid_def.name] is solid_def:
                    first_name, second_name = sorted(
                        [solid_to_pipeline[solid_def.name], pipeline.name]
                    )
                    raise DagsterInvalidDefinitionError(
                        (
                            'You have defined two solid definitions named "{solid_def_name}" '
                            'in repository "{repository_name}". Solid definition names must be '
                            'unique within a repository. The solid definition has been defined in '
                            'pipeline "{first_pipeline_name}" and it has been defined '
                            'again in pipeline "{second_pipeline_name}."'
                        ).format(
                            solid_def_name=solid_def.name,
                            repository_name=self.name,
                            first_pipeline_name=first_name,
                            second_pipeline_name=second_name,
                        )
                    )

        return solid_defs

    def solid_def_named(self, name):
        check.str_param(name, 'name')

        self.get_all_solid_defs()

        if name not in self._solid_defs:
            check.failed('could not find solid_def {}'.format(name))

        return self._solid_defs[name]
