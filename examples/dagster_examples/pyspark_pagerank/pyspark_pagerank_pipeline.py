# pylint: disable=no-value-for-parameter

import re
from operator import add

from dagster import (
    pipeline,
    solid,
    Field,
    Int,
    InputDefinition,
    ModeDefinition,
    OutputDefinition,
    Path,
)

from dagster_pyspark import spark_session_resource, SparkRDD


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


@solid(
    input_defs=[InputDefinition('pagerank_data', Path)], output_defs=[OutputDefinition(SparkRDD)]
)
def parse_pagerank_data(context, pagerank_data):
    lines = context.resources.spark.read.text(pagerank_data).rdd.map(lambda r: r[0])
    return lines.map(parseNeighbors)


@solid(input_defs=[InputDefinition('urls', SparkRDD)], output_defs=[OutputDefinition(SparkRDD)])
def compute_links(_context, urls):
    return urls.distinct().groupByKey().cache()


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


@solid(
    input_defs=[InputDefinition(name='links', dagster_type=SparkRDD)],
    output_defs=[OutputDefinition(name='ranks', dagster_type=SparkRDD)],
    config={'iterations': Field(Int, is_optional=True, default_value=1)},
)
def calculate_ranks(context, links):
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    iterations = context.solid_config['iterations']
    for iteration in range(iterations):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        context.log.info('Completed iteration {}'.format(iteration))

    return ranks


@solid(input_defs=[InputDefinition(name='ranks', dagster_type=SparkRDD)])
def log_ranks(context, ranks):
    for (link, rank) in ranks.collect():
        context.log.info("%s has rank: %s." % (link, rank))

    return ranks.collect()


@pipeline(mode_defs=[ModeDefinition(resource_defs={'spark': spark_session_resource})])
def pyspark_pagerank():
    log_ranks(calculate_ranks(links=compute_links(urls=parse_pagerank_data())))
