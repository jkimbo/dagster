'''NOTE: THIS FILE IS AUTO-GENERATED. DO NOT EDIT

Produced via:
python automation/parse_spark_configs.py \
	--output-file \
	libraries/dagster-spark/dagster_spark/configs_spark.py \

'''


from dagster import Bool, Field, Float, Int, PermissiveDict, String


# pylint: disable=line-too-long
def spark_config():
    return Field(
        PermissiveDict(
            fields={
                'spark': Field(
                    PermissiveDict(
                        fields={
                            'app': Field(
                                PermissiveDict(
                                    fields={
                                        'name': Field(
                                            String,
                                            description='''Application Properties: The name of your application. This will appear in the UI and in log data.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'driver': Field(
                                PermissiveDict(
                                    fields={
                                        'cores': Field(
                                            Int,
                                            description='''Application Properties: Number of cores to use for the driver process, only in cluster mode.''',
                                            is_optional=True,
                                        ),
                                        'maxResultSize': Field(
                                            String,
                                            description='''Application Properties: Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.''',
                                            is_optional=True,
                                        ),
                                        'memory': Field(
                                            String,
                                            description='''Application Properties: Amount of memory to use for the driver process, i.e. where SparkContext is initialized, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g). Note: In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please set this through the --driver-memory command line option or in your default properties file.''',
                                            is_optional=True,
                                        ),
                                        'memoryOverhead': Field(
                                            String,
                                            description='''Application Properties: The amount of off-heap memory to be allocated per driver in cluster mode, in MiB unless otherwise specified. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the container size (typically 6-10%). This option is currently supported on YARN and Kubernetes.''',
                                            is_optional=True,
                                        ),
                                        'supervise': Field(
                                            Bool,
                                            description='''Application Properties: If true, restarts the driver automatically if it fails with a non-zero exit status. Only has effect in Spark standalone mode or Mesos cluster deploy mode.''',
                                            is_optional=True,
                                        ),
                                        'extraClassPath': Field(
                                            String,
                                            description='''Runtime Environment: Extra classpath entries to prepend to the classpath of the driver. Note: In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please set this through the --driver-class-path command line option or in your default properties file.''',
                                            is_optional=True,
                                        ),
                                        'extraJavaOptions': Field(
                                            String,
                                            description='''Runtime Environment: A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. Note that it is illegal to set maximum heap size (-Xmx) settings with this option. Maximum heap size settings can be set with spark.driver.memory in the cluster mode and through the --driver-memory command line option in the client mode. Note: In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please set this through the --driver-java-options command line option or in your default properties file.''',
                                            is_optional=True,
                                        ),
                                        'extraLibraryPath': Field(
                                            String,
                                            description='''Runtime Environment: Set a special library path to use when launching the driver JVM. Note: In client mode, this config must not be set through the SparkConf directly in your application, because the driver JVM has already started at that point. Instead, please set this through the --driver-library-path command line option or in your default properties file.''',
                                            is_optional=True,
                                        ),
                                        'userClassPathFirst': Field(
                                            Bool,
                                            description='''Runtime Environment: (Experimental) Whether to give user-added jars precedence over Spark's own jars when loading classes in the driver. This feature can be used to mitigate conflicts between Spark's dependencies and user dependencies. It is currently an experimental feature. This is used in cluster mode only.''',
                                            is_optional=True,
                                        ),
                                        'blockManager': Field(
                                            PermissiveDict(
                                                fields={
                                                    'port': Field(
                                                        String,
                                                        description='''Networking: Driver-specific port for the block manager to listen on, for cases where it cannot use the same configuration as executors.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'bindAddress': Field(
                                            String,
                                            description='''Networking: Hostname or IP address where to bind listening sockets. This config overrides the SPARK_LOCAL_IP environment variable (see below). It also allows a different address from the local one to be advertised to executors or external systems. This is useful, for example, when running containers with bridged networking. For this to properly work, the different ports used by the driver (RPC, block manager and UI) need to be forwarded from the container's host.''',
                                            is_optional=True,
                                        ),
                                        'host': Field(
                                            String,
                                            description='''Networking: Hostname or IP address for the driver. This is used for communicating with the executors and the standalone Master.''',
                                            is_optional=True,
                                        ),
                                        'port': Field(
                                            String,
                                            description='''Networking: Port for the driver to listen on. This is used for communicating with the executors and the standalone Master.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'executor': Field(
                                PermissiveDict(
                                    fields={
                                        'memory': Field(
                                            String,
                                            description='''Application Properties: Amount of memory to use per executor process, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).''',
                                            is_optional=True,
                                        ),
                                        'pyspark': Field(
                                            PermissiveDict(
                                                fields={
                                                    'memory': Field(
                                                        String,
                                                        description='''Application Properties: The amount of memory to be allocated to PySpark in each executor, in MiB unless otherwise specified. If set, PySpark memory for an executor will be limited to this amount. If not set, Spark will not limit Python's memory use and it is up to the application to avoid exceeding the overhead memory space shared with other non-JVM processes. When PySpark is run in YARN or Kubernetes, this memory is added to executor resource requests.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'memoryOverhead': Field(
                                            String,
                                            description='''Application Properties: The amount of off-heap memory to be allocated per executor, in MiB unless otherwise specified. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the executor size (typically 6-10%). This option is currently supported on YARN and Kubernetes.''',
                                            is_optional=True,
                                        ),
                                        'extraClassPath': Field(
                                            String,
                                            description='''Runtime Environment: Extra classpath entries to prepend to the classpath of executors. This exists primarily for backwards-compatibility with older versions of Spark. Users typically should not need to set this option.''',
                                            is_optional=True,
                                        ),
                                        'extraJavaOptions': Field(
                                            String,
                                            description='''Runtime Environment: A string of extra JVM options to pass to executors. For instance, GC settings or other logging. Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this option. Spark properties should be set using a SparkConf object or the spark-defaults.conf file used with the spark-submit script. Maximum heap size settings can be set with spark.executor.memory. The following symbols, if present will be interpolated: {{APP_ID}} will be replaced by application ID and {{EXECUTOR_ID}} will be replaced by executor ID. For example, to enable verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of: -verbose:gc -Xloggc:/tmp/{{APP_ID}}-{{EXECUTOR_ID}}.gc''',
                                            is_optional=True,
                                        ),
                                        'extraLibraryPath': Field(
                                            String,
                                            description='''Runtime Environment: Set a special library path to use when launching executor JVM's.''',
                                            is_optional=True,
                                        ),
                                        'logs': Field(
                                            PermissiveDict(
                                                fields={
                                                    'rolling': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'maxRetainedFiles': Field(
                                                                    Int,
                                                                    description='''Runtime Environment: Sets the number of latest rolling log files that are going to be retained by the system. Older log files will be deleted. Disabled by default.''',
                                                                    is_optional=True,
                                                                ),
                                                                'enableCompression': Field(
                                                                    Bool,
                                                                    description='''Runtime Environment: Enable executor log compression. If it is enabled, the rolled executor logs will be compressed. Disabled by default.''',
                                                                    is_optional=True,
                                                                ),
                                                                'maxSize': Field(
                                                                    Int,
                                                                    description='''Runtime Environment: Set the max size of the file in bytes by which the executor logs will be rolled over. Rolling is disabled by default. See spark.executor.logs.rolling.maxRetainedFiles for automatic cleaning of old logs.''',
                                                                    is_optional=True,
                                                                ),
                                                                'strategy': Field(
                                                                    String,
                                                                    description='''Runtime Environment: Set the strategy of rolling of executor logs. By default it is disabled. It can be set to "time" (time-based rolling) or "size" (size-based rolling). For "time", use spark.executor.logs.rolling.time.interval to set the rolling interval. For "size", use spark.executor.logs.rolling.maxSize to set the maximum file size for rolling.''',
                                                                    is_optional=True,
                                                                ),
                                                                'time': Field(
                                                                    PermissiveDict(
                                                                        fields={
                                                                            'interval': Field(
                                                                                String,
                                                                                description='''Runtime Environment: Set the time interval by which the executor logs will be rolled over. Rolling is disabled by default. Valid values are daily, hourly, minutely or any interval in seconds. See spark.executor.logs.rolling.maxRetainedFiles for automatic cleaning of old logs.''',
                                                                                is_optional=True,
                                                                            )
                                                                        }
                                                                    )
                                                                ),
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                        ),
                                        'userClassPathFirst': Field(
                                            Bool,
                                            description='''Runtime Environment: (Experimental) Same functionality as spark.driver.userClassPathFirst, but applied to executor instances.''',
                                            is_optional=True,
                                        ),
                                        'cores': Field(
                                            Int,
                                            description='''Execution Behavior: The number of cores to use on each executor. In standalone and Mesos coarse-grained modes, for more detail, see this description.''',
                                            is_optional=True,
                                        ),
                                        'heartbeatInterval': Field(
                                            String,
                                            description='''Execution Behavior: Interval between each executor's heartbeats to the driver. Heartbeats let the driver know that the executor is still alive and update it with metrics for in-progress tasks. spark.executor.heartbeatInterval should be significantly less than spark.network.timeout''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'extraListeners': Field(
                                String,
                                description='''Application Properties: A comma-separated list of classes that implement SparkListener; when initializing SparkContext, instances of these classes will be created and registered with Spark's listener bus. If a class has a single-argument constructor that accepts a SparkConf, that constructor will be called; otherwise, a zero-argument constructor will be called. If no valid constructor can be found, the SparkContext creation will fail with an exception.''',
                                is_optional=True,
                            ),
                            'local': Field(
                                PermissiveDict(
                                    fields={
                                        'dir': Field(
                                            String,
                                            description='''Application Properties: Directory to use for "scratch" space in Spark, including map output files and RDDs that get stored on disk. This should be on a fast, local disk in your system. It can also be a comma-separated list of multiple directories on different disks. NOTE: In Spark 1.0 and later this will be overridden by SPARK_LOCAL_DIRS (Standalone), MESOS_SANDBOX (Mesos) or LOCAL_DIRS (YARN) environment variables set by the cluster manager.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'logConf': Field(
                                Bool,
                                description='''Application Properties: Logs the effective SparkConf as INFO when a SparkContext is started.''',
                                is_optional=True,
                            ),
                            'master': Field(
                                String,
                                description='''Application Properties: The cluster manager to connect to. See the list of allowed master URL's.''',
                                is_optional=True,
                            ),
                            'submit': Field(
                                PermissiveDict(
                                    fields={
                                        'deployMode': Field(
                                            String,
                                            description='''Application Properties: The deploy mode of Spark driver program, either "client" or "cluster", Which means to launch driver program locally ("client") or remotely ("cluster") on one of the nodes inside the cluster.''',
                                            is_optional=True,
                                        ),
                                        'pyFiles': Field(
                                            String,
                                            description='''Runtime Environment: Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps. Globs are allowed.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'log': Field(
                                PermissiveDict(
                                    fields={
                                        'callerContext': Field(
                                            String,
                                            description='''Application Properties: Application information that will be written into Yarn RM log/HDFS audit log when running on Yarn/HDFS. Its length depends on the Hadoop configuration hadoop.caller.context.max.size. It should be concise, and typically can have up to 50 characters.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'redaction': Field(
                                PermissiveDict(
                                    fields={
                                        'regex': Field(
                                            String,
                                            description='''Runtime Environment: Regex to decide which Spark configuration properties and environment variables in driver and executor environments contain sensitive information. When this regex matches a property key or value, the value is redacted from the environment UI and various logs like YARN and event logs.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'python': Field(
                                PermissiveDict(
                                    fields={
                                        'profile': Field(
                                            PermissiveDict(
                                                fields={
                                                    'root': Field(
                                                        Bool,
                                                        description='''Runtime Environment: Enable profiling in Python worker, the profile result will show up by sc.show_profiles(), or it will be displayed before the driver exits. It also can be dumped into disk by sc.dump_profiles(path). If some of the profile results had been displayed manually, they will not be displayed automatically before driver exiting. By default the pyspark.profiler.BasicProfiler will be used, but this can be overridden by passing a profiler class in as a parameter to the SparkContext constructor.''',
                                                        is_optional=True,
                                                    ),
                                                    'dump': Field(
                                                        String,
                                                        description='''Runtime Environment: The directory which is used to dump the profile result before driver exiting. The results will be dumped as separated file for each RDD. They can be loaded by ptats.Stats(). If this is specified, the profile result will not be displayed automatically.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'worker': Field(
                                            PermissiveDict(
                                                fields={
                                                    'memory': Field(
                                                        String,
                                                        description='''Runtime Environment: Amount of memory to use per python worker process during aggregation, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g). If the memory used during aggregation goes above this amount, it will spill the data into disks.''',
                                                        is_optional=True,
                                                    ),
                                                    'reuse': Field(
                                                        Bool,
                                                        description='''Runtime Environment: Reuse Python worker or not. If yes, it will use a fixed number of Python workers, does not need to fork() a Python process for every task. It will be very useful if there is large broadcast, then the broadcast will not be needed to transferred from JVM to Python worker for every task.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'files': Field(
                                PermissiveDict(
                                    fields={
                                        'root': Field(
                                            String,
                                            description='''Runtime Environment: Comma-separated list of files to be placed in the working directory of each executor. Globs are allowed.''',
                                            is_optional=True,
                                        ),
                                        'fetchTimeout': Field(
                                            String,
                                            description='''Execution Behavior: Communication timeout to use when fetching files added through SparkContext.addFile() from the driver.''',
                                            is_optional=True,
                                        ),
                                        'useFetchCache': Field(
                                            Bool,
                                            description='''Execution Behavior: If set to true (default), file fetching will use a local cache that is shared by executors that belong to the same application, which can improve task launching performance when running many executors on the same host. If set to false, these caching optimizations will be disabled and all executors will fetch their own copies of files. This optimization may be disabled in order to use Spark local directories that reside on NFS filesystems (see SPARK-6313 for more details).''',
                                            is_optional=True,
                                        ),
                                        'overwrite': Field(
                                            Bool,
                                            description='''Execution Behavior: Whether to overwrite files added through SparkContext.addFile() when the target file exists and its contents do not match those of the source.''',
                                            is_optional=True,
                                        ),
                                        'maxPartitionBytes': Field(
                                            Int,
                                            description='''Execution Behavior: The maximum number of bytes to pack into a single partition when reading files.''',
                                            is_optional=True,
                                        ),
                                        'openCostInBytes': Field(
                                            Int,
                                            description='''Execution Behavior: The estimated cost to open a file, measured by the number of bytes could be scanned at the same time. This is used when putting multiple files into a partition. It is better to overestimate, then the partitions with small files will be faster than partitions with bigger files.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'jars': Field(
                                PermissiveDict(
                                    fields={
                                        'root': Field(
                                            String,
                                            description='''Runtime Environment: Comma-separated list of jars to include on the driver and executor classpaths. Globs are allowed.''',
                                            is_optional=True,
                                        ),
                                        'packages': Field(
                                            String,
                                            description='''Runtime Environment: Comma-separated list of Maven coordinates of jars to include on the driver and executor classpaths. The coordinates should be groupId:artifactId:version. If spark.jars.ivySettings is given artifacts will be resolved according to the configuration in the file, otherwise artifacts will be searched for in the local maven repo, then maven central and finally any additional remote repositories given by the command-line option --repositories. For more details, see Advanced Dependency Management.''',
                                            is_optional=True,
                                        ),
                                        'excludes': Field(
                                            String,
                                            description='''Runtime Environment: Comma-separated list of groupId:artifactId, to exclude while resolving the dependencies provided in spark.jars.packages to avoid dependency conflicts.''',
                                            is_optional=True,
                                        ),
                                        'ivy': Field(
                                            String,
                                            description='''Runtime Environment: Path to specify the Ivy user directory, used for the local Ivy cache and package files from spark.jars.packages. This will override the Ivy property ivy.default.ivy.user.dir which defaults to ~/.ivy2.''',
                                            is_optional=True,
                                        ),
                                        'ivySettings': Field(
                                            String,
                                            description='''Runtime Environment: Path to an Ivy settings file to customize resolution of jars specified using spark.jars.packages instead of the built-in defaults, such as maven central. Additional repositories given by the command-line option --repositories or spark.jars.repositories will also be included. Useful for allowing Spark to resolve artifacts from behind a firewall e.g. via an in-house artifact server like Artifactory. Details on the settings file format can be found at http://ant.apache.org/ivy/history/latest-milestone/settings.html''',
                                            is_optional=True,
                                        ),
                                        'repositories': Field(
                                            String,
                                            description='''Runtime Environment: Comma-separated list of additional remote repositories to search for the maven coordinates given with --packages or spark.jars.packages.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'pyspark': Field(
                                PermissiveDict(
                                    fields={
                                        'driver': Field(
                                            PermissiveDict(
                                                fields={
                                                    'python': Field(
                                                        String,
                                                        description='''Runtime Environment: Python binary executable to use for PySpark in driver. (default is spark.pyspark.python)''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'python': Field(
                                            String,
                                            description='''Runtime Environment: Python binary executable to use for PySpark in both driver and executors.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'reducer': Field(
                                PermissiveDict(
                                    fields={
                                        'maxSizeInFlight': Field(
                                            String,
                                            description='''Shuffle Behavior: Maximum size of map outputs to fetch simultaneously from each reduce task, in MiB unless otherwise specified. Since each output requires us to create a buffer to receive it, this represents a fixed memory overhead per reduce task, so keep it small unless you have a large amount of memory.''',
                                            is_optional=True,
                                        ),
                                        'maxReqsInFlight': Field(
                                            Int,
                                            description='''Shuffle Behavior: This configuration limits the number of remote requests to fetch blocks at any given point. When the number of hosts in the cluster increase, it might lead to very large number of inbound connections to one or more nodes, causing the workers to fail under load. By allowing it to limit the number of fetch requests, this scenario can be mitigated.''',
                                            is_optional=True,
                                        ),
                                        'maxBlocksInFlightPerAddress': Field(
                                            Int,
                                            description='''Shuffle Behavior: This configuration limits the number of remote blocks being fetched per reduce task from a given host port. When a large number of blocks are being requested from a given address in a single fetch or simultaneously, this could crash the serving executor or Node Manager. This is especially useful to reduce the load on the Node Manager when external shuffle is enabled. You can mitigate this issue by setting it to a lower value.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'maxRemoteBlockSizeFetchToMem': Field(
                                Int,
                                description='''Shuffle Behavior: The remote block will be fetched to disk when size of the block is above this threshold in bytes. This is to avoid a giant request that takes too much memory. By default, this is only enabled for blocks > 2GB, as those cannot be fetched directly into memory, no matter what resources are available. But it can be turned down to a much lower value (eg. 200m) to avoid using too much memory on smaller blocks as well. Note this configuration will affect both shuffle fetch and block manager remote block fetch. For users who enabled external shuffle service, this feature can only be used when external shuffle service is newer than Spark 2.2.''',
                                is_optional=True,
                            ),
                            'shuffle': Field(
                                PermissiveDict(
                                    fields={
                                        'compress': Field(
                                            Bool,
                                            description='''Shuffle Behavior: Whether to compress map output files. Generally a good idea. Compression will use spark.io.compression.codec.''',
                                            is_optional=True,
                                        ),
                                        'file': Field(
                                            PermissiveDict(
                                                fields={
                                                    'buffer': Field(
                                                        String,
                                                        description='''Shuffle Behavior: Size of the in-memory buffer for each shuffle file output stream, in KiB unless otherwise specified. These buffers reduce the number of disk seeks and system calls made in creating intermediate shuffle files.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'io': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxRetries': Field(
                                                        Int,
                                                        description='''Shuffle Behavior: (Netty only) Fetches that fail due to IO-related exceptions are automatically retried if this is set to a non-zero value. This retry logic helps stabilize large shuffles in the face of long GC pauses or transient network connectivity issues.''',
                                                        is_optional=True,
                                                    ),
                                                    'numConnectionsPerPeer': Field(
                                                        Int,
                                                        description='''Shuffle Behavior: (Netty only) Connections between hosts are reused in order to reduce connection buildup for large clusters. For clusters with many hard disks and few hosts, this may result in insufficient concurrency to saturate all disks, and so users may consider increasing this value.''',
                                                        is_optional=True,
                                                    ),
                                                    'preferDirectBufs': Field(
                                                        Bool,
                                                        description='''Shuffle Behavior: (Netty only) Off-heap buffers are used to reduce garbage collection during shuffle and cache block transfer. For environments where off-heap memory is tightly limited, users may wish to turn this off to force all allocations from Netty to be on-heap.''',
                                                        is_optional=True,
                                                    ),
                                                    'retryWait': Field(
                                                        String,
                                                        description='''Shuffle Behavior: (Netty only) How long to wait between retries of fetches. The maximum delay caused by retrying is 15 seconds by default, calculated as maxRetries * retryWait.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'service': Field(
                                            PermissiveDict(
                                                fields={
                                                    'enabled': Field(
                                                        Bool,
                                                        description='''Shuffle Behavior: Enables the external shuffle service. This service preserves the shuffle files written by executors so the executors can be safely removed. This must be enabled if spark.dynamicAllocation.enabled is "true". The external shuffle service must be set up in order to enable it. See dynamic allocation configuration and setup documentation for more information.''',
                                                        is_optional=True,
                                                    ),
                                                    'port': Field(
                                                        Int,
                                                        description='''Shuffle Behavior: Port on which the external shuffle service will run.''',
                                                        is_optional=True,
                                                    ),
                                                    'index': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'cache': Field(
                                                                    PermissiveDict(
                                                                        fields={
                                                                            'size': Field(
                                                                                String,
                                                                                description='''Shuffle Behavior: Cache entries limited to the specified memory footprint in bytes.''',
                                                                                is_optional=True,
                                                                            )
                                                                        }
                                                                    )
                                                                )
                                                            }
                                                        )
                                                    ),
                                                }
                                            )
                                        ),
                                        'maxChunksBeingTransferred': Field(
                                            Int,
                                            description='''Shuffle Behavior: The max number of chunks allowed to be transferred at the same time on shuffle service. Note that new incoming connections will be closed when the max number is hit. The client will retry according to the shuffle retry configs (see spark.shuffle.io.maxRetries and spark.shuffle.io.retryWait), if those limits are reached the task will fail with fetch failure.''',
                                            is_optional=True,
                                        ),
                                        'sort': Field(
                                            PermissiveDict(
                                                fields={
                                                    'bypassMergeThreshold': Field(
                                                        Int,
                                                        description='''Shuffle Behavior: (Advanced) In the sort-based shuffle manager, avoid merge-sorting data if there is no map-side aggregation and there are at most this many reduce partitions.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'spill': Field(
                                            PermissiveDict(
                                                fields={
                                                    'compress': Field(
                                                        Bool,
                                                        description='''Shuffle Behavior: Whether to compress data spilled during shuffles. Compression will use spark.io.compression.codec.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'accurateBlockThreshold': Field(
                                            Int,
                                            description='''Shuffle Behavior: Threshold in bytes above which the size of shuffle blocks in HighlyCompressedMapStatus is accurately recorded. This helps to prevent OOM by avoiding underestimating shuffle block size when fetch shuffle blocks.''',
                                            is_optional=True,
                                        ),
                                        'registration': Field(
                                            PermissiveDict(
                                                fields={
                                                    'timeout': Field(
                                                        Int,
                                                        description='''Shuffle Behavior: Timeout in milliseconds for registration to the external shuffle service.''',
                                                        is_optional=True,
                                                    ),
                                                    'maxAttempts': Field(
                                                        Int,
                                                        description='''Shuffle Behavior: When we fail to register to the external shuffle service, we will retry for maxAttempts times.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'memoryFraction': Field(
                                            Float,
                                            description='''Memory Management: (deprecated) This is read only if spark.memory.useLegacyMode is enabled. Fraction of Java heap to use for aggregation and cogroups during shuffles. At any given time, the collective size of all in-memory maps used for shuffles is bounded by this limit, beyond which the contents will begin to spill to disk. If spills are often, consider increasing this value at the expense of spark.storage.memoryFraction.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'eventLog': Field(
                                PermissiveDict(
                                    fields={
                                        'logBlockUpdates': Field(
                                            PermissiveDict(
                                                fields={
                                                    'enabled': Field(
                                                        String,
                                                        description='''Spark UI: Whether to log events for every block update, if spark.eventLog.enabled is true. *Warning*: This will increase the size of the event log considerably.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'longForm': Field(
                                            PermissiveDict(
                                                fields={
                                                    'enabled': Field(
                                                        String,
                                                        description='''Spark UI: If true, use the long form of call sites in the event log. Otherwise use the short form.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'compress': Field(
                                            String,
                                            description='''Spark UI: Whether to compress logged events, if spark.eventLog.enabled is true. Compression will use spark.io.compression.codec.''',
                                            is_optional=True,
                                        ),
                                        'dir': Field(
                                            String,
                                            description='''Spark UI: Base directory in which Spark events are logged, if spark.eventLog.enabled is true. Within this base directory, Spark creates a sub-directory for each application, and logs the events specific to the application in this directory. Users may want to set this to a unified location like an HDFS directory so history files can be read by the history server.''',
                                            is_optional=True,
                                        ),
                                        'enabled': Field(
                                            String,
                                            description='''Spark UI: Whether to log Spark events, useful for reconstructing the Web UI after the application has finished.''',
                                            is_optional=True,
                                        ),
                                        'overwrite': Field(
                                            String,
                                            description='''Spark UI: Whether to overwrite any existing files.''',
                                            is_optional=True,
                                        ),
                                        'buffer': Field(
                                            PermissiveDict(
                                                fields={
                                                    'kb': Field(
                                                        String,
                                                        description='''Spark UI: Buffer size to use when writing to output streams, in KiB unless otherwise specified.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'ui': Field(
                                PermissiveDict(
                                    fields={
                                        'dagGraph': Field(
                                            PermissiveDict(
                                                fields={
                                                    'retainedRootRDDs': Field(
                                                        String,
                                                        description='''Spark UI: How many DAG graph nodes the Spark UI and status APIs remember before garbage collecting.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'enabled': Field(
                                            String,
                                            description='''Spark UI: Whether to run the web UI for the Spark application.''',
                                            is_optional=True,
                                        ),
                                        'killEnabled': Field(
                                            String,
                                            description='''Spark UI: Allows jobs and stages to be killed from the web UI.''',
                                            is_optional=True,
                                        ),
                                        'liveUpdate': Field(
                                            PermissiveDict(
                                                fields={
                                                    'period': Field(
                                                        String,
                                                        description='''Spark UI: How often to update live entities. -1 means "never update" when replaying applications, meaning only the last write will happen. For live applications, this avoids a few operations that we can live without when rapidly processing incoming task events.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'port': Field(
                                            String,
                                            description='''Spark UI: Port for your application's dashboard, which shows memory and workload data.''',
                                            is_optional=True,
                                        ),
                                        'retainedJobs': Field(
                                            String,
                                            description='''Spark UI: How many jobs the Spark UI and status APIs remember before garbage collecting. This is a target maximum, and fewer elements may be retained in some circumstances.''',
                                            is_optional=True,
                                        ),
                                        'retainedStages': Field(
                                            String,
                                            description='''Spark UI: How many stages the Spark UI and status APIs remember before garbage collecting. This is a target maximum, and fewer elements may be retained in some circumstances.''',
                                            is_optional=True,
                                        ),
                                        'retainedTasks': Field(
                                            String,
                                            description='''Spark UI: How many tasks the Spark UI and status APIs remember before garbage collecting. This is a target maximum, and fewer elements may be retained in some circumstances.''',
                                            is_optional=True,
                                        ),
                                        'reverseProxy': Field(
                                            String,
                                            description='''Spark UI: Enable running Spark Master as reverse proxy for worker and application UIs. In this mode, Spark master will reverse proxy the worker and application UIs to enable access without requiring direct access to their hosts. Use it with caution, as worker and application UI will not be accessible directly, you will only be able to access them through spark master/proxy public URL. This setting affects all the workers and application UIs running in the cluster and must be set on all the workers, drivers and masters.''',
                                            is_optional=True,
                                        ),
                                        'reverseProxyUrl': Field(
                                            String,
                                            description='''Spark UI: This is the URL where your proxy is running. This URL is for proxy which is running in front of Spark Master. This is useful when running proxy for authentication e.g. OAuth proxy. Make sure this is a complete URL including scheme (http/https) and port to reach your proxy.''',
                                            is_optional=True,
                                        ),
                                        'showConsoleProgress': Field(
                                            String,
                                            description='''Spark UI: Show the progress bar in the console. The progress bar shows the progress of stages that run for longer than 500ms. If multiple stages run at the same time, multiple progress bars will be displayed on the same line.''',
                                            is_optional=True,
                                        ),
                                        'retainedDeadExecutors': Field(
                                            String,
                                            description='''Spark UI: How many dead executors the Spark UI and status APIs remember before garbage collecting.''',
                                            is_optional=True,
                                        ),
                                        'filters': Field(
                                            String,
                                            description='''Spark UI: Comma separated list of filter class names to apply to the Spark Web UI. The filter should be a standard javax servlet Filter. Filter parameters can also be specified in the configuration, by setting config entries of the form spark.<class name of filter>.param.<param name>=<value> For example: spark.ui.filters=com.test.filter1 spark.com.test.filter1.param.name1=foo spark.com.test.filter1.param.name2=bar''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'worker': Field(
                                PermissiveDict(
                                    fields={
                                        'ui': Field(
                                            PermissiveDict(
                                                fields={
                                                    'retainedExecutors': Field(
                                                        String,
                                                        description='''Spark UI: How many finished executors the Spark UI and status APIs remember before garbage collecting.''',
                                                        is_optional=True,
                                                    ),
                                                    'retainedDrivers': Field(
                                                        String,
                                                        description='''Spark UI: How many finished drivers the Spark UI and status APIs remember before garbage collecting.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'sql': Field(
                                PermissiveDict(
                                    fields={
                                        'ui': Field(
                                            PermissiveDict(
                                                fields={
                                                    'retainedExecutions': Field(
                                                        String,
                                                        description='''Spark UI: How many finished executions the Spark UI and status APIs remember before garbage collecting.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'streaming': Field(
                                PermissiveDict(
                                    fields={
                                        'ui': Field(
                                            PermissiveDict(
                                                fields={
                                                    'retainedBatches': Field(
                                                        String,
                                                        description='''Spark Streaming: How many batches the Spark Streaming UI and status APIs remember before garbage collecting.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'backpressure': Field(
                                            PermissiveDict(
                                                fields={
                                                    'enabled': Field(
                                                        String,
                                                        description='''Spark Streaming: Enables or disables Spark Streaming's internal backpressure mechanism (since 1.5). This enables the Spark Streaming to control the receiving rate based on the current batch scheduling delays and processing times so that the system receives only as fast as the system can process. Internally, this dynamically sets the maximum receiving rate of receivers. This rate is upper bounded by the values spark.streaming.receiver.maxRate and spark.streaming.kafka.maxRatePerPartition if they are set (see below).''',
                                                        is_optional=True,
                                                    ),
                                                    'initialRate': Field(
                                                        String,
                                                        description='''Spark Streaming: This is the initial maximum receiving rate at which each receiver will receive data for the first batch when the backpressure mechanism is enabled.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'blockInterval': Field(
                                            String,
                                            description='''Spark Streaming: Interval at which data received by Spark Streaming receivers is chunked into blocks of data before storing them in Spark. Minimum recommended - 50 ms. See the performance tuning section in the Spark Streaming programing guide for more details.''',
                                            is_optional=True,
                                        ),
                                        'receiver': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxRate': Field(
                                                        String,
                                                        description='''Spark Streaming: Maximum rate (number of records per second) at which each receiver will receive data. Effectively, each stream will consume at most this number of records per second. Setting this configuration to 0 or a negative number will put no limit on the rate. See the deployment guide in the Spark Streaming programing guide for mode details.''',
                                                        is_optional=True,
                                                    ),
                                                    'writeAheadLog': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'enable': Field(
                                                                    String,
                                                                    description='''Spark Streaming: Enable write-ahead logs for receivers. All the input data received through receivers will be saved to write-ahead logs that will allow it to be recovered after driver failures. See the deployment guide in the Spark Streaming programing guide for more details.''',
                                                                    is_optional=True,
                                                                ),
                                                                'closeFileAfterWrite': Field(
                                                                    String,
                                                                    description='''Spark Streaming: Whether to close the file after writing a write-ahead log record on the receivers. Set this to 'true' when you want to use S3 (or any file system that does not support flushing) for the data WAL on the receivers.''',
                                                                    is_optional=True,
                                                                ),
                                                            }
                                                        )
                                                    ),
                                                }
                                            )
                                        ),
                                        'unpersist': Field(
                                            String,
                                            description='''Spark Streaming: Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared. Setting this to false will allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be cleared automatically. But it comes at the cost of higher memory usage in Spark.''',
                                            is_optional=True,
                                        ),
                                        'stopGracefullyOnShutdown': Field(
                                            String,
                                            description='''Spark Streaming: If true, Spark shuts down the StreamingContext gracefully on JVM shutdown rather than immediately.''',
                                            is_optional=True,
                                        ),
                                        'kafka': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxRatePerPartition': Field(
                                                        String,
                                                        description='''Spark Streaming: Maximum rate (number of records per second) at which data will be read from each Kafka partition when using the new Kafka direct stream API. See the Kafka Integration guide for more details.''',
                                                        is_optional=True,
                                                    ),
                                                    'minRatePerPartition': Field(
                                                        String,
                                                        description='''Spark Streaming: Minimum rate (number of records per second) at which data will be read from each Kafka partition when using the new Kafka direct stream API.''',
                                                        is_optional=True,
                                                    ),
                                                    'maxRetries': Field(
                                                        String,
                                                        description='''Spark Streaming: Maximum number of consecutive retries the driver will make in order to find the latest offsets on the leader of each partition (a default value of 1 means that the driver will make a maximum of 2 attempts). Only applies to the new Kafka direct stream API.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'driver': Field(
                                            PermissiveDict(
                                                fields={
                                                    'writeAheadLog': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'closeFileAfterWrite': Field(
                                                                    String,
                                                                    description='''Spark Streaming: Whether to close the file after writing a write-ahead log record on the driver. Set this to 'true' when you want to use S3 (or any file system that does not support flushing) for the metadata WAL on the driver.''',
                                                                    is_optional=True,
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'broadcast': Field(
                                PermissiveDict(
                                    fields={
                                        'compress': Field(
                                            String,
                                            description='''Compression and Serialization: Whether to compress broadcast variables before sending them. Generally a good idea. Compression will use spark.io.compression.codec.''',
                                            is_optional=True,
                                        ),
                                        'blockSize': Field(
                                            String,
                                            description='''Execution Behavior: Size of each piece of a block for TorrentBroadcastFactory, in KiB unless otherwise specified. Too large a value decreases parallelism during broadcast (makes it slower); however, if it is too small, BlockManager might take a performance hit.''',
                                            is_optional=True,
                                        ),
                                        'checksum': Field(
                                            String,
                                            description='''Execution Behavior: Whether to enable checksum for broadcast. If enabled, broadcasts will include a checksum, which can help detect corrupted blocks, at the cost of computing and sending a little more data. It's possible to disable it if the network has other mechanisms to guarantee data won't be corrupted during broadcast.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'io': Field(
                                PermissiveDict(
                                    fields={
                                        'compression': Field(
                                            PermissiveDict(
                                                fields={
                                                    'codec': Field(
                                                        String,
                                                        description='''Compression and Serialization: The codec used to compress internal data such as RDD partitions, event log, broadcast variables and shuffle outputs. By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify the codec, e.g. org.apache.spark.io.LZ4CompressionCodec, org.apache.spark.io.LZFCompressionCodec, org.apache.spark.io.SnappyCompressionCodec, and org.apache.spark.io.ZStdCompressionCodec.''',
                                                        is_optional=True,
                                                    ),
                                                    'lz4': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'blockSize': Field(
                                                                    String,
                                                                    description='''Compression and Serialization: Block size in bytes used in LZ4 compression, in the case when LZ4 compression codec is used. Lowering this block size will also lower shuffle memory usage when LZ4 is used.''',
                                                                    is_optional=True,
                                                                )
                                                            }
                                                        )
                                                    ),
                                                    'snappy': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'blockSize': Field(
                                                                    String,
                                                                    description='''Compression and Serialization: Block size in bytes used in Snappy compression, in the case when Snappy compression codec is used. Lowering this block size will also lower shuffle memory usage when Snappy is used.''',
                                                                    is_optional=True,
                                                                )
                                                            }
                                                        )
                                                    ),
                                                    'zstd': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'level': Field(
                                                                    String,
                                                                    description='''Compression and Serialization: Compression level for Zstd compression codec. Increasing the compression level will result in better compression at the expense of more CPU and memory.''',
                                                                    is_optional=True,
                                                                ),
                                                                'bufferSize': Field(
                                                                    String,
                                                                    description='''Compression and Serialization: Buffer size in bytes used in Zstd compression, in the case when Zstd compression codec is used. Lowering this size will lower the shuffle memory usage when Zstd is used, but it might increase the compression cost because of excessive JNI call overhead.''',
                                                                    is_optional=True,
                                                                ),
                                                            }
                                                        )
                                                    ),
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'kryo': Field(
                                PermissiveDict(
                                    fields={
                                        'classesToRegister': Field(
                                            String,
                                            description='''Compression and Serialization: If you use Kryo serialization, give a comma-separated list of custom class names to register with Kryo. See the tuning guide for more details.''',
                                            is_optional=True,
                                        ),
                                        'referenceTracking': Field(
                                            String,
                                            description='''Compression and Serialization: Whether to track references to the same object when serializing data with Kryo, which is necessary if your object graphs have loops and useful for efficiency if they contain multiple copies of the same object. Can be disabled to improve performance if you know this is not the case.''',
                                            is_optional=True,
                                        ),
                                        'registrationRequired': Field(
                                            String,
                                            description='''Compression and Serialization: Whether to require registration with Kryo. If set to 'true', Kryo will throw an exception if an unregistered class is serialized. If set to false (the default), Kryo will write unregistered class names along with each object. Writing class names can cause significant performance overhead, so enabling this option can enforce strictly that a user has not omitted classes from registration.''',
                                            is_optional=True,
                                        ),
                                        'registrator': Field(
                                            String,
                                            description='''Compression and Serialization: If you use Kryo serialization, give a comma-separated list of classes that register your custom classes with Kryo. This property is useful if you need to register your classes in a custom way, e.g. to specify a custom field serializer. Otherwise spark.kryo.classesToRegister is simpler. It should be set to classes that extend KryoRegistrator. See the tuning guide for more details.''',
                                            is_optional=True,
                                        ),
                                        'unsafe': Field(
                                            String,
                                            description='''Compression and Serialization: Whether to use unsafe based Kryo serializer. Can be substantially faster by using Unsafe Based IO.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'kryoserializer': Field(
                                PermissiveDict(
                                    fields={
                                        'buffer': Field(
                                            PermissiveDict(
                                                fields={
                                                    'root': Field(
                                                        String,
                                                        description='''Compression and Serialization: Initial size of Kryo's serialization buffer, in KiB unless otherwise specified. Note that there will be one buffer per core on each worker. This buffer will grow up to spark.kryoserializer.buffer.max if needed.''',
                                                        is_optional=True,
                                                    ),
                                                    'max': Field(
                                                        String,
                                                        description='''Compression and Serialization: Maximum allowable size of Kryo serialization buffer, in MiB unless otherwise specified. This must be larger than any object you attempt to serialize and must be less than 2048m. Increase this if you get a "buffer limit exceeded" exception inside Kryo.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'rdd': Field(
                                PermissiveDict(
                                    fields={
                                        'compress': Field(
                                            String,
                                            description='''Compression and Serialization: Whether to compress serialized RDD partitions (e.g. for StorageLevel.MEMORY_ONLY_SER in Java and Scala or StorageLevel.MEMORY_ONLY in Python). Can save substantial space at the cost of some extra CPU time. Compression will use spark.io.compression.codec.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'serializer': Field(
                                PermissiveDict(
                                    fields={
                                        'root': Field(
                                            String,
                                            description='''Compression and Serialization: Class to use for serializing objects that will be sent over the network or need to be cached in serialized form. The default of Java serialization works with any Serializable Java object but is quite slow, so we recommend using org.apache.spark.serializer.KryoSerializer and configuring Kryo serialization when speed is necessary. Can be any subclass of org.apache.spark.Serializer.''',
                                            is_optional=True,
                                        ),
                                        'objectStreamReset': Field(
                                            String,
                                            description='''Compression and Serialization: When serializing using org.apache.spark.serializer.JavaSerializer, the serializer caches objects to prevent writing redundant data, however that stops garbage collection of those objects. By calling 'reset' you flush that info from the serializer, and allow old objects to be collected. To turn off this periodic reset set it to -1. By default it will reset the serializer every 100 objects.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'memory': Field(
                                PermissiveDict(
                                    fields={
                                        'fraction': Field(
                                            Float,
                                            description='''Memory Management: Fraction of (heap space - 300MB) used for execution and storage. The lower this is, the more frequently spills and cached data eviction occur. The purpose of this config is to set aside memory for internal metadata, user data structures, and imprecise size estimation in the case of sparse, unusually large records. Leaving this at the default value is recommended. For more detail, including important information about correctly tuning JVM garbage collection when increasing this value, see this description.''',
                                            is_optional=True,
                                        ),
                                        'storageFraction': Field(
                                            Float,
                                            description='''Memory Management: Amount of storage memory immune to eviction, expressed as a fraction of the size of the region set aside by spark.memory.fraction. The higher this is, the less working memory may be available to execution and tasks may spill to disk more often. Leaving this at the default value is recommended. For more detail, see this description.''',
                                            is_optional=True,
                                        ),
                                        'offHeap': Field(
                                            PermissiveDict(
                                                fields={
                                                    'enabled': Field(
                                                        Bool,
                                                        description='''Memory Management: If true, Spark will attempt to use off-heap memory for certain operations. If off-heap memory use is enabled, then spark.memory.offHeap.size must be positive.''',
                                                        is_optional=True,
                                                    ),
                                                    'size': Field(
                                                        Int,
                                                        description='''Memory Management: The absolute amount of memory in bytes which can be used for off-heap allocation. This setting has no impact on heap memory usage, so if your executors' total memory consumption must fit within some hard limit then be sure to shrink your JVM heap size accordingly. This must be set to a positive value when spark.memory.offHeap.enabled=true.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'useLegacyMode': Field(
                                            Bool,
                                            description='''Memory Management: Whether to enable the legacy memory management mode used in Spark 1.5 and before. The legacy mode rigidly partitions the heap space into fixed-size regions, potentially leading to excessive spilling if the application was not tuned. The following deprecated memory fraction configurations are not read unless this is enabled: spark.shuffle.memoryFraction spark.storage.memoryFraction spark.storage.unrollFraction''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'storage': Field(
                                PermissiveDict(
                                    fields={
                                        'memoryFraction': Field(
                                            Float,
                                            description='''Memory Management: (deprecated) This is read only if spark.memory.useLegacyMode is enabled. Fraction of Java heap to use for Spark's memory cache. This should not be larger than the "old" generation of objects in the JVM, which by default is given 0.6 of the heap, but you can increase it if you configure your own old generation size.''',
                                            is_optional=True,
                                        ),
                                        'unrollFraction': Field(
                                            Float,
                                            description='''Memory Management: (deprecated) This is read only if spark.memory.useLegacyMode is enabled. Fraction of spark.storage.memoryFraction to use for unrolling blocks in memory. This is dynamically allocated by dropping existing blocks when there is not enough free storage space to unroll the new block in its entirety.''',
                                            is_optional=True,
                                        ),
                                        'replication': Field(
                                            PermissiveDict(
                                                fields={
                                                    'proactive': Field(
                                                        Bool,
                                                        description='''Memory Management: Enables proactive block replication for RDD blocks. Cached RDD block replicas lost due to executor failures are replenished if there are any existing available replicas. This tries to get the replication level of the block to the initial number.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'memoryMapThreshold': Field(
                                            String,
                                            description='''Execution Behavior: Size in bytes of a block above which Spark memory maps when reading a block from disk. This prevents Spark from memory mapping very small blocks. In general, memory mapping has high overhead for blocks close to or below the page size of the operating system.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'cleaner': Field(
                                PermissiveDict(
                                    fields={
                                        'periodicGC': Field(
                                            PermissiveDict(
                                                fields={
                                                    'interval': Field(
                                                        String,
                                                        description='''Memory Management: Controls how often to trigger a garbage collection. This context cleaner triggers cleanups only when weak references are garbage collected. In long-running applications with large driver JVMs, where there is little memory pressure on the driver, this may happen very occasionally or not at all. Not cleaning at all may lead to executors running out of disk space after a while.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'referenceTracking': Field(
                                            PermissiveDict(
                                                fields={
                                                    'root': Field(
                                                        Bool,
                                                        description='''Memory Management: Enables or disables context cleaning.''',
                                                        is_optional=True,
                                                    ),
                                                    'blocking': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'root': Field(
                                                                    Bool,
                                                                    description='''Memory Management: Controls whether the cleaning thread should block on cleanup tasks (other than shuffle, which is controlled by spark.cleaner.referenceTracking.blocking.shuffle Spark property).''',
                                                                    is_optional=True,
                                                                ),
                                                                'shuffle': Field(
                                                                    Bool,
                                                                    description='''Memory Management: Controls whether the cleaning thread should block on shuffle cleanup tasks.''',
                                                                    is_optional=True,
                                                                ),
                                                            }
                                                        )
                                                    ),
                                                    'cleanCheckpoints': Field(
                                                        Bool,
                                                        description='''Memory Management: Controls whether to clean checkpoint files if the reference is out of scope.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'default': Field(
                                PermissiveDict(
                                    fields={
                                        'parallelism': Field(
                                            Int,
                                            description='''Execution Behavior: Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'hadoop': Field(
                                PermissiveDict(
                                    fields={
                                        'cloneConf': Field(
                                            Bool,
                                            description='''Execution Behavior: If set to true, clones a new Hadoop Configuration object for each task. This option should be enabled to work around Configuration thread-safety issues (see SPARK-2546 for more details). This is disabled by default in order to avoid unexpected performance regressions for jobs that are not affected by these issues.''',
                                            is_optional=True,
                                        ),
                                        'validateOutputSpecs': Field(
                                            Bool,
                                            description='''Execution Behavior: If set to true, validates the output specification (e.g. checking if the output directory already exists) used in saveAsHadoopFile and other variants. This can be disabled to silence exceptions due to pre-existing output directories. We recommend that users do not disable this except if trying to achieve compatibility with previous versions of Spark. Simply use Hadoop's FileSystem API to delete output directories by hand. This setting is ignored for jobs generated through Spark Streaming's StreamingContext, since data may need to be rewritten to pre-existing output directories during checkpoint recovery.''',
                                            is_optional=True,
                                        ),
                                        'mapreduce': Field(
                                            PermissiveDict(
                                                fields={
                                                    'fileoutputcommitter': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'algorithm': Field(
                                                                    PermissiveDict(
                                                                        fields={
                                                                            'version': Field(
                                                                                Int,
                                                                                description='''Execution Behavior: The file output committer algorithm version, valid algorithm version number: 1 or 2. Version 2 may have better performance, but version 1 may handle failures better in certain situations, as per MAPREDUCE-4815.''',
                                                                                is_optional=True,
                                                                            )
                                                                        }
                                                                    )
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'rpc': Field(
                                PermissiveDict(
                                    fields={
                                        'message': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxSize': Field(
                                                        String,
                                                        description='''Networking: Maximum message size (in MB) to allow in "control plane" communication; generally only applies to map output size information sent between executors and the driver. Increase this if you are running jobs with many thousands of map and reduce tasks and see messages about the RPC message size.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'numRetries': Field(
                                            String,
                                            description='''Networking: Number of times to retry before an RPC task gives up. An RPC task will run at most times of this number.''',
                                            is_optional=True,
                                        ),
                                        'retry': Field(
                                            PermissiveDict(
                                                fields={
                                                    'wait': Field(
                                                        String,
                                                        description='''Networking: Duration for an RPC ask operation to wait before retrying.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'askTimeout': Field(
                                            String,
                                            description='''Networking: Duration for an RPC ask operation to wait before timing out.''',
                                            is_optional=True,
                                        ),
                                        'lookupTimeout': Field(
                                            String,
                                            description='''Networking: Duration for an RPC remote endpoint lookup operation to wait before timing out.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'blockManager': Field(
                                PermissiveDict(
                                    fields={
                                        'port': Field(
                                            String,
                                            description='''Networking: Port for all block managers to listen on. These exist on both the driver and the executors.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'network': Field(
                                PermissiveDict(
                                    fields={
                                        'timeout': Field(
                                            String,
                                            description='''Networking: Default timeout for all network interactions. This config will be used in place of spark.core.connection.ack.wait.timeout, spark.storage.blockManagerSlaveTimeoutMs, spark.shuffle.io.connectionTimeout, spark.rpc.askTimeout or spark.rpc.lookupTimeout if they are not configured.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'port': Field(
                                PermissiveDict(
                                    fields={
                                        'maxRetries': Field(
                                            String,
                                            description='''Networking: Maximum number of retries when binding to a port before giving up. When a port is given a specific value (non 0), each subsequent retry will increment the port used in the previous attempt by 1 before retrying. This essentially allows it to try a range of ports from the start port specified to port + maxRetries.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'core': Field(
                                PermissiveDict(
                                    fields={
                                        'connection': Field(
                                            PermissiveDict(
                                                fields={
                                                    'ack': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'wait': Field(
                                                                    PermissiveDict(
                                                                        fields={
                                                                            'timeout': Field(
                                                                                String,
                                                                                description='''Networking: How long for the connection to wait for ack to occur before timing out and giving up. To avoid unwilling timeout caused by long pause like GC, you can set larger value.''',
                                                                                is_optional=True,
                                                                            )
                                                                        }
                                                                    )
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'cores': Field(
                                PermissiveDict(
                                    fields={
                                        'max': Field(
                                            String,
                                            description='''Scheduling: When running on a standalone deploy cluster or a Mesos cluster in "coarse-grained" sharing mode, the maximum amount of CPU cores to request for the application from across the cluster (not from each machine). If not set, the default will be spark.deploy.defaultCores on Spark's standalone cluster manager, or infinite (all available cores) on Mesos.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'locality': Field(
                                PermissiveDict(
                                    fields={
                                        'wait': Field(
                                            PermissiveDict(
                                                fields={
                                                    'root': Field(
                                                        String,
                                                        description='''Scheduling: How long to wait to launch a data-local task before giving up and launching it on a less-local node. The same wait will be used to step through multiple locality levels (process-local, node-local, rack-local and then any). It is also possible to customize the waiting time for each level by setting spark.locality.wait.node, etc. You should increase this setting if your tasks are long and see poor locality, but the default usually works well.''',
                                                        is_optional=True,
                                                    ),
                                                    'node': Field(
                                                        String,
                                                        description='''Scheduling: Customize the locality wait for node locality. For example, you can set this to 0 to skip node locality and search immediately for rack locality (if your cluster has rack information).''',
                                                        is_optional=True,
                                                    ),
                                                    'process': Field(
                                                        String,
                                                        description='''Scheduling: Customize the locality wait for process locality. This affects tasks that attempt to access cached data in a particular executor process.''',
                                                        is_optional=True,
                                                    ),
                                                    'rack': Field(
                                                        String,
                                                        description='''Scheduling: Customize the locality wait for rack locality.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'scheduler': Field(
                                PermissiveDict(
                                    fields={
                                        'maxRegisteredResourcesWaitingTime': Field(
                                            String,
                                            description='''Scheduling: Maximum amount of time to wait for resources to register before scheduling begins.''',
                                            is_optional=True,
                                        ),
                                        'minRegisteredResourcesRatio': Field(
                                            String,
                                            description='''Scheduling: The minimum ratio of registered resources (registered resources / total expected resources) (resources are executors in yarn mode and Kubernetes mode, CPU cores in standalone mode and Mesos coarse-grained mode ['spark.cores.max' value is total expected resources for Mesos coarse-grained mode] ) to wait for before scheduling begins. Specified as a double between 0.0 and 1.0. Regardless of whether the minimum ratio of resources has been reached, the maximum amount of time it will wait before scheduling begins is controlled by config spark.scheduler.maxRegisteredResourcesWaitingTime.''',
                                            is_optional=True,
                                        ),
                                        'mode': Field(
                                            String,
                                            description='''Scheduling: The scheduling mode between jobs submitted to the same SparkContext. Can be set to FAIR to use fair sharing instead of queueing jobs one after another. Useful for multi-user services.''',
                                            is_optional=True,
                                        ),
                                        'revive': Field(
                                            PermissiveDict(
                                                fields={
                                                    'interval': Field(
                                                        String,
                                                        description='''Scheduling: The interval length for the scheduler to revive the worker resource offers to run tasks.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'listenerbus': Field(
                                            PermissiveDict(
                                                fields={
                                                    'eventqueue': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'capacity': Field(
                                                                    String,
                                                                    description='''Scheduling: Capacity for event queue in Spark listener bus, must be greater than 0. Consider increasing value (e.g. 20000) if listener events are dropped. Increasing this value may result in the driver using more memory.''',
                                                                    is_optional=True,
                                                                )
                                                            }
                                                        )
                                                    )
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'blacklist': Field(
                                PermissiveDict(
                                    fields={
                                        'enabled': Field(
                                            String,
                                            description='''Scheduling: If set to "true", prevent Spark from scheduling tasks on executors that have been blacklisted due to too many task failures. The blacklisting algorithm can be further controlled by the other "spark.blacklist" configuration options.''',
                                            is_optional=True,
                                        ),
                                        'timeout': Field(
                                            String,
                                            description='''Scheduling: (Experimental) How long a node or executor is blacklisted for the entire application, before it is unconditionally removed from the blacklist to attempt running new tasks.''',
                                            is_optional=True,
                                        ),
                                        'task': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxTaskAttemptsPerExecutor': Field(
                                                        String,
                                                        description='''Scheduling: (Experimental) For a given task, how many times it can be retried on one executor before the executor is blacklisted for that task.''',
                                                        is_optional=True,
                                                    ),
                                                    'maxTaskAttemptsPerNode': Field(
                                                        String,
                                                        description='''Scheduling: (Experimental) For a given task, how many times it can be retried on one node, before the entire node is blacklisted for that task.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'stage': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxFailedTasksPerExecutor': Field(
                                                        String,
                                                        description='''Scheduling: (Experimental) How many different tasks must fail on one executor, within one stage, before the executor is blacklisted for that stage.''',
                                                        is_optional=True,
                                                    ),
                                                    'maxFailedExecutorsPerNode': Field(
                                                        String,
                                                        description='''Scheduling: (Experimental) How many different executors are marked as blacklisted for a given stage, before the entire node is marked as failed for the stage.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                        'application': Field(
                                            PermissiveDict(
                                                fields={
                                                    'maxFailedTasksPerExecutor': Field(
                                                        String,
                                                        description='''Scheduling: (Experimental) How many different tasks must fail on one executor, in successful task sets, before the executor is blacklisted for the entire application. Blacklisted executors will be automatically added back to the pool of available resources after the timeout specified by spark.blacklist.timeout. Note that with dynamic allocation, though, the executors may get marked as idle and be reclaimed by the cluster manager.''',
                                                        is_optional=True,
                                                    ),
                                                    'maxFailedExecutorsPerNode': Field(
                                                        String,
                                                        description='''Scheduling: (Experimental) How many different executors must be blacklisted for the entire application, before the node is blacklisted for the entire application. Blacklisted nodes will be automatically added back to the pool of available resources after the timeout specified by spark.blacklist.timeout. Note that with dynamic allocation, though, the executors on the node may get marked as idle and be reclaimed by the cluster manager.''',
                                                        is_optional=True,
                                                    ),
                                                    'fetchFailure': Field(
                                                        PermissiveDict(
                                                            fields={
                                                                'enabled': Field(
                                                                    String,
                                                                    description='''Scheduling: (Experimental) If set to "true", Spark will blacklist the executor immediately when a fetch failure happens. If external shuffle service is enabled, then the whole node will be blacklisted.''',
                                                                    is_optional=True,
                                                                )
                                                            }
                                                        )
                                                    ),
                                                }
                                            )
                                        ),
                                        'killBlacklistedExecutors': Field(
                                            String,
                                            description='''Scheduling: (Experimental) If set to "true", allow Spark to automatically kill the executors when they are blacklisted on fetch failure or blacklisted for the entire application, as controlled by spark.blacklist.application.*. Note that, when an entire node is added to the blacklist, all of the executors on that node will be killed.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'speculation': Field(
                                PermissiveDict(
                                    fields={
                                        'root': Field(
                                            String,
                                            description='''Scheduling: If set to "true", performs speculative execution of tasks. This means if one or more tasks are running slowly in a stage, they will be re-launched.''',
                                            is_optional=True,
                                        ),
                                        'interval': Field(
                                            String,
                                            description='''Scheduling: How often Spark will check for tasks to speculate.''',
                                            is_optional=True,
                                        ),
                                        'multiplier': Field(
                                            String,
                                            description='''Scheduling: How many times slower a task is than the median to be considered for speculation.''',
                                            is_optional=True,
                                        ),
                                        'quantile': Field(
                                            String,
                                            description='''Scheduling: Fraction of tasks which must be complete before speculation is enabled for a particular stage.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'task': Field(
                                PermissiveDict(
                                    fields={
                                        'cpus': Field(
                                            String,
                                            description='''Scheduling: Number of cores to allocate for each task.''',
                                            is_optional=True,
                                        ),
                                        'maxFailures': Field(
                                            String,
                                            description='''Scheduling: Number of failures of any particular task before giving up on the job. The total number of failures spread across different tasks will not cause the job to fail; a particular task has to fail this number of attempts. Should be greater than or equal to 1. Number of allowed retries = this value - 1.''',
                                            is_optional=True,
                                        ),
                                        'reaper': Field(
                                            PermissiveDict(
                                                fields={
                                                    'enabled': Field(
                                                        String,
                                                        description='''Scheduling: Enables monitoring of killed / interrupted tasks. When set to true, any task which is killed will be monitored by the executor until that task actually finishes executing. See the other spark.task.reaper.* configurations for details on how to control the exact behavior of this monitoring. When set to false (the default), task killing will use an older code path which lacks such monitoring.''',
                                                        is_optional=True,
                                                    ),
                                                    'pollingInterval': Field(
                                                        String,
                                                        description='''Scheduling: When spark.task.reaper.enabled = true, this setting controls the frequency at which executors will poll the status of killed tasks. If a killed task is still running when polled then a warning will be logged and, by default, a thread-dump of the task will be logged (this thread dump can be disabled via the spark.task.reaper.threadDump setting, which is documented below).''',
                                                        is_optional=True,
                                                    ),
                                                    'threadDump': Field(
                                                        String,
                                                        description='''Scheduling: When spark.task.reaper.enabled = true, this setting controls whether task thread dumps are logged during periodic polling of killed tasks. Set this to false to disable collection of thread dumps.''',
                                                        is_optional=True,
                                                    ),
                                                    'killTimeout': Field(
                                                        String,
                                                        description='''Scheduling: When spark.task.reaper.enabled = true, this setting specifies a timeout after which the executor JVM will kill itself if a killed task has not stopped running. The default value, -1, disables this mechanism and prevents the executor from self-destructing. The purpose of this setting is to act as a safety-net to prevent runaway noncancellable tasks from rendering an executor unusable.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                            'stage': Field(
                                PermissiveDict(
                                    fields={
                                        'maxConsecutiveAttempts': Field(
                                            String,
                                            description='''Scheduling: Number of consecutive stage attempts allowed before a stage is aborted.''',
                                            is_optional=True,
                                        )
                                    }
                                )
                            ),
                            'dynamicAllocation': Field(
                                PermissiveDict(
                                    fields={
                                        'enabled': Field(
                                            String,
                                            description='''Dynamic Allocation: Whether to use dynamic resource allocation, which scales the number of executors registered with this application up and down based on the workload. For more detail, see the description here. This requires spark.shuffle.service.enabled to be set. The following configurations are also relevant: spark.dynamicAllocation.minExecutors, spark.dynamicAllocation.maxExecutors, and spark.dynamicAllocation.initialExecutors spark.dynamicAllocation.executorAllocationRatio''',
                                            is_optional=True,
                                        ),
                                        'executorIdleTimeout': Field(
                                            String,
                                            description='''Dynamic Allocation: If dynamic allocation is enabled and an executor has been idle for more than this duration, the executor will be removed. For more detail, see this description.''',
                                            is_optional=True,
                                        ),
                                        'cachedExecutorIdleTimeout': Field(
                                            String,
                                            description='''Dynamic Allocation: If dynamic allocation is enabled and an executor which has cached data blocks has been idle for more than this duration, the executor will be removed. For more details, see this description.''',
                                            is_optional=True,
                                        ),
                                        'initialExecutors': Field(
                                            String,
                                            description='''Dynamic Allocation: Initial number of executors to run if dynamic allocation is enabled. If `--num-executors` (or `spark.executor.instances`) is set and larger than this value, it will be used as the initial number of executors.''',
                                            is_optional=True,
                                        ),
                                        'maxExecutors': Field(
                                            String,
                                            description='''Dynamic Allocation: Upper bound for the number of executors if dynamic allocation is enabled.''',
                                            is_optional=True,
                                        ),
                                        'minExecutors': Field(
                                            String,
                                            description='''Dynamic Allocation: Lower bound for the number of executors if dynamic allocation is enabled.''',
                                            is_optional=True,
                                        ),
                                        'executorAllocationRatio': Field(
                                            String,
                                            description='''Dynamic Allocation: By default, the dynamic allocation will request enough executors to maximize the parallelism according to the number of tasks to process. While this minimizes the latency of the job, with small tasks this setting can waste a lot of resources due to executor allocation overhead, as some executor might not even do any work. This setting allows to set a ratio that will be used to reduce the number of executors w.r.t. full parallelism. Defaults to 1.0 to give maximum parallelism. 0.5 will divide the target number of executors by 2 The target number of executors computed by the dynamicAllocation can still be overridden by the spark.dynamicAllocation.minExecutors and spark.dynamicAllocation.maxExecutors settings''',
                                            is_optional=True,
                                        ),
                                        'schedulerBacklogTimeout': Field(
                                            String,
                                            description='''Dynamic Allocation: If dynamic allocation is enabled and there have been pending tasks backlogged for more than this duration, new executors will be requested. For more detail, see this description.''',
                                            is_optional=True,
                                        ),
                                        'sustainedSchedulerBacklogTimeout': Field(
                                            String,
                                            description='''Dynamic Allocation: Same as spark.dynamicAllocation.schedulerBacklogTimeout, but used only for subsequent executor requests. For more detail, see this description.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'r': Field(
                                PermissiveDict(
                                    fields={
                                        'numRBackendThreads': Field(
                                            String,
                                            description='''SparkR: Number of threads used by RBackend to handle RPC calls from SparkR package.''',
                                            is_optional=True,
                                        ),
                                        'command': Field(
                                            String,
                                            description='''SparkR: Executable for executing R scripts in cluster modes for both driver and workers.''',
                                            is_optional=True,
                                        ),
                                        'driver': Field(
                                            PermissiveDict(
                                                fields={
                                                    'command': Field(
                                                        String,
                                                        description='''SparkR: Executable for executing R scripts in client modes for driver. Ignored in cluster modes.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'shell': Field(
                                            PermissiveDict(
                                                fields={
                                                    'command': Field(
                                                        String,
                                                        description='''SparkR: Executable for executing sparkR shell in client modes for driver. Ignored in cluster modes. It is the same as environment variable SPARKR_DRIVER_R, but take precedence over it. spark.r.shell.command is used for sparkR shell while spark.r.driver.command is used for running R script.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        ),
                                        'backendConnectionTimeout': Field(
                                            String,
                                            description='''SparkR: Connection timeout set by R process on its connection to RBackend in seconds.''',
                                            is_optional=True,
                                        ),
                                        'heartBeatInterval': Field(
                                            String,
                                            description='''SparkR: Interval for heartbeats sent from SparkR backend to R process to prevent connection timeout.''',
                                            is_optional=True,
                                        ),
                                    }
                                )
                            ),
                            'graphx': Field(
                                PermissiveDict(
                                    fields={
                                        'pregel': Field(
                                            PermissiveDict(
                                                fields={
                                                    'checkpointInterval': Field(
                                                        String,
                                                        description='''GraphX: Checkpoint interval for graph and message in Pregel. It used to avoid stackOverflowError due to long lineage chains after lots of iterations. The checkpoint is disabled by default.''',
                                                        is_optional=True,
                                                    )
                                                }
                                            )
                                        )
                                    }
                                )
                            ),
                            'deploy': Field(
                                PermissiveDict(
                                    fields={
                                        'recoveryMode': Field(
                                            String,
                                            description='''Deploy: The recovery mode setting to recover submitted Spark jobs with cluster mode when it failed and relaunches. This is only applicable for cluster mode when running with Standalone or Mesos.''',
                                            is_optional=True,
                                        ),
                                        'zookeeper': Field(
                                            PermissiveDict(
                                                fields={
                                                    'url': Field(
                                                        String,
                                                        description='''Deploy: When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to.''',
                                                        is_optional=True,
                                                    ),
                                                    'dir': Field(
                                                        String,
                                                        description='''Deploy: When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper directory to store recovery state.''',
                                                        is_optional=True,
                                                    ),
                                                }
                                            )
                                        ),
                                    }
                                )
                            ),
                        }
                    )
                )
            }
        )
    )


# pylint: enable=line-too-long
