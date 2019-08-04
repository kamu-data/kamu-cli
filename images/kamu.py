from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class


SPARK_INIT_CODE = """
spark.sparkContext._jvm.org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator.registerAll(sc._jvm.SQLContext(sc._jsc.sc()))
"""


SPARK_IMPORT_DATASET_CODE = """
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem.get(hadoop.conf.Configuration())

path = hadoop.fs.Path("{dataset_id}")
if not fs.exists(path):
    raise Exception("Dataset {dataset_id} does not exist")

{alias} = spark.read.parquet(str(path) + "/*")
{alias}.createOrReplaceTempView("`{dataset_id}`")
{alias}.createOrReplaceTempView("{alias}")
"""


@magics_class
class KamuMagics(Magics):
    @line_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument(
        '--executor-instances',
        type=int,
        default=2,
        help='Number of executor instances to run'
    )
    def kamu(self, line):
        args = magic_arguments.parse_argstring(self.kamu, line)
        code = SPARK_INIT_CODE
        self.shell.run_cell_magic('spark', '', code)

    @line_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('dataset_id',
        nargs=1,
        help='ID of the dataset to load'
    )
    @magic_arguments.argument('--alias',
        help='Also registers the dataset under provided alias'
    )
    def import_dataset(self, line):
        args = magic_arguments.parse_argstring(self.import_dataset, line)
        if not args.alias:
            args.alias = '_df'
        code = SPARK_IMPORT_DATASET_CODE.format(
            dataset_id=args.dataset_id[0], alias=args.alias)
        self.shell.run_cell_magic('spark', '', code)



def load_ipython_extension(ipython):
    ipython.register_magics(KamuMagics)
