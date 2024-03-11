import re
from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class


SPARK_INIT_CODE = """
spark.sparkContext._jvm.org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator.registerAll(sc._jvm.SQLContext(sc._jsc.sc()))
"""


SPARK_IMPORT_DATASET_CODE = """
import os

def resolve_dataset_ref(dataset_ref):
    if "/" not in dataset_ref:
        # Single-tenant
        data_path = os.path.join(dataset_ref, "data")
        if os.path.exists(data_path):
            return data_path
    else:
        # Multi-tenant
        # Assuming layout <account_name>/<dataset_id>/info/alias
        account_path, name = dataset_ref.split("/", 1)
        if os.path.isdir(account_path):
            for dataset_id in os.listdir(account_path):
                alias_path = os.path.join(account_path, dataset_id, "info", "alias")
                if not os.path.exists(alias_path):
                    continue
                with open(alias_path) as f:
                    alias = f.read().strip()
                if alias != name:
                    continue
                return os.path.join(account_path, dataset_id, "data")

    raise Exception(f"Dataset {{dataset_ref}} not found")

data_path = resolve_dataset_ref("{ref}")
{alias} = spark.read.parquet(os.path.join(data_path, "*"))
{alias}.createOrReplaceTempView("`{ref}`")
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
    @magic_arguments.argument('dataset_ref',
        nargs=1,
        help='Dataset to load'
    )
    @magic_arguments.argument('--alias',
        help='Also registers the dataset under provided alias'
    )
    def import_dataset(self, line):
        args = magic_arguments.parse_argstring(self.import_dataset, line)
        dataset_ref = args.dataset_ref[0]
        if not args.alias:
            args.alias = re.sub(r"[\.\-/]", "_", dataset_ref)
        code = SPARK_IMPORT_DATASET_CODE.format(
            ref=dataset_ref,
            alias=args.alias,
        )
        self.shell.run_cell_magic('spark', '', code)



def load_ipython_extension(ipython):
    ipython.register_magics(KamuMagics)
