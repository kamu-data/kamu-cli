import os
import re
import json
import time
import socket
import signal
import subprocess
from collections import namedtuple
from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class
from IPython.display import clear_output


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
        account_path, _ = dataset_ref.split("/", 1)
        if os.path.isdir(account_path):
            for dataset_id in os.listdir(account_path):
                alias_path = os.path.join(account_path, dataset_id, "info", "alias")
                if not os.path.exists(alias_path):
                    continue
                with open(alias_path) as f:
                    alias = f.read().strip()
                if alias != dataset_ref:
                    continue
                return os.path.join(account_path, dataset_id, "data")

    raise Exception(f"Dataset {{dataset_ref}} not found")

data_path = resolve_dataset_ref("{ref}")
{alias} = spark.read.parquet(os.path.join(data_path, "*"))
{alias}.createOrReplaceTempView("`{ref}`")
{alias}.createOrReplaceTempView("{alias}")
"""


LIVY_START_TIMEOUT = 60
LIVY_PIDFILE = os.path.expanduser("~/.local/kamu/livy.pid")
LIVY_STDOUT = os.path.expanduser("~/.local/kamu/livy.out.txt")
LIVY_STDERR = os.path.expanduser("~/.local/kamu/livy.err.txt")


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
        self._ensure_livy_is_running()

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
        self._ensure_images()
        self._ensure_livy_is_running()

        args = magic_arguments.parse_argstring(self.import_dataset, line)
        dataset_ref = args.dataset_ref[0]
        if not args.alias:
            args.alias = re.sub(r"[\.\-/]", "_", dataset_ref)
        code = SPARK_IMPORT_DATASET_CODE.format(
            ref=dataset_ref,
            alias=args.alias,
        )
        self.shell.run_cell_magic('spark', '', code)

    def _ensure_livy_is_running(self):
        livy = LivyProcessHelper()
        procinfo = livy.get_proc_info(check_running=True)
        if procinfo is None:
            print("Starting Livy server")
            livy.start(timeout=LIVY_START_TIMEOUT)
            clear_output()

    def _ensure_images(self):
        out = subprocess.run(["kamu", "init", "--pull-images", "--list-only"], capture_output=True)
        assert out.returncode == 0, "Failed to list images from kamu"
        images = [
            img for img in out.stdout.decode("utf-8").split("\n")
            if "spark" in img
        ]
        assert len(images) > 0, "No images in output"

        touch_image_statuses = (
            subprocess.run(["podman", "inspect", img], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for img in images
        )
        images_pulled = all(
            status.returncode == 0
            for status in touch_image_statuses
        )
        if images_pulled:
            return

        print("First time run. Please wait while we pull the necessary images.")
        for image in images:
            print(f"Pulling: {image}")
            out = subprocess.run(["podman", "pull", image])
            assert out.returncode == 0, f"Failed to pull image: {image}"

        clear_output()

    @line_magic
    def stop_livy(self, line):
         livy = LivyProcessHelper()
         livy.stop()


LivyProcInfo = namedtuple("LivyProcInfo", ["pid", "port"])


class LivyProcessHelper:
    def __init__(self, pidfile=LIVY_PIDFILE):
        self._pidfile = pidfile

    def get_proc_info(self, check_running=True):
        if not os.path.exists(self._pidfile):
            return None
        
        with open(self._pidfile, 'r') as f:
            procinfo = LivyProcInfo(**json.load(f))

        if not check_running:
            return procinfo

        if not self.is_running(procinfo=procinfo):
            return None

        return procinfo

    def save_proc_info(self, procinfo):
        pi_dir, _ = os.path.split(self._pidfile)
        os.makedirs(pi_dir, exist_ok=True)
        with open(self._pidfile, "w") as f:
            json.dump(procinfo._asdict(), f)

    def is_running(self, procinfo=None):
        if procinfo is None:
            procinfo = self.get_proc_info(check_running=False)
            if procinfo is None:
                return False

        return (
            self.is_process_running(procinfo.pid) and 
            self.is_port_open(procinfo.port)
        )

    def is_process_running(self, pid=None):
        if pid is None:
            procinfo = self.get_proc_info(check_running=False)
            if procinfo is None:
                return False
            pid = procinfo.pid

        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def is_port_open(self, port=None):
        if port is None:
            procinfo = self.get_proc_info(check_running=False)
            if procinfo is None:
                return False
            port = procinfo.port

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except socket.error:
            return False

    def start(self, timeout):
        if not self.is_in_workspace():
            raise Exception(
                "Current directory is not under kamu workspace. "
                "Create a workspace in the desired location by running `kamu init` in the terminal "
                "and place your notebook in that directory."
            )

        # TODO: Other pords are not supported due to podman running in host networking mode
        port = 8998

        out_dir, _ = os.path.split(LIVY_STDOUT)
        os.makedirs(out_dir, exist_ok=True)
        
        p = subprocess.Popen(
            ["/usr/local/bin/kamu", "sql", "server", "--livy", "--port", str(port)],
            stdout=open(LIVY_STDOUT, "w"),
            stderr=open(LIVY_STDERR, "w"),
            close_fds=True
        )

        deadline = time.time() + timeout
        while True:
            try:
                status = p.wait(1)
                raise Exception(
                    f"Livy failed to start with status code: {status}\n"
                    f"See logs for details:\n"
                    f"- {LIVY_STDOUT}\n"
                    f"- {LIVY_STDERR}"
                )
            except subprocess.TimeoutExpired:
                pass
            
            if self.is_port_open(port):
                break
            
            if time.time() >= deadline:
                p.send_signal(signal.SIGTERM)
                raise Exception(
                    f"Livy failed to start within {timeout} seconds\n"
                    f"See logs for details:\n"
                    f"- {LIVY_STDOUT}\n"
                    f"- {LIVY_STDERR}"
                )

        procinfo = LivyProcInfo(pid=p.pid, port=port)
        self.save_proc_info(procinfo)
        return procinfo

    def stop(self):
        procinfo = self.get_proc_info(check_running=False)
        if procinfo is None:
            return
        
        try:
            os.kill(procinfo.pid, signal.SIGTERM)
            print("Stopping Livy")
        except OSError:
            pass

    def is_in_workspace(self, cwd=None):
        p = subprocess.run(
            ["/usr/local/bin/kamu", "list"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=cwd,
        )

        return p.returncode == 0


def load_ipython_extension(ipython):
    ipython.register_magics(KamuMagics)
