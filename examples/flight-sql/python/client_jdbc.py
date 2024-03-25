import jpype
import jpype.dbapi2
import os

DRIVER_PATH = os.path.join(os.getcwd(), "path-to/flight-sql-jdbc-driver-13.0.0.jar")
if not os.path.exists(DRIVER_PATH):
    raise Exception(f"Driver not found at: {DRIVER_PATH}")

jpype.startJVM(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    classpath=DRIVER_PATH
)

# No-TLS local connection
#
# To test with local server use:
#   cd examples/reth-vs-snp500
#   kamu -vv sql server --flight-sql --port 50050 --address 0.0.0.0
#
conn = jpype.dbapi2.connect(
    "jdbc:arrow-flight-sql://127.0.0.1:50050?useEncryption=false",
    driver_args={
        'user': 'kamu',
        'password': 'kamu',
    }
)

# Secure remote connection
# conn = jpype.dbapi2.connect(
#     "jdbc:arrow-flight-sql://node.demo.kamu.dev:50050",
#     driver_args={
#         'user': 'kamu',
#         'password': 'kamu',
#     }
# )

cursor = conn.cursor()
res = cursor.execute("show tables").fetchall()
print(res)

cursor.close()
conn.close()
