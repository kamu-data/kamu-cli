import jpype
import jpype.dbapi2
import os


classpath = os.path.join(os.getcwd(), "path-to/flight-sql-jdbc-driver-13.0.0.jar")
jpype.startJVM(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    classpath=classpath
)

conn = jpype.dbapi2.connect(
    "jdbc:arrow-flight-sql://127.0.0.1:50050?useEncryption=false", 
    driver_args={
        'user': 'kamu', 
        'password': 'kamu',
    }
)

cursor = conn.cursor()
res = cursor.execute("show tables").fetchall()
print(res)

cursor.close()
conn.close()
