
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf
import os
from dotenv import load_dotenv

load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

def sparksession():
    """
    Creates and configures a SparkSession with custom settings for AWS S3, MinIO, Delta Lake, and more.

    This function initializes a SparkSession with specific configurations:
    - Increases the limit for debug string fields.
    - Disables AWS SDK certificate checking for both the driver and executors.
    - Adds necessary JAR packages, including support for Hadoop AWS, Delta Lake, and PostgreSQL.
    - Enables Delta Lake features by extending Spark SQL with Delta-specific configurations.
    - Disables Delta retention duration checks.

    :return: (SparkSession) A SparkSession object with the applied configurations.
    """


    conf = SparkConf()
    conf.set("spark.sql.debug.maxToStringFields", "100")
    conf.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "io.delta:delta-core_2.12:1.2.1,"
            "org.postgresql:postgresql:42.2.18")

    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    spark = SparkSession.builder \
        .appName("Mtrix Environment Spark") \
        .master("local[*]") \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark

def properties_postgresql():
    """
    Creates a dictionary of connection properties for PostgreSQL.

    This function returns a dictionary containing the necessary properties for 
    connecting to a PostgreSQL database. The properties include:
    - User credentials (username and password).
    - The PostgreSQL JDBC driver.

    :return: (dict) A dictionary with PostgreSQL connection properties: 'user', 'password', and 'driver'.
    """

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    return properties


def write_dataset(dataframe, mode, format, output_path):
    """
    Writes a Spark DataFrame to a specified output path in the desired format.

    This function writes the provided DataFrame to the given output path in either Parquet or CSV format, 
    based on the value of the 'format' parameter. It supports writing with a specified save mode.

    Supported formats:
    - Parquet
    - CSV (with ';' as the separator and header included)

    :param dataframe: (DataFrame) The Spark DataFrame to be written.
    :param mode: (str) The save mode ('overwrite', 'append', etc.) for writing the data.
    :param format: (str) The format in which to write the data ('parquet' or 'csv').
    :param output_path: (str) The destination path where the data will be saved.
    
    :return: None
    """

    print(f"[LOG] >> Escrevendo dataset no formato {format}")
    if format == "parquet":
        dataframe.write.parquet(output_path, mode=mode)
    elif format == "csv":
        dataframe.write.csv(output_path, sep=";", header=True)
    
    print(f"[LOG] >> Dataset escrito com sucesso no formato {format}")


def write_jdbc(dataframe, url, table_name, mode, properties):
    """
    Writes a Spark DataFrame to a JDBC table.

    This function inserts the provided DataFrame into a specified table in a database 
    using a JDBC connection. The table name, database URL, save mode, and connection 
    properties (e.g., username, password, driver) are required for the insertion.

    :param dataframe: (DataFrame) The Spark DataFrame to be inserted into the database.
    :param url: (str) The JDBC URL of the database.
    :param table_name: (str) The name of the target table in the database.
    :param mode: (str) The save mode for writing the data (e.g., 'append', 'overwrite').
    :param properties: (dict) Connection properties including 'user', 'password', and 'driver'.
    
    :return: None
    """

    print(f"[LOG] >> Inserindo dataset na tabela {table_name}")
    dataframe.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

    print(f"[LOG] >> Dataset Inserido com sucesso na tabela {table_name}")


def read_jdbc(table, url):
    """
    Reads data from a JDBC source into a Spark DataFrame.

    This function connects to a JDBC database using the provided table name and URL, 
    and reads the data into a Spark DataFrame. The connection properties, such as 
    the user, password, and JDBC driver, are retrieved from the `properties_postgresql` function.

    :param table: (str) The name of the table to read from the database.
    :param url: (str) The JDBC URL of the database.

    :return: (DataFrame) The Spark DataFrame containing the data read from the table.
    """

    print(f"[LOG] >> Lendo tabela {table}")
    spark = sparksession()
    properties = properties_postgresql()
    vendas_df = spark.read.jdbc(url=url, table=table, properties=properties)

    return vendas_df



