from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("Pagila").config("spark.jars", "postgresql-42.4.0.jar").getOrCreate()


def df_loader(table_name: str):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", f"{table_name}") \
        .option("user", "postgres") \
        .option("password", "secret") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df