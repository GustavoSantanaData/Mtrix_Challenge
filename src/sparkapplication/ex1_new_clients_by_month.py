from pyspark.sql.functions import col, date_format
from pyspark.sql import Window
import pyspark.sql.functions as F
import sparkmtrix as sparkmtrix


spark = sparkmtrix.sparksession()

url = f"jdbc:postgresql://localhost:5432/vendas_db"
properties = sparkmtrix.properties_postgresql()

try:
    vendas_df = sparkmtrix.read_jdbc(
        table="vendas",
        url=url
        )

    distribuidores_df = sparkmtrix.read_jdbc(
                        table="distribuidores",
                        url=url
                        )
except Exception as e:
    print(f"Ocorreu um erro: {e}")

df = vendas_df.join(distribuidores_df, vendas_df.cod_distribuicao == distribuidores_df.cod_distribuicao, 'inner') \
    .select("NM_DISTRIBUICAO", "CLIENTE", "DATA_VENDA")

df = df.withColumn("Ano_Mes", date_format(col("DATA_VENDA"), "yyyy-MM"))

window_spec = Window.partitionBy("CLIENTE").orderBy("DATA_VENDA")
df_new_customers = df.withColumn("first_purchase", F.min("DATA_VENDA").over(window_spec)) \
    .filter(F.col("DATA_VENDA") == F.col("first_purchase")) \
    .select("NM_DISTRIBUICAO", "CLIENTE", "Ano_Mes") \
    .distinct()

df_pivot = df_new_customers.groupBy("NM_DISTRIBUICAO", "CLIENTE") \
    .pivot("Ano_Mes") \
    .agg(F.lit(1))

df_pivot.show()

try:
    output_path = "./saved_tables/exercicio1_novos_clientes.parquet"
    sparkmtrix.write_dataset(
        dataframe=df_pivot,
        mode="overwrite",
        format="parquet",
        output_path=output_path
        )
except Exception as e:
    print(f"Ocorreu um erro: {e}")

try:
    sparkmtrix.write_jdbc(
        dataframe=df_pivot,
        url=url,
        table_name="novos_clientes",
        mode="overwrite",
        properties=properties)
except Exception as e:
    print(f"[DEBUG] >> Ocorreu um erro: {e}")

spark.stop()
