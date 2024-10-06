from pyspark.sql.functions import col, sum, date_format, quarter
from pyspark.sql.window import Window
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

vendas_df = vendas_df.withColumn("data_venda", F.to_date(col("data_venda"), "yyyy-MM-dd"))

vendas_df = vendas_df.join(distribuidores_df, "cod_distribuicao")

# 1- Ranking Mensal
print("[LOG] >> Iniciando Ranking mensal")
vendas_mensal = vendas_df.withColumn("Mes", date_format(col("data_venda"), "yyyy-MM")) \
                         .groupBy("nm_distribuicao", "Mes", "nm_vendedor") \
                         .agg(sum("vl_venda").alias("Valor"))

window_mensal = Window.partitionBy("Mes", "nm_distribuicao").orderBy(col("Valor").desc())

ranking_mensal = vendas_mensal.withColumn("rank", F.row_number().over(window_mensal)) \
                              .filter(col("rank") <= 10) \
                              .select("nm_distribuicao", "Mes", "nm_vendedor", "Valor")

try:
    sparkmtrix.write_jdbc(
        dataframe=ranking_mensal,
        url=url,
        table_name="ranking_mensal",
        mode="overwrite",
        properties=properties)
except Exception as e:
    print(f"[DEBUG] >> Ocorreu um erro: {e}")



# 2- Ranking Trimestral
print("[LOG] >> Iniciando Ranking trimestral")
vendas_trimestral = vendas_df.withColumn("Quarter", quarter(col("data_venda"))) \
                             .withColumn("Ano", date_format(col("data_venda"), "yyyy")) \
                             .groupBy("nm_distribuicao", "Ano", "Quarter", "nm_vendedor") \
                             .agg(sum("vl_venda").alias("Valor"))

window_trimestral = Window.partitionBy("Ano", "Quarter", "nm_distribuicao").orderBy(col("Valor").desc())

ranking_trimestral = vendas_trimestral.withColumn("rank", F.row_number().over(window_trimestral)) \
                                      .filter(col("rank") <= 10) \
                                      .select("nm_distribuicao", "Ano", "Quarter", "nm_vendedor", "Valor")

try:
    sparkmtrix.write_jdbc(
        dataframe=ranking_trimestral,
        url=url,
        table_name="ranking_trimestral",
        mode="overwrite",
        properties=properties)
except Exception as e:
    print(f"[DEBUG] >> Ocorreu um erro: {e}")



# 3- Ranking Anual
print("[LOG] >> Iniciando Ranking anual")
vendas_anual = vendas_df.withColumn("Ano", date_format(col("data_venda"), "yyyy")) \
                        .groupBy("nm_distribuicao", "Ano", "nm_vendedor") \
                        .agg(sum("vl_venda").alias("Valor"))

window_anual = Window.partitionBy("Ano", "nm_distribuicao").orderBy(col("Valor").desc())

ranking_anual = vendas_anual.withColumn("rank", F.row_number().over(window_anual)) \
                            .filter(col("rank") <= 10) \
                            .select("nm_distribuicao", "Ano", "nm_vendedor", "Valor")

try:
    sparkmtrix.write_jdbc(
        dataframe=ranking_anual,
        url=url,
        table_name="ranking_anual",
        mode="overwrite",
        properties=properties)
except Exception as e:
    print(f"[DEBUG] >> Ocorreu um erro: {e}")

