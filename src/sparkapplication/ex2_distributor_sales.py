from pyspark.sql.functions import col, date_format, countDistinct, lit
from pyspark.sql import functions as F
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

vendas_df = vendas_df.withColumn("AnoMes", date_format(col("data_venda"), "yyyy-MM"))

vendas_por_dia = vendas_df.groupBy("cod_distribuicao", "AnoMes", "data_venda") \
                          .agg(countDistinct("data_venda").alias("DiasComVenda"))

dias_no_mes = vendas_por_dia.groupBy("cod_distribuicao", "AnoMes") \
                            .agg(F.countDistinct("data_venda").alias("DiasComVenda"))

dias_no_mes = dias_no_mes.withColumn("DiasTotais", lit(30))

dias_no_mes = dias_no_mes.withColumn("DiasSemVenda", col("DiasTotais") - col("DiasComVenda"))

resultado_df = dias_no_mes.join(distribuidores_df, "cod_distribuicao") \
    .select("nm_distribuicao", "AnoMes", "DiasTotais", "DiasComVenda", "DiasSemVenda")

resultado_df.show(10)


try:
    output_path = "./saved_tables/exercicio2_dias_vendas_distribuidores.csv"
    sparkmtrix.write_dataset(
        dataframe=resultado_df,
        mode="overwrite",
        format="csv",
        output_path=output_path
        )
except Exception as e:
    print(f"Ocorreu um erro: {e}")


try:
    sparkmtrix.write_jdbc(
        dataframe=resultado_df,
        url=url,
        table_name="dias_vendas_distribuidores",
        mode="overwrite",
        properties=properties)
except Exception as e:
    print(f"[DEBUG] >> Ocorreu um erro: {e}")

spark.stop()