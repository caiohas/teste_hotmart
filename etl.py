from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import Tuple

# criacao da SparkSession
spark = SparkSession.builder \
    .appName("TesteHotmartETL") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# funcao para extracao do dado das tabelas utilizadas no pipeline
def extract_raw_data() -> Tuple[DataFrame, DataFrame, DataFrame]:
    
    # simulacao das tabelas
    purchase_sample_data = [
        ('2023-01-20 22:00:00', '2023-01-20', 55, 15947, 5, '2023-01-20', '2023-01-20', 852852),
        ('2023-01-26 00:01:00', '2023-01-26', 56, 369798, 746520, '2023-01-25', None, 963963),
        ('2023-02-05 10:00:00', '2023-02-05', 55, 160001, 5, '2023-01-20', '2023-01-20', 852852),
        ('2023-02-26 07:00:00', '2023-02-26', 69, 160001, 18, '2023-02-26', '2023-02-28', 96967),
        ('2023-07-15 09:00:00', '2023-07-15', 55, 160001, 5, '2023-01-20', '2023-03-01', 852852),
    ]
    purchase_cols = ["transaction_datetime", "transaction_date", "purchase_id", "buyer_id", "prod_item_id", "order_date", "release_date", "producer_id"]
    df_purchase = spark.createDataFrame(purchase_sample_data, purchase_cols)


    product_item_sample_data  = [
        ('2023-01-20 22:02:00', '2023-01-20', 55, 696969, 10, 50.00),
        ('2023-01-25 29:59:59', '2023-01-25', 56, 808080, 120, 2400.00),
        ('2023-02-26 03:00:00', '2023-02-26', 69, 373737, 2, 2000.00),
        ('2023-07-12 09:00:00', '2023-07-12', 55, 696969, 10, 55.00),
    ]
    product_item_cols = ["transaction_datetime", "transaction_date", "purchase_id", "product_id", "item_quantity", "purchase_value"]
    df_product_item = spark.createDataFrame(product_item_sample_data, product_item_cols)

    purchase_extra_info_sample_data = [
        ('2023-01-23 00:05:00', '2023-01-23', 55, 'nacional'),
        ('2023-01-25 23:59:59', '2023-01-25', 56, 'internacional'),
        ('2023-02-28 01:10:00', '2023-01-28', 69, 'nacional'),
        ('2023-03-12 07:00:00', '2023-03-12', 69, 'internacional'),
    ]
    purchase_extra_info_cols = ["transaction_datetime", "transaction_date", "purchase_id", "subsidiary"]
    df_purchase_extra_info = spark.createDataFrame(purchase_extra_info_sample_data, purchase_extra_info_cols)

    return df_purchase, df_product_item, df_purchase_extra_info

# funcao responsavel por realizar as transformacoes necessarias no df
def transform_gmv_data(df_purchase: DataFrame, df_product_item: DataFrame, df_purchase_extra_info: DataFrame) -> DataFrame:

    # realizar o join entre as tabelas
    df_gmv = df_purchase.alias("purchase").join(
        df_product_item.alias("product_item"),
        on="purchase_id",
        how="inner"
    ).join(
        df_purchase_extra_info.alias("purchase_extra_info"),
        on="purchase_id",
        how="inner"
    )

    # selecionando apenas as colunas relevantes para o calculo do gmv
    df_gmv = df_gmv.select(
        F.col("purchase.purchase_id"),
        F.col("purchase.transaction_date"),
        F.col("purchase.release_date"),
        F.col("purchase_extra_info.subsidiary"),
        F.col("product_item.purchase_value")
    )

    # filtrar apenas o que tem release_date preenchido
    df_gmv = df_gmv.filter(F.col("release_date").isNotNull())

    # calculo do gmv diario por subsidiaria
    df_gmv = df_gmv.groupBy(
        "transaction_date", "subsidiary"
    ).agg(
        F.sum("purchase_value").alias("daily_gmv")
    ).orderBy("transaction_date", "subsidiary")

    return df_gmv


def main() -> None:
    
    # leitura dos dados
    df_purchase, df_product_item, df_purchase_extra_info = extract_raw_data()
    
    # transformacao dos dados
    df_gmv_final = transform_gmv_data(df_purchase, df_product_item, df_purchase_extra_info)

    df_gmv_final.show()
    
if __name__ == "__main__":
    main()