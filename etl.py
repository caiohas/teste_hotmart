# importacao das bibliotecas utilizadas e config. spark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import Tuple
from datetime import date, timedelta
import glob

spark = SparkSession.builder \
    .appName("TesteHotmartETL") \
    .master("local[*]") \
    .getOrCreate()

# funcao responsavel pela extracao dos dados utilizados
def extract_raw_data() -> Tuple[DataFrame, DataFrame, DataFrame]:

    # criacao do df_purchase 
    purchase_sample_data = [
        ('2023-01-20 22:00:00', '2023-01-20', 55, 15947, 5, '2023-01-20', '2023-01-20', 852852),
        ('2023-01-26 00:01:00', '2023-01-26', 56, 369798, 746520, '2023-01-25', None, 963963),
        ('2023-02-05 10:00:00', '2023-02-05', 55, 160001, 5, '2023-01-20', '2023-01-20', 852852),
        ('2023-02-26 07:00:00', '2023-02-26', 69, 160001, 18, '2023-02-26', '2023-02-28', 96967),
        ('2023-07-15 09:00:00', '2023-07-15', 55, 160001, 5, '2023-01-20', '2023-03-01', 852852),
    ]
    purchase_cols = ["transaction_datetime", "transaction_date", "purchase_id", "buyer_id", "prod_item_id", "order_date", "release_date", "producer_id"]
    df_purchase = spark.createDataFrame(purchase_sample_data, purchase_cols)

    # criacao do df_product_item 
    product_item_sample_data  = [
        ('2023-01-20 22:02:00', '2023-01-20', 55, 696969, 10, 50.00),
        ('2023-01-25 23:59:59', '2023-01-25', 56, 808080, 120, 2400.00),
        ('2023-02-26 03:00:00', '2023-02-26', 69, 373737, 2, 2000.00),
        ('2023-07-12 09:00:00', '2023-07-12', 55, 696969, 10, 55.00),
    ]
    product_item_cols = ["transaction_datetime", "transaction_date", "purchase_id", "product_id", "item_quantity", "purchase_value"]
    df_product_item = spark.createDataFrame(product_item_sample_data, product_item_cols)

    # criacao do df_purchase_extra_info 
    purchase_extra_info_sample_data = [
        ('2023-01-23 00:05:00', '2023-01-23', 55, 'nacional'),
        ('2023-01-25 23:59:59', '2023-01-25', 56, 'internacional'),
        ('2023-02-28 01:10:00', '2023-01-28', 69, 'nacional'),
        ('2023-03-12 07:00:00', '2023-03-12', 69, 'internacional'),
    ]
    purchase_extra_info_cols = ["transaction_datetime", "transaction_date", "purchase_id", "subsidiary"]
    df_purchase_extra_info = spark.createDataFrame(purchase_extra_info_sample_data, purchase_extra_info_cols)

    return df_purchase, df_product_item, df_purchase_extra_info

# funcao responsavel pela etapa de transformacoes do ETL
def transform_gmv_data(df_purchase: DataFrame, df_product_item: DataFrame, df_purchase_extra_info: DataFrame) -> DataFrame:
    
    # join entre os dfs
    df_gmv = df_purchase.alias("purchase").join(
        df_product_item.alias("product_item"),
        on="purchase_id",
        how="inner"
    ).join(
        df_purchase_extra_info.alias("purchase_extra_info"),
        on="purchase_id",
        how="inner"
    )

    # selecionando apenas colunas de interesse no calculo do GMV
    df_gmv = df_gmv.select(
        F.col("purchase.purchase_id"),
        F.col("purchase.transaction_date"),
        F.col("purchase.release_date"),
        F.col("purchase_extra_info.subsidiary"),
        F.col("product_item.purchase_value")
    )

    # filtragem do df para pegarmos apenas transacoes com "release_date" preenchidos
    df_gmv = df_gmv.filter(F.col("release_date").isNotNull())

    # calculo do GMV diario por subsidiaria ordenando por data da transacao
    df_gmv = df_gmv.groupBy(
        "transaction_date", "subsidiary"
    ).agg(
        F.sum("purchase_value").alias("daily_gmv")
    ).orderBy("transaction_date", "subsidiary")

    return df_gmv

# funcao responsavel pela carga do historico inicial
def load_gmv_data_history(df_gmv_history: DataFrame) -> None:
    
    # obter um array com as datas das transacoes
    transaction_dates = [row['transaction_date'] for row in df_gmv_history.select("transaction_date").distinct().collect()]

    # vamos percorrer o array criando um arquivo parquet para cada "transaction_date"
    for transaction_date in transaction_dates:
        # filtrando o df para pegarmos a data de transacao correspondente aos valores do array
        df_filtered = df_gmv_history.filter(F.col("transaction_date") == transaction_date)
        
        # path onde salvaremos o arquivo e escrita dos dados localmente
        output_path = f"/home/caiohas/Documentos/teste_hotmart/parquet_files/hotmart_gmv_daily_parquet_{transaction_date}"
        df_filtered.write.mode("overwrite").parquet(output_path)
        print(f"Arquivo Parquet salvo para a data {transaction_date} em: {output_path}")

    print("Todos os arquivos foram gerados com sucesso.")

# funcao responsavel pela leitura dos arquivos parquet e criacao da view a ser utilizada pela area de negocio
def read_parquet_files() -> DataFrame:
    # leitura dos arquivos e criacao de view com o historico
    paths = glob.glob("/home/caiohas/Documentos/teste_hotmart/parquet_files/hotmart_gmv_daily_parquet_*")
    df = spark.read.parquet(*paths)

    return df

# funcao main com as etapas do ETL
def main():
    
    # extração dos dados
    df_purchase, df_product_item, df_purchase_extra_info = extract_raw_data()
    
    # transformação dos dados
    df_gmv_history = transform_gmv_data(df_purchase, df_product_item, df_purchase_extra_info)

    df_gmv_history.show()

    # carga do histórico
    load_gmv_data_history(df_gmv_history)

    # leitura dos arquivos parquet
    df_parquet = read_parquet_files()

    df_parquet.show()
    
    df_parquet.createOrReplaceTempView("gmv_daily")

    # Consulta SQL
    query = """SELECT * FROM gmv_daily ORDER BY transaction_date DESC"""
    result = spark.sql(query)
    result.show(truncate=False)

if __name__ == "__main__":
    main()

