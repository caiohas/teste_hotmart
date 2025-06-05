# importacao das bibliotecas utilizadas e config. spark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import Tuple
from datetime import date, timedelta
import glob
import os

spark = SparkSession.builder \
    .appName("TesteHotmartETL") \
    .master("local[*]") \
    .getOrCreate()

def extract_raw_data() -> Tuple[DataFrame, DataFrame, DataFrame]:
    """ Funcao responsavel por gerar os dados a serem a serem utilizados
    no desenvolvimento do script. Não possui parametros e retorna uma Tupla 
    com os DataFrames gerados.
    """

    purchase_sample_data = [
        ('2023-01-20 22:00:00', '2023-01-20', 55, 15947, 5, '2023-01-20', '2023-01-20', 852852),
        ('2023-01-26 00:01:00', '2023-01-26', 56, 369798, 746520, '2023-01-25', None, 963963),
        ('2023-02-05 10:00:00', '2023-02-05', 55, 160001, 5, '2023-01-20', '2023-01-20', 852852),
        ('2023-02-26 07:00:00', '2023-02-26', 69, 160001, 18, '2023-02-26', '2023-02-28', 96967),
        ('2023-07-15 09:00:00', '2023-07-15', 55, 160001, 5, '2023-01-20', '2023-03-01', 852852),
        ('2025-06-03 09:00:00', '2025-06-03', 55, 160001, 5, '2025-06-03', '2025-06-03', 852852), ## teste
        ('2025-06-01 09:00:00', '2025-06-01', 15, 160001, 5, '2025-06-01', '2025-06-01', 852852), ## teste
    ]
    purchase_cols = ["transaction_datetime", "transaction_date", "purchase_id", "buyer_id", "prod_item_id", "order_date", "release_date", "producer_id"]
    df_purchase = spark.createDataFrame(purchase_sample_data, purchase_cols)

    product_item_sample_data  = [
        ('2023-01-20 22:02:00', '2023-01-20', 55, 696969, 10, 50.00),
        ('2023-01-25 23:59:59', '2023-01-25', 56, 808080, 120, 2400.00),
        ('2023-02-26 03:00:00', '2023-02-26', 69, 373737, 2, 2000.00),
        ('2023-07-12 09:00:00', '2023-07-12', 55, 696969, 10, 55.00),
        ('2025-06-03 09:00:00', '2025-06-03', 55, 696969, 10, 50.00), ## teste
        ('2025-06-01 09:00:00', '2025-06-01', 15, 696969, 10, 10.00), ## teste
    ]
    product_item_cols = ["transaction_datetime", "transaction_date", "purchase_id", "product_id", "item_quantity", "purchase_value"]
    df_product_item = spark.createDataFrame(product_item_sample_data, product_item_cols)

    purchase_extra_info_sample_data = [
        ('2023-01-23 00:05:00', '2023-01-23', 55, 'nacional'),
        ('2023-01-25 23:59:59', '2023-01-25', 56, 'internacional'),
        ('2023-02-28 01:10:00', '2023-01-28', 69, 'nacional'),
        ('2023-03-12 07:00:00', '2023-03-12', 69, 'internacional'),
        ('2025-06-03 09:00:00', '2025-06-03', 55, 'internacional'), ## teste
        ('2025-06-01 09:00:00', '2025-06-01', 15, 'nacional'), ## teste
    ]
    purchase_extra_info_cols = ["transaction_datetime", "transaction_date", "purchase_id", "subsidiary"]
    df_purchase_extra_info = spark.createDataFrame(purchase_extra_info_sample_data, purchase_extra_info_cols)

    return df_purchase, df_product_item, df_purchase_extra_info

def transform_gmv_data(df_purchase: DataFrame, df_product_item: DataFrame, df_purchase_extra_info: DataFrame) -> DataFrame:
    """ Funcao responsavel pelas transformacoes aplicadas aos dados do exercicio.
    Sao realizadas operacoes como joins entre os DFs, selecao de colunas, renomear
    colunas, alterar tipo, filtragem etc. Recebe como parametro os DFs que participarao
    da etapa de transformacao e retorna um DataFrame ao final de sua exxecucao.
    """

    # join entre os dfs
    df_gmv = df_purchase.alias("purchase").join(
        df_product_item.alias("product_item"),
        on="purchase_id",
        how="left"
    ).join(
        df_purchase_extra_info.alias("purchase_extra_info"),
        on="purchase_id",
        how="left"
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

def load_gmv_data_history_inicial(df_gmv_history: DataFrame) -> None:
    """ Funcao utilizada para gerar um historico inicial com os
    arquivos parquet a partir da amostra de dados fornecida.
    """

    df_gmv_history = df_gmv_history.withColumn(
        "pk", F.concat_ws("_", F.col("transaction_date"), F.col("subsidiary"))
    )

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

def load_gmv_data_history(df_gmv_history: DataFrame) -> None:
    """ Funcao utilziada para realizar a carga dos dados no formato 
    parquet para posterior leitura dos arquivos e criacao da tabela. 
    Nela validaremos se já existe pasta para determinado dia e se na 
    pasta temos valores NULL ainda nao preenchidos em alguma das tabelas 
    de origem.
    """

    df_gmv_history = df_gmv_history.withColumn(
        "pk", F.concat_ws("_", F.col("transaction_date"), F.col("subsidiary"))
    )

    # leitura dos arquivos parquet
    df_parquet_atual = read_parquet_files()

    # vamos filtrar os arquivos para declararmos um array com os dias que possuem informações incompletas
    df_parquet_atual_null = df_parquet_atual.filter((F.col("subsidiary").isNull()) | (F.col("daily_gmv").isNull()))
    transaction_dates_null = [row['transaction_date'] for row in df_parquet_atual_null.select("transaction_date").distinct().collect()]

    # declarar um array com as datas das transacoes lidas
    transaction_dates = [row['transaction_date'] for row in df_gmv_history.select("transaction_date").distinct().collect()]

    # vamos percorrer o array criando um arquivo parquet para cada "transaction_date"
    for transaction_date in transaction_dates:
        # filtrando o df para pegarmos a data de transacao correspondente aos valores do array
        df_filtered = df_gmv_history.filter(F.col("transaction_date") == transaction_date)
        
        # path onde salvaremos o arquivo e escrita dos dados localmente
        output_path = f"/home/caiohas/Documentos/teste_hotmart/parquet_files/hotmart_gmv_daily_parquet_{transaction_date}"
        dir_exists = os.path.exists(output_path)

        # se o diretorio nao existir ou tivermos valores null nos dados existentes, vamos escrever os dados
        if not dir_exists or transaction_date in transaction_dates_null:
            df_filtered.write.mode("overwrite").parquet(output_path)
            print(f"Arquivo Parquet salvo para a data {transaction_date} em: {output_path}")
        else:
            print(f"Arquivo para a data {transaction_date} já existe e não possui NULLs. Pulando...")

    print("Todos os arquivos foram gerados com sucesso.")

def read_parquet_files() -> DataFrame:
    """ Funcao responsavel pela leitura dos arquivos parquet e criacao 
    da view a ser utilizada pela area de negocio.
    """
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
    
    # carga do histórico
    # load_gmv_data_history_inicial(df_gmv_history)
    load_gmv_data_history(df_gmv_history)

    # leitura dos arquivos parquet
    df_parquet = read_parquet_files()

    df_parquet.createOrReplaceTempView("gmv_daily")

    # consulta SQL
    query = """SELECT * FROM gmv_daily ORDER BY transaction_date DESC"""
    result = spark.sql(query)
    result.show(truncate=False)

if __name__ == "__main__":
    main()

