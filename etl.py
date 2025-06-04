from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# criacao da SparkSession
spark = SparkSession.builder.appName("TesteHotmartETL").getOrCreate()

# simulacao das tabelas
purchase_data = [
    ('2023-01-20 22:00:00', '2023-01-20', 55, 15947, 5, '2023-01-20', '2023-01-20', 852852),
    ('2023-01-23 00:00:00', '2023-01-25', 55, 15947, 9, None, None, 852852),
    ('2023-01-25 10:00:00', '2023-01-25', 56, 160001, 5, '2023-01-25', '2023-01-25', 852852),
    ('2023-03-12 07:00:00', '2023-03-12', 69, 160001, 5, '2023-03-12', '2023-03-12', 852852),
    ('2023-07-15 09:00:00', '2023-07-15', 55, 160001, 5, '2023-07-15', '2023-07-15', 852852),
]
purchase_cols = ["transaction_datetime", "transaction_date", "purchase_id", "buyer_id", "prod_item_id", "order_date", "release_date", "producer_id"]
df_purchase = spark.createDataFrame(purchase_data, purchase_cols)

df_purchase.show()