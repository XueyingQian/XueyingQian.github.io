##Create Spark Session and import necessary Packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]") \
        .appName('TESTING') \
        .getOrCreate()

##Get user input
##"/home/jovyan/products.txt"
##"/home/jovyan/transactions.txt"
p_id = input("Please enter the product id that you would like to find top 5 co-purchased item: ")
product_path = input("please input the path for products.txt: ")
txn_path = input("please input the path for transactions.txt: ")


##Read Product text file

df_product_raw = spark.read.format("text") \
.option("header",True) \
.load(product_path)

df_product = df_product_raw \
            .withColumn("product_id", split(df_product_raw['value'], "\t").getItem(0)) \
            .withColumn("MCH_code", split(df_product_raw['value'], "\t").getItem(1)) \
            .withColumn("product_name", split(df_product_raw['value'], "\t").getItem(2)) \
            .drop('value')

##Read transaction text file

df_txn = spark.read.format("json") \
.option("header",True) \
.load(txn_path)

##Get the list of product_id from ItemList for each transaction

df_id_ls = df_txn.select(col("itemList").item.alias("id_ls"))

def five_rec_product(product_id):
    
 #Step1: filter the product_id list that contains the target product_id
    df_id_ls_with_target = df_id_ls.select("id_ls",array_contains("id_ls",product_id).alias("check")).filter("check is true")
    
 #Step2: Explode the product_id list in rows for Step3
    df_id_explode = df_id_ls_with_target.select(explode(col("id_ls")).alias("id"))
    
 #Step3: Count the number of times that each product_id occurs,remove the target id, and keep the top 5 counts
    df_id_top5 = df_id_explode.groupBy("id").agg(count("id").alias("ct_id")) \
                              .filter(col("id") != product_id) \
                              .sort(col("ct_id").desc()).limit(5)
    
 #Step4: Join the top 5 recommended product_id with product table to get the name
    return df_id_top5.join(df_product,df_id_top5.id == df_product.product_id) \
                     .sort(col("ct_id").desc()) \
                     .select(df_id_top5.id,df_product.product_name)
    
five_rec_product(p_id).show(10,False)