from pyspark.sql import SparkSession
from src.main import get_product_category_pairs

def test_product_category_pairs():
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    
    products = spark.createDataFrame([(1, "Product A")], ["product_id", "product_name"])
    categories = spark.createDataFrame([(1, "Category X")], ["category_id", "category_name"])
    links = spark.createDataFrame([(1, 1)], ["product_id", "category_id"])
    
    result = get_product_category_pairs(products, categories, links)
    assert result.count() == 1