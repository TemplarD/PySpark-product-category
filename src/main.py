from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def get_product_category_pairs(products_df: DataFrame, 
                            categories_df: DataFrame, 
                            product_category_links_df: DataFrame) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами "Имя продукта – Имя категории" 
    и продуктами без категорий.
    
    Параметры:
    - products_df: Датафрейм продуктов с колонками ['product_id', 'product_name']
    - categories_df: Датафрейм категорий с колонками ['category_id', 'category_name']
    - product_category_links_df: Датафрейм связей с колонками ['product_id', 'category_id']
    
    Возвращает:
    - Датафрейм с колонками ['product_name', 'category_name']
    """
    # Соединяем продукты с их категориями через таблицу связей
    product_category_pairs = (
        products_df.join(
            product_category_links_df, 
            on='product_id', 
            how='left'
        )
        .join(
            categories_df, 
            on='category_id', 
            how='left'
        )
        .select(
            col('product_name'), 
            col('category_name')
        )
    )
    
    return product_category_pairs