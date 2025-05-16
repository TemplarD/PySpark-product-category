# Product-Category Pairs in PySpark

Решение для получения пар "Продукт-Категория" и продуктов без категорий.

## Как использовать
```python
from src.main import get_product_category_pairs

result = get_product_category_pairs(products_df, categories_df, product_category_links_df)
