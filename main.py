from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import split

# Преобразование строки в массив


def get_product_category_pairs(products_df, categories_df):
    # Разбиваем строку с категориями на массив
    categories_df = categories_df.withColumn("categories_array", split(col("categories"), ", "))

    # Развернем столбец с категориями в отдельные строки
    exploded_df = categories_df.withColumn("category", explode(col("categories_array")))

    # Присоединим датафреймы по общему столбцу (например, 'product_id')
    joined_df = products_df.join(exploded_df, products_df.product_id == exploded_df.product_id, "left")

    # Выберем нужные столбцы и переименуем их
    result_df = joined_df.select(products_df.product_name, exploded_df.category.alias("category_name"))

    return result_df


# Инициализация SparkSession
spark = SparkSession.builder.appName("product-category").getOrCreate()

# Загрузка данных в датафреймы (предположим, что у вас есть CSV файлы products.csv и categories.csv)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
categories_df = spark.read.csv("categories.csv", header=True, inferSchema=True)

# Вызов метода
result_df = get_product_category_pairs(products_df, categories_df)

# Показать результат
result_df.show()