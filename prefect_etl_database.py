from pyspark.sql import SparkSession
from prefect import flow, task

@task
def read_data_from_postgresql():
    # JDBC connection properties
    jdbc_driver_path = "C:\\Users\\Michael\\Pyspark\\postgresql-42.7.3.jar"
    jdbc_url = "jdbc:postgresql://localhost:5432/project2"
    connection_properties = {
        "user": "postgres",
        "password": "************************",
        "driver": "org.postgresql.Driver"
    }

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL with PySpark and Prefect") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    # Load DataFrames from the database
    products_df = spark.read.jdbc(url=jdbc_url, table="products", properties=connection_properties)
    categories_df = spark.read.jdbc(url=jdbc_url, table="categories", properties=connection_properties)
    orders_df = spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_properties)
    order_details_df = spark.read.jdbc(url=jdbc_url, table="order_details", properties=connection_properties)
    customers_df = spark.read.jdbc(url=jdbc_url, table="customers", properties=connection_properties)

    return products_df, categories_df, orders_df, order_details_df, customers_df, spark

@task
def process_data(products_df, categories_df, orders_df, order_details_df, customers_df, spark):
    # Create temporary views for SQL queries
    products_df.createOrReplaceTempView("products")
    categories_df.createOrReplaceTempView("categories")
    orders_df.createOrReplaceTempView("orders")
    order_details_df.createOrReplaceTempView("order_details")
    customers_df.createOrReplaceTempView("customers")

    # SQL query to perform the required joins and select the desired columns
    result_df = spark.sql("""
        SELECT
            p.productName,
            p.quantityPerUnit,
            p.unitPrice AS product_unitPrice,
            p.discontinued,
            c.categoryName,
            o.shipCountry,
            od.unitPrice AS order_unitPrice,
            od.quantity,
            od.discount,
            cu.contactName,
            cu.contactTitle,
            cu.country AS customer_country
        FROM
            products p
        INNER JOIN
            categories c ON p.categoryID = c.categoryID
        INNER JOIN
            order_details od ON p.productID = od.productID
        INNER JOIN
            orders o ON od.orderID = o.orderID
        INNER JOIN
            customers cu ON o.customerID = cu.customerID
    """)

    return result_df

@task
def save_data_to_postgresql(result_df, spark):
    # JDBC connection properties
    jdbc_url = "jdbc:postgresql://localhost:5432/project2"
    connection_properties = {
        "user": "postgres",
        "password": "permataputihg101",
        "driver": "org.postgresql.Driver"
    }

    # Save the result to a new table in PostgreSQL with table name "sales_data"
    result_df.write.jdbc(url=jdbc_url, table="sales_data", mode="overwrite", properties=connection_properties)

    # Stop the Spark session
    spark.stop()

@flow
def etl_workflow():
    products_df, categories_df, orders_df, order_details_df, customers_df, spark = read_data_from_postgresql()
    result_df = process_data(products_df, categories_df, orders_df, order_details_df, customers_df, spark)
    save_data_to_postgresql(result_df, spark)

# Run the Prefect flow
if __name__ == "__main__":
    etl_workflow()
