from spark_singleton import SparkSingleton
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from product import Product
from transaction import Transaction
from order import Order
from customer import Customer
import random as rand

class OrderGenerator:
    rogue_options = ["ROGUEROGUE", "XXXXXXXX", "", "----////----"]

    def get_row():
        spark_session = SparkSingleton.getInstance()
        schema = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("payment_type", StringType(), True),
            StructField("qty", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("datetime", TimestampType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("ecommerce_website_name", StringType(), True),
            StructField("payment_txn_id", IntegerType(), True),
            StructField("payment_txn_success", StringType(), True),
            StructField("failure_reason", StringType(), True)
        ])
        product = Product()
        transaction = Transaction()
        order = Order()
        customer = Customer()
        df = spark_session.createDataFrame([(order.orderid, 
                                             customer.id, 
                                             customer.name, 
                                             product.id, 
                                             product.name, 
                                             product.category,
                                             transaction.paymentType, 
                                             order.qty, 
                                             product.price, 
                                             order.timestamp,
                                             customer.country,
                                             customer.city,
                                             order.ecommercename,
                                             transaction.paymentTxnId,
                                             transaction.paymentTxnSuccess,
                                             transaction.failureReason)], 
                                             schema=schema)
        
        return df
    
    def get_rogue_row():
        spark_session = SparkSingleton.getInstance()
        schema = StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("payment_type", StringType(), True),
            StructField("qty", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("datetime", TimestampType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("ecommerce_website_name", StringType(), True),
            StructField("payment_txn_id", IntegerType(), True),
            StructField("payment_txn_success", StringType(), True),
            StructField("failure_reason", StringType(), True)
        ])
        product = Product()
        transaction = Transaction()
        order = Order()
        customer = Customer()
        rogue_row = (order.orderid, 
                     customer.id, 
                     customer.name, 
                     product.id, 
                     product.name, 
                     product.category,
                     transaction.paymentType, 
                     order.qty, 
                     product.price, 
                     order.timestamp,
                     customer.country,
                     customer.city,
                     order.ecommercename,
                     transaction.paymentTxnId,
                     transaction.paymentTxnSuccess,
                     transaction.failureReason)
        rogue_row[rand.randint(0,15)] = OrderGenerator.rogue_options[rand.randint(0,3)]
        rogue_row[rand.randint(0,15)] = OrderGenerator.rogue_options[rand.randint(0,3)]
        rogue_row[rand.randint(0,15)] = OrderGenerator.rogue_options[rand.randint(0,3)]
        rogue_row[rand.randint(0,15)] = OrderGenerator.rogue_options[rand.randint(0,3)]
        df = spark_session.createDataFrame([rogue_row], 
                                             schema=schema)
        
        return df

def main():
    row = OrderGenerator.get_row()




if __name__ == '__main__':
    main()
