package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

def techMahindra(spark: SparkSession): DataFrame = {
 import spark.implicts._ 
 
 // Define the schema for Customers DataFrame
 val customersSchema = StructType(Seq(
 StructField("Id", IntegerType, nullable = true),
 StructField("NameCust", StringType, nullable = true)
 ))
 
 // Define the data for Customers
 val customersData = Seq(
 Row(1, "Joe"),
 Row(2, "Henry"),
 Row(3, "Sam"),
 Row(4, "Max")
 )
 
 // Define the schema for Orders DataFrame
 val ordersSchema = StructType(Seq(
 StructField("Id", IntegerType, nullable = true),
 StructField("CustomerId", IntegerType, nullable = true)
 ))
 
 // Define the data for Orders
 val ordersData = Seq(
 Row(1, 3),
 Row(2, 1)
 )
 
 val customersDF = spark.createDataFrame(spark.sparkContext.parallelize(customersData), customersSchema)

 val ordersDF = spark.createDataFrame(spark.sparkContext.parallelize(ordersData), ordersSchema)
 
 val resultDF = customersDF.join(ordersDF, customersDF("Id") === ordersDF("CustomerId"), "leftanti")

 resultDF 
}