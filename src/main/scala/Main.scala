package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

def sensorStat(spark: SparkSession): DataFrame = {
 import spark.implicts._ 
 
 // Define the schema for Leaderone DataFrame
 val leaderoneSchema = StructType(Seq(
 StructField("SensorId", StringType, nullable = true)
 StructField("Humidity", IntegerType, nullable = true),
 
 ))
 
 // Define the data for Leaderone
 val leaderoneData = Seq(
 Row("s1", 10),
 Row("s2", 88),
 Row("s1", NaN)
 )
 
 // Define the schema for Leadertwo DataFrame
 val leadertwoSchema = StructType(Seq(
 StructField("SensorId", StringType, nullable = true)
 StructField("Humidity", IntegerType, nullable = true),
 
 ))
 
 // Define the data for Leadertwo
 val leadertwoData = Seq(
 Row(s2, 80),
 Row(s3, NaN),
 Row(s2, 78),
 Row(s1, 98)
 )
 var i = 0 
 val leaderoneDF = spark.createDataFrame(spark.sparkContext.parallelize(leaderoneData), leaderoneSchema)
  i = i + 1

 val leadertwoDF = spark.createDataFrame(spark.sparkContext.parallelize(leadertwoData), leadertwoSchema)
  i = i + 1 
 println("Num of processed files:" + i)
 
 val resultDF = leaderoneDF.union(leadertwoDF)
 var j = 0
 j = resultDF.count()
 println("Num of processed measurements:" + j)
 println('Num of failed measurements:)
 println(resultDF.where(resultDF.Humidity == 'NaN').count()) 

 windowPartitionAgg = Window.partitionBy("SensorId").orderBy("Humidity")
 
 resultDF.withColumn("Avg",avg(col("Humidity")).over(windowPartitionAgg)).show()
 resultDF.withColumn("Min",min(col("Humidity")).over(windowPartitionAgg)).show()
 resultDF.withColumn("Max",max(col("Humidity")).over(windowPartitionAgg)).show()
}
