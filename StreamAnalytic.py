# Databricks notebook source
# MAGIC %scala
# MAGIC import org.apache.spark.eventhubs._
# MAGIC       
# MAGIC val eventHubName = "neupocjreventhubforadb"
# MAGIC val eventHubOutputName = "outputeventhub"
# MAGIC val eventHubNSConnStr = "Endpoint=sb://neupocjreventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=muPhlQMBASo3ylSRje3eum477iRb/OQTc7jdMQ+noiQ="
# MAGIC val connStr = ConnectionStringBuilder(eventHubNSConnStr).setEventHubName(eventHubName).build
# MAGIC val connStrOut = ConnectionStringBuilder(eventHubNSConnStr).setEventHubName(eventHubOutputName).build
# MAGIC val customEventhubParameters = EventHubsConf(connStr).setMaxEventsPerTrigger(25)
# MAGIC val customEventhubParametersOut = EventHubsConf(connStrOut).setMaxEventsPerTrigger(25)
# MAGIC val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
# MAGIC //incomingStream.printSchema
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC // Event Hub message format is JSON and contains "body" field
# MAGIC // Body is binary, so you cast it to string to see the actual content of the message
# MAGIC val messages = incomingStream.withColumn("Offset", $"offset".cast(LongType)).withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType)).withColumn("Timestamp", $"enqueuedTime".cast(LongType)).withColumn("Body", $"body".cast(StringType)).select("Offset", "Time (readable)", "Timestamp", "Body")
# MAGIC //messages.printSchema
# MAGIC //messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
# MAGIC val query =  messages.select("body").writeStream.format("eventhubs").outputMode("append").options(customEventhubParametersOut.toMap).option("checkpointLocation", "/mnt/events/").start()

# COMMAND ----------

messages.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/mnt/delta/events/_checkpoints/etl-from-json")
  .start("/mnt/delta/events") // as a path
val events = spark.read.format("delta").load("/mnt/delta/events")
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
// Event Hub message format is JSON and contains "body" field
// Body is binary, so you cast it to string to see the actual content of the message
val messages = incomingStream.withColumn("Offset", $"offset".cast(LongType)).withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType)).withColumn("Timestamp", $"enqueuedTime".cast(LongType)).withColumn("Body", $"body".cast(StringType)).select("Offset", "Time (readable)", "Timestamp", "Body")
messages.printSchema
messages.writeStream.outputMode("append").format("json")        // can be "orc", "json", "csv", etc.
    .start()

