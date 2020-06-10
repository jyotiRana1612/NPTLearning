# Databricks notebook source
			from pyspark.sql.functions import avg
			diamonds_df = spark.read.csv('/FileStore/tables/Films.csv', header="true", inferSchema="true")
display(diamonds_df.select("Title","OscarWins"))

# COMMAND ----------

			sparkDF = spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").option("inferSchema", "true").load("/FileStore/tables/Directors.xlsx")
display(sparkDF.select("FullName","Gender"))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val jdbcHostname = "neupocjrsqlserver.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "WideWorldImportersDW"
# MAGIC 
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", "jrvm1612")
# MAGIC connectionProperties.put("password", "Startlearning@123")
# MAGIC 
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)
# MAGIC 
# MAGIC val date_DF = spark.read.jdbc(jdbcUrl, "Dimension.Date", connectionProperties)
# MAGIC val sale_DF = spark.read.jdbc(jdbcUrl,"Fact.Sale",connectionProperties)
# MAGIC val city_DF = spark.read.jdbc(jdbcUrl,"Dimension.City",connectionProperties)
# MAGIC 
# MAGIC date_DF.createOrReplaceTempView("dates_table")
# MAGIC sale_DF.createOrReplaceTempView("sales_table")
# MAGIC city_DF.createOrReplaceTempView("city_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select `Fiscal Year`, SUM(`Profit`) from dates_table inner join sales_table  on dates_table.Date = sales_table.`Invoice Date Key` group by `Fiscal Year`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE WIDGET DROPDOWN City DEFAULT "Abbottsburg" CHOICES SELECT DISTINCT city FROM city_table order by city

# COMMAND ----------

# MAGIC %sql
# MAGIC select `Fiscal Year`, `Fiscal Month Label`, City, SUM(`Profit`) from dates_table inner join sales_table  on dates_table.Date = sales_table.`Invoice Date Key` inner join  city_table on city_table.`City Key` = sales_table.`City Key` WHERE city = getArgument("City") group by `Fiscal Year`,`Fiscal Month Label` , City order by `Fiscal Year`
