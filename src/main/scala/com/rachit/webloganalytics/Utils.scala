package com.rachit.webloganalytics

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types._

object Utils {

  def saveToDisk(weblog_sessionIds: DataFrame,outputPath : String) : Unit = {
    weblog_sessionIds.coalesce(1)
      .write
      .option("header","true")
      .option("delimiter"," ")
      .mode(SaveMode.Overwrite)
      .csv(outputPath)
  }


  def getSchema = {
    new StructType()
      .add("timestamp", TimestampType)
      .add("elb", StringType)
      .add("clientPort", StringType)
      .add("backendPort", StringType)
      .add("request_processing_time", DoubleType)
      .add("backend_processing_time", DoubleType)
      .add("response_processing_time", DoubleType)
      .add("elb_status_code", IntegerType)
      .add("backend_status_code", IntegerType)
      .add("received_bytes", IntegerType)
      .add("sent_bytes", IntegerType)
      .add("request", StringType)
      .add("user_agent", StringType)
      .add("ssl_cipher", StringType)
      .add("ssl_protocol", StringType)
  }

}