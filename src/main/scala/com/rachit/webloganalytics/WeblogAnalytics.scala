package com.rachit.webloganalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.storage.StorageLevel

object WeblogAnalytics {

  /*parameters
  weblogPath : input Path for the weblog
  outputPath : output path to write the results
 */

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("webLogChallenge")
      .getOrCreate()

    val weblogPath = arg(0)
    val outputPath = arg(1)

    val csvSchema = Utils.getSchema

    /*Read the weblog to dataframe from weblogPath*/

    val weblog_df = spark.read.format("csv")
      .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
      .option("delimiter"," ")
      .option("header",false)
      .schema(csvSchema)
      .load(weblogPath)

    weblog_df.createOrReplaceTempView("weblog_df")

    /*select the required columns and transform them*/

    val reqCols = spark.sql("SELECT " +
      "split(clientPort,':')[0] as user_ip" +
      ",timestamp" +
      ",split(request,' ')[1] as request " +
      "from weblog_df")

    reqCols.createOrReplaceTempView("reqCols")

    /*SESSIONIZE WITH DEFAULT 15 MINUTE INACTIVITY WINDOW*/

    /*
    1. add a new column to reqCols "is_new_session" - values - 0 = same session, 1 = new session
    2. value for is_new_session is determined based on difference between current and previous entry for the user,
       if difference < 15 minutes then 0 else 1.
    3. Find the running total for "is_new_session" for each user and assign the value as session ID
     */

    val weblog_sessions = spark.sql("SELECT *," +
      " CASE WHEN UNIX_TIMESTAMP(timestamp) - LAG (UNIX_TIMESTAMP(timestamp)) OVER (PARTITION BY user_ip ORDER BY timestamp) >= 15 * 60  THEN 1 ELSE 0 END AS is_new_session" +
      " FROM reqCols")

    weblog_sessions.createOrReplaceTempView("weblog_sessions")

    val weblog_sessionIds = spark.sql("select *," +
      " SUM(is_new_session) OVER (PARTITION BY user_ip ORDER BY timestamp) AS session_id" +
      " FROM weblog_sessions")

    weblog_sessionIds.createOrReplaceTempView("weblog_sessionIds")

    /*persist weblog_sessionIds , will be used for further analysis*/

    weblog_sessionIds.persist(StorageLevel.MEMORY_AND_DISK)

    Utils.saveToDisk(weblog_sessionIds,outputPath+"sessionIds")

    /* AVERAGE SESSION TIME - IN SECONDS */

    /*
    1.group the weblog_sessionIds on user_ip, session_id and find the difference of max timestamp and min timestamp for getting per session time
    2.take the average for per session time to get the average session time
    */

    val grouped_sessions = spark.sql("SELECT user_ip," +
      " session_id," +
      " MAX(unix_timestamp(timestamp)) - MIN(unix_timestamp(timestamp)) AS per_session_time" +
      " FROM weblog_sessionIds" +
      " GROUP BY user_ip, session_id")

    Utils.saveToDisk(grouped_sessions.agg(avg("per_session_time")),outputPath+"avgSessionTime")

    /*MOST ENGAGED USERS*/

    /*find the average session time of each user from grouped sessions and take the top 50 rows to find the most engaged users*/

    grouped_sessions.createOrReplaceTempView("grouped_sessions")

    val average_sessions = spark.sql("SELECT user_ip," +
      " AVG(per_session_time) AS avg_session_time_per_user" +
      " FROM grouped_sessions" +
      " GROUP BY user_ip" +
      " ORDER BY avg_session_time_per_user" +
      " DESC LIMIT 50")

    Utils.saveToDisk(average_sessions,outputPath+"mostEngagedUsers")

    /*UNIQUE URL VISITS PER SESSION*/

    val weblog_uniqueCount_per_user_session = spark.sql("SELECT user_ip," +
      "session_id," +
      "COUNT(DISTINCT(request)) as count_per_user_session" +
      " FROM weblog_sessionIds" +
      " GROUP BY user_ip,session_id")

    Utils.saveToDisk(weblog_uniqueCount_per_user_session.agg(avg("count_per_user_session")),outputPath+"uniqueCounts")

    spark.stop()

  }


}
