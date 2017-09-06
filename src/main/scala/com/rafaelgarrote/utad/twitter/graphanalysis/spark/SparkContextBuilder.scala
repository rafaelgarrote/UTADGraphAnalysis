package com.rafaelgarrote.utad.twitter.graphanalysis.spark

import com.rafaelgarrote.utad.twitter.graphanalysis.conf.AppProperties
import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkContextBuilder {

  def createSessionContext(config: Config): SparkSession = {
    val sparkConf = getSparkConf
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  private def getSparkConf: SparkConf = {
    val conf = new SparkConf().setAppName("Tweet Analysis").setMaster("local[2]")
    AppProperties.getConfAsMap("neo4j").map(entry => conf.set(entry._1, entry._2))
    conf
  }
}
