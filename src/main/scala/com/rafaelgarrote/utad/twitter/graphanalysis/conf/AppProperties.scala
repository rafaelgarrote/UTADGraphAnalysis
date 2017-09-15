package com.rafaelgarrote.utad.twitter.graphanalysis.conf

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.util.Try

object AppProperties {

  lazy val config: Config = ConfigFactory.load()
  private val elasticsearchDatasourceKey = "elasticsearch.datasource"

  def getDataSource: String = Try(config.getString(elasticsearchDatasourceKey)).getOrElse("")

  def getElasticserachPorpertiesAsMap(resource: String): Map[String, String] =
    Map(
      "org.elasticsearch.spark.sql.nodes" -> config.getString("elasticsearch.org.elasticsearch.spark.sql.nodes"),
      "org.elasticsearch.spark.sql.port" -> config.getString("elasticsearch.org.elasticsearch.spark.sql.port"),
      "resource" -> config.getString(s"elasticsearch.org.elasticsearch.spark.sql.$resource.resource")
    )

  def getConfAsMap(path: String): Map[String, String] = {
    import scala.collection.JavaConverters._
    val pathConfig = config.getConfig(path)
    pathConfig.entrySet().asScala
      .map(entry => entry.getKey -> pathConfig.getString(entry.getKey)).toMap
  }
}
