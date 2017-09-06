package com.rafaelgarrote.utad.twitter.graphanalysis.conf

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object AppProperties {

  lazy val config: Config = ConfigFactory.load()

  def getConfAsMap(path: String): Map[String, String] = {
    import scala.collection.JavaConverters._
    val pathConfig = config.getConfig(path)
    pathConfig.entrySet().asScala
      .map(entry => entry.getKey -> pathConfig.getString(entry.getKey)).toMap
  }
}
