package com.rafaelgarrote.utad.twitter.graphanalysis

import com.rafaelgarrote.utad.twitter.graphanalysis.conf.AppProperties
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.SparkContextBuilder
import org.neo4j.spark.Neo4j
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.GraphAnalysisDsl._
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.GenericWriter._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank
import org.graphframes.GraphFrame

object Main {

  def main(args: Array[String]): Unit = {
    lazy val conf = AppProperties.config
    val sparkSession = SparkContextBuilder.createSessionContext(conf)
    implicit val session = sparkSession
    implicit val neo: Neo4j = Neo4j(session.sparkContext)

    sparkSession.sparkContext.setLogLevel("ERROR")

    lazy val datasource: String = AppProperties.getDataSource
    lazy val options: Map[String, String] = AppProperties.getElasticserachPorpertiesAsMap

    val sentimentGraph = neo.getSentimentRTGraph.cache()
    val positiveSentimentGraph = sentimentGraph.getPositiveRTSubGraph.cache()
    val negativeSentimentGraph = sentimentGraph.getNegativeRTSubGraph.cache()
//    positiveSentimentGraph.runPageRank.getRank.toDF().show()
//    negativeSentimentGraph.runPageRank.getRank.toDF().show()

    positiveSentimentGraph.runTriangleCount
    negativeSentimentGraph.runTriangleCount

//    neo.getPositiveRTGraph.runTriangleCount
//    neo.getPositiveRTGraph.runPageRank.getRank.toDF().writeDF(datasource, options)
//    neo.getNegativeRTGraph.runPageRank.getRank.toDF().writeDF(datasource, options)


  }
}
