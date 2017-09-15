package com.rafaelgarrote.utad.twitter.graphanalysis

import com.rafaelgarrote.utad.twitter.graphanalysis.conf.AppProperties
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.SparkContextBuilder
import org.neo4j.spark.Neo4j
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.GraphAnalysisDsl._
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.GenericWriter._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.lit


object Main {

  def main(args: Array[String]): Unit = {
    lazy val conf = AppProperties.config
    val sparkSession = SparkContextBuilder.createSessionContext(conf)
    implicit val session = sparkSession
    implicit val neo: Neo4j = Neo4j(session.sparkContext)

    sparkSession.sparkContext.setLogLevel("ERROR")

    lazy val datasource: String = AppProperties.getDataSource
    lazy val optionsRank: Map[String, String] = AppProperties.getElasticserachPorpertiesAsMap("rank")
    lazy val optionsTriangle: Map[String, String] = AppProperties.getElasticserachPorpertiesAsMap("triangle")

    val sentimentGraph = neo.getSentimentRTGraph.cache()
    val positiveSentimentGraph = sentimentGraph.getPositiveRTSubGraph.cache()
    val negativeSentimentGraph = sentimentGraph.getNegativeRTSubGraph.cache()
    val positiveRank = positiveSentimentGraph.runPageRank.getRank.toDF()
      .withColumn("sentiment", lit("Positive")).cache()
    val negativeRank = negativeSentimentGraph.runPageRank.getRank.toDF()
      .withColumn("sentiment", lit("Negative")).cache()

    positiveRank.union(negativeRank).writeDF(datasource, optionsRank)

    val positiveRankWithComunities = positiveSentimentGraph.runTriangleCount.join(positiveRank, "id")
    val negativeRankWithCommunities = negativeSentimentGraph.runTriangleCount.join(negativeRank, "id")

    positiveRankWithComunities.union(negativeRankWithCommunities).writeDF(datasource, optionsTriangle)

  }
}
