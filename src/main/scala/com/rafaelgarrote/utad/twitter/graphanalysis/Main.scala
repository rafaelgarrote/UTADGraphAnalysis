package com.rafaelgarrote.utad.twitter.graphanalysis

import com.rafaelgarrote.utad.twitter.graphanalysis.conf.AppProperties
import com.rafaelgarrote.utad.twitter.graphanalysis.spark.SparkContextBuilder
import org.neo4j.spark.Neo4j

object Main {

  def main(args: Array[String]): Unit = {
    import org.graphframes._
    lazy val conf = AppProperties.config
    val sparkSession = SparkContextBuilder.createSessionContext(conf)
    implicit val session = sparkSession
    implicit val neo: Neo4j = Neo4j(session.sparkContext)

    val rts: GraphFrame = neo
      .cypher("MATCH (p:User)-[r:RT]->(m:User) RETURN p.screen_name , r.sentiment, m.screen_name")
      .loadGraphFrame
    rts.cache()

    val pageRankFrame = rts.pageRank.maxIter(5).run()
    val ranked = pageRankFrame.vertices
    ranked.printSchema()
    ranked.orderBy(ranked.col("pagerank").desc).show()
//    val top10 = ranked.orderBy(ranked.col("pagerank").desc).take(10)
  }
}
