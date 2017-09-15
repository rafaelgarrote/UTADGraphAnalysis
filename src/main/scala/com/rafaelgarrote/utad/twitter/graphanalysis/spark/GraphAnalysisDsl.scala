package com.rafaelgarrote.utad.twitter.graphanalysis.spark

import org.apache.spark.graphx.VertexRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.neo4j.spark.Neo4j

class Neo4jFunctions(self: Neo4j)
                    (implicit session: SparkSession) {

  def getSentimentRTGraph: GraphFrame = {
    val nodesCount = self
      .cypher(s"MATCH (p:User)-[r:RT]->(m:User) RETURN p.screen_name , r.sentiment, m.screen_name")
      .loadRowRdd.count()
    self.pattern(("User","screen_name"), ("RT","sentiment"), ("User","screen_name"))
      .partitions(3).rows(nodesCount)
      .loadGraphFrame
  }

}

class NetworkMeasuresAnalysis(self: GraphFrame)
                             (implicit session: SparkSession) {

  import com.rafaelgarrote.utad.twitter.graphanalysis.spark.GenericWriter._

  def getRTSubGraph(sentiments: List[String]): GraphFrame = {
    val filterQuery = sentiments.map(sentiment => s"e.value = '$sentiment'").reduce(_ + " OR " + _)
    val paths = self.find("(a)-[e]->(b)")
      .filter(filterQuery)
    val edges = paths.select("e.src", "e.dst", "e.value")
    GraphFrame(self.vertices, edges)
  }

  def getPositiveRTSubGraph: GraphFrame =
    getRTSubGraph(List[String]("Positive"))

  def getNegativeRTSubGraph: GraphFrame =
    getRTSubGraph(List[String]("Negative", "Very negative"))

  def runPageRank: GraphFrame =
    self.pageRank.maxIter(10).run()

  def getRank: DataFrame = {
    self.vertices.toDF
//    ranked.orderBy(ranked.col("pagerank").desc).toDF
  }

  def runTriangleCount: DataFrame = {
    val communities = self.toGraphX.triangleCount()
    val verticesRdd: VertexRDD[Int] = communities.vertices
//    val schema = StructType(List(
//      StructField("id", LongType, false),
//      StructField("community", IntegerType, false))
//    )
    session.sqlContext.createDataFrame(verticesRdd).selectExpr("_1 as id", "_2 as community")
  }

}

trait GraphAnalysisDsl {

  implicit def neo4jFunctions(neo4j: Neo4j)
                             (implicit session: SparkSession) =
    new Neo4jFunctions(neo4j)

  implicit def measuresAnalysis(graph: GraphFrame)
                               (implicit session: SparkSession) =
    new NetworkMeasuresAnalysis(graph)

}

object GraphAnalysisDsl extends GraphAnalysisDsl
