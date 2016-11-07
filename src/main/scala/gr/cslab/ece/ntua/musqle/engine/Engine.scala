package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Engine(){
  override def equals(obj: scala.Any): Boolean = {
    this.getClass.equals(obj.getClass)
  }
  def getMoveCost(plan: DPJoinPlan): Double
  def getQueryCost(sql: String): Double
  def getDF(sql: String): DataFrame
}

class HDFSFormat()
case class Json() extends HDFSFormat
case class Parquet() extends HDFSFormat

object Engine{
  def SPARK(sparkSession: SparkSession) = Spark(sparkSession)
  def POSTGRES(sparkSession: SparkSession)  = Postgres(sparkSession)
}
