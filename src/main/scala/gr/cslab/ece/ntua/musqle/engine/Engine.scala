package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Scan}
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Engine(){
  val logger = Logger.getLogger(this.getClass)
  //logger.setLevel(Level.DEBUG)
  override def equals(obj: scala.Any): Boolean = {
    this.getClass.equals(obj.getClass)
  }

  def createView(plan: MuSQLEScan, srcTable: String, projection: String)
  def inject(plan: DPJoinPlan)
  def supportsMove(engine: Engine): Boolean
  def move(dPJoinPlan: DPJoinPlan)
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
