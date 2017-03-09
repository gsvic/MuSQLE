package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Scan}
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Engine(val sparkSession: SparkSession, val mc: MuSQLEContext){
  val logger = Logger.getLogger(this.getClass)
  logger.setLevel(Level.OFF)
  override def equals(obj: scala.Any): Boolean = {
    this.getClass.equals(obj.getClass)
  }

  def createView(plan: MuSQLEScan, srcTable: String, path: String,projection: String)
  def inject(plan: DPJoinPlan)
  def supportsMove(engine: Engine): Boolean
  def move(dPJoinPlan: DPJoinPlan)
  def getMoveCost(plan: DPJoinPlan): Double
  def getCost(plan: DPJoinPlan): Double
  def getRowsEstimation(plan: DPJoinPlan): Long
  def getDF(sql: String): DataFrame
  def cleanTmpResults: Unit = {}
  def rename(t1: String, t2: String): Unit = {}
}

class HDFSFormat()
case class Json() extends HDFSFormat
case class Parquet() extends HDFSFormat

object Engine{

  def SPARK(sparkSession: SparkSession, mc: MuSQLEContext) = Spark(sparkSession, mc)
  def POSTGRES(sparkSession: SparkSession, mc: MuSQLEContext)  = Postgres(sparkSession, mc)

  def matchEngine(logicalRelation: LogicalRelation) = {
    Spark.getClass
  }
}
