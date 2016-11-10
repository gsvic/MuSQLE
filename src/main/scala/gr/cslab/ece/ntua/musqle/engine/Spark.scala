package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.cost.SparkSQLCost
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vic on 2/11/2016.
  */

case class Spark(val sparkSession: SparkSession) extends Engine {
    val costEstimator = new SparkSQLCost()

  override def supportsMove(engine: Engine): Boolean = {
    true
  }
  override def getMoveCost(plan: DPJoinPlan): Double = 0.0
  override def getQueryCost(sql: String): Double  = {
    if (sparkSession == null) {
      throw new Exception("null spark session")
    }
    //TODO: Implement Spark SQL cost estimator module
    val c = 0.0//costEstimator.estimateCost(sparkSession.sql(sql))
    c
  }
  override def getDF(sql: String): DataFrame = sparkSession.sql(sql)
  /*
    override def getMoveCost(plan: DPJoinPlan): Double = {
      println("Asking Spark for move")
      0.0
    }*/
  override def toString: String = {"Spark SQL"}
}

