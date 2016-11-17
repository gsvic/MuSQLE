package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.cost.SparkSQLCost
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vic on 2/11/2016.
  */

case class Spark(val sparkSession: SparkSession) extends Engine {
  val costEstimator = new SparkSQLCost()

  override def createView(plan: MuSQLEScan, srcTable: String, projection: String): Unit = {
    val viewSQL =
      s"""
        |SELECT $projection
        |FROM $srcTable
      """.stripMargin

    sparkSession.sql(viewSQL).createOrReplaceTempView(plan.tmpName)
  }

  override def inject(plan: DPJoinPlan): Unit = {
    plan.left.engine.getDF(plan.toSQL).createOrReplaceTempView(plan.tmpName)
  }
  override def supportsMove(engine: Engine): Boolean = {
    true
  }
  override def move(move: DPJoinPlan): Unit = {
    logger.info(s"Moving ${move.tmpName}")
    move.left.engine.getDF(move.toSQL).createOrReplaceTempView(move.tmpName)
  }
  override def getMoveCost(plan: DPJoinPlan): Double = 0.0
  override def getQueryCost(sql: String): Double  = {
    logger.debug(s"Getting query cost: ${sql}")

    if (sparkSession == null) {
      throw new Exception("null spark session")
    }
    //TODO: Implement Spark SQL cost estimator module
    val c = costEstimator.estimateCost(sparkSession.sql(sql))
    c
  }
  override def getDF(sql: String): DataFrame = sparkSession.sql(sql)
  /*
    override def getMoveCost(plan: DPJoinPlan): Double = {
      println("Asking Spark for move")
      0.0
    }*/
  override def toString: String = {"SparkSQL"}
}

