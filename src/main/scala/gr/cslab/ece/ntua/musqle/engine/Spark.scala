package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.cost.SparkSQLCost
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vic on 2/11/2016.
  */

case class Spark(val sparkSession: SparkSession, mc: MuSQLEContext) extends Engine {
  val costEstimator = new SparkSQLCost(mc)

  override def createView(plan: MuSQLEScan, srcTable: String, projection: String): Unit = {
    logger.info(s"Creating view ${plan.tmpName}")
    val viewSQL =
      s"""
        |SELECT $projection
        |FROM $srcTable
      """.stripMargin

    sparkSession.sql(viewSQL).createOrReplaceTempView(plan.tmpName)
  }

  override def inject(plan: DPJoinPlan): Unit = {
    logger.info(s"Injecting ${plan.tmpName}")
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

  override def getCost(plan: DPJoinPlan): Double  = {
    logger.debug(s"Getting query cost: ${plan.toSQL}")

    if (sparkSession == null) {
      throw new Exception("null spark session")
    }
    //TODO: Implement Spark SQL cost estimator module

    plan match {
      case _ => {
        val start = System.currentTimeMillis()
        val c = costEstimator.getCostMetrics(sparkSession.sql(plan.toSQL)).totalCost
        Spark.totalGetCost += (System.currentTimeMillis() - start) / 1000.0

        c
      }
    }

  }

  override def getRowsEstimation(plan: DPJoinPlan): Long = {
    val df = sparkSession.sql(plan.toSQL)
    costEstimator.getCostMetrics(df).rows
  }
  override def getDF(sql: String): DataFrame = sparkSession.sql(sql)
  override def toString: String = {"SparkSQL"}
}

object Spark {
  var totalGetCost = 0.0
}
