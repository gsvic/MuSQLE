package gr.cslab.ece.ntua.musqle.engine

import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.cost.SparkSQLCost
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vic on 2/11/2016.
  */

case class Spark(override val sparkSession: SparkSession, override val mc: MuSQLEContext)
  extends Engine(sparkSession, mc) {
  val costEstimator = new SparkSQLCost(mc)

  override def createView(plan: MuSQLEScan, srcTable: String, path: String, projection: String): Unit = {
    logger.info(s"Creating view ${plan.tmpName}")
    val viewSQL =
      s"""
        |SELECT $projection
        |FROM ${plan.tmpName}
      """.stripMargin

    val df = sparkSession.read.parquet(path)

    df.createOrReplaceTempView(plan.tmpName)
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
    val df = move.left.engine.getDF(move.toSQL)
    df.createOrReplaceTempView(move.tmpName)
    df.cache()
  }
  override def getMoveCost(plan: DPJoinPlan): Double = 0.0

  override def getCost(plan: DPJoinPlan): Double  = {
    logger.debug(s"Getting query cost: ${plan.toSQL}")

    if (sparkSession == null) {
      throw new Exception("null spark session")
    }
    //TODO: Implement Spark SQL cost estimator module

    val cost = plan match {
      case _ => {
        val start = System.currentTimeMillis()
        val c = 30.0//costEstimator.getCostMetrics(sparkSession.sql(plan.toSQL)).totalCost
        Spark.totalGetCost += (System.currentTimeMillis() - start) / 1000.0

        c
      }
    }

    logger.debug(s"Getting query cost: ${plan.toSQL}: ${cost}")
    cost
  }

  override def getRowsEstimation(plan: DPJoinPlan): Long = {
    val df = sparkSession.sql(plan.toSQL)
    costEstimator.getCostMetrics(df).rows
  }
  override def getDF(sql: String): DataFrame = sparkSession.sql(sql)

  override def rename(t1: String, t2: String): Unit = {
    logger.info(s"Renaming $t1 to $t2")
  }
  override def toString: String = {"SparkSQL"}
}

object Spark {
  var totalGetCost = 0.0
}
