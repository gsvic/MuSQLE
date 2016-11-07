package gr.cslab.ece.ntua.musqle.engine

import java.util.Properties

import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vic on 7/11/2016.
  */
case class Postgres(sparkSession: SparkSession) extends Engine {
  val jdbcURL = s"jdbc:postgresql://147.102.4.129:5432/tpcds1?user=musqle&password=musqle"
  val props = new Properties()
  props.setProperty("driver", "org.postgresql.Driver")

  override def getMoveCost(plan: DPJoinPlan): Double = 100000
  override def getQueryCost(sql: String): Double = 100000

  def writeDF(dataFrame: DataFrame, name: String): Unit = {
    dataFrame.write.jdbc(jdbcURL, name, props)
  }


  def getDF(sql: String): DataFrame = {
    println(s"Postgres: Executing: ${sql}")
    val df = sparkSession.read.jdbc(jdbcURL, s"""(${sql}) AS SubQuery""", props)
    println(df)
    df
  }
}
