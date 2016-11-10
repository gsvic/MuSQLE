package gr.cslab.ece.ntua.musqle.engine

import java.util.Properties

import com.github.mauricio.async.db.{QueryResult, RowData}
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
  * Created by vic on 7/11/2016.
  */
case class Postgres(sparkSession: SparkSession) extends Engine {
  val jdbcURL = s"jdbc:postgresql://147.102.4.129:5432/tpcds1?user=musqle&password=musqle"
  val props = new Properties()
  props.setProperty("driver", "org.postgresql.Driver")

  val connection = {
    val configuration = URLParser.parse(jdbcURL)
    val con = new PostgreSQLConnection(configuration)
    Await.result(con.connect, 5 seconds)

    con
  }

  override def supportsMove(engine: Engine): Boolean = false
  override def getMoveCost(plan: DPJoinPlan): Double = 100000
  override def getQueryCost(sql: String): Double = {
    println(s"Asking: ${sql} = [postgres]")

    val start = System.currentTimeMillis()
    val future: Future[QueryResult] = connection.sendQuery(s"EXPLAIN ${sql.replaceAll("`", "")}")
    val mapResult: Future[Any] = future.map(queryResult => queryResult.rows match {
      case Some(resultSet) => {
        val row: RowData = resultSet.head
        row(0)
      }
      case None => -1
    })

    lazy val pageFetches = {
      Await.result(mapResult, 20 seconds)
      val p = mapResult.value.get.get.toString
        .split("  ")(1)
        .split(" ")(0)
        .split("\\.\\.")

      val min = p(0).split("=")(1)
      val max = p(1).toDouble

      println(s"[${min}, ${max}]")

      max
    }

    val singleFetchCost = 1
    val cost = pageFetches * singleFetchCost

    cost
  }

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
