package gr.cslab.ece.ntua.musqle.engine

import java.util.Properties

import com.github.mauricio.async.db.{QueryResult, RowData}
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Scan}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import org.apache.spark.sql.types._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
  * Created by vic on 7/11/2016.
  */
case class Postgres(sparkSession: SparkSession) extends Engine {
  val jdbcURL = s"jdbc:postgresql://147.102.4.129:5432/tpcds1?user=musqle&password=musqle"
  val props = new Properties()
  logger.info("Initializing postgres")
  props.setProperty("driver", "org.postgresql.Driver")

  val connection = {
    val configuration = URLParser.parse(jdbcURL)
    val con = new PostgreSQLConnection(configuration)
    Await.result(con.connect, 5 seconds)

    con
  }

  override def createView(plan: MuSQLEScan, srcTable: String, projection: String): Unit = {
    val viewQuery =
      s"""
         |CREATE OR REPLACE VIEW ${plan.tmpName}
         |AS SELECT $projection
         |FROM $srcTable
         |""".stripMargin

    Await.result(connection.sendQuery(viewQuery), 20 seconds)
  }

  override def inject(plan: DPJoinPlan): Unit ={
    logger.info(s"Injecting ${plan.tmpName}")
    val df = plan.left.engine.getDF(plan.toSQL)
    val name = plan.tmpName

    var table = s"CREATE TABLE ${name} ("
    var row = "("
    val sc = df.schema.iterator
    sc.foreach{dt =>
      dt.dataType match {
        case IntegerType => {
          row += "1"
          table += s"${dt.name } integer"
        }
        case StringType => {
          row += "'str'"
          table += s"${dt.name} varchar(5)"
        }
        case LongType =>  {
          row += "1 "
          table += s"${dt.name} integer"
        }
        case DoubleType => {
          row += "2.2 "
          table += s"${dt.name} real"
        }
        case x: DecimalType => {
          row += "2.2 "
          table += s"${dt.name} real"
        }
        case TimestampType => {
          row += s"${System.nanoTime()} "
          table += s"${dt.name} real"
        }
        case DateType => {
          row += s"${System.nanoTime()} "
          table += s"${dt.name} real"
        }
      }
      if (sc.hasNext) {row += ","; table+= ","}
    }

    row += ")"
    table += ")"

    val injectADummyRow = s"""INSERT INTO ${name.toLowerCase} VALUES ${row}"""
    val script = s"${table};\n${injectADummyRow};"
    Await.result(connection.sendQuery(script), 10 seconds)
  }
  override def supportsMove(engine: Engine): Boolean = true
  override def move(move: DPJoinPlan): Unit = {
    logger.info(s"Moving ${move.tmpName}")
    this.writeDF(move.left.engine.getDF(move.toSQL), move.tmpName)
  }
  override def getMoveCost(plan: DPJoinPlan): Double = 100000
  override def getQueryCost(sql: String): Double = {
    logger.debug(s"Getting query cost: ${sql}")

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

      max
    }

    val singleFetchCost = 1
    val cost = pageFetches * singleFetchCost

    cost
  }

  def cleanResults() {
    val tables = connection.sendQuery("""select tablename from pg_tables""")
    val res = Await.result(tables, 20 seconds)
    res.rows.get.foreach { row =>
      if (row(0).toString.contains("result")) {
        logger.debug(s"Deleting table ${row(0)}")
        Await.result(connection.sendQuery(s"drop table ${row(0)}"), 20 seconds)
      }
    }
    logger.info("Done.")
  }

  def cleanViews(): Unit = {
    logger.info("Deleting past intermediate results from Postgres")

    val views = connection.sendQuery("""select table_name from INFORMATION_SCHEMA.views""")
    val viewRes = Await.result(views, 20 seconds)
    viewRes.rows.get.foreach { row =>
      if (row(0).toString.contains("result")) {
        logger.debug(s"Deleting view ${row(0)}")
        Await.result(connection.sendQuery(s"drop view ${row(0)}"), 20 seconds)
      }
    }
  }

  def writeDF(dataFrame: DataFrame, name: String): Unit = {
    dataFrame.write.jdbc(jdbcURL, name, props)
  }


  def getDF(sql: String): DataFrame = {
    val df = sparkSession.read.jdbc(jdbcURL, s"""(${sql}) AS SubQuery""", props)
    df
  }

  override def toString: String = "PostgreSQL"
}

object Postgres extends App{
val p = new Postgres(null)
p.cleanResults()
}