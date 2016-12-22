package gr.cslab.ece.ntua.musqle.engine

import java.util.Properties

import com.github.mauricio.async.db.{QueryResult, RowData}
import gr.cslab.ece.ntua.musqle.plan.hypergraph.{DPJoinPlan, Scan}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser
import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEScan
import gr.cslab.ece.ntua.musqle.tools.ConfParser
import org.apache.spark.sql.types._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
  * Created by vic on 7/11/2016.
  */
case class Postgres(sparkSession: SparkSession, mc: MuSQLEContext) extends Engine {
  val postgresHost = ConfParser.getConf("postgres.host").get
  val postgresPort = ConfParser.getConf("postgres.port").get
  val postgresDB = ConfParser.getConf("postgres.db").get
  val postgresUsr = ConfParser.getConf("postgres.user").get
  val postgresPass = ConfParser.getConf("postgres.pass").get

  val jdbcURL = s"jdbc:postgresql://${postgresHost}:${postgresPort}/${postgresDB}?user=${postgresUsr}&password=${postgresPass}"
  val props = new Properties()
  logger.info("Initializing postgres")
  props.setProperty("driver", "org.postgresql.Driver")

  val connection = {
    val configuration = URLParser.parse(jdbcURL)
    val con = new PostgreSQLConnection(configuration)
    Await.result(con.connect, 5 seconds)

    con
  }

  private def executeQuery(query: String): Unit = {
    Await.result(connection.sendQuery(query), 20 seconds)
  }

  override def createView(plan: MuSQLEScan, srcTable: String, projection: String): Unit = {
    logger.info(s"Creating view ${plan.tmpName}")
    val viewQuery =
      s"""
         |CREATE VIEW ${plan.tmpName}
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

    sc.foreach{ dt =>
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

    /* TODO: Stats injection - https://github.com/ossc-db/pg_dbms_stats*/
    val getOId = s"SELECT oid FROM pg_class WHERE relname = '${name.toLowerCase}' "
    val estimRows = plan.getRowsEstimation
    var pages = sc.map(_.dataType.defaultSize).sum / 16
    val injectResult = s"""INSERT INTO  dbms_stats.relation_stats_locked VALUES( (${getOId}), '${name.toLowerCase}', ${pages}, ${estimRows})"""
    /* Stats injection*/

    val injectADummyRow = s"""INSERT INTO ${name.toLowerCase} VALUES ${row}"""

    val script = s"${table};\n${injectADummyRow};"
    Await.result(connection.sendQuery(script), 10 seconds)
  }

  override def getCost(plan: DPJoinPlan): Double = {
    getCostMetrics(plan).cost
  }

  override def supportsMove(engine: Engine): Boolean = true

  override def move(move: DPJoinPlan): Unit = {
    logger.info(s"Moving ${move.tmpName} ${move.toSQL}")
    executeQuery(s"DROP TABLE IF EXISTS ${move.tmpName}")
    val moveDF = move.left.engine.getDF(move.toSQL)
    this.writeDF(moveDF, move.tmpName)
  }

  override def getMoveCost(plan: DPJoinPlan): Double = 0.0

  override def getRowsEstimation(plan: DPJoinPlan): Long = {
    getCostMetrics(plan).rows
  }

  override def getDF(sql: String): DataFrame = {
    val df = sparkSession.read.jdbc(jdbcURL, s"""(${sql}) AS SubQuery""", props)
    df
  }

  override def toString: String = "PostgreSQL"

  def cleanResults() {
    val tables = connection.sendQuery("""select tablename from pg_tables""")
    val res = Await.result(tables, 20 seconds)
    res.rows.get.foreach { row =>
      if (row(0).toString.contains("mtpmres")) {
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
      if (row(0).toString.contains("mtpmres")) {
        logger.debug(s"Deleting view ${row(0)}")
        Await.result(connection.sendQuery(s"drop view ${row(0)}"), 20 seconds)
      }
    }
  }

  override def cleanTmpResults: Unit = {
    cleanViews()
    cleanResults()
  }

  def writeDF(dataFrame: DataFrame, name: String): Unit = {
    dataFrame.write.jdbc(jdbcURL, name, props)
  }

  def getCostMetrics(plan: DPJoinPlan): CostMetrics = {
    logger.debug(s"Getting query cost: ${plan.toSQL}")

    val start = System.currentTimeMillis()
    val future: Future[QueryResult] = connection.sendQuery(s"EXPLAIN ${plan.toSQL.replaceAll("`", "")}")
    val mapResult: Future[Any] = future.map(queryResult => queryResult.rows match {
      case Some(resultSet) => {
        val row: RowData = resultSet.head
        row(0)
      }
      case None => -1
    })

    Await.result(mapResult, 20 seconds)

    val rows = {
      val r = mapResult.value.get.get.toString
        .split("  ")(1)
        .split(" ")(1)
        .split("=")(1)


      java.lang.Long.valueOf(r)
    }

    val pageFetches = {
      val p = mapResult.value.get.get.toString
        .split("  ")(1)
        .split(" ")(0)
        .split("\\.\\.")

      val min = p(0).split("=")(1)
      val max = p(1).toDouble

      max
    }

    val singleFetchCost = java.lang.Double.parseDouble(ConfParser.getConf("postgres.fetchCost").get)
    val cost = pageFetches * singleFetchCost

    Postgres.totalGetCost += (System.currentTimeMillis() - start) / 1000.0

    CostMetrics(rows, cost)
  }

  case class CostMetrics(val rows: Long, val cost: Double)
}

object Postgres {
  var totalGetCost = 0.0
}