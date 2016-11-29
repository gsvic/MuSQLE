package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.SparkSession
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.{AllQueries, FixedQueries}
import gr.cslab.ece.ntua.musqle.engine.{Engine, Postgres, Spark}
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEMove
import gr.cslab.ece.ntua.musqle.tools.ConfParser
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.log4j.Logger
import org.apache.log4j.Level

class MuSQLEContext(sparkSession: SparkSession) {
  val logger = Logger.getLogger(classOf[MuSQLEContext])
  logger.setLevel(Level.DEBUG)

  val catalog = Catalog.getInstance
  var planner: DPhypSpark = new DPhypSpark(sparkSession)

  val post = Engine.POSTGRES(sparkSession)

  catalog.tableMap.values.foreach { tableEntry =>
    logger.info(s"Loading: ${tableEntry}")
    val tableDF = {
      tableEntry.engine match {
        case "spark" => {
          sparkSession.read.format(tableEntry.format).load(tableEntry.tablePath)
        }
        case "postgres" => {
          sparkSession.read.jdbc(post.jdbcURL, tableEntry.tablePath, post.props)
        }
      }
    }
    planner.qInfo.planToTableName.put(tableDF.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], tableEntry.tableName)
    tableDF.createOrReplaceTempView(tableEntry.tableName)
  }

  def showTables: Unit = { getTableList.foreach(println)}
  def getTableList: Seq[String] = {
    catalog.tableMap.map { table =>
      s"Table: ${table._1}, Engine: ${table._2.engine}, Path: ${table._2.tablePath}"
    }.toSeq
  }


  def query(sql: String): MuSQLEQuery = {
    val df = sparkSession.sql(sql)
    val optPlan = df.queryExecution.optimizedPlan

    planner.setLogicalPlan(optPlan)

    post.cleanResults()
    post.cleanViews()

    val start = System.currentTimeMillis()
    val p = planner.plan()
    val planningTime = (System.currentTimeMillis() - start) / 1000.0

    logger.info(s"Planning took ${planningTime}s.")
    logger.debug(s"Total inject time: ${MuSQLEMove.totalInject}")
    logger.debug(s"Spark getCost time: ${Spark.totalGetCost}")
    logger.debug(s"Postgres getCost time: ${Postgres.totalGetCost}")

    post.cleanResults()
    new MuSQLEQuery(sparkSession, p)
  }

}

object test extends App{
  lazy val sparkSession = SparkSession
    .builder()
    .master(s"spark://${ConfParser.getConf("spark.master").get}:7077")
    .appName(ConfParser.getConf("spark.appName").get)
    .config("spark.driver.memory", ConfParser.getConf("spark.driver.memory").get)
    .config("spark.executor.memory", ConfParser.getConf("spark.executor.memory").get)
    .config("spark.files", "./jars/postgresql-9.4.1212.jre6.jar")
    .config("spark.jars", "./jars/postgresql-9.4.1212.jre6.jar")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val mc = new MuSQLEContext(sparkSession)

  val mq = mc.query(FixedQueries.queries(0)._2)

  mq.explain
  mq.execute.explain
}