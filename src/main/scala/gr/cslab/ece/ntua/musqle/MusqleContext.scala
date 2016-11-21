package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.AllQueries
import gr.cslab.ece.ntua.musqle.engine.{Engine, Postgres, Spark}
import gr.cslab.ece.ntua.musqle.plan.hypergraph.DPJoinPlan
import gr.cslab.ece.ntua.musqle.plan.spark.{Execution, MuSQLEMove}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.log4j.Logger
import org.apache.log4j.Level

class MusqleContext {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val mcLogger = Logger.getLogger(classOf[MusqleContext])
  mcLogger.setLevel(Level.DEBUG)

  lazy val sparkSession = SparkSession
    .builder()
    .master("spark://master:7077").appName("MuSQLE")
    .config("spark.files", "./jars/postgresql-9.4.1212.jre6.jar")
    .config("spark.jars", "./jars/postgresql-9.4.1212.jre6.jar")
    .getOrCreate()

  val catalog = Catalog.getInstance
  val planner: DPhypSpark = new DPhypSpark(sparkSession)

  val post = Engine.POSTGRES(sparkSession)

  catalog.tableMap.values.foreach { tableEntry =>
    mcLogger.info(s"Loading: ${tableEntry}")
  val tableDF = {
    tableEntry.engine match {
      case "spark" => {
        sparkSession.read.format (tableEntry.format).load(tableEntry.tablePath)
      }
      case "postgres" => {
        sparkSession.read.jdbc(post.jdbcURL, tableEntry.tablePath, post.props)
      }
    }
  }
    planner.qInfo.planToTableName.put(tableDF.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], tableEntry.tableName)
    tableDF.createOrReplaceTempView(tableEntry.tableName)
  }


  def query(sql: String): DataFrame = {
    val df = sparkSession.sql(sql)
    val optPlan = df.queryExecution.optimizedPlan
    planner.setLogicalPlan(optPlan)

    post.cleanResults()
    post.cleanViews()

    val start = System.currentTimeMillis()
    val p = planner.plan()
    val planningTime = (System.currentTimeMillis() - start) / 1000.0

    mcLogger.info(s"Planning took ${planningTime}s.")

    //post.cleanResults()

    val executor = new Execution(sparkSession)
    val result = executor.execute(p)
    result.explain()

    println(s"Total inject time: ${MuSQLEMove.totalInject}")
    println(s"Total getCost time: ${DPJoinPlan.totalGetCost}")
    println(s"Spark getCost time: ${Spark.totalGetCost}")
    println(s"Postgres getCost time: ${Postgres.totalGetCost}")

    null
  }

}

object test extends App{
  val mc = new MusqleContext()
  val q = AllQueries.tpcds1_4Queries(11)._2
  val q2 = AllQueries.tpcds1_4Queries(6)._2

  val t = """select d1.d_date_sk, d4.d_date_sk from date_dim d1, date_dim d2, date_dim d3, date_dim d4
            |where d1.d_date_sk = d2.d_date_sk
            |and d2.d_date_sk = d3.d_date_sk
            |and d3.d_date_sk = d4.d_date_sk""".stripMargin

  mc.query(q2)
}