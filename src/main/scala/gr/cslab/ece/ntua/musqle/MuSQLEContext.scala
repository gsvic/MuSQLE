package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.SparkSession
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.{AllQueries, FixedQueries}
import gr.cslab.ece.ntua.musqle.engine.{Engine, Postgres, Spark}
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEMove
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.log4j.Logger
import org.apache.log4j.Level

class MuSQLEContext {
  val logger = Logger.getLogger(classOf[MuSQLEContext])
  logger.setLevel(Level.DEBUG)

  lazy val sparkSession = SparkSession
    .builder()
    .master("spark://vicbook:7077").appName("MuSQLE")
    .config("spark.files", "./jars/postgresql-9.4.1212.jre6.jar")
    .config("spark.jars", "./jars/postgresql-9.4.1212.jre6.jar")
    .getOrCreate()

  val catalog = Catalog.getInstance
  val planner: DPhypSpark = new DPhypSpark(sparkSession)

  val post = Engine.POSTGRES(sparkSession)

  catalog.tableMap.values.foreach { tableEntry =>
    logger.info(s"Loading: ${tableEntry}")
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

    p.explain()

    post.cleanResults()
    new MuSQLEQuery(sparkSession, p)
  }

}

object test extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val mc = new MuSQLEContext()
  val q = FixedQueries.queries(2)._2

  val p = mc.query(q)
  val df = p.execute

  df.explain

  val t = """select d1.d_date_sk, d4.d_date_sk from date_dim d1, date_dim d2, date_dim d3, date_dim d4
            |where d1.d_date_sk = d2.d_date_sk
            |and d2.d_date_sk = d3.d_date_sk
            |and d3.d_date_sk = d4.d_date_sk""".stripMargin

  /*val start = System.currentTimeMillis()
  val query = mc.query(q)
  val c1 = query.execute.count
  val end = (System.currentTimeMillis() - start)/1000.0

  val start1 = System.currentTimeMillis()
  val c2 = mc.sparkSession.sql(q).count
  val end1 = (System.currentTimeMillis() - start)/1000.0

  println(end)
  println(c1)
  println(end1)
  println(c2)*/
}