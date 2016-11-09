package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.AllQueries
import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.sql.SparkPlanGenerator
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.LogicalRelation

class MusqleContext {
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

    val p = planner.plan()
    val planGenerator = new SparkPlanGenerator(sparkSession)
    val sparkLogical = planGenerator.toSparkLogicalPlan(p)

    //val qe = new QueryExecution(sparkSession, sparkLogical)
    val dataFrame = new Dataset[Row](sparkSession, sparkLogical, RowEncoder(sparkLogical.schema))

    val a = {
      val start = System.currentTimeMillis()
      df.count()
      System.currentTimeMillis() - start
    }
    val b = {
      val start = System.currentTimeMillis()
      dataFrame.count()
      System.currentTimeMillis() - start
    }

    println(s"Spark: ${a / 1000.0}\nMuSQLE: ${b / 1000.0}")


    dataFrame
  }

}

object test extends App{
  val mc = new MusqleContext()
  val q = AllQueries.tpcds1_4Queries(6)._2

  val postgres = Engine.POSTGRES(mc.sparkSession)
  postgres.writeDF(mc.sparkSession.table("store_sales"), "store_sales")
}