package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.AllQueries
import gr.cslab.ece.ntua.musqle.engine.Engine
import gr.cslab.ece.ntua.musqle.sql.SparkPlanGenerator
import org.apache.spark.sql.catalyst.encoders.RowEncoder
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
    println(tableDF.queryExecution.sparkPlan)
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
    println(sparkLogical)
    val dataFrame = new Dataset[Row](sparkSession, sparkLogical, RowEncoder(sparkLogical.schema))

    println(df.queryExecution.sparkPlan)
    println(dataFrame.queryExecution.sparkPlan)

    p.explain()

    dataFrame
  }

}

object test extends App{
  val mc = new MusqleContext()
  val q = AllQueries.tpcds1_4Queries(2)._2
  val q1 = mc.query(q).count

  println(q1)

}