package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.AllQueries
import gr.cslab.ece.ntua.musqle.sql.SparkPlanGenerator
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.LogicalRelation

class MusqleContext {
  val catalog = Catalog.getInstance
  val planner: DPhypSpark = new DPhypSpark()

  lazy val sparkSession = {
      val ss = SparkSession.builder()
        .master("spark://master:7077").appName("MuSQLE").getOrCreate()

      catalog.tableMap.values.foreach { tableEntry =>
        val tableDF = ss.read.format(tableEntry.format).load(tableEntry.tablePath)
        planner.qInfo.planToTableName.put(tableDF.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], tableEntry.tableName)
        tableDF.createOrReplaceTempView(tableEntry.tableName)
      }
      ss
  }


  def query(sql: String): DataFrame = {
    val df = sparkSession.sql(sql)
    val optPlan = df.queryExecution.optimizedPlan
    planner.setLogicalPlan(optPlan)


    val p = planner.plan()
    val planGenerator = new SparkPlanGenerator()
    val sparkLogical = planGenerator.toSparkLogicalPlan(p)
    val dataFrame = new Dataset[Row](sparkSession, sparkLogical, RowEncoder(sparkLogical.schema))

    println(df.queryExecution.optimizedPlan)
    println(dataFrame.queryExecution.optimizedPlan)

    dataFrame
  }

}

object test extends App{
  val mc = new MusqleContext()
  val q = AllQueries.tpcds1_4Queries(2)._2
  val q1 = mc.query(q)

}