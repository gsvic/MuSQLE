package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.SimpleQueries
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
    optPlan.subqueries.foreach{sq =>
      println()
    }
    val p = planner.plan()

    df
  }

}

object test extends App{
  val mc = new MusqleContext()
  val q = SimpleQueries.q7Derived(4)._2
  val q1 = mc.query(q)

}