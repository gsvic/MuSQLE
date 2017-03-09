package gr.cslab.ece.ntua.musqle

import gr.cslab.ece.ntua.musqle.catalog.Catalog
import gr.cslab.ece.ntua.musqle.spark.DPhypSpark
import org.apache.spark.sql.SparkSession
import gr.cslab.ece.ntua.musqle.engine.{Engine, Postgres, Spark}
import gr.cslab.ece.ntua.musqle.plan.Cache
import gr.cslab.ece.ntua.musqle.plan.hypergraph.AbstractPlan
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEMove
import gr.cslab.ece.ntua.musqle.tools.ConfParser
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

import scala.collection.mutable

class MuSQLEContext(sparkSession: SparkSession) {
  val logger = Logger.getLogger(classOf[MuSQLEContext])
  logger.setLevel(Level.DEBUG)

  val cache = new Cache
  val catalog = new Catalog(sparkSession, this)

  def showTables: Unit = { getTableList.foreach(println) }

  def getTableList: Seq[String] = {
    catalog.getTableMap.map { table =>
      s"Table: ${table._1}, Engine: ${table._2.engine}, Path: ${table._2.tablePath}"
    }.toSeq
  }


  def query(sql: String): MuSQLEQuery = {
    val planner = new DPhypSpark(sparkSession, catalog, this)
    val df = sparkSession.sql(sql)
    val optPlan = df.queryExecution.optimizedPlan

    catalog.engines.foreach(_.cleanTmpResults)

    planner.setLogicalPlan(optPlan)

    val start = System.currentTimeMillis()
    val p = planner.plan()
    val planningTime = (System.currentTimeMillis() - start) / 1000.0

    logger.info(s"Planning took ${planningTime}s.")
    logger.debug(s"Total inject time: ${MuSQLEMove.totalInject}")
    logger.debug(s"Spark getCost time: ${Spark.totalGetCost}")
    logger.debug(s"Postgres getCost time: ${Postgres.totalGetCost}")

    new MuSQLEQuery(this, sparkSession, p)
  }

}

object kati {
  val s1 = mutable.HashSet("customer", "nation")
  val s2 = mutable.HashSet("nation", "customer")

  val a1 = AbstractPlan(s1, null)
  val a2 = AbstractPlan(s2, null)

  val eq = a1.equals(a2)

  println(eq)
}

object test extends App {
  SparkContext
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
  val post = new Postgres(sparkSession, mc)

  val tpch3 =
    """
      |select
      |	l_orderkey,
      |	sum(l_extendedprice * (1 - l_discount)) as revenue,
      |	o_orderdate,
      |	o_shippriority
      |from
      |	customer,
      |	orders,
      |	lineitem
      |where
      |	c_mktsegment = 'AUTOMOBILE'
      |	and c_custkey = o_custkey
      |	and l_orderkey = o_orderkey
      |group by
      |	l_orderkey,
      |	o_orderdate,
      |	o_shippriority
      |order by
      |	revenue desc,
      |	o_orderdate
    """.stripMargin

  val tpch5 =
    """
      |select
      |	n_name,
      |	sum(l_extendedprice * (1 - l_discount)) as revenue
      |from
      |	customer,
      |	orders,
      |	lineitem,
      |	supplier,
      |	nation,
      |	region
      |where
      |	c_custkey = o_custkey
      |	and l_orderkey = o_orderkey
      |	and l_suppkey = s_suppkey
      |	and c_nationkey = s_nationkey
      |	and s_nationkey = n_nationkey
      |	and n_regionkey = r_regionkey
      |	and r_name = 'MIDDLE EAST'
      |group by
      |	n_name
      |order by
      |	revenue desc
    """.stripMargin

  val q1 = mc.query(tpch3)
  q1.explain
  q1.execute

  val q2 = mc.query(tpch5)
  q2.explain
  q2.execute


}