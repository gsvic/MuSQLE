package gr.cslab.ece.ntua.musqle.catalog

import java.io._

import scala.collection.mutable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.engine.{Engine, Postgres, Spark}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * The Catalog
  * Holds metadata about tables
  * */
class Catalog(sparkSession: SparkSession, mc: MuSQLEContext) {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val planToTableName: mutable.HashMap[LogicalRelation, String] = new mutable.HashMap[LogicalRelation, String]()
  val engines = new mutable.HashSet[Engine]()
  engines.add(Spark(sparkSession, mc))
  engines.add(Postgres(sparkSession, mc))
  val tableEngines: mutable.HashMap[LogicalRelation, Seq[Engine]] =
    new mutable.HashMap[LogicalRelation, Seq[Engine]]()

  private val pathToTable: mutable.HashMap[String, CatalogEntry] = new mutable.HashMap[String, CatalogEntry]()
  private val tableMap: mutable.HashMap[String, CatalogEntry] = {
    val tables = new mutable.HashMap[String, CatalogEntry]()
    val metaFolder = new File("./metastore")
    val catalogFolder = new File("./metastore/catalog")

    if (!metaFolder.exists()) {metaFolder.mkdir()}
    if (!catalogFolder.exists()) {catalogFolder.mkdir()}

    catalogFolder.list().foreach { entry =>
      if (entry.endsWith(".mt")) {
          val lines = (new BufferedReader(new FileReader(s"./metastore/catalog/$entry"))).lines().toArray()
          val tEngines = lines
            .map(str => mapper.readValue(str.toString, classOf[CatalogEntry]).engine)
            .map {e =>
              e match {
                case "spark" => Spark(sparkSession, mc)
                case "postgres" => Postgres(sparkSession, mc)
              }
            }

          val str = lines(0)
          val catalogRecord = mapper.readValue(str.toString, classOf[CatalogEntry])
          pathToTable.put(catalogRecord.tablePath, catalogRecord)
          tables.put(catalogRecord.tableName, catalogRecord)

          val tableDF = {
            catalogRecord.engine match {
              case "spark" => {
                val df = sparkSession.read.format(catalogRecord.format).load(catalogRecord.tablePath)
                tableEngines.put(df.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], tEngines)
                df
              }
              case "postgres" => {
                val post = Engine.POSTGRES(sparkSession, mc)
                val df = sparkSession.read.jdbc(post.jdbcURL, catalogRecord.tablePath, post.props)
                tableEngines.put(df.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], tEngines)
                df
              }
            }
          }
          tableEngines.put(tableDF.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], tEngines)
          this.planToTableName.put(tableDF.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation], catalogRecord.tableName)
          tableDF.createOrReplaceTempView(catalogRecord.tableName)
        }
      }

      tables
  }

  def getTableEntryByPath(path: String): CatalogEntry = pathToTable.get(path).get
  def getTableMap: mutable.HashMap[String, CatalogEntry] = tableMap

  /** Saves all tables in disk */
  def saveChanges = {
    tableMap.values.foreach{ ce =>
      val fw = (new FileWriter(new File(s"./metastore/catalog/${ce.tableName}.mt")))
      val sw = new StringWriter()
      mapper.writeValue(sw, ce)
      fw.write(sw.toString)
      fw.close()
    }
  }

  /**
    * Adds a new table to the catalog
    * @param name The table's name
    * @param path The table's path:
    *             - For HDFS enter the full path, for example: hdfs://host:9000/path
    *             - For JDBC enter the table name
    * @param engine The engine in which the table resides
    * @param format The table's format ('parquet'-'json', HDFS ONLY)
    * */
  def add(name: String, path: String, engine: String, format: String, numRows: Long): Unit ={
    tableMap.put(name, CatalogEntry(name, path, engine, format, numRows))
    saveChanges
  }
}