package gr.cslab.ece.ntua.musqle.catalog

import java.io._
import scala.collection.mutable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * The Catalog
  * Holds metadata about tables
  * */
class Catalog {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private val pathToTable: mutable.HashMap[String, CatalogEntry] = new mutable.HashMap[String, CatalogEntry]()
  private val tableMap: mutable.HashMap[String, CatalogEntry] = {
    val tables = new mutable.HashMap[String, CatalogEntry]()
    val metaFolder = new File("./metastore")
    val catalogFolder = new File("./metastore/catalog")

    if (!metaFolder.exists()) {metaFolder.mkdir()}
    if (!catalogFolder.exists()) {catalogFolder.mkdir()}

    catalogFolder.list().foreach{entry =>
      if (entry.endsWith(".mt")) {
        val str = (new BufferedReader(new FileReader(s"./metastore/catalog/$entry"))).readLine()
        val cr = mapper.readValue(str, classOf[CatalogEntry])
        pathToTable.put(cr.tablePath, cr)
        tables.put(cr.tableName, cr)
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

object Catalog {
  val getInstance = new Catalog()
}