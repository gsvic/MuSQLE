package gr.cslab.ece.ntua.musqle.catalog

import java.io._
import scala.collection.mutable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class Catalog {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val tableMap: mutable.HashMap[String, CatalogEntry] = {
  val tables = new mutable.HashMap[String, CatalogEntry]()
  val metaFolder = new File("./metastore")
  val catalogFolder = new File("./metastore/catalog")

  if (!metaFolder.exists()) {metaFolder.mkdir()}
  if (!catalogFolder.exists()) {catalogFolder.mkdir()}

  catalogFolder.list().foreach{entry =>
    if (entry.endsWith(".mt")) {
      val str = (new BufferedReader(new FileReader(s"./metastore/catalog/$entry"))).readLine()
      val cr = mapper.readValue(str, classOf[CatalogEntry])
      tables.put(cr.tableName, cr)
    }
  }

  tables
  }

  def saveChanges = {
    tableMap.values.foreach{ ce =>
      val fw = (new FileWriter(new File(s"./metastore/catalog/${ce.tableName}.mt")))
      val sw = new StringWriter()
      mapper.writeValue(sw, ce)
      fw.write(sw.toString)
      fw.close()
    }
  }

  def add(name: String, path: String, engine: String, t: String): Unit ={
    tableMap.put(name, CatalogEntry(name, path, engine, t))
    saveChanges
  }
}

object Catalog {
  val getInstance = new Catalog()
}