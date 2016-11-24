package gr.cslab.ece.ntua.musqle.tools

import java.io.File

import scala.collection.mutable.HashMap
import scala.io.Source

object ConfParser{
  private val confMap = {
    val map = new HashMap[String, String]()
    val dir = (new File(".")).getCanonicalPath()

    Source.fromFile(s"${dir}/conf/musqle.conf")
      .getLines()
      .filter(line => line != " ")
      .filter(line => !line.startsWith("#"))
      .map(line => line.split("="))
      .foreach { pair =>
        map.put(pair(0), pair(1))
      }

    map
  }

  def getConf(name: String) = {
    confMap.get(name)
  }
}
