package gr.cslab.ece.ntua.musqle.plan

import gr.cslab.ece.ntua.musqle.plan.hypergraph.{AbstractPlan, DPJoinPlan}
import gr.cslab.ece.ntua.musqle.plan.spark.MuSQLEMove
import org.apache.log4j.Logger

import scala.collection.mutable

/**
  * Created by vic on 9/3/2017.
  */
class Cache {
  val logger = Logger.getLogger(this.getClass)
  val results = mutable.HashMap[AbstractPlan, (DPJoinPlan, String)]()
  var resultIdx = 0

  load()

  def hit(abstractPlan: AbstractPlan): DPJoinPlan = {
    val found = results.contains(abstractPlan)

    logger.debug(s"Searching cache for ${abstractPlan}. Found: ${found}")

    if (found) {
      val cachedResult = results.get(abstractPlan).get
      logger.info(s"Result ${abstractPlan} found in cache! [${cachedResult._2}]")
    }
    null
  }

  def cacheResult(result: DPJoinPlan): Unit = {
    val name = s"cachedResult${resultIdx}"
    logger.info(s"Caching ${result} as $name")
    results.put(result.toAbstract(), (result, name))

    resultIdx += 1
  }

  def persistCache(): Unit = {
    results.foreach{ result =>
      logger.info(s"Persisting result ${result}")
      val plan = result._2._1
      plan match {
        case move: MuSQLEMove => {
          move.engine.rename(move.tmpName, result._2._2)
        }
        case _ => {}
      }
    }
  }

  def load(): Unit = {
    logger.info("Loading cache metadata")
  }

}
