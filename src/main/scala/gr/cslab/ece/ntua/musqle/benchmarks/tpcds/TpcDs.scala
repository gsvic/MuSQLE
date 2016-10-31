package gr.cslab.ece.ntua.musqle.benchmarks.tpcds

import com.databricks.spark.sql.perf.tpcds.Tables
import gr.cslab.ece.ntua.musqle.catalog.Catalog
import org.apache.spark.sql.SparkSession

/**
  * Created by vic on 24/10/2016.
  */
object TpcDs {
  def dataGen(spark: SparkSession, dsgenDir: String, outPath: String,scaleFactor: Int = 1): Unit ={
    val catalog = Catalog.getInstance
    val tables = new Tables(spark.sqlContext, dsgenDir, scaleFactor)
    tables.genData(outPath, "parquet", true, false, false, false, false)
    tables.tables.foreach{table =>
      catalog.add(table.name, s"$outPath/${table.name}", "spark", "parquet")
    }
  }

  def genCatalogFiles(spark: SparkSession, location: String): Unit ={
    val catalog = Catalog.getInstance
    val tables = (new Tables(spark.sqlContext, "", 1)).tables
    tables.foreach{table =>
      catalog.add(table.name, s"$location/${table.name}", "spark", "parquet")
    }
  }
}

object tpcds extends App{
  val spark = SparkSession.builder()
    .master("spark://master:7077").appName("MuSQLE").getOrCreate()
  TpcDs.genCatalogFiles(spark,"hdfs://master:9000/tpcds/1")
}