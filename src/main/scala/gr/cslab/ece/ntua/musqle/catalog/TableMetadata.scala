package gr.cslab.ece.ntua.musqle.catalog

import gr.cslab.ece.ntua.musqle.engine.{Engine, Json, Postgres, Spark}

/**
  * Created by vic on 19/10/2016.
  */
class TableMetadata(val name: String, val path: String, engine: Engine)
