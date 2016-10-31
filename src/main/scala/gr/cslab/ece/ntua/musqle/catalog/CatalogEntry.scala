package gr.cslab.ece.ntua.musqle.catalog

import com.fasterxml.jackson.annotation.JsonProperty

case class CatalogEntry(@JsonProperty("tableName") val tableName: String,
                        @JsonProperty("tablePath")val tablePath: String,
                        @JsonProperty("engine") val engine: String,
                        @JsonProperty("format") val format: String)