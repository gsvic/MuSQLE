#MuSQLE
##Multi-Engine SQL Query Execution Over Spark SQL

#Instructions
##Installation
1. Clone the project<br>
`git clone https://github.com/gsvic/MuSQLE.git`
2. Build it with maven<br>
`mvn install`
3. Start a Spark Shell by including the MuSQLE's jar<br>
`spark-shell --jars MuSQLE-1.0-SNAPSHOT-jar-with-dependencies.jar`
4. Create a MuSQLEContext instance and run an example TPC-DS query
```scala
import gr.cslab.ece.ntua.musqle.MuSQLEContext
import gr.cslab.ece.ntua.musqle.benchmarks.tpcds.FixedQueries

val mc = new MuSQLEContext(spark)
val q = FixedQueries.queries(0)._2

val mq = mc.query(q)

/** See the execution plan */
mq.explain
```
which results into the following execution plan:
```
Join [result9, result5] on Set(1) , Engine: [SparkSQL], Cost: [2.6401074374777824], [result10]
	Move [result3] from PostgreSQL to SparkSQL, Cost 0.07390589783733907 [result9]
		*Scan MuSQLEScan: date_dim, Engine: [PostgreSQL], Cost: [0.07390589783733907], [result3] 
	Join [result4, result1] on Set(2) , Engine: [SparkSQL], Cost: [2.5662015396404434], [result5]
		Move [result2] from PostgreSQL to SparkSQL, Cost 2.5662015396404434 [result4]
			*Scan MuSQLEScan: store_sales, Engine: [PostgreSQL], Cost: [2.5662015396404434], [result2] 
		*Scan MuSQLEScan: item, Engine: [SparkSQL], Cost: [0.0], [result1] 
```
##Adding tables to Catalog
1. Create a folder named 'catalog' in the project's parent directory.
2. A table can be defined as a file with .mt extension. Two example files are provided:<br>


PostgreSQL
```javascript
{"tableName":"date_dim","tablePath":"date_dim","engine":"postgres"}
```
Parquet file in HDFS
```javascript
{"tableName":"item","tablePath":"hdfs://master:9000/tpcds/1/item","engine":"spark","format":"parquet"}
```
