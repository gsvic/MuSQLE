package gr.cslab.ntua.musqle

/**
  * Created by Victor on 23/7/2016.
  */

object Queries {


  val join1 =
    """
      |select c_name, n_name
      |from customer, nation
      |where c_nationkey = n_nationkey
      |""".stripMargin


  val join2 =
    """
      |select *
      |from customer, orders
      |where c_custkey = o_orderkey
    """.stripMargin


  val join3 =
    """
      |select *
      |from orders, customer, nation
      |where o_custkey = c_custkey
      |and c_nationkey = n_nationkey
    """.stripMargin

  val join4 =
    """
      |select *
      |from lineitem, orders, customer, nation
      |where l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
    """.stripMargin

  val join5 =
    """
      |select *
      |from part, lineitem, orders, customer, nation
      |where p_partkey = l_partkey
      |and l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
    """.stripMargin

  val join6 =
    """
      |select *
      |from customer, nation, orders, lineitem, part, partsupp
      |where c_nationkey = n_nationkey
      |and c_custkey = o_custkey
      |and l_orderkey = o_orderkey
      |and l_partkey = p_partkey
      |and p_partkey = ps_partkey
    """.stripMargin

  val join7 =
    """
      |select * from customer, nation, orders, lineitem, part, partsupp, supplier
      |where c_nationkey = n_nationkey
      |and c_custkey = o_orderkey
      |and l_orderkey = o_orderkey
      |and l_partkey = p_partkey
      |and p_partkey = ps_partkey
      |and s_suppkey = ps_suppkey
    """.stripMargin

  val join8 =
    """
      |select c_name, r_name, n_name
      |from customer, nation, region
      |where c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |""".stripMargin

  val join9 =
    """
      |select p_name, p_brand, ps_supplycost
      |from part, partsupp
      |where p_partkey = ps_partkey
      |""".stripMargin


  val joinWithFilter1 =
    """
      |select *
      |from customer, nation, region,orders
      |where c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and r_name = 'EUROPE'
      |and c_custkey = o_custkey
      |and c_custkey < 200
      |""".stripMargin

  val joinWithFilter2 =
    """
      |select *
      |from lineitem, orders, customer, nation, region
      |where l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and r_name = 'AFRICA'
    """.stripMargin

  val joinWithFilter3 =
    """
      |select c_name
      |from customer, orders, lineitem, nation
      |where c_custkey = o_custkey
      |and o_orderkey = l_orderkey
      |and c_nationkey = n_nationkey
      |and c_nationkey = 8
    """.stripMargin

  val joinWithFilter4 =
    """
      |select *
      |from customer, nation, region,orders
      |where c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and r_name = 'EUROPE'
      |and c_custkey = o_custkey
      |""".stripMargin

package gr.cslab.ntua.musqle

/**
  * Created by Victor on 23/7/2016.
  */

val q0 =
    """
      |select c_name
      |from customer, nation
      |where c_nationkey = n_nationkey
      |""".stripMargin


  val q1 =
    """
      |select c_name
      |from customer, orders
      |where c_custkey = o_orderkey
    """.stripMargin


  val q2 =
    """
      |select c_name
      |from orders, customer, nation
      |where o_custkey = c_custkey
      |and c_nationkey = n_nationkey
    """.stripMargin

  val q3 =
    """
      |select l_linenumber
      |from lineitem, orders, customer, nation
      |where l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
    """.stripMargin

  val q4 =
    """
      |select l_linenumber
      |from part, lineitem, orders, customer, nation
      |where p_partkey = l_partkey
      |and l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
    """.stripMargin

  val q5 =
    """
      |select o_orderdate
      |from customer, nation, orders, lineitem, part, partsupp
      |where c_nationkey = n_nationkey
      |and c_custkey = o_custkey
      |and l_orderkey = o_orderkey
      |and l_partkey = p_partkey
      |and p_partkey = ps_partkey
    """.stripMargin

  val q6 =
    """
      |select ps_availqty
      |from customer, nation, orders, lineitem, part, partsupp, supplier
      |where c_nationkey = n_nationkey
      |and c_custkey = o_orderkey
      |and l_orderkey = o_orderkey
      |and l_partkey = p_partkey
      |and p_partkey = ps_partkey
      |and s_suppkey = ps_suppkey
    """.stripMargin

  val q7 =
    """
      |select c_name
      |from customer, nation, region
      |where c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |""".stripMargin

  val q8 =
    """
      |select p_name
      |from part, partsupp
      |where p_partkey = ps_partkey
      |""".stripMargin


  val q9 =
    """
      |select r_name
      |from customer, nation, region,orders
      |where c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and r_name = 'EUROPE'
      |and c_custkey = o_custkey
      |and c_custkey < 200
      |""".stripMargin

  val q10 =
    """
      |select l_discount
      |from lineitem, orders, customer, nation, region
      |where l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and r_name = 'AFRICA'
    """.stripMargin

  val q11 =
    """
      |select c_name
      |from customer, orders, lineitem, nation
      |where c_custkey = o_custkey
      |and o_orderkey = l_orderkey
      |and c_nationkey = n_nationkey
      |and c_nationkey = 8
    """.stripMargin

  val q12 =
    """
      |select c_name
      |from customer, nation, region,orders
      |where c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and r_name = 'EUROPE'
      |and c_custkey = o_custkey
      |""".stripMargin

  //Possible Data Movement from HDFS to Postgres
  val q13 =
    """
      |select o_orderkey
      |from orders, customer, nation, region
      |where o_custkey = c_custkey
      |and c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and o_orderkey < 100
    """.stripMargin

  val q14 =
    """
      |select l_partkey
      |from lineitem, orders, partsupp, part
      |where l_orderkey = o_orderkey
      |and l_partkey = ps_partkey
      |and p_partkey = ps_partkey
      |and l_orderkey = 5
    """.stripMargin

  val q15 =
    """
      |select o_orderdate
      |from partsupp, part, lineitem, orders, customer, nation
      |where p_partkey = ps_partkey
      |and p_partkey = l_partkey
      |and l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
      |and p_retailprice < 2000
      |and n_name = 'AFRICA'
    """.stripMargin

  val q16 =
    """
      |select n_nationkey
      |from partsupp, supplier, nation
      |where ps_suppkey = s_suppkey
      |and s_nationkey = n_nationkey
      |and n_name = 'EUROPE'
    """.stripMargin

  val q17 =
    """
      |select p_name
      |from part, partsupp, supplier
      |where p_partkey = ps_partkey
      |and ps_suppkey = s_suppkey
      |and p_partkey = 3
      |""".stripMargin
