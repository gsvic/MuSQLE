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

  //Possible Data Movement from HDFS to Postgres
  val joinWithFilter5 =
    """
      |select o_orderkey
      |from orders, customer, nation, region
      |where o_custkey = c_custkey
      |and c_nationkey = n_nationkey
      |and n_regionkey = r_regionkey
      |and o_orderkey > 1
    """.stripMargin

  val joinWithFilter6 =
    """
      |select *
      |from lineitem, orders, partsupp, part
      |where l_orderkey = o_orderkey
      |and l_partkey = ps_partkey
      |and p_partkey = ps_partkey
      |and l_orderkey = 5
    """.stripMargin

  val joinWithFilter7 =
    """
      |select *
      |from partsupp, part, lineitem, orders, customer, nation
      |where p_partkey = ps_partkey
      |and p_partkey = l_partkey
      |and l_orderkey = o_orderkey
      |and o_custkey = c_custkey
      |and c_nationkey = n_nationkey
      |and c_custkey < 200
      |and n_name = 'AFRICA'
    """.stripMargin

  val joinWithFilter8 =
    """
      |select *
      |from lineitem, orders, partsupp, part
      |where l_orderkey = o_orderkey
      |and l_partkey = ps_partkey
      |and p_partkey = ps_partkey
      |and ps_supplycost < 20
    """.stripMargin

  val joinWithFilter9 =
    """
      |select p_name, s_name, s_address
      |from part, partsupp, supplier
      |where p_partkey = ps_partkey
      |and ps_suppkey = s_suppkey
      |and p_partkey = 3
      |""".stripMargin

}
