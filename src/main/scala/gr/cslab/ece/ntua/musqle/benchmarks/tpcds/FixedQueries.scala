package gr.cslab.ece.ntua.musqle.benchmarks.tpcds

/**
  * Created by vic on 22/11/2016.
  */
object FixedQueries {
  val queries = Seq(
    ("q3", """
             | SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
             | FROM  date_dim dt, store_sales, item
             | WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
             |   AND store_sales.ss_item_sk = item.i_item_sk
             |   AND item.i_manufact_id = 128
             |   AND dt.d_moy=11
             | GROUP BY dt.d_year, item.i_brand, item.i_brand_id
             | ORDER BY dt.d_year, sum_agg desc, brand_id
             | LIMIT 100
           """.stripMargin),

    ("q12", """
              | select
              |  i_item_desc, i_category, i_class, i_current_price,
              |  sum(ws_ext_sales_price) as itemrevenue
              | from
              |	web_sales, item, date_dim
              | where
              |	ws_item_sk = i_item_sk
              |  	and i_category in ('Sports', 'Books', 'Home')
              |  	and ws_sold_date_sk = d_date_sk
              |	and d_date between cast('1999-02-22' as date)
              |				and (cast('1999-02-22' as date) + interval 30 days)
              | group by
              |	i_item_id, i_item_desc, i_category, i_class, i_current_price
              | order by
              |	i_category, i_class, i_item_id, i_item_desc
              | LIMIT 100
            """.stripMargin),
    ("q17", """
              | select i_item_id
              |       ,i_item_desc
              |       ,s_state
              |       ,count(ss_quantity) as store_sales_quantitycount
              |       ,avg(ss_quantity) as store_sales_quantityave
              |       ,count(sr_return_quantity) as_store_returns_quantitycount
              |       ,avg(sr_return_quantity) as_store_returns_quantityave
              |       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
              | from store_sales, store_returns, catalog_sales, date_dim d1, date_dim d2, date_dim d3, store, item
              | where d1.d_quarter_name = '2001Q1'
              |   and d1.d_date_sk = ss_sold_date_sk
              |   and i_item_sk = ss_item_sk
              |   and s_store_sk = ss_store_sk
              |   and ss_customer_sk = sr_customer_sk
              |   and ss_item_sk = sr_item_sk
              |   and ss_ticket_number = sr_ticket_number
              |   and sr_returned_date_sk = d2.d_date_sk
              |   and d2.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
              |   and sr_customer_sk = cs_bill_customer_sk
              |   and sr_item_sk = cs_item_sk
              |   and cs_sold_date_sk = d3.d_date_sk
              |   and d3.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
              | group by i_item_id, i_item_desc, s_state
              | order by i_item_id, i_item_desc, s_state
              | limit 100
            """.stripMargin),
    ("q18", """
              | select i_item_id,
              |        ca_country,
              |        ca_state,
              |        ca_county,
              |        avg( cast(cs_quantity as decimal(12,2))) agg1,
              |        avg( cast(cs_list_price as decimal(12,2))) agg2,
              |        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
              |        avg( cast(cs_sales_price as decimal(12,2))) agg4,
              |        avg( cast(cs_net_profit as decimal(12,2))) agg5,
              |        avg( cast(c_birth_year as decimal(12,2))) agg6,
              |        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
              | from catalog_sales, customer_demographics cd1,
              |      customer_demographics cd2, customer, customer_address, date_dim, item
              | where cs_sold_date_sk = d_date_sk and
              |       cs_item_sk = i_item_sk and
              |       cs_bill_cdemo_sk = cd1.cd_demo_sk and
              |       cs_bill_customer_sk = c_customer_sk and
              |       cd1.cd_gender = 'F' and
              |       cd1.cd_education_status = 'Unknown' and
              |       c_current_cdemo_sk = cd2.cd_demo_sk and
              |       c_current_addr_sk = ca_address_sk and
              |       c_birth_month in (1,6,8,9,12,2) and
              |       d_year = 1998 and
              |       ca_state  in ('MS','IN','ND','OK','NM','VA','MS')
              | group by rollup (i_item_id, ca_country, ca_state, ca_county)
              | order by ca_country, ca_state, ca_county, i_item_id
              | LIMIT 100
            """.stripMargin),
    ("q20", """
              |select i_item_desc
              |       ,i_category
              |       ,i_class
              |       ,i_current_price
              |       ,sum(cs_ext_sales_price) as itemrevenue
              | from catalog_sales, item, date_dim
              | where cs_item_sk = i_item_sk
              |   and i_category in ('Sports', 'Books', 'Home')
              |   and cs_sold_date_sk = d_date_sk
              | and d_date between cast('1999-02-22' as date)
              | 				and (cast('1999-02-22' as date) + interval 30 days)
              | group by i_item_id, i_item_desc, i_category, i_class, i_current_price
              | order by i_category, i_class, i_item_id, i_item_desc
              | limit 100
            """.stripMargin),
    ("custom1", """select d1.d_date_sk, d4.d_date_sk from date_dim d1, date_dim d2, date_dim d3, date_dim d4
                  |where d1.d_date_sk = d2.d_date_sk
                  |and d2.d_date_sk = d3.d_date_sk
                  |and d3.d_date_sk = d4.d_date_sk""".stripMargin)
  )

}
