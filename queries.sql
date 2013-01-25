/* Q1
 * CHANGED: lineitem.l_shipdate <= date '1998-12-01' - interval '90' day
 * TO: lineitem.l_shipdate <= '1998-09-01'
 * REASON: General SQL Parser complains about "date" keyword, PostgreSQL complains if keyword "date" is omitted.
 */
select lineitem.l_returnflag, lineitem.l_linestatus, sum(lineitem.l_quantity) as sum_qty, sum(lineitem.l_extendedprice) as sum_base_price, sum(lineitem.l_extendedprice*(1-lineitem.l_discount)) as sum_disc_price, sum(lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax)) as sum_charge, avg(lineitem.l_quantity) as avg_qty, avg(lineitem.l_extendedprice) as avg_price, avg(lineitem.l_discount) as avg_disc, count(*) as count_order from lineitem where lineitem.l_shipdate <= '1998-09-01' group by lineitem.l_returnflag, lineitem.l_linestatus order by lineitem.l_returnflag, lineitem.l_linestatus

/* Q2
 * OMITTED
 * REASON: Correlated.
 */

/* Q3
 * CHANGED: orders.o_orderdate < date '1995-03-15' and lineitem.l_shipdate > date '1995-03-15'
 * TO: orders.o_orderdate < '1995-03-15' and lineitem.l_shipdate > '1995-03-15'
 * REASON: General SQL Parser complains about "date" keyword.
 */
select lineitem.l_orderkey, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue, orders.o_orderdate, orders.o_shippriority from customer, orders, lineitem where customer.c_mktsegment = 'BUILDING' and customer.c_custkey = orders.o_custkey and lineitem.l_orderkey = orders.o_orderkey and orders.o_orderdate < '1995-03-15' and lineitem.l_shipdate > '1995-03-15' group by lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority order by revenue desc, orders.o_orderdate limit 10

/* Q4
 * OMITTED
 * REASON: Correlated.
 */

/* Q5
 * CHANGED: orders.o_orderdate >= date '1994-01-01' and orders.o_orderdate < date '1994-01-01' + interval '1' year
 * TO: orders.o_orderdate >= '1994-01-01' and orders.o_orderdate < '1995-01-01'
 * REASON: General SQL Parser complains about "date" keyword, PostgreSQL complains if keyword "date" is omitted in second clause.
 */
select nation.n_name, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where customer.c_custkey = orders.o_custkey and lineitem.l_orderkey = orders.o_orderkey and lineitem.l_suppkey = supplier.s_suppkey and customer.c_nationkey = supplier.s_nationkey and supplier.s_nationkey = nation.n_nationkey and nation.n_regionkey = region.r_regionkey and region.r_name = 'AFRICA' and orders.o_orderdate >= '1994-01-01' and orders.o_orderdate < '1995-01-01' group by nation.n_name order by revenue desc

/* Q6
 * CHANGED: lineitem.l_shipdate >= date '1994-01-01' and lineitem.l_shipdate < date '1994-01-01' + interval '1' year
 * TO: lineitem.l_shipdate >= '1994-01-01' and lineitem.l_shipdate < '1995-01-01'
 * REASON: General SQL Parser complains about "date" keyword, PostgreSQL complains if keyword "date" is omitted in second clause.
 */
select sum(lineitem.l_extendedprice*lineitem.l_discount) as revenue from lineitem where lineitem.l_shipdate >= '1994-01-01' and lineitem.l_shipdate < '1995-01-01' and lineitem.l_discount between 0.06 - 0.01 and 0.06 + 0.01 and lineitem.l_quantity < 24

/* Q7
 * CHANGED: [NATION1] is 'GERMANY', [NATION2] is 'FRANCE'
 * TO: [NATION1] is 'ARGENTINA', [NATION2] is 'ALGERIA'
 * REASON: No results for scale 0.001.
 * CHANGED: ((n1.n_name = 'ARGENTINA' AND n2.n_name = 'ALGERIA') OR (n1.n_name = 'ALGERIA' AND n2.n_name = 'ARGENTINA'))
 * TO: n1.n_name = 'ARGENTINA' AND n2.n_name = 'ALGERIA'
 * REASON: OR operator not supported.
 * CHANGED: lineitem.l_shipdate between date '1995-01-01' and date '1996-12-31'
 * TO: lineitem.l_shipdate between '1995-01-01' and '1996-12-31'
 * REASON: General SQL Parser complains about the "date" keyword.
 */
SELECT shipping.supp_nation, shipping.cust_nation, shipping.l_year, SUM(shipping.volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM lineitem.l_shipdate) AS l_year, lineitem.l_extendedprice * (1 - lineitem.l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE supplier.s_suppkey = lineitem.l_suppkey AND orders.o_orderkey = lineitem.l_orderkey AND customer.c_custkey = orders.o_custkey AND supplier.s_nationkey = n1.n_nationkey AND customer.c_nationkey = n2.n_nationkey AND n1.n_name = 'ARGENTINA' AND n2.n_name = 'ALGERIA' AND lineitem.l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') shipping GROUP BY shipping.supp_nation, shipping.cust_nation, shipping.l_year ORDER BY shipping.supp_nation, shipping.cust_nation, shipping.l_year

/* Q8
 * CHANGED: sum(case when all_nations.nation = 'BRAZIL' then all_nations.volume else 0 end) / sum(all_nations.volume) as mkt_share
 * TO: sum(all_nations.volume) as total_volume
 * REASON: CASE operator not supported.
 * CHANGED: orders.o_orderdate between date '1995-01-01' and date '1996-12-31'
 * TO: o_orderdate between '1995-01-01' and '1996-12-31'
 * REASON: General SQL Parser complains about the "date" keyword.
 */
select all_nations.o_year, sum(all_nations.volume) as total_volume from ( select extract(year from orders.o_orderdate) as o_year, lineitem.l_extendedprice * (1-lineitem.l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where part.p_partkey = lineitem.l_partkey and supplier.s_suppkey = lineitem.l_suppkey and lineitem.l_orderkey = orders.o_orderkey and orders.o_custkey = customer.c_custkey and customer.c_nationkey = n1.n_nationkey and n1.n_regionkey = region.r_regionkey and region.r_name = 'AMERICA' and supplier.s_nationkey = n2.n_nationkey and orders.o_orderdate between '1995-01-01' and '1996-12-31' and part.p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by all_nations.o_year order by all_nations.o_year

/* Q9
 */
select profit.nation, profit.o_year, sum(profit.amount) as sum_profit from ( select nation.n_name as nation, extract(year from orders.o_orderdate) as o_year, lineitem.l_extendedprice * (1 - lineitem.l_discount) - partsupp.ps_supplycost * lineitem.l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where supplier.s_suppkey = lineitem.l_suppkey and partsupp.ps_suppkey = lineitem.l_suppkey and partsupp.ps_partkey = lineitem.l_partkey and part.p_partkey = lineitem.l_partkey and orders.o_orderkey = lineitem.l_orderkey and supplier.s_nationkey = nation.n_nationkey and part.p_name like '%green%' ) as profit group by profit.nation, profit.o_year order by profit.nation, profit.o_year desc

/* Q10
 * CHANGED: orders.o_orderdate >= date '1993-10-01' and orders.o_orderdate < date '1993-10-01' + interval '3' month
 * TO: orders.o_orderdate >= '1993-10-01' and orders.o_orderdate < '1994-01-01'
 * REASON: General SQL Parser complains about the "date" keyword, PostgreSQL complains if keyword "date" is omitted in second clause.
 */
select customer.c_custkey, customer.c_name, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue, customer.c_acctbal, nation.n_name, customer.c_address, customer.c_phone, customer.c_comment from customer, orders, lineitem, nation where customer.c_custkey = orders.o_custkey and lineitem.l_orderkey = orders.o_orderkey and orders.o_orderdate >= '1993-10-01' and orders.o_orderdate < '1994-01-01' and lineitem.l_returnflag = 'R' and customer.c_nationkey = nation.n_nationkey group by customer.c_custkey, customer.c_name, customer.c_acctbal, customer.c_phone, nation.n_name, customer.c_address, customer.c_comment order by revenue desc limit 20

/* Q11
 * CHANGED: [NATION] is 'GERMANY'
 * TO: [NATION] is 'ARGENTINA'
 * REASON: No results for scale 0.001.
 */
select partsupp.ps_partkey, sum(partsupp.ps_supplycost * partsupp.ps_availqty) as value from partsupp, supplier, nation where partsupp.ps_suppkey = supplier.s_suppkey and supplier.s_nationkey = nation.n_nationkey and nation.n_name = 'ARGENTINA' group by partsupp.ps_partkey having sum(partsupp.ps_supplycost * partsupp.ps_availqty) > ( select sum(partsupp.ps_supplycost * partsupp.ps_availqty) * 0.0001 from partsupp, supplier, nation where partsupp.ps_suppkey = supplier.s_suppkey and supplier.s_nationkey = nation.n_nationkey and nation.n_name = 'ARGENTINA' ) order by value desc

/* Q12
 * OMITTED
 * REASON: Contains an IN list.
 */

/* Q13
 * CHANGED: customer LEFT OUTER JOIN orders on [CONDITIONS]
 * TO: customer, orders WHERE [CONDITIONS]
 * REASON: LEFT OUTER JOIN not supported.
 * CHANGED: (SELECT customer.c_custkey, count(orders.o_orderkey) [...]) AS c_orders (c_custkey, c_count)
 * TO: (SELECT customer.c_custkey AS c_custkey, count(orders.o_orderkey) AS c_count [...]) AS c_orders
 * REASON: Syntax not supported.
 */
select c_orders.c_count, count(*) as custdist from ( select customer.c_custkey as c_custkey, count(orders.o_orderkey) as c_count from customer, orders where customer.c_custkey = orders.o_custkey and orders.o_comment not like '%special%requests%' group by customer.c_custkey )as c_orders group by c_orders.c_count order by custdist desc, c_orders.c_count desc

/* Q14
 * CHANGED: select 100.00 * sum(case when part.p_type like 'PROMO%' [...]) / [TOTAL] as promo_revenue
 * TO: select [TOTAL] as promo_revenue
 *     (added in WHERE) part.p_type like 'PROMO%'
 * REASON: CASE clause not supported.
 * CHANGED: lineitem.l_shipdate >= date '1995-09-01' and lineitem.l_shipdate < date '1995-09-01' + interval '1' month
 * TO: lineitem.l_shipdate >= '1995-09-01' and lineitem.l_shipdate < '1995-10-01'
 * REASON: General SQL Parser complains about "date" keyword, PostgreSQL complains if keyword "date" is omitted in second clause.
 */
select sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as promo_revenue from lineitem, part where lineitem.l_partkey = part.p_partkey and lineitem.l_shipdate >= '1995-09-01' and lineitem.l_shipdate < '1995-10-01' and part.p_type like 'PROMO%'

/* Q15
 * OMITTED
 * REASON: Creating VIEWs is not supported.
 */

/* Q16
 * OMITTED
 * REASON: Contains an IN list.
 */

/* Q17
 * OMITTED
 * REASON: Correlated.
 */

/* Q18
 * CHANGED: [QUANTITY] is 300
 * TO: [QUANTITY] is 250
 * REASON: No results for scale 0.001.
 */
select customer.c_name, customer.c_custkey, orders.o_orderkey, orders.o_orderdate, orders.o_totalprice, sum(lineitem.l_quantity) from customer, orders, lineitem where orders.o_orderkey in ( select lineitem.l_orderkey from lineitem group by lineitem.l_orderkey having sum(lineitem.l_quantity) > 250 ) and customer.c_custkey = orders.o_custkey and orders.o_orderkey = lineitem.l_orderkey group by customer.c_name, customer.c_custkey, orders.o_orderkey, orders.o_orderdate, orders.o_totalprice order by orders.o_totalprice desc, orders.o_orderdate limit 100

/* Q19
 * OMITTED
 * REASON: Contains IN lists.
 */

/* Q20
 * OMITTED
 * REASON: Correlated.
 */

/* Q21
 * OMITTED
 * REASON: Correlated.
 */

/* Q22
 * OMITTED
 * REASON: Contains IN lists and SUBSTRING function.
 */