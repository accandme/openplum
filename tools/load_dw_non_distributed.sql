INSERT INTO dim_customers (id, region, nation, name)
SELECT DISTINCT c.c_custkey, r.r_name, n.n_name, c.c_name
FROM customer c JOIN nation n ON c_nationkey = n_nationkey 
     JOIN region r ON r.r_regionkey = n.n_nationkey
;

INSERT INTO dim_suppliers (id, region, nation, name)
SELECT DISTINCT s.s_suppkey, r.r_name, n.n_name, s.s_name
FROM supplier s JOIN nation n ON s_nationkey = n_nationkey 
     JOIN region r ON r.r_regionkey = n.n_nationkey
;

INSERT INTO dim_parts (id, mfgr, brand, type, name)
SELECT DISTINCT p_partkey, p_mfgr, p_brand, p_type, p_name
FROM part
;

INSERT INTO dim_time (id, year, month, day)
SELECT DISTINCT o_orderdate, EXTRACT(YEAR FROM o_orderdate), EXTRACT(MONTH FROM o_orderdate), EXTRACT(DAY FROM o_orderdate)
FROM orders
;

INSERT INTO fact_sales (part_id, customer_id, supplier_id, time_id, quantity, extendedprice)
SELECT l.l_partkey, o.o_custkey, l.l_suppkey, o.o_orderdate, SUM(l.l_quantity), SUM(l.l_extendedprice)
FROM lineitem l JOIN orders o ON l.l_orderkey = o.o_orderkey
GROUP BY l.l_partkey, o.o_custkey, l.l_suppkey, o.o_orderdate;

