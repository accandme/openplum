DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_parts;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_suppliers;
DROP TABLE IF EXISTS dim_time;
DROP VIEW IF EXISTS view_fact_sales;

CREATE TABLE fact_sales (
	part_id INTEGER NOT NULL,
	customer_id INTEGER NOT NULL,
	supplier_id INTEGER NOT NULL,
	time_id DATE NOT NULL,
	quantity INTEGER NOT NULL,
	extendedprice DECIMAL(15,2) NOT NULL,
	PRIMARY KEY (part_id, customer_id, supplier_id, time_id)
);

CREATE TABLE dim_parts (
	id INTEGER NOT NULL,
	mfgr CHAR(25) NOT NULL,
	brand CHAR(10) NOT NULL,
	name VARCHAR(55),
	PRIMARY KEY (id)
);

CREATE TABLE dim_customers (
	id INTEGER NOT NULL,
	region CHAR(25) NOT NULL,
	nation CHAR(25) NOT NULL,
	name VARCHAR(25) NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE dim_suppliers (
	id INTEGER NOT NULL,
	region CHAR(25) NOT NULL,
	nation CHAR(25) NOT NULL,
	name VARCHAR(25) NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE dim_time (
	id DATE NOT NULL,
	year CHAR(4) NOT NULL,
	month CHAR(2) NOT NULL,
	day CHAR(2) NOT NULL,
	PRIMARY KEY (id)
);

CREATE VIEW view_fact_sales AS
SELECT p.mfgr as part_mfgr, 
	   p.brand as part_brand, 
	   p.name as part_name, 
	   c.region as customer_region,
	   c.nation as customer_nation, 
	   c.name as customer_name, 
	   s.region as supplier_region, 
	   s.nation as supplier_nation, 
	   s.name as supplier_name,
	   t.year as time_year,
	   t.month as time_month, 
	   t.day as time_day, 
	   f.quantity, 
	   f.extendedprice
FROM dim_parts p, dim_customers c, dim_suppliers s, dim_time t, fact_sales f 
WHERE f.part_id = p.id 
  AND f.customer_id = c.id 
  AND f.supplier_id = s.id 
  AND f.time_id = t.id;