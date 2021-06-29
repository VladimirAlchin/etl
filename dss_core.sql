
DROP TABLE IF EXISTS core.hub_suppliers ;
create table core.hub_suppliers(
    hub_sup_key SERIAL PRIMARY KEY,
    sat_sup_id int,
    source_system varchar(20),
    processed_dttm timestamp
);



DROP TABLE IF EXISTS core.sat_suppliers ;
create table core.sat_suppliers (
    sat_sup_id int4,
    SuppliersName varchar(25),
    Address varchar(40),
    Phone varchar(15),
    Balance numeric(15,2),
    Descr varchar(101)
);



DROP TABLE IF EXISTS core.hub_products ;
create table core.hub_products (
    hub_prod_key SERIAL PRIMARY KEY,
    sat_prod_id int,
    source_system varchar(20),
    processed_dttm timestamp

);


DROP TABLE IF EXISTS core.sat_products  ;
create table core.sat_products (
    sat_prod_id int4,
    ProductName varchar(55),
    ProductType varchar(25),
    ProductSize int4,
    RetailPrice numeric(15,2)
);


drop table if exists core.hub_orders;
create table core.hub_orders (
h_order_rk SERIAL PRIMARY KEY,
order_id int,
source_system varchar(20),
processed_dttm timestamp,

);

drop table if exists core.sat_orders;
create table core.sat_orders (
h_order_rk int,
order_date timestamp,
order_status varchar(1),
order_priority varchar(15),
clerk varchar(15),
source_system text,
valid_from_dttm timestamp,
valid_to_dttm timestamp,
processed_dttm timestamp
);

drop table if exists core.l_prod_supl;
create table core.l_prod_supl (
l_prod_supl_pk SERIAL PRIMARY KEY,
hub_sup_key int,
hub_prod_key int,
source_system varchar(20),
processed_dttm timestamp
);


drop table if exists core.l_prod_ord;
create table core.l_prod_ord (
l_prod_ord_pk SERIAL PRIMARY KEY,
hub_sup_key int,
hub_orders_key int,
source_system varchar(20),
processed_dttm timestamp
);
