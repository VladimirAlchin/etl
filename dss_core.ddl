

create table core.hub_suppliers (
    hub_sup_key SERIAL PRIMARY KEY,
    sat_sup_id int,
    source_system string,
    processed_dttm datetime
    UNIQUE(sat_sup_id)
);


create table core.sat_suppliers (
    sat_sup_id int4,
    SuppliersName varchar(25),
    Address varchar(40),
    Phone varchar(15),
    Balance numeric(15,2),
    Descr varchar(101)
);

create table core.hub_products (
    hub_prod_key SERIAL PRIMARY KEY,
    sat_prod_id int,
    source_system string,
    processed_dttm datetime
    UNIQUE(sat_sup_id)
);


create table core.sat_products (
    sat_prod_id int4,
    ProductName varchar(55),
    ProductType varchar(25),
    ProductSize int4,
    RetailPrice numeric(15,2)
);

drop table if exists core.h_orders;
create table core.h_orders (
h_order_rk SERIAL PRIMARY KEY,
order_id int,
source_system varchar(20),
processed_dttm timestamp,
UNIQUE(order_id)
);

drop table if exists core.s_orders;
create table core.s_orders (
h_order_rk int,
order_date timestamp,
order_status varchar(1),
order_priority varchar(15),
clerk varchar(15)
);

