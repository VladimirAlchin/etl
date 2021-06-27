

create table dv.hub_suppliers (
    hub_sup_key SERIAL PRIMARY KEY,
    sat_sup_id int,
    source_system string,
    processed_dttm datetime
    UNIQUE(sat_sup_id)
);


create table dv.sat_suppliers (
    sat_sup_id int4,
    SuppliersName varchar(25),
    Address varchar(40),
    Phone varchar(15),
    Balance numeric(15,2),
    Descr varchar(101)
--    (ключ FK для провеки корректности работы рекомендуется)
);

create table dv.hub_products (
    hub_prod_key SERIAL PRIMARY KEY,
    sat_prod_id int,
    source_system string,
    processed_dttm datetime
    UNIQUE(sat_sup_id)
);


create table dv.sat_products (
    sat_prod_id int4,
    ProductName varchar(55),
    ProductType varchar(25),
    ProductSize int4,
    RetailPrice numeric(15,2)
--    (ключ FK для провеки корректности работы рекомендуется)
);

