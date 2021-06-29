
insert into
hub_products(sat_prod_id, processed_dttm, source_system)
select    distinct productid, now() as tim, 'postgresql' from stage.products where productid not in (select sat_prod_id from hub_products)

insert into
sat_products
select  * from stage.products where productid in (select sat_prod_id from hub_products)


insert into
core.hub_suppliers(sat_sup_id , processed_dttm, source_system)
select distinct supplierid, now() as tim, 'postgresql' from stage.suppliers where supplierid not in (select sat_sup_id from core.hub_suppliers)

insert into
sat_suppliers
select  * from stage.suppliers where supplierid in (select sat_sup_id from hub_suppliers)


insert into
core.hub_orders(order_id , processed_dttm, source_system)
select distinct orderid, now() as tim, 'postgresql' from stage.orders where orderid not in (select order_id from core.hub_orders)

insert into
sat_orders
select  * , '4udo' from stage.orders where orderid in (select orderid from hub_orders)