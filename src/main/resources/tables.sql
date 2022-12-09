-- auto-generated definition
create table products
(
    product_id   integer not null,
    product_name varchar(50),
    product_desc varchar(100),
    unit         integer,
    price        numeric
);

alter table products
    owner to postgres;