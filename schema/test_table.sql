create schema if not exists mySchema;


create table if not exists mySchema.tableA (
	id int
	, Contract_id int
	, Executed_by int

) DISTSTYLE AUTO SORTKEY(Contract_id);


create table if not exists mySchema.tableB (
	id int
	, Contract_id int
	, Executed_by int

) DISTSTYLE AUTO SORTKEY(Contract_id);


create table if not exists mySchema.tableC (
	id int
	, Contract_id int
	, Executed_by int

) DISTSTYLE AUTO SORTKEY(Contract_id);


create or replace view mySchema.v_myView as
	select * from mySchema.tableA;
