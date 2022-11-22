create schema if not exists myschema;


create table if not exists myschema.tableA (
	id int
	, Contract_id int
	, Executed_by int

) DISTSTYLE AUTO SORTKEY(Contract_id);


create table if not exists myschema.tableB (
	id int
	, Contract_id int
	, Executed_by int

) DISTSTYLE AUTO SORTKEY(Contract_id);


create table if not exists myschema.tableC (
	id int
	, Contract_id int
	, Executed_by int

) DISTSTYLE AUTO SORTKEY(Contract_id);


create or replace view myschema.v_myView as
	select * from mySchema.tableA;
