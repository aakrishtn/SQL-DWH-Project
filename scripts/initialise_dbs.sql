/* creating a new db; deleting it and recreating if it already exists */


USE master;
GO

if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate; --forcing into single_user mode i.e. only i can access; immediately kills all other exisitng connections and rolls back uncommitted transactions
	drop database DataWarehouse;
end;
go

create database DataWarehouse;
GO
use DataWarehouse;
GO

-- CREATING SCHEMAS FOR EACH LAYER
CREATE SCHEMA bronze;
GO --separator; tells to first execute above cmd completely b4 going onto the nxt one

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


