/* 
Stored Procedure: Load Bronze Layer (Source -> Bronze) 
cmd: exec bronze.load_bronze
*/

create or alter procedure bronze.load_bronze as   -- load_bronze is stored rpocedure for loading data into bronze layer
begin
	
	declare @start_time datetime, @end_time datetime; --to track etl duration
	begin try
		print 'loading bronze layer';
		print '-----------------------------------------';

		print 'loading CRM tables';


		set @start_time = getdate();
		truncate table bronze.crm_cust_info -- deleting all existing content from the table
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Neeru Bhatia\Desktop\AAKRISHT\COMP SCIENCE\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with(  --telling sql server how to handle the file
			firstrow = 2, --start reading data from 2nd row
			fieldterminator = ',', -- file delimiter
			tablock -- table is locked while data is being loaded into it
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'; 

		print'-----------------------------------';

		set @start_time = getdate();
		truncate table bronze.crm_prd_info 
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Neeru Bhatia\Desktop\AAKRISHT\COMP SCIENCE\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with(  
			firstrow = 2, 
			fieldterminator = ',', 
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'; 
		print'-----------------------------------';

		set @start_time = getdate();
		truncate table bronze.crm_sales_details 
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Neeru Bhatia\Desktop\AAKRISHT\COMP SCIENCE\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with(  
			firstrow = 2, 
			fieldterminator = ',', 
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'; 
		print'-----------------------------------';

		
		print 'loading ERP tables';

		set @start_time = getdate();
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Neeru Bhatia\Desktop\AAKRISHT\COMP SCIENCE\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		with(  
			firstrow = 2, 
			fieldterminator = ',', 
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'; 
		print'-----------------------------------';

		set @start_time = getdate();
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Neeru Bhatia\Desktop\AAKRISHT\COMP SCIENCE\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		with(  
			firstrow = 2, 
			fieldterminator = ',', 
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'; 
		print'-----------------------------------';

		set @start_time = getdate();
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Neeru Bhatia\Desktop\AAKRISHT\COMP SCIENCE\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		with(  
			firstrow = 2, 
			fieldterminator = ',', 
			tablock
		);
		set @end_time = getdate();
		print '>>Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'; 
		print'-----------------------------------';
	end try
	begin catch
		print 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + error_message();
		print 'Error No.: ' + cast(error_number() as nvarchar);
		print 'Error State: ' + cast(error_state() as nvarchar);


	end catch
end


