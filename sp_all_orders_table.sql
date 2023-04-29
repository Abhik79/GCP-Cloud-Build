CREATE OR REPLACE PROCEDURE `plated-field-383807.prod_dataset.sp_all_orders_table`()
BEGIN
DECLARE log_date string;
DECLARE custom_message string;
DECLARE v_project_name string;
DECLARE v_dataset_name string;
DECLARE v_table_name string;
DECLARE v_truncate_query string;
DECLARE final_query string;

    BEGIN
                SET custom_message ='Error during dynamic inserts';

		FOR source_tables IN
		    (
		      with base_tables AS
		        (
		        select 'plated-field-383807' as project_name,'manual_input' as dataset_name,'all_orders_table' as table_name union all
		        select 'plated-field-383807' as project_name,'manual_input' as dataset_name,'hockey_data' as table_name
		        )
		        select project_name,dataset_name,table_name from base_tables 
		        --where table_name='exec_dash_manual_input_actuals_wtd' --remove this line
		    )
		  DO
		
		    SET v_project_name = source_tables.project_name;
		    SET v_dataset_name = source_tables.dataset_name;
		    SET v_table_name = source_tables.table_name;
		    SET v_truncate_query = FORMAT("""TRUNCATE TABLE `%s.%s.%s`;""",v_project_name,'prod_dataset',v_table_name);
		
		    select v_truncate_query;
		
		    SET final_query = FORMAT("""INSERT into `%s.%s.%s` (%s) select %s from `%s.%s.%s`;""",
						v_project_name,
						'prod_dataset',
						v_table_name,
						(SELECT STRING_AGG( upper(column_name), ',')
		                from(
		                SELECT  column_name
		                FROM 
		                `plated-field-383807.prod_dataset.INFORMATION_SCHEMA.COLUMNS`
		                where table_name =v_table_name
		                order by ordinal_position )),
		
						(SELECT STRING_AGG( upper(column_name), ',')
		                from(
		                SELECT concat('SAFE_CAST(',column_name,' AS ',data_type, ') AS ',column_name) as column_name
		                FROM 
		                `plated-field-383807.prod_dataset.INFORMATION_SCHEMA.COLUMNS`
		                where table_name =v_table_name
		                order by ordinal_position )),
						v_project_name,
						v_dataset_name,
						v_table_name);
		
		    select final_query;
		
		 
		
		    EXECUTE IMMEDIATE v_truncate_query;    
		    EXECUTE IMMEDIATE final_query;

  END FOR;
                
    EXCEPTION WHEN ERROR THEN 

    SET LOG_DATE=( SELECT CAST(CURRENT_DATETIME() AS STRING) AS LOG_DATE); 
    
    
    EXECUTE IMMEDIATE "insert into `plated-field-383807.manual_input.error_log_table` (LOG_DATE, PROJECT_NAME, DATASET_NAME, PROCEDURE_NAME,ERROR_STATEMENT_TEXT, ERROR_MESSAGE, CUSTOM_MESSAGE) values (?,?,?,?,?,?,?)" USING LOG_DATE,'plated-field-383807','prod_dataset','sp_all_orders_table',@@error.statement_text,@@error.message,custom_message;
    
        END;
END;