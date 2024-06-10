
%macro m_ecoa_process(do_tasks=ALL);

	%if &do_tasks. = ALL or &do_tasks. = DQI %then %do;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - convert SFMC to DQI - opportunity + disposition data  
		+---------------------------------------------------------------------------------------------*/
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute( 
			update &c_s_table_name.
			set source_team = 'DQI'
			where oppty_evnt_id > 9000000000000000
			and source_team <> 'DQI' 
		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 		  

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute( 
			update &c_s_table_opp.  
			set source_team = 'DQI'
			where oppty_evnt_id > 9000000000000000
			and source_team <> 'DQI'
		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 		  
		  
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute( 
			update &c_s_table_disp.  
			set source_team = 'DQI'
			where oppty_evnt_id > 9000000000000000
			and source_team <> 'DQI'
		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute( 
			update &c_s_table_files.  
			set source_team = 'DQI'
			where source_new_file like any (

				select distinct '%'||substring(source_opp_file from (POSITION('.' IN source_opp_file)+1),8)||'%'
				from &c_s_table_opp. a
				where source_team = 'DQI' 
				union
				select distinct '%'||substring(source_opp_file from (POSITION('.' IN source_opp_file)+1),8)||'%'
				from &c_s_table_disp. a
				where source_team = 'DQI'
			) 
			and source_team = 'SFMC'
		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 
		
	%end;
	
	%if &do_tasks. = ALL or &do_tasks. = TEST %then %do;	
	
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			update &c_s_table_files. set execution_sftp = 'TEST' where execution_date = current_date - &execution_date.

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 
		
		%let c_s_email_subject = EA ECOA - TEST Complete Step &ecoa_test.;
		
		%m_abend_handler(abend_report=%str(EA ECOA - TEST Complete),
		abend_message=%str(EA ECOA - TEST Complete));	
		
	%end;
	
	%if &do_tasks. = NOABEND %then %do;	
	
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			update &c_s_table_files. set execution_sftp = 'TEST' where execution_date = current_date - &execution_date.

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 	
		
	%end;	
	
%mend m_ecoa_process;