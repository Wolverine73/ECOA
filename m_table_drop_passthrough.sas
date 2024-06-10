
/* SASDOC --------------------------------------------------------------------------------------
| MACRO:    m_table_drop_passthrough
|
| LOCATION: 
|
| USAGE:     
|
| PURPOSE:  
|
| LOGIC:      
|
| TABLES:    
|
| INPUT:    
|
| OUTPUT:   
|
+------------------------------------------------------------------------------
| HISTORY:  31JAN2013 - DQI Team - Original
|		 :  11SEP2014 - Prashant J
+-----------------------------------------------------------------------------------------------*/

%macro m_table_drop_passthrough(data_in=);

	libname &c_s_tdtempx. teradata user=&td_id. password="&td_pw"
			  server=&c_s_server.
			  database=&c_s_tdtempx.             
			  connection=unique
			  defer=yes 
			  fastload=yes;

	%if %sysfunc(exist(&data_in.)) %then %do;	

		%put NOTE: Dropping temporary table: &data_in.;

		proc sql;
			connect to teradata as tera (user=&td_id. password="&td_pw." server=&c_s_server. database=&c_s_tdtempx.);
			execute (drop table &data_in.) by tera;
			execute (commit) by tera;
			disconnect from tera;
		quit;

	%end;
	%else %do;
		%put NOTE: Dropping temporary table: DNE - &data_in.;
	%end;
	
%mend m_table_drop_passthrough;

/*example to run the macro*/
/*%m_table_drop_passthrough(data_in=&c_s_tdtempx..&claims_out2.);*/
