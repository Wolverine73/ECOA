
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    m_bulkload_sasdata
|
| LOCATION:  
| 
| PURPOSE:  a throttle to determine bulkload is yes or no
|
| REQUIREMENT:  
|
| LOGIC:    
|
| INPUT:    
|
| OUTPUT:   
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201201 - DQI Team - Original  
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_bulkload_sasdata(bulk_data=, bulk_base=, bulk_option=NONE, bulk_index=NONE, bulk_default=Y, copy_data=N);

	%let cnt_bulkload = 1000;

	%if &bulk_data. ne %then %do;
		%let bulk_default=N;
	%end;
	
	%if &bulk_default. = N %then %do;
		proc sql noprint;
		select count(*) into: cnt_bulkload separated by ''
		from &bulk_data.;
		quit;
	%end;
	
	%if &copy_data. = Y %then %do;
	
		data working.&bulk_data.;
		set &bulk_data. ;
		run;
	
	%end;
	
	%put NOTE: cnt_bulkload = &cnt_bulkload. ;

	%if &cnt_bulkload. < 5000 %then %do;
		%let c_s_fastload = NO;
	%end;
	%else %do;
		%let c_s_fastload = YES;
	%end;

	%put NOTE: bulk_data = &bulk_data. ;
	%put NOTE: bulk_base = &bulk_base. ;
	%put NOTE: bulk_option = &bulk_option. ;
	%put NOTE: bulk_index = &bulk_index. ;
	%put NOTE: bulk_default = &bulk_default. ;
	%put NOTE: c_s_fastload = &c_s_fastload. ;

	%if %symexist(c_s_tdtempx) %then %do;
	
		libname &c_s_tdtempx. teradata user=&td_id. password="&td_pw"
				  server=&c_s_server.
				  database=&c_s_tdtempx.             
				  connection=unique
				  defer=yes 
				  %if %upcase(&c_s_fastload.) = NO %then %do;
					fastload=no;
				  %end;
				  %else %do;
					fastload=yes;
				  %end;				  
	%end;
	
	%if &bulk_option. = APPEND %then %do;
		proc append data = &bulk_data.  force base = &bulk_base. 
			  %if %upcase(&c_s_fastload.) = NO %then %do; 
			  %end;
			  %else %do;
				(fastload=yes tenacity=3 sessions=4 sleep=20 tpt=yes tpt_restart=yes tpt_checkpoint_data=2000)
			  %end;	
			  ;		 
		run;
	%end;
	%else %if &bulk_option. = DATASTEP %then %do;
		data &bulk_base.
			  %if %upcase(&c_s_fastload.) = NO %then %do; 
			        ( TPT=YES sleep=6 bulkload=no  tenacity=2 sessions=4 fbufsize=8000 dbcreate_table_opts="PRIMARY INDEX(&bulk_index.)")
			  %end;
			  %else %do;
				( TPT=YES sleep=6 bulkload=yes tenacity=2 sessions=4 fbufsize=8000 dbcreate_table_opts="PRIMARY INDEX(&bulk_index.)")
			  %end;	
			  ;		
		set &bulk_data.;
		run;
	%end;
	
%mend m_bulkload_sasdata; 