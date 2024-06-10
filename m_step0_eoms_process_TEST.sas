
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step0_eoms_process
|
| LOCATION:  
| 
| PURPOSE:	collect history of files for cpl + sfmc + dqi + stars
|
| LOGIC: 
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step0_eoms_process;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - list of files to process for today
	+---------------------------------------------------------------------------------------------*/		
	data eoms_process;
	rc=filename("mydir","&put_linux_directory.");
	did=dopen("mydir");
	memcount=dnum(did);
	do i=1 to memcount;
		name=dread(did,i); 
		match1=find(name,"opp","i t",1);   /**<------------------- all opp  files **/
		match2=find(name,"disp","i t",1);  /**<------------------- all disp files **/
		if match1 or match2 then output;
	end;
	rc=dclose(did);
	run;

	proc sort data=eoms_process;
	by name;
	run;

	data eoms_process;
	format today $8. ;
	set eoms_process;
	year=year(today() - &execution_date. );
	month=put(month(today() - &execution_date. ),z2.);
	day=put(day(today() - &execution_date. ),z2.);
	today=compress(year||month||day);
	name2=scan(name,1,'_');
	if index(name,today) > 0;
	file_subject=scan(name,1,'.');
	run;
	
	%let cnt_process = 0;
	
	proc sql noprint;
	select count(*) into: cnt_process separated by ''
	from eoms_process;
	quit;
	
	%put NOTE: cnt_process = &cnt_process. ;
	
	%if &cnt_process. ne 0 %then %do;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - collect metrics on processed files in process file table
		+---------------------------------------------------------------------------------------------*/ 
		%let mbr_process_id = 123456;
		
 		data template;
 		set &c_s_table_process. (obs=0);
 		run;
 
 		proc contents data = template out = contents1 noprint;
 		run;
 
 		proc sql noprint;
 		select name into: keep_variables separated by ' '
 		from contents1;
 		quit;
 		
 		data eoms_process;
 		set eoms_process;
			EOMS_ROW_GID = 0;
			SOURCE_TEAM = upcase(scan(name,1,'_'));
			SOURCE_FILE = name;
			SOURCE_FILE_METRIC = 0;
			SOURCE_NEW_FILE = name2;
			SOURCE_NEW_FILE_METRIC = 0;
			SOURCE_NEW_IT_SFTP_FILE='X';
			EXECUTION_TRIAGE = 'NONE';
			EXECUTION_SFTP = 'PROCESS';
			EXECUTION_DATE = today() - &execution_date. ; 
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - build processed files for process file table
		+---------------------------------------------------------------------------------------------*/  
 		data load_history_files;
 		set eoms_process;  	 
 		keep &keep_variables. ;
 		run;		
 
 		%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
 
 		proc sql;
 		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
 		execute(  
 
 			create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_process. ) x) with no data 
 
 		) by tera;
 		execute (commit work) by tera;  
 		disconnect from tera;
 		quit; 
 		
 		%m_bulkload_sasdata(bulk_data=load_history_files, bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id., bulk_option=APPEND); 
 		
 		%put NOTE: &syserr. ;	
 
 		%if &syserr. > 6 %then %do;
 		  %let c_s_email_subject = EA ECOA Hisotry SQL Load Data Failure;
 		  %m_abend_handler(abend_report=%str(EA ECOA - proc append data failure - step 0),
 				   abend_message=%str(EA ECOA - proc append data failure - step 0));		  
 		%end;	 


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - load processed files in process file table
		+---------------------------------------------------------------------------------------------*/  
 		proc sql;
 		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
 		execute(  
 
 			insert &c_s_table_process. 
 			select * 
 			from dss_cea2.dqi_eoms_tera_&mbr_process_id.
 
 		) by tera;
 		execute (commit work) by tera;  
 		disconnect from tera;
 		quit; 
 
 		%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.); 
 		%m_table_statistics(data_in=&c_s_table_process., index_in=eoms_row_gid);	
 		
	%end;
	

%mend m_step0_eoms_process;


%let put_linux_directory= %str(/anr_dep/communication_history/eoms_files);      /** <--------------------- location of LINUX raw data files **/
%let eoms_directory	= &put_linux_directory.;                                /** <--------------------- location of CTT raw data files **/
%let eoms_history	= %str(/anr_dep/communication_history/eoms_history);    /** <--------------------- location of history of data files **/
%let get_etl_directory	= %str(/anr_dep/communication_history/eoms_IT);	
%let c_s_table_process  = %str(dss_cea2.dqi_eoms_files_process);
%let c_s_table_history  = %str(dss_cea2.dqi_eoms_files_history);
%m_step0_eoms_process;
