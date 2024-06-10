


/* SASDOC ------------------------------------------------------------------------------------(dashes)
|  Variables for the project, ticket to be used in defining directories
+(dashes)--------------------------------------------------------------------------------------SASDOC*/
%let c_s_core_dir=/anr_dep/dqi/prod/core;					
%let c_s_core_dir=/mbr_engmt/reserve/SASControl/PROD/data_integration/eoms_cmctn_history;

options mautosource sasautos = ("&c_s_core_dir." "/anr_dep/dqi/prod/core" "/anr_dep/dqi/prod/sasmacro" sasautos);   
options mprint nomlogic nosymbolgen source2 sastrace=',,,d' sastraceloc=saslog;	/* DO NOT CHANGE - standard SAS options */


/*----------------------------------Directory Variables---------------------------------*/
%let c_s_mailing_name	= %str(Data Integration);			/* campaign mailing name */
%let c_s_client_nm 	= %str(DI);					/* client name to be used in vendor mailing file */
%let c_s_rootdir 	= %str(/mbr_engmt/reserve/SASData/PROD);  	/* root directory for the aprimo request  */
%let c_s_program 	= %str(data_integration); 			/* next 1st level down from root directory  */
%let c_s_proj 		= %str(eoms_cmctn_history);  			/* next 2nd level down from program directory  */

%let c_s_dir1 		= &c_s_program.;                          	/* DO NOT CHANGE - 1st level for library or root directory  */
%let c_s_dir2 		= &c_s_proj.;                             	/* DO NOT CHANGE - 2nd level for library or root directory  */
%let c_s_maindir 	= &c_s_rootdir./&c_s_dir1./&c_s_dir2./;		/* DO NOT CHANGE - combines the root +  1st level + 2nd level for library  */
%let c_s_logdir 	= &c_s_rootdir./&c_s_dir1./&c_s_dir2.;
%let c_s_datadir	= &c_s_rootdir./&c_s_dir1./&c_s_dir2.;
%let c_s_filedir 	= %str(&c_s_rootdir./&c_s_program./&c_s_proj.);		/* DO NOT CHANGE - combines the root +  program level + project level for library  */
%let c_s_comm_file	= %str(/anr_dep/dqi/prod/data/cmctn_history/posted/);  	/* DO NOT CHANGE - location of posted data  */
%let c_s_dqi_production	= N;							/* to run within the production environment or development environment */
%let c_s_dqi_campaign_id= 80;   						/* dqi campaign id - dss_cea2.dqi_campaigns */
%let c_s_run_validation = Y;							/* DO NOT CHANGE */
%let sashost 		= CAPL1P;
%let execution_date     = 0;
%let ecoa_test		= NO; 							/* NO + YES1B + YES2B + YES3A B + YES4A B + YES5B + YES6A B **/
%let ecoa_zero		= NO;


/*----------------------------------CFC Program Specific Variables----------------------*/
%let c_s_email_alerts 	= Y;						/* DO NOT CHANGE - option to receive email alerts */
%let c_s_email_to 	= %str(brian.stropich3@caremark.com, akhilesh.mannava2@cvshealth.com, santoshmadhav.behara@cvshealth.com );		/* users email address for email alerts */ 
%let c_s_email_subject 	= %str(DQI Data Integration);			/* DO NOT CHANGE - email subject for email alerts */



/*----------------------------------Assign campaign Information ----------------------------------------------------*/
data x;
format c_s_file_date_delete c_s_file_date_linux $10.;
x='19JUN22'd; 
x=today(); 
e=today();  
xx=round(time()); 
c_s_file_date=left(trim(put(x,yymmddn8.)));
c_s_file_date_delete='%'||left(trim(c_s_file_date))||'%';
c_s_file_date_linux='*'||left(trim(c_s_file_date))||'*';
c_s_log_date=left(trim(put(x,yymmddn8.)))||left(trim(xx)); 
c_s_dqi_campaign_id="&c_s_dqi_campaign_id.";
c_s_ticket=trim(c_s_log_date)||trim(left(c_s_dqi_campaign_id));
execution_date=e-x;
call symput('c_s_file_date',compress(c_s_file_date));
call symput('c_s_file_date_delete',compress(c_s_file_date_delete));
call symput('c_s_file_date_linux',compress(c_s_file_date_linux));
call symput('c_s_log_date',compress(c_s_log_date));
call symput('c_s_ticket',compress(c_s_ticket));
call symput('c_s_aprimo_activity',compress(c_s_ticket));
call symput('execution_date',compress(execution_date));
run;



/*---------------------------------Teradata Database Variables-----------------------------------*/
%let td_uid		= %str(uy2huh5);				/* users ID for teradata */
%let use_dqi_id		= N;						/* option to use the user ID or production ID for teradata */ 
%let tdname 		= %trim(t&c_s_ticket.);				/* DO NOT CHANGE - prefix for the temporary table and dataset names */
%let c_s_schema 	= %str(dwu_edw);				/* DO NOT CHANGE - production schema data views for data targeting */
%let c_s_server 	= %str(prdedw1);				/* DO NOT CHANGE - production server */
%let c_s_tdtempx 	= %str(dss_cea2);				/* DO NOT CHANGE - mco schema for storing temporary tables */
%let sftp_etl_id 	= ftpuser;					/* DO NOT CHANGE - sftp ID for ETL EDW server */



/*---------------------------------SFTP Server Variables - DEVELOPMENT -------------------------*/
%let sftp_eoms_id		= %str(uy2huh5a); 
%let c_s_dqi_production		= Y; 		/* prod environment = Y + dev environment = N **/
%let c_s_sftp_get_production	= Y;		/* options servers -  webtransport.caremark.com = Y + tstwebtransport.caremark.com = N + dont send = X **/
%let c_s_sftp_put_production	= Y;		/* options servers -  prod = eaz1etlapa1d = Y + dev = eaz1etlada1a= N + dont send = X  **/
%let c_s_files_raw      	= Y;		/* process files in EA yes = Y + process files in EA no = N **/

%let c_s_it_send 		= Y;		/* sftp files to prod IT yes = Y(c_s_dqi_production=Y) + sftp files to dev IT yes = Y(c_s_dqi_production=N) + sftp files to IT no = N **/
%let c_s_cmctn_send     	= Y;		/* sftp files to prod CMCTN yes = Y (c_s_dqi_production=Y) + sftp files to CMCTN dev = Y (c_s_dqi_production=N) + sftp files CMCTN no = N **/
%let c_s_level1_disposition 	= Y;		/* CMCTN History if letter or email has L1 disposition of 1. = y = prod + n = test cmctn files **/
%let c_s_dwv_campgn_IDs		= N;		/* evaluate if OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID are used within dwu_edw - dss_cea2.dqi_eoms_format_sql **/
%let c_s_mcs_send       	= N; 		/* sftp files to MCS yes = Y + sftp files to MCS no = N **/



/*---------------------------------SFTP Server Variables - PRODUCTION --------------------------*/
%let sftp_eoms_id		= %str(uy2huh5a); 
%let c_s_dqi_production		= Y; 		/* prod environment = Y + dev environment = N **/
%let c_s_sftp_get_production	= Y;		/* options servers -  webtransport.caremark.com = Y + tstwebtransport.caremark.com = N **/
%let c_s_sftp_put_production	= Y;		/* options servers -  prod = eaz1etlapa1d = Y + dev = eaz1etlada1a= N **/
%let c_s_files_raw      	= Y;		/* process files in EA yes = Y + process files in EA no = N **/

%let c_s_it_send 		= Y;		/* sftp files to prod IT yes = Y(c_s_dqi_production=Y) + sftp files to dev IT yes = Y(c_s_dqi_production=N) + sftp files to IT no = N **/
%let c_s_cmctn_send     	= Y;		/* sftp files to prod CMCTN yes = Y (c_s_dqi_production=Y) + sftp files to CMCTN dev = Y (c_s_dqi_production=N) + sftp files CMCTN no = N **/
%let c_s_level1_disposition 	= Y;		/* CMCTN History if letter or email has L1 disposition of 1. = y = prod + n = test cmctn files **/
%let c_s_dwv_campgn_IDs		= Y;		/* evaluate if OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID are used within dwu_edw **/
%let c_s_mcs_send       	= N; 		/* sftp files to MCS yes = Y + sftp files to MCS no = N **/



/*---------------------------------SFTP Server Files--------------------------------------------*/
%let sftp_put_file1	= cpl*;
%let sftp_put_file2	= sfmc*;
%let sftp_get_file1	= cpl*.&c_s_file_date.*;
%let sftp_get_file2	= sfmc*.&c_s_file_date.*; 

%include "/mbr_engmt/reserve/SASControl/DEV/bss_test/macros/m_function_check_roles.sas";

%m_environment;









/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step0_non_processed_files
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

%macro m_step0_non_processed_files;

	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get processed history files
	+---------------------------------------------------------------------------------------------*/		
	%let lookback_date = 8;

	proc sql; 
	connect to teradata as tera(server=&c_s_server. user=DQI_APP password="Care_2015");
	create table processed_10_days as
	select * from connection to tera(

		select b.year_of_calendar, b.calendar_date, CAST( (CAST (b.calendar_date AS FORMAT 'MMMBDD,BYYYY')) AS CHAR(12)) as word_date, a.source_file, a.execution_date
		from sys_calendar.calendar b left join
		     dss_cea2.dqi_eoms_files_process a   
		on a.execution_date = b.calendar_date 
		where b.calendar_date < current_date 
		and b.calendar_date > current_date - &lookback_date.   
		order by a.execution_date, a.source_file
	) ;
	quit;	

	data processed_10_days; 
	set processed_10_days; 
	c_s_file_date=left(trim(put(calendar_date,yymmddn8.)));  
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - history files of cpl only or missed days dates and exclude rx files
	+---------------------------------------------------------------------------------------------*/		
	data processed_10_days;  
	set processed_10_days; 
	if index(source_file,'cpl') > 0 or length(source_file) < 5;  
	if index(source_file,'rx') = 0; 
	run;
	
	proc sort data = processed_10_days out = processed_10_daysb nodupkey;
	by c_s_file_date;
	run;
	
	data _null_;
	set processed_10_daysb;
	put _all_;
	run;	

	proc sql noprint;
	select count(c_s_file_date) into: cnt_processed separated by ''
	from processed_10_daysb;
	quit;

	%put NOTE: cnt_processed = &cnt_processed. ;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get history of sftp history files
	+---------------------------------------------------------------------------------------------*/ 
	%if &cnt_processed. ne 0 %then %do;

		%let file_total = 0;
		
		data temp001;
		set processed_10_daysb end=eof;
		ii=left(put(_n_,4.)); 
		call symput('file'||ii, trim(left(c_s_file_date))); 
		if eof then call symput('file_total',ii);
		run;
		
		%put NOTE: file_total = &file_total. ;

		%macro m_get_sftp_10_days;
				
			%do j = 1 %to &file_total. ;

				%let sftp_get_file1b	= cpl_opportunity.&&file&j*;
				%let sftp_get_file2b	= cpl_disposition.&&file&j*; 

				%put NOTE: ------------------------------------------------------------------------ ;
				%put NOTE: loop = &j. of  &file_total.;
				%put NOTE: ------------------------------------------------------------------------ ;
				%put NOTE: sftp_get_file1b   = &sftp_get_file1b.     ;
				%put NOTE: sftp_get_file2b   = &sftp_get_file2b.     ;
				%put NOTE: get directory     = &get_sftp_directory.  ;
				%put NOTE: put directory     = &put_linux_directory. ;
				%put NOTE: history directory = &eoms_history.        ;
				%put NOTE: ------------------------------------------------------------------------ ;


				%include "/mbr_engmt/reserve/SASControl/PROD/data_integration/eoms_cmctn_history/m_sftp_get.sas"; 	 	
				%m_sftp_get(get_directory=&get_sftp_directory., put_directory=&eoms_history., get_file1=&sftp_get_file1b., get_file2=&sftp_get_file2b.);			 

			%end;
			
		%mend m_get_sftp_10_days;
		%**m_get_sftp_10_days;
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - compare processed history files vs sftp history files
		+---------------------------------------------------------------------------------------------*/
		%let cnt_non_processed = 0;	

		data history_10_days;
		rc=filename("mydir","&eoms_history.");
		did=dopen("mydir");
		memcount=dnum(did);
		do i=1 to memcount;
			name=dread(did,i); 
			match1=find(name,"cpl","i t",1);  /**<------------------- CPL only - check if any prior days did not process **/
			if match1 then output;
		end;
		rc=dclose(did);
		run;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - sftp files of cpl only or missed days dates and exclude rx files
		+---------------------------------------------------------------------------------------------*/		
		data history_10_days;
		format today mmddyy10. ;
		set history_10_days; 
		suffix2=scan(name,2,'.'); 
		y=substr(suffix2,1,4);
		m=substr(suffix2,5,2);
		d=substr(suffix2,7,2); 
		today=mdy(m,d,y);
		name2=scan(name,1,'_'); 
		file_subject=scan(name,1,'.');
		if today > today()  - &lookback_date. and today < today();
		if index(name,'cpl') > 0 ;
		if index(name,'rx')  = 0; 
		run;


		proc sort data=history_10_days out=temp001 nodupkey; 
		by today; 
		run;
		
		data history_10_days;
		set history_10_days;
		source_file = name;
		run;

		proc sort data=history_10_days;
		by source_file;
		run;
		
		proc sort data=processed_10_days;
		by source_file;
		run;
		
		data nonprocessed_10_days;
		merge history_10_days (in=a) processed_10_days (in=b);
		by source_file;
		if execution_date = .;
		run;	
		
		proc sql noprint;
		select count(*) into: cnt_non_processed separated by ''
		from nonprocessed_10_days;
		quit;

		%put NOTE: cnt_non_processed = &cnt_non_processed. ;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - if missing files exist then rename missed files to reprocess for today
		+---------------------------------------------------------------------------------------------*/ 
		%if &cnt_non_processed. ne 0 %then %do;		

			data nonprocessed_10_days;
			set nonprocessed_10_days;
			new_name_suffix1=scan(source_file,1,'.');
			new_name_suffix2=scan(source_file,2,'.');
			new_name_suffix3=scan(source_file,3,'.');
			run;

			proc sort data=nonprocessed_10_days out=nonprocessed_10_daysb nodupkey;
			by new_name_suffix2;
			run;

			data nonprocessed_10_daysb;
			set nonprocessed_10_daysb;
			n=_n_+100000;
			run;

			proc sort data=nonprocessed_10_days ;
			by new_name_suffix2;
			run;

			proc sort data=nonprocessed_10_daysb;
			by new_name_suffix2;
			run;

			data nonprocessed_10_daysc;
			format new_name_suffix2b new_name $100. ;
			merge nonprocessed_10_days (in=a) nonprocessed_10_daysb (in=b);
			by new_name_suffix2;
			x=today(); 
			e=today();  
			xx=round(time()); 
			xxx=left(trim(put(x,yymmddn8.)));		
			new_name_suffix2b=trim(left(xxx))||trim(left(n));
			if length(new_name_suffix3) > 2 then do;
				new_name=trim(left(new_name_suffix1))||'.'||trim(left(new_name_suffix2b))||'.'||trim(left(new_name_suffix3));
			end;
			else do;
				new_name=trim(left(new_name_suffix1))||'.'||trim(left(new_name_suffix2b));
			end;
			run;	
			
			%let name_total = 0;

			data _null_;
			set nonprocessed_10_daysc end=eof;
			ii=left(put(_n_,4.)); 
			call symput('old_name'||ii, trim(left(source_file)));
			call symput('new_name'||ii, trim(left(new_name))); 
			if eof then call symput('name_total',ii);
			run;
			
			%put NOTE: name_total = &name_total. ;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - move and rename missed files with todays date for reprocessing for today
			+---------------------------------------------------------------------------------------------*/
			%do k = 1 %to &name_total. ;

				%put NOTE: old name =  &&old_name&k ;
				%put NOTE: new name =  &&new_name&k ;

%macro dontdo;
				data _null_;
				x dir "&put_linux_directory.";
				x mv "&eoms_history./&&old_name&k"  "&put_linux_directory./&&new_name&k" ;
				run;			 
%mend dontdo;

			%end;	
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - collect metrics on missed files in process file table
			+---------------------------------------------------------------------------------------------*/ 
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
			set nonprocessed_10_daysc;
				EOMS_ROW_GID = 0;
				SOURCE_TEAM = upcase(scan(source_file,1,'_'));
				SOURCE_FILE = source_file;
				SOURCE_FILE_METRIC = 0;
				SOURCE_NEW_FILE = new_name;
				SOURCE_NEW_FILE_METRIC = 0;
				SOURCE_NEW_IT_SFTP_FILE='X';
				EXECUTION_TRIAGE = 'NONE';
				EXECUTION_SFTP = 'REPROCESS';
				EXECUTION_DATE = today() - &execution_date. ; 
			run;

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build missed files for process file table
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
			  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 0),
					   abend_message=%str(EA ECOA - Proc Append Data failed - step 0));		  
			%end;	 

%macro dontdo;
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load missed files in process file table
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
			
%mend dontdo;	

			
		%end;
		
	%end;

%mend m_step0_non_processed_files;


%let put_linux_directory= %str(/anr_dep/communication_history/eoms_files);      /** <--------------------- location of LINUX raw data files **/
%let eoms_directory	= &put_linux_directory.;                                /** <--------------------- location of CTT raw data files **/
%let eoms_history	= %str(/anr_dep/communication_history/eoms_history);    /** <--------------------- location of history of data files **/
%let get_etl_directory	= %str(/anr_dep/communication_history/eoms_IT);	
%let c_s_table_process  = %str(dss_cea2.dqi_eoms_files_process);
%let c_s_table_history  = %str(dss_cea2.dqi_eoms_files_history);
%m_step0_non_processed_files;