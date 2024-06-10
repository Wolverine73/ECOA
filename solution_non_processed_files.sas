


/* HEADER---------------------------------------------------------------------------------------
| MACRO:    ctl_data_integration_eoms
|
| LOCATION:  
| 
| PURPOSE:  
|
| LOGIC:     
|
| INPUT:     
|
| OUTPUT:    
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20190201 - Clinical Stars Team - Original (version 2019.02.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/

/* SASDOC ------------------------------------------------------------------------------------(dashes)
|  Variables for the project, ticket to be used in defining directories
+(dashes)--------------------------------------------------------------------------------------SASDOC*/
%let c_s_core_dir=/anr_dep/dqi/prod/core;					
%let c_s_core_dir=/mbr_engmt/reserve/SASControl/PROD/data_integration/eoms_cmctn_history;

options mautosource sasautos = ("&c_s_core_dir." "/anr_dep/dqi/prod/core" "/anr_dep/dqi/prod/sasmacro" sasautos);   
options mprint nomlogic nosymbolgen source2 sastrace=',,,d' sastraceloc=saslog;	/* DO NOT CHANGE - standard SAS options */

%include "/mbr_engmt/reserve/SASControl/DEV/bss_test/macros/m_function_check_roles.sas";


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


%macro doit;

	proc sql; 
	connect to teradata as tera(server=&c_s_server. user=DQI_APP password="Care_2015");
	create table _processed_10 as
	select * from connection to tera(

		select b.year_of_calendar, b.calendar_date, CAST( (CAST (b.calendar_date AS FORMAT 'MMMBDD,BYYYY')) AS CHAR(12)) as word_date, a.execution_date, count(*) as cnt
		from sys_calendar.calendar b left join
		     dss_cea2.dqi_eoms_opportunity a   
		on a.execution_date = b.calendar_date 
		where year_of_calendar = 2023
		and b.calendar_date < current_date 
		and b.calendar_date > current_date - 10
		group by 1,2,3,4
		order by 1,2 desc 		
	) ;
	quit;

	data _processed_10; 
	set _processed_10;  
	if calendar_date = today()-3 then execution_date = . ;
	if calendar_date = today()-4 then execution_date = . ;
	run;

	data _processed_10; 
	set _processed_10; 
	c_s_file_date=left(trim(put(calendar_Date,yymmddn8.))); 
	if execution_date = . ;
	run;

	proc sql noprint;
	select count(*) into: cnt_processed separated by ''
	from _processed_10;
	quit;

	%put NOTE: cnt_processed = &cnt_processed. ;

	%if &cnt_processed. ne 0 %then %do;


		%let get_sftp_directory	= %str(/ant591/IVR/incoming);								/** <--------------------- location of SFTP raw data files **/
		%let put_linux_directory= %str(/anr_dep/communication_history/eoms_files_prior);    /** <--------------------- location of LINUX raw data files **/

		data _null_;
		x "rm &put_linux_directory./*.* ";
		run;	 

		data _null_;
		set _processed_10 end=eof;
		ii=left(put(_n_,4.)); 
		call symput('file', trim(left(c_s_file_date)));
		if eof then call symput('file_total',ii);
		run;
		
		%put NOTE: file_total = &file_total. ;


		/** step 1 - get sftp files  **/  
		%do j = 1 %to &file_total. ;

			%let sftp_get_file1b	= cpl_opportunity.&&c_s_file_date.&j*;
			%let sftp_get_file2b	= cpl_disposition.&&c_s_file_date.&j*; 

			%put NOTE: sftp_get_file1b = &sftp_get_file1b. ;
			%put NOTE: sftp_get_file2b = &sftp_get_file2b. ;
			%put NOTE: get_sftp_directory = &get_sftp_directory. ;
			%put NOTE: put_linux_directory = &put_linux_directory. ;

			%include "/mbr_engmt/reserve/SASControl/PROD/data_integration/eoms_cmctn_history/m_sftp_get.sas";
			%m_sftp_get(get_directory=&get_sftp_directory., put_directory=&put_linux_directory., get_file1=&sftp_get_file1b., get_file2=&sftp_get_file2b.);			 

		%end;

		/** step 2 - rename sftp files to todays date  **/
		%let cpl_total = 0;	
	
		data eoms_today;
		rc=filename("mydir","&put_linux_directory.");
		did=dopen("mydir");
		memcount=dnum(did);
		do i=1 to memcount;
			name=dread(did,i); 
			match1=find(name,"cpl","i t",1);  /**<------------------- CPL only **/
			if match1 then output;
		end;
		rc=dclose(did);
		run;

		proc sort data=eoms_today;
		by name;
		run;

		data eoms_today;
		set eoms_today;
		new_name_suffix1=scan(name,1,'.');
		new_name_suffix2=scan(name,2,'.');
		new_name_suffix3=scan(name,3,'.');
		run;

		proc sort data=eoms_today out=eoms_today2 nodupkey;
		by new_name_suffix2;
		run;

		data eoms_today2;
		set eoms_today2;
		n=_n_+100000;
		run;

		proc sort data=eoms_today ;
		by new_name_suffix2;
		run;

		proc sort data=eoms_today2;
		by new_name_suffix2;
		run;

		data eoms_today;
		format new_name_suffix2b new_name $100. ;
		merge eoms_today (in=a) eoms_today2 (in=b);
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
		
		data _null_;
		set eoms_today end=eof;
		ii=left(put(_n_,4.)); 
		call symput('old_name'||ii, trim(left(name)));
		call symput('new_name'||ii, trim(left(new_name))); 
		if eof then call symput('name_total',ii);
		run;
		
		%put NOTE: name_total = &name_total. ;
 
		%do k = 1 %to &name_total. ;

			%put NOTE: old name =  &&old_name&k ;
			%put NOTE: new name =  &&new_name&k ;

			data _null_;
			x dir "&put_linux_directory.";
			x mv "&put_linux_directory./&&old_name&k"  "&put_linux_directory./&&new_name&k" ;
			run;			 

		%end;		


	%end;

%mend doit;

%m_environment;

options nosymbolgen nomlogic mprint;
%doit;
