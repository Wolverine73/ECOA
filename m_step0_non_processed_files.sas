
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step0_non_processed_files
|
| LOCATION:  
| 
| PURPOSE:	collect history of files for cpl + sfmc + dqi + stars
|
| LOGIC: 	1. create calendar of dates for opps and disps - processed_10_days b sub sql
|		2. create list of process files for ecoa       - processed_10_days p sub sql
|		3. determine what has been processed and what is missing for the past 7 days for opps and disps
|		4. missing files will be collected and renamed for todays date and reprocessed with todays files
|		5. each days process will be published within dss_cea2.dqi_eoms_files_process to NONE in m_step0_eoms_process
|		6. each days process if successful will be updated within dss_cea2.dqi_eoms_files_process to COMPLETE in m_step6_eoms_output
|		7. if files need to be reprocessed then update execution_triage within dss_cea2.dqi_eoms_files_process to NONE
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step0_non_processed_files;

	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get processed history files + keep only CPL opps and CPL disps files
	+---------------------------------------------------------------------------------------------*/		
	%let lookback_date = 8;


	proc sql; 
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw");
	create table processed_10_days as
	select * from connection to tera(
 
			select  
			b.calendar_date,
			b.file_date as c_s_file_date,
			b.file_type as calendar_file_type,
			cast((cast (b.calendar_date as format 'MMMBDD,BYYYY')) as char(12)) as word_date, 
			p.execution_date,
			p.file_type,
			p.file_date,
			p.execution_triage,
			p.execution_sftp,
			p.source_file

			from (
				select calendar_date, year_of_calendar, CAST(calendar_date AS FORMAT 'YYYYMMDD') (VARCHAR(10)) as file_date, 'OPPORTUNITY' as file_type
				from sys_calendar.calendar 
				where calendar_date < current_date
				and calendar_date   > current_date - &lookback_date.
				union
				select calendar_date, year_of_calendar, CAST(calendar_date AS FORMAT 'YYYYMMDD') (VARCHAR(10)) as file_date, 'DISPOSITION2' as file_type
				from sys_calendar.calendar 
				where calendar_date < current_date 
				and calendar_date   > current_date - &lookback_date.		
			) b left join
			 (
				select 
					(case
					    when (position('opportunity' in source_file) > 0 ) then 'OPPORTUNITY' 
					    when (position('disposition' in source_file) > 0 ) then 'DISPOSITION' 
					    else 'OTHER'
					end) as file_type,
					substr(strtok(source_file,'.',2),1,8) as file_date, 
					source_file, 
					execution_triage, 
					source_new_file, 
					execution_date, 
					execution_sftp 
				from &c_s_tdtempx..dqi_eoms_files_process
				where execution_triage = 'COMPLETE'
				and upper(source_file)     like '%CPL%'
				and upper(source_file) not like '%RX%' 				
				and execution_date < current_date 
				and execution_date > current_date - &lookback_date. 				
			 ) p on b.file_date = p.file_date and b.file_type = p.file_type
 
			order by 1,2 desc	    
	) ;
	quit;
	
	data processed_10_days; 
	set processed_10_days; 
	**if c_s_file_date='20230829' then delete;  
	run;	

	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - history files of cpl only or missed days dates and exclude rx files
	+---------------------------------------------------------------------------------------------*/		
	data processed_10_days;  
	format flag $30. ;
	set processed_10_days; 
	if index(source_file,'cpl') > 0 or length(source_file) < 5;  
	if index(source_file,'rx')  = 0; 
	if execution_date = . then flag = 'MISSED FILES';
	else flag = 'PROCESSED FILES';
	run;
	
	proc sort data = processed_10_days out = processed_10_daysb nodupkey;
	by c_s_file_date;
	run;
	
	data _null_;
	set processed_10_daysb (keep = c_s_file_date execution_date flag);
	put _all_;
	run;	

	%let cnt_processed = 0;
	%let cnt_missed    = 0;
	
	proc sql noprint;
	select count(c_s_file_date) into: cnt_processed separated by ''
	from processed_10_daysb;
	quit;
	
	proc sql noprint;
	select count(*) into: cnt_missed separated by ''
	from processed_10_daysb
	where flag = 'MISSED FILES';
	quit;	

	%put NOTE: cnt_processed = the count will be 7 each day if files were processed successfully or not;
	%put NOTE: cnt_processed = &cnt_processed. ;
	%put NOTE: cnt_missed    = &cnt_missed. ;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get missed files from sftp history files
	+---------------------------------------------------------------------------------------------*/ 
	%if &cnt_missed. ne 0 %then %do;

		%let file_total = 0;
		
		data temp001;
		set processed_10_daysb;
		if flag = 'MISSED FILES';
		run;
		
		data _null_;
		set temp001 (keep = c_s_file_date flag word_date);
		put _all_;
		run;		
		
		data temp001;
		set temp001 end=eof;
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
 	 	
				%m_sftp_get(get_directory=&get_sftp_directory., put_directory=&eoms_history., get_file1=&sftp_get_file1b., get_file2=&sftp_get_file2b.);			 

			%end;
			
		%mend m_get_sftp_10_days;
		%m_get_sftp_10_days;
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - compare processed history files vs sftp history files
		+---------------------------------------------------------------------------------------------*/
		%let cnt_history_processed = 0;	
		
		%let eoms_history	= %str(/anr_dep/communication_history/eoms_history);

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
		
		data _null_;
		set history_10_days;
		put _all_;
		run;		

		proc sort data=history_10_days out=temp002 nodupkey; 
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
		if execution_date = . and length(source_file) > 5;
		run;	
		
		data _null_;
		set nonprocessed_10_days (keep = source_file today);
		put _all_;
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

			data _null_;
			set nonprocessed_10_daysc (keep = file_subject source_file new_name);
			put _all_;
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

				data _null_;
				x dir "&put_linux_directory.";
				x mv "&eoms_history./&&old_name&k"  "&put_linux_directory./&&new_name&k" ;
				run;			 

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

				create multiset table &c_s_tdtempx..dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_process. ) x) with no data 

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_bulkload_sasdata(bulk_data=load_history_files, bulk_base=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id., bulk_option=APPEND); 

			%put NOTE: &syserr. ;	

			%if &syserr. > 6 %then %do;
			  %let c_s_email_subject = EA ECOA Hisotry SQL Load Data Failure;
			  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 0),
					   abend_message=%str(EA ECOA - Proc Append Data failed - step 0));		  
			%end;	 


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load missed files in process file table
			+---------------------------------------------------------------------------------------------*/ 
			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				insert &c_s_table_process. 
				select * 
				from &c_s_tdtempx..dqi_eoms_tera_&mbr_process_id. b
				/** where b.source_file not in (select source_file from &c_s_table_process. ) **/

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
			%m_table_statistics(data_in=&c_s_table_process., index_in=eoms_row_gid);
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - email notificaiton 
			+---------------------------------------------------------------------------------------------*/
			proc sql noprint;
			select source_file into: missed_files separated by ','
			from eoms_process;
			quit;
			
			proc sql noprint;
			select source_new_file into: reprocess_files separated by ','
			from eoms_process;
			quit;	
			
			%put NOTE: missed_files = &missed_files. ;
			%put NOTE: reprocess_files = &reprocess_files. ;
			

			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - REPROCESS of ECOA Prior Day Missed Files ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

			options noquotelenmax;

			data _null_;
			file xemail;

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - assign html attributes
				+---------------------------------------------------------------------------------------------*/
				put '<html>';
				put '<meta content="text/html; charset=ISO-8859-1" http-equiv="content-type">';
				put '<body>';
				put '<span style="font-size: 10pt;';
				put 'font-family: &quot;Calibri Light&quot;,&quot;serif&quot;;">';

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email introduction
				+---------------------------------------------------------------------------------------------*/
				put '<br>';  								
				put "The ECOA process detected and renamed the missed files and will reprocess the files for todays ECOA proccess.";
				put '<br>';
				put '<br>'; 
				put "Review table dss_cea2.dqi_eoms_files_process for complete list of missed files"; 
				put "     missed_files = &missed_files. ";
				put "     reprocess_files = &reprocess_files. ";
				put '<br>';
				put '<br>'; 				

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email closing
				+---------------------------------------------------------------------------------------------*/	
				put "Thank you and have a great week."; put '<br>';
				put '<br>';
				put "Sincerely,"; put '<br>';
				put "EA - Campaign Targeting Team"; put '<br>';
				put " "; put '<br>';

			run;				
			
		%end;
		
	%end;

%mend m_step0_non_processed_files;