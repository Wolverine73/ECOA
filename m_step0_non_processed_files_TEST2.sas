



%let c_s_schema 	= %str(dwu_edw);				/* DO NOT CHANGE - production schema data views for data targeting */
%let c_s_server 	= %str(prdedw1);				/* DO NOT CHANGE - production server */
%let c_s_tdtempx 	= %str(dss_cea2);				/* DO NOT CHANGE - mco schema for storing temporary tables */
%let c_s_table_process  = %str(dss_cea2.dqi_eoms_files_process);
%let eoms_history	= %str(/anr_dep/communication_history/eoms_history); 
%let td_id 		= %str(DQI_APP);
%let td_pw 		= %str(Care_2015);
%let execution_date     = 0;

libname dss_cea2 teradata user=uy2huh5 password="Maizy_123" server=prdedw1 database=dss_cea2 connection=unique defer=yes fastload=yes;
options mprint;




%macro m_step0_non_processed_files;

	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get processed history files + keep only opps and disps files
	+---------------------------------------------------------------------------------------------*/		
	%let lookback_date = 8;

	proc sql; 
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw");
	create table calendar_10_days as
	select * from connection to tera(

		select b.year_of_calendar, b.calendar_date, CAST( (CAST (b.calendar_date AS FORMAT 'MMMBDD,BYYYY')) AS CHAR(12)) as word_date 
		from sys_calendar.calendar b     
		where b.calendar_date < current_date 
		and   b.calendar_date > current_date - &lookback_date.    
	) ;
	quit;	
	
	proc sql; 
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw");
	create table processed_10_days as
	select * from connection to tera(

		select a.source_file, a.execution_date
		from &c_s_tdtempx..dqi_eoms_files_process a 
		where execution_triage = 'COMPLETE'
		and a.execution_date < current_date 
		and a.execution_date > current_date - &lookback_date.   
		order by a.execution_date, a.source_file
	) ;
	quit;
	
	data calendar_10_days; 
	set calendar_10_days;  
	c_s_file_date=left(trim(put(calendar_date,yymmddn8.)));  
	run;	

	data processed_10_days; 
	set processed_10_days; 
	c_s_file_date=substr(scan(source_file,2,'.'),1,8);  
	run;
	
	data processed_10_days; 
	set processed_10_days; 
	**if c_s_file_date='20230829' then delete;  
	run;	
	
	proc sort data = calendar_10_days;
	by c_s_file_date;
	run;
	
	proc sort data = processed_10_days;
	by c_s_file_date;
	run;	
	
	data processed_10_days;
	merge processed_10_days (in=a)
	      calendar_10_days  (in=b);
	by c_s_file_date;
	run;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - history files of cpl only or missed days dates and exclude rx files
	+---------------------------------------------------------------------------------------------*/		
	data processed_10_days;  
	format flag $30. ;
	set processed_10_days; 
	if index(source_file,'cpl') > 0 or length(source_file) < 5;  
	if index(source_file,'rx') = 0; 
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
		%**m_get_sftp_10_days;
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - compare processed history files vs sftp history files
		+---------------------------------------------------------------------------------------------*/
		%let cnt_history_processed = 0;	

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

				
			
		%end;
		
	%end;

%mend m_step0_non_processed_files;


%m_step0_non_processed_files;
		
		
			