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
				where calendar_date < current_date + 1
				and calendar_date   > current_date - &lookback_date.
				union
				select calendar_date, year_of_calendar, CAST(calendar_date AS FORMAT 'YYYYMMDD') (VARCHAR(10)) as file_date, 'DISPOSITION2' as file_type
				from sys_calendar.calendar 
				where calendar_date < current_date + 2
				and calendar_date   > current_date - &lookback_date.		
			) b left join
			 (
				select (CASE
				    WHEN (position('opportunity' IN source_file) > 0 ) THEN 'OPPORTUNITY' 
				    WHEN (position('disposition' IN source_file) > 0 ) THEN 'DISPOSITION' 
				    ELSE 'OTHER'
				END) AS file_type,
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
		if today > today()  - &lookback_date. and today < today()+2;
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
