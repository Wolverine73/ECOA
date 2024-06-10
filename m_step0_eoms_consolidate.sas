
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step0_eoms_consolidate
|
| LOCATION:  
| 
| PURPOSE:	CPL creates multiple files per day - CTT will consolidate them into 1 file 
|
| LOGIC: 
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step0_eoms_consolidate(team_id=, layout_id=);


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - list of files to process for today
	+---------------------------------------------------------------------------------------------*/
	%let cpl_total = 0;	
	
	data eoms_today;
	rc=filename("mydir","&eoms_directory.");
	did=dopen("mydir");
	memcount=dnum(did);
	do i=1 to memcount;
		name=dread(did,i); 
		match1=find(name,"&team_id.","i t",1);  /**<------------------- CPL only **/
		if match1 then output;
	end;
	rc=dclose(did);
	run;

	proc sort data=eoms_today;
	by name;
	run;

	data eoms_today;
	format today $8. ;
	set eoms_today;
	year=year(today() - &execution_date. );
	month=put(month(today() - &execution_date. ),z2.);
	day=put(day(today() - &execution_date. ),z2.);
	today=compress(year||month||day);
	name2=scan(name,1,'_');
	if index(name,today) > 0;
	file_subject=scan(name,1,'.');
	run;

	proc sort data = eoms_today;
	by name;
	run;
	
	data eoms_today2; 
	set eoms_today; 
	if index(name,'CNTL') = 0;
	run;	
	
	proc sql noprint;
	create table eoms_today2 as
	select file_subject, count(*) as cnt
	from eoms_today2
	group by 1
	having cnt > 1;
	quit;

	%let today_files = 0;
	
	data eoms_today2;
	set eoms_today2;
	i=_n_;
	call symput('today_files',left(trim(i)));
	put i file_subject ;
	run;

	%put NOTE: today_files = &today_files.; 

	%if &today_files. ne 0  %then %do;
		


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - move the files
		+---------------------------------------------------------------------------------------------*/		
		data _null_;				
		x "cd &eoms_directory.";
		x "mv &eoms_directory/&sftp_get_file1.  &eoms_consolidate.";
		run; 
		
		proc sql noprint;
		select "'"||left(trim(file_subject))||"'" into: name_consolidate separated by ','
		from eoms_today2;
		quit;
		
		%put NOTE: name_consolidate = &name_consolidate. ;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - consolidation email notification
		+---------------------------------------------------------------------------------------------*/	
	
		filename xemail email 
		to=(&c_s_email_to.)
		subject="CTT Data Profiling - CONSOLIDATION of ECOA Files ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

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
			put "The ECOA has received multiple files for the day and will consolidate them into 1 file for processing. ";
			put '<br>';
			put '<br>'; 
			put "The Step 0 - ECOA consolidation Logic: only for CPL files + keep early file as the name of file ";
			put '<br>';
			put '<br>'; 			
			put "The CONSOLIDATION Files: &name_consolidate. ";
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
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - non-consolidated files
		+---------------------------------------------------------------------------------------------*/ 
		data eoms_today_cntl1 eoms_today_data1;
		set eoms_today;
		where file_subject not in (&name_consolidate.);
		if index(name,'.CNTL') > 0 then output eoms_today_cntl1;
		else output eoms_today_data1;
		run;
		
		data eoms_today_data1;
		set eoms_today_data1;
		ii=_n_;
		mod=ii;
		mod2=mod;
		mod3=mod;
		file_wildcard=scan(name,1,'.');
		file_wildcard=trim(left(file_wildcard))||'.'||trim(left(year))||trim(left(month))||trim(left(day));
		run;

		/** sort - have the earliest files 1st and latest files 2nd then rename new files with early file names **/
		proc sort data = eoms_today_data1 ;
		by name;
		run;

		/** sort - keep the earliest files since we are importing wildcard files **/
		proc sort data =  eoms_today_data1  out =  eoms_today_data1b nodupkey;
		by file_wildcard;
		run;		
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - process the data files + create new file
		+---------------------------------------------------------------------------------------------*/
		%macro do_data_files;

				%let cpl_total = 0;
				%let cpln1=0;
				%let cpln2=0;
				%let cpln3=0;
				%let cpln4=0;				
				
				data _null_;
				set eoms_today_data1b ;
				call symput('cpl'||trim(left(mod3)),trim(left(file_wildcard)) );
				call symput('cplr'||trim(left(mod3)),trim(left(name)) );
				call symput('cpln'||trim(left(mod3)),trim(left(mod3)) );
				call symput('cpl_total',left(trim(mod3)));
				run;

				%put NOTE: cpl_total = &cpl_total. ; 

				%do i = 1 %to &cpl_total;
				
					%if &&cpln&i. ne 0 %then %do;
				
						data _null_;				
						x "cd &eoms_consolidate.";
						x "mv &eoms_consolidate.&SYS_DIR.&&cplr&i. &eoms_directory.";
						run;
					
					%end;

				%end;

		%mend do_data_files;
		%do_data_files;
		
		proc sort data = eoms_today_cntl1 ;
		by name;
		run;

		data eoms_today_cntl1;
		set eoms_today_cntl1;
		ii=_n_;
		mod=ii;
		mod2=mod;
		mod3=mod;
		file_wildcard=scan(name,1,'.');
		file_wildcard=trim(left(file_wildcard))||'.'||trim(left(year))||trim(left(month))||trim(left(day));
		run;

		proc sort data = eoms_today_cntl1 ;
		by name;
		run;

		proc sort data =  eoms_today_cntl1  out =  eoms_today_cntl1b nodupkey;
		by file_wildcard;
		run; 
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - process the control files + create new file
		+---------------------------------------------------------------------------------------------*/
		%macro do_cntl_files;

				%let cpl_total = 0;
				%let cpln1=0;
				%let cpln2=0;
				%let cpln3=0;
				%let cpln4=0;				
				
				data _null_;
				set eoms_today_cntl1b ;
				call symput('cpl'||trim(left(mod3)),trim(left(file_wildcard)) );
				call symput('cplr'||trim(left(mod3)),trim(left(name)) );
				call symput('cpln'||trim(left(mod3)),trim(left(mod3)) );
				call symput('cpl_total',left(trim(mod3)));
				run;

				%put NOTE: cpl_total = &cpl_total. ; 

				%do i = 1 %to &cpl_total;
				
					%if &&cpln&i. ne 0 %then %do;
				
						data _null_;				
						x "cd &eoms_consolidate.";
						x "mv &eoms_consolidate.&SYS_DIR.&&cplr&i. &eoms_directory.";
						run;	
					
					%end;

				%end;

		%mend do_cntl_files;
		%do_cntl_files;		
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - consolidated files
		+---------------------------------------------------------------------------------------------*/
		data eoms_today_cntl2 eoms_today_data2;
		set eoms_today;
		where file_subject in (&name_consolidate.);
		if index(name,'.CNTL') > 0 then output eoms_today_cntl2;
		else output eoms_today_data2;
		run;		

		data eoms_today_data2;
		set eoms_today_data2;
		ii=_n_;
		mod=mod(ii, 2);
		mod2=mod+ii;
		mod3=mod2/2;
		file_wildcard=scan(name,1,'.');
		file_wildcard=trim(left(file_wildcard))||'.'||trim(left(year))||trim(left(month))||trim(left(day));
		run;

		/** sort - have the earliest files 1st and latest files 2nd then rename new files with early file names **/
		proc sort data = eoms_today_data2 ;
		by name;
		run;

		/** sort - keep the earliest files since we are importing wildcard files **/
		proc sort data =  eoms_today_data2  out =  eoms_today_data2b nodupkey;
		by file_wildcard;
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - process the data files + create new file
		+---------------------------------------------------------------------------------------------*/
		%macro do_data_files;

				%let cpl_total = 0;
				%let cpln1=0;
				%let cpln2=0;
				%let cpln3=0;
				%let cpln4=0;
				
				data _null_;
				set eoms_today_data2b ;
				call symput('cpl'||trim(left(mod3)),trim(left(file_wildcard)) );
				call symput('cplr'||trim(left(mod3)),trim(left(name)) ); 
				call symput('cpln'||trim(left(mod3)),trim(left(mod3)) ); 
				call symput('cpl_total',left(trim(mod3)));
				run;

				%put NOTE: cpl_total = &cpl_total. ; 

				%do i = 1 %to &cpl_total;
				
					%if &&cpln&i. ne 0 %then %do;

						/** data - importing wildcard files **/
						data version1 ;
						length version $30000. ;
						infile "&eoms_consolidate.&SYS_DIR.&&cpl&i..*" firstobs=1 missover pad lrecl=32000 delimiter='=' dsd;
						input version; 
						if substr(version,1,7) in ('OD|cpl_', 'OP|cpl_') then delete; 
						run; 

						/** data - export earliest file names **/
						data _null_;
						set version1;
						file "&eoms_directory.&SYS_DIR.&&cplr&i."    lrecl=32000 ;
						put version	;
						run;
					
					%end;

				%end;


		%mend do_data_files;
		%do_data_files;


		proc sort data = eoms_today_cntl2 ;
		by name;
		run;

		data eoms_today_cntl2;
		set eoms_today_cntl2;
		ii=_n_;
		mod=mod(ii, 2);
		mod2=mod+ii;
		mod3=mod2/2;
		file_wildcard=scan(name,1,'.');
		file_wildcard=trim(left(file_wildcard))||'.'||trim(left(year))||trim(left(month))||trim(left(day));
		run;

		proc sort data = eoms_today_cntl2 ;
		by name;
		run;

		proc sort data =  eoms_today_cntl2  out =  eoms_today_cntl2b nodupkey;
		by file_wildcard;
		run; 
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - process the control files + create new file
		+---------------------------------------------------------------------------------------------*/
		%macro do_cntl_files;

				%let cpl_total = 0;
				%let cpln1=0;
				%let cpln2=0;
				%let cpln3=0;
				%let cpln4=0;				
				
				data _null_;
				set eoms_today_cntl2b ;
				call symput('cpl'||trim(left(mod3)),trim(left(file_wildcard)) );
				call symput('cplr'||trim(left(mod3)),trim(left(name)) );
				call symput('cpln'||trim(left(mod3)),trim(left(mod3)) );
				call symput('cpl_total',left(trim(mod3)));
				run;

				%put NOTE: cpl_total = &cpl_total. ; 

				%do i = 1 %to &cpl_total;
				
					%if &&cpln&i. ne 0 %then %do;

						/** data - importing wildcard files **/
						data version1;
						length version $30000. ;
						infile "&eoms_consolidate.&SYS_DIR.&&cpl&i..*" firstobs=1 missover pad lrecl=32000 delimiter='=' dsd;
						input version; 
						if substr(version,1,7) in ('OD|cpl_', 'OP|cpl_') ; 
						run;

						/*** get the earliest data file with the summation count **/
						proc sort data =  version1 ;
						by descending version;
						run;

						data version1;
						set version1 end=last;
						cnt=scan(version,3,'|');      /** <----------- get counts for each file **/
						retain sum_cnt;
						sum_cnt = sum(sum_cnt, cnt);  /** <----------- summation of counts **/
						x1=scan(version,1,'|');
						x2=scan(version,2,'|');
						x3=scan(version,3,'|');
						x4=scan(version,4,'|');
						x5=scan(version,5,'|');
						x6=scan(version,6,'|');
						x7=scan(version,7,'|');
						x8=scan(version,8,'|');
						x9=scan(version,9,'|');
						version_new=compress(trim(left(x1))||'|'||trim(left(x2))||'|'||trim(left(sum_cnt))||'|'||trim(left(x4))||'|'||trim(left(x5))||'|'||trim(left(x6))||'|'||trim(left(x7))||'|'||trim(left(x8))||'|'||trim(left(x9)));
						if last; /** <----------- remove later file + if more than 2 files **/
						run;

						data _null_;
						set version1;  /** <----------- export early file with summation of both files **/
						file "&eoms_directory.&SYS_DIR.&&cplr&i."    lrecl=32000 ;
						put version_new	;
						run;
					
					%end;

				%end;


		%mend do_cntl_files;
		%do_cntl_files;

	%end;
	%else %do;
		%put NOTE:  No CPL files to consolidate;
	%end;
	

%mend m_step0_eoms_consolidate;
