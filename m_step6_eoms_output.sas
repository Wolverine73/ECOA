
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step6_eoms_output
|
| LOCATION:  
| 
| PURPOSE:	Adhere to IT Standards for Opportunity + Opportunity RX + Disposition + Disposition RX 
|
| LOGIC:
|		step6 = sends data to IT Team = select * from dss_cea2.dqi_eoms_files where execution_date = current_date
|
| INPUT:     
|
| OUTPUT: 
|
|	select top 100 * from dss_cea2.dqi_eoms_opportunity where execution_date = current_date and oppty_src_cd = 'CPL';
|	select top 100 * from dss_cea2.dqi_eoms_dispositions where execution_date = current_date and oppty_src_cd = 'CPL';
|	select top 100 * from dss_cea2.dqi_eoms_cmctn_history_test where execution_date = current_date and source_team = 'CPL';
|	select top 100 * from dss_cea2.dqi_eoms_cmctn_history  where execution_date = current_date;
|	select top 100 * from dss_cea2.dqi_eoms_files  where execution_date = current_date;
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step6_eoms_output(team_id=, layout_id=);


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine files to process
	+---------------------------------------------------------------------------------------------*/
	%m_create_work_list(data_out=temp01_output); 
	
	data working.qc_step6a;
	x=1;
	run;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - history of valid + invalid opp and disp
	+---------------------------------------------------------------------------------------------*/
	proc sql noprint;
	connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
	create table eoms_output_files as
	select * from connection to tera
	( 			
		select distinct
		a.clm_evnt_gid, 
		a.clnt_cd, 
		coalesce(a.source_opp_file_metric,a.source_opprx_file_metric) as file_cnt, 
		b.eoms_row_gid,
		b.source_new_file as source_file, 
		b.execution_date,
		b.execution_sftp,
		c.source_new_file, 
		coalesce(c.execution_triage,'0_0_0_0_0') as execution_triage, 
		c.cnt
		from (select * from &c_s_table_name. where clm_evnt_gid < 5 and  execution_date = current_date - &execution_date.) a inner join
		     (select * from &c_s_table_files. where execution_date = current_date - &execution_date.) b 
		on coalesce(a.source_opp_file,a.source_opprx_file) = b.source_new_file left join
		(	select source_opp_file as source_new_file, max(execution_triage) as execution_triage, count(*) as cnt
			from &c_s_table_opp.
			where execution_date = current_date - &execution_date.
			group by 1 
			union
			select source_opprx_file as source_new_file, max(execution_triage) as execution_triage, count(*) as cnt
			from &c_s_table_opp.
			where execution_date = current_date - &execution_date.
			and rx_fill_evnt_id is not null
			group by 1 			
			union
			select source_opp_file as source_new_file, max(execution_triage) as execution_triage, count(*) as cnt
			from &c_s_table_disp.
			where execution_date = current_date - &execution_date.
			group by 1 
			union
			select source_opprx_file as source_new_file, max(execution_triage) as execution_triage, count(*) as cnt
			from &c_s_table_disp.
			where execution_date = current_date - &execution_date.
			and rx_fill_evnt_id is not null
			group by 1 			
		) c
		on b.source_new_file = c.source_new_file
		
		where b.execution_date = current_date - &execution_date.
		  and b.execution_sftp in ('PENDING','FAILURE')
	);
	disconnect FROM tera;
	quit;
	
	%let qc_step6 = 0;
	
	proc sql noprint;
	select count(*) into: qc_step6 separated by ''
	from eoms_output_files;
	quit;
	
	%put NOTE: qc_step6 = &qc_step6. ;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/
	%if &ecoa_test.	= YES6A %then %do;
	
		data working.qc_step6a;
		set eoms_output_files ;
		run;	
		
		%m_ecoa_process(do_tasks=ALL);	
		
	%end;
	
	%if &qc_step6 = 16 %then %do;
	%end;
	%else %do;
	
		%let c_s_email_subject = EA ECOA CPL SMFC Missing 16 Daily Files Failure;
		%m_abend_handler(abend_report=%str(EA ECOA - Missing 16 Daily Files - step 6),
			   abend_message=%str(EA ECOA - Missing 16 Daily Files));
	
	%end;
	
	
	data eoms_output_files;
	set eoms_output_files;
	data_file=upcase(scan(source_file,2,'.')); 
	source_team=scan(source_file,1,'_'); 
	if scan(scan(source_file,1,'.'),2,'_') = 'opportunity' then source_type='1'; 
	if scan(scan(source_file,1,'.'),2,'_') = 'disposition' then source_type='3';
	if scan(scan(source_file,1,'.'),2,'_') = 'rx' and scan(scan(source_file,1,'.'),3,'_') = 'opportunity' then source_type='2';
	if scan(scan(source_file,1,'.'),2,'_') = 'rx' and scan(scan(source_file,1,'.'),3,'_') = 'disposition' then source_type='4'; 
	if index(source_file,'opportunity') > 0 then subject_type='1'; 
	if index(source_file,'disposition') > 0 then subject_type='3';
	run;	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine valid + invalid opp and disp
	+---------------------------------------------------------------------------------------------*/	
	data eoms_output_files_valid   (keep = source_team data_file subject_type)
             eoms_output_files_invalid (keep = source_team data_file subject_type);
	set eoms_output_files;
	execution_triage2=compress(execution_triage,'_')*1; 
	if execution_triage2 > 9999 then output eoms_output_files_invalid;  /**<--------------------------------- primary rule = 1st position of 5 positions  **/ 
	else output eoms_output_files_valid;
	run;

	proc sort data = eoms_output_files;
	by source_team data_file subject_type;
	run;

	proc sort data = eoms_output_files_valid;
	by source_team data_file subject_type;
	run;

	proc sort data = eoms_output_files_invalid;
	by source_team data_file subject_type;
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - collect valid + invalid opp and disp for the file groups
	+---------------------------------------------------------------------------------------------*/
	data eoms_output_files2;
	format flag $20. ;
	merge eoms_output_files (in=a) eoms_output_files_valid (in=b) eoms_output_files_invalid (in=c);
	by source_team data_file subject_type;
	if a and c then flag='INVALID';
	else if a and b then flag='VALID';
	run;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create unique suffix for additional files from prior days or same day re-runs 
	|         IT will not process files if they are the same name due to the IT file process control table
	|	  the process will send the data with new suffix to IT which will allow the ETL process to load the data
	+---------------------------------------------------------------------------------------------*/
	data time;
	time=time();
	new_suffix=put(left(compress(time,'.')),6.);
	call symput('new_suffix',left(trim(new_suffix)));
	run;	

	%put NOTE: new_suffix = &new_suffix. ; 

	data eoms_output_files2;
	set eoms_output_files2;
	sort_id=scan(source_file,2,'.'); 
	run;

	proc sort data = eoms_output_files2 out = sort_id (keep = sort_id) nodupkey;
	by sort_id;
	run;

	data sort_id; 
	set sort_id ;
	suffix=put(left(&new_suffix. + _n_),6.);
	run;

	proc sort data = eoms_output_files2 ;
	by sort_id;
	run;

	proc sort data = sort_id ;
	by sort_id;
	run;

	data eoms_output_files2;
	merge eoms_output_files2 sort_id;
	by sort_id;
	run;

	%let cnt_files_invalid = 0;
	%let cnt_files_valid = 0;

	proc sql noprint; 
	select count(*) into: cnt_files_valid separated by ''
	from eoms_output_files2
	where flag='VALID';
	quit;	

	proc sql noprint; 
	select count(*) into: cnt_files_invalid separated by ''
	from eoms_output_files2 
	where flag='INVALID';
	quit;		

	%put NOTE: ************************************************************************;
	%put NOTE: cnt_files_valid   = &cnt_files_valid. ;
	%put NOTE: cnt_files_invalid = &cnt_files_invalid. ;
	%put NOTE: ************************************************************************;

 

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - valid CPL + SFMC opp and disp files
	+---------------------------------------------------------------------------------------------*/	
	%if &cnt_files_valid. ne 0 %then %do;  /**<----------------------- do cnt_files_valid start **/
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - create data files + trigger files + toucn file - zero records
		+---------------------------------------------------------------------------------------------*/ 	
		data temp_files; 
		format sas_file $100. ;
		set eoms_output_files2 ; 
		where flag = 'VALID';
		d=put(day(today() - &execution_date. ),z2.);
		m=put(month(today() - &execution_date. ),z2.);
		y=put(year(today() - &execution_date. ),4.);
		date=compress(y||m||d); 
		x=trim(left(substr(data_file,1,8)))||'.'||trim(left(substr(data_file,1,8)))||'.'||trim(left(substr(left(suffix),1,6)));
		if index(source_file,'CNTL') > 0 then cntl='CNTL';
		if index(source_file,'CNTL') > 0 then target_file=compress(scan(source_file,1,'.')||'.TRIGGER.'||x);
		else if index(source_file,'CNTL') = 0 then target_file=compress(scan(source_file,1,'.')||'.'||x);
		
		/**
		target_file=tranwrd(target_file,'rx_opportunity','opprx');  
		target_file=tranwrd(target_file,'rx_disposition','disprx'); 
		target_file=tranwrd(target_file,'opportunity','opp');  
		target_file=tranwrd(target_file,'disposition','disp'); 
		**/
		
		target_file=tranwrd(target_file,'rx_opportunity','opportunity_rx');  
		target_file=tranwrd(target_file,'rx_disposition','disposition_rx'); 
		target_file=tranwrd(target_file,'opportunity','opportunity');  
		target_file=tranwrd(target_file,'disposition','disposition');
		
		target_file=upcase(target_file);
		sas_file='load_'||trim(left(subject_type))||'_'||trim(left(data_file));
		run;
		
		/** align counts from NONCTL files since CNTL file header has discrepancy for CPL **/
		data temp_counts;
		set temp_files;
		file_cnt2=file_cnt;
		keep sas_file clm_evnt_gid file_cnt2 cntl ; 
		run;

		proc sort data = temp_counts nodupkey;
		by sas_file clm_evnt_gid cntl;
		run;

		proc sort data = temp_counts (drop= cntl) nodupkey;
		by sas_file clm_evnt_gid;
		run;

		proc sort data = temp_files;
		by sas_file clm_evnt_gid;
		run;

		data temp_files;
		merge temp_files  (in=a)
		      temp_counts (in=b);
		file_cnt=file_cnt2;
		by sas_file clm_evnt_gid;
		if a;
		drop file_cnt2;
		run;
		
		data _null_;
		set temp_files;
		put _all_;
		run;		
			
		data _null_;
		set temp_files;
		call symput('sascnt'||trim(left(_n_)),trim(left(file_cnt)));
		call symput('sasf'||trim(left(_n_)),trim(left(sas_file)));
		call symput('old'||trim(left(_n_)),trim(left(source_file)));
		call symput('new'||trim(left(_n_)),trim(left(target_file)));
		call symput('cntl'||trim(left(_n_)),trim(left(cntl)));
		call symput('src'||trim(left(_n_)),trim(left(source_type)));
		call symput('srcteam'||trim(left(_n_)),trim(left(source_team)));
		call symput('sbj'||trim(left(_n_)),trim(left(subject_type)));
		call symput('total',trim(left(_n_)));
		run; 
		
		proc sql noprint; 
		update &c_s_table_files. history
		set source_new_it_sftp_file = ( select target_file
						from temp_files xref
						where history.source_new_file = xref.source_file
						)
		where execution_date = today() - &execution_date. ;
		quit;


		%macro do_data_files;
			%do i = 1 %to &total;

				%if &&cntl&i ne CNTL %then %do;
				
					%put NOTE: old = NONCNTL - &&old&i ; 
					%put NOTE: old = NONCNTL counts - &&sascnt&i ;
					
					data template;
					set dss_cea2.dqi_eoms_layout ;
					where layout_id = &&src&i;
					run;

					proc sort data = template ;
					by sequence_id;
					run;

					proc sql noprint;
					select data_attribute_name into: keep_variables separated by ' '
					from template;
					quit;

					data x&i;
					set &&sasf&i;
					%if &&srcteam&i = sfmc %then %do;
						where lowcase(oppty_src_cd) in ( "&&srcteam&i", "dqi");
					%end;
					%else %do;
						where lowcase(oppty_src_cd) = "&&srcteam&i";
					%end;
					keep &keep_variables. ;
					run;


					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - break data apart by unique record IDs 
					+---------------------------------------------------------------------------------------------*/					
					%if &&src&i = 1 or &&src&i = 3 %then %do;
						proc sort data = x&i nodupkey;
						by oppty_evnt_id msg_evnt_id;
						run;
					%end;
					%if &&src&i = 2 or &&src&i = 4 %then %do;
						proc sort data = x&i nodupkey;
						by oppty_evnt_id msg_evnt_id rx_fill_evnt_id;
						run;
					%end;					

					proc sort data = template; by sequence_id;run;

					data _null_; 
					set template ;  
					call symput('name'||trim(left(_n_)),trim(left(data_attribute_name))); 
					call symput('total_name',trim(left(_n_)));
					run;

	
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - 
					|  1. update not last lines to 0A or LF since it is required by IT ETL
					|  2. update last line to 2 since it will concatenate to the REC_UPD_USER_ID value 
					|  3. NOTE - if i do a 0A on last line then cursor goes to next line which 
					|     causes issues in the IT ETL
					|
					+---------------------------------------------------------------------------------------------*/					
				
					%if &&sascnt&i = 0 %then %do;
						data _null_;
						file "&eoms_IT.&SYS_DIR.&&new&i" lrecl =3000; 
						run;					
					%end;
					%else %do;
 		
						data _null_;   
						file "&eoms_IT.&SYS_DIR.&&new&i" dsd recfm=n lrecl=5000;
						set x&i end=last; 
						put 
						%do j=1 %to &total_name; 
							%if &j ne &total_name. %then %do;
							&&name&j. +(-1)'|~|' 
							%end;
							%else %do;
							&&name&j. 
							%end;
						%end;;
						if not last then put +(-1) '0A'x;
						if     last then put +(-1) '2'  ;
						run;						
					
					%end;
					
				%end;
				%else %if &&cntl&i = CNTL %then %do;


					%put NOTE: old = CNTL - &&old&i ;
					%put NOTE: old = CNTL counts - &&sascnt&i ;

					data x&i;
					length x $5000 ;
					infile "&eoms_directory.&SYS_DIR.&&old&i" lrecl =3000;
					input x ; 
					cnt=scan(x,3,'|'); 
					cnt="&&sascnt&i"; 
					
					t1="EA";
					t2="&&old&i";
					t2=scan(t2,2,'.');
					t3="&&new&i";
					/**t3=compress(t3,'TRIGGER'); **/
					t3=tranwrd(t3,'TRIGGER','');
					t3=compress(t3,' ');
					t3=tranwrd(t3,'..','.');
					t4='I';
					t5='0';
					%if &&sascnt&i = 0 %then %do;
						t6='0';    /**<----------------------------- zero count **/
					%end;
					%else %do;
						t6=cnt;    /**<----------------------------- file count **/
					%end;
					t7=substr(t2,1,8);
					t8=substr(t2,1,8);
					t9='0';
					t10='0';
					t11='DWV_CAMPGN'; 
					file "&eoms_IT.&SYS_DIR.&&new&i" lrecl =3000;
					put @1 t1 @21 t2 @36 t3 @77 t4 @79 t5 @85 t6 @106 t7 @115 t8 @124 t9 @140 t10 @156 t11  ;
					run; 
					
				%end;

			%end;
		%mend do_data_files; 
		%do_data_files;		
		
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - touch files
		+---------------------------------------------------------------------------------------------*/
		data _null_; 
		set temp_files ; 
		where index(target_file,'TRIGGER') > 0;
		xxx=compress(scan(target_file,1,'.')||'.TCH');
		call symput('new'||trim(left(_n_)),trim(left(xxx)));
		call symput('total',trim(left(_n_)));
		run;


		%macro do_touch_files;
			%do i = 1 %to &total;

				%put NOTE: old = &&old&i ; 

				data x; 
				file "&eoms_IT.&SYS_DIR.&&new&i" lrecl =3000; 
				run;

			%end;
		%mend do_touch_files; 
		%do_touch_files; 
  

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - update valid opp and disp - changed format with put since was converting to exponential
		+---------------------------------------------------------------------------------------------*/
		%let eoms_row_gid = 0;

		proc sql noprint;
		select distinct put(coalesce(eoms_row_gid,0),18.) into: eoms_row_gid separated by ','
		from eoms_output_files2 
		where flag='VALID';
		quit;

		%put NOTE: eoms_row_gid = &eoms_row_gid. ;

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			update &c_s_table_files.
			set execution_sftp = 'SUCCESS'
			where eoms_row_gid in (&eoms_row_gid. )

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - test - halt before SFTP to IT
		|
		+---------------------------------------------------------------------------------------------*/		
		%if &ecoa_test.	= YES6B %then %do;

			data working.qc_step6b;
			x=1 ;
			run;	
			
			%m_ecoa_process(do_tasks=ALL);	

		%end;
		%else %do;

			data working.qc_step6a;
			x=1;
			run;			

		%end;		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - sftp data files + trigger files + toucn file
		|  STEP - move sftp data files + trigger files + toucn file to archive
		+---------------------------------------------------------------------------------------------*/
		%if &ecoa_test.	= YES6C %then %do;
		
			%m_ecoa_process(do_tasks=NOABEND);
			
		%end;
		%else %do;
			%if &c_s_it_send. = Y %then %do;

				%m_sftp_put2(get_directory=&get_etl_directory., put_directory=&put_etl_directory., 
					      put_file1=&sftp_put_file1., put_file2=&sftp_put_file2.); 

			%end;
		%end; 
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - email failure + assessment report
		+---------------------------------------------------------------------------------------------*/
		%let cnt_invalid = 0;
		
		proc sql noprint;
		select count(*) into: cnt_valid separated by ''
		from temp_files
		where flag = 'VALID' ;
		quit;	
		
		%put NOTE: cnt_valid = &cnt_valid. ;
		
		%if &cnt_valid. ne 0 %then %do;  /**<----------------------- do cnt_valid start **/

			%let cnt_valid = 0;
			
			proc sql noprint;
			select distinct upcase(source_team) into: zzz separated by ' ' 
			from temp_files
			where flag = 'VALID' ;
			quit;	
			
			%put NOTE: zzz = &zzz. ;
		
			%let z = 1 ;
			%let zz = &zzz.;
			%do %while (%length(%sysfunc(scan(&zz, &z, ' '))) ^= 0 ) ;  /**<----------------------- do while start **/

				%let src_team = %sysfunc(scan(&zz, &z, ' ')); 
				%let z = %eval(&z. +1);
				
				
				proc sql noprint;
				select distinct source_file into: files_valid separated by ','
				from temp_files
				where flag = 'VALID'
				and upcase(source_team) = %tslit(%upcase(&src_team.));
				quit;
				
				proc sql noprint;
				select distinct data_file into: sas_valid separated by ''
				from temp_files
				where flag = 'VALID'
				and upcase(source_team) = %tslit(%upcase(&src_team.));
				quit;				

				%put NOTE: src_team = &src_team. ;
				%put NOTE: sas_valid = &sas_valid. ;
				%put NOTE: files_valid = &files_valid. ;
				
				proc sql noprint;
				select count(*) into: cnt_op separated by ''
				from work.load_1_&sas_valid.
				where rule_total1 = 1;
				quit;
				
				proc sql noprint;
				select count(*) into: cnt_dp separated by ''
				from work.load_3_&sas_valid.
				where rule_total1 = 1;
				quit;				
				
				%put NOTE: cnt_op = &cnt_op;
				%put NOTE: cnt_dp = &cnt_dp;

				data dqi_eoms_opportunity ;
				%if &cnt_op. = 0 %then %do;
					set work.load_1_&sas_valid. ;**(obs=0);
				%end;
				%if &cnt_op. ne 0 %then %do;
					set work.load_1_&sas_valid.;
				%end;				
				run;

				proc sql noprint;
				select count(*) into: cnt_dp separated by ''
				from work.load_3_&sas_valid.
				where rule_total1 = 1;
				quit;
				
				%put NOTE: cnt_dp = &cnt_dp;
				
				data dqi_eoms_dispositions ; 
				%if &cnt_dp. = 0 %then %do;
					set work.load_3_&sas_valid. ;**(obs=0);
				%end;
				%if &cnt_dp. ne 0 %then %do;
					set work.load_3_&sas_valid.;
				%end;					
				run;	
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - keep required data elements
				+---------------------------------------------------------------------------------------------*/
				data template;
				set &c_s_table_opp. (obs=0);
				run;
				
				proc sql noprint; 
				select data_attribute_name into: keep_variables separated by ' ' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (1,2)
				and length(data_rules) > 1 ;
				quit;				

				proc contents data = template out = contents1 noprint;
				run;

				proc sql noprint;
				select name into: keep_variables separated by ' '
				from contents1;
				quit;		

				data dqi_eoms_opportunity ;
				set dqi_eoms_opportunity ;
				keep n rule: execution_triage source_opp_file source_opprx_file &keep_variables. ;
				run;			
				
				data template;
				set &c_s_table_disp. (obs=0);
				run;
				
				proc sql noprint; 
				select data_attribute_name into: keep_variables separated by ' ' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (3,4)
				and length(data_rules) > 1 ;
				quit;				

				proc contents data = template out = contents1 noprint;
				run;

				proc sql noprint;
				select name into: keep_variables separated by ' '
				from contents1;
				quit;

				data dqi_eoms_dispositions ;
				set dqi_eoms_dispositions ;
				keep n rule: execution_triage source_opp_file source_opprx_file &keep_variables. ;
				run;					
				
				data today;
				x='01MAR18'd; 
				x=today() - &execution_date. ;
				report_date=put(x,yymmddn8.);  
				call symput('report_date',compress(report_date)); 
				run; 

				proc sql noprint;
				create table dqi_eoms_rules as
				select *
				from dss_cea2.dqi_eoms_rules
				order by layout_id, rule_id;
				quit;
				
				data dqi_eoms_rules;
				retain EOMS_ROW_GID RULE_ID RULE_TYPE LAYOUT_ID	LAYOUT_DESCRIPTION SEQUENCE_ID 
				VLOOKUP	DATA_ATTRIBUTE_NAME DESCRIPTION	DATA_RULES DATA_RULES2 DATA_RULES_SEVERITY DATA_REQUIRED RULE_TYPE_DESCRIPTION;
				set dqi_eoms_rules;
				VLOOKUP='rule'||trim(left(RULE_ID));
				run;


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create email excel report of sample of valid records
				+---------------------------------------------------------------------------------------------*/
				%macro export_qc(team=, data=, sheetn=);
					data excel1;
					set &data. (obs=1000);
					run;
					
					proc export data = excel1  
					outfile = "&eoms_report.&SYS_DIR.Data Profiling Report - Success Files &team._&report_date..xlsx" 
					dbms=xlsx label replace; 
					sheet=&sheetn.; 
					run; 
				%mend export_qc; 


				data temp1;
				set dqi_eoms_opportunity;
				label 
				rule_total1='Primary QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total2='Secondary QC - Warning Data Elements (0=Success 1=Failure)'
				rule_total3='Tertiary QC - Note Data Elements (0=Success 1=Failure)'
				rule_total4='CMCTN History QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total5='Anthem MCS History QC - Critical Data Elements (0=Success 1=Failure)'
				;
				run;

				%export_qc(team=&src_team., data=temp1,  sheetn='Opportunity - Assessment'); 
				
				data temp2;
				set dqi_eoms_dispositions;
				label 
				rule_total1='Primary QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total2='Secondary QC - Warning Data Elements (0=Success 1=Failure)'
				rule_total3='Tertiary QC - Note Data Elements (0=Success 1=Failure)'
				rule_total4='CMCTN History QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total5='Anthem MCS History QC - Critical Data Elements (0=Success 1=Failure)'
				;
				run;
				
				%export_qc(team=&src_team., data=temp2, sheetn='Dispostion - Assessment'); 
				%export_qc(team=&src_team., data=dqi_eoms_rules,        sheetn='Data Profiling - Rules');				
				
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create zip file of deliverables
				+---------------------------------------------------------------------------------------------*/
				filename zipfile "&eoms_report./data_profiling_&src_team._&report_date..zip";

				data _null_;
				  if (fexist('zipfile')) then rc = fdelete('zipfile');
				run;
				
				data _null_;
				x cd &eoms_report.;
				x del *.bak;
				run;				

				filename zipfile clear; 

				data _null_;
				x cd &eoms_report.;
				x zip -r -9 -j "data_profiling_&src_team._&report_date." . -i \Data*Success*&src_team.*&report_date..xlsx @ -x \*.bak  @;
				run;				

										
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create email content
				+---------------------------------------------------------------------------------------------*/
				proc sql;
				connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
				create table process_files  as 
				select * from connection to tera
				(				
					select source_team, source_file as process_file, source_new_file, c as process_files_metric, execution_sftp
					from &c_s_table_files. a,
					(
					
						select coalesce(source_opp_file, source_opprx_file) as f, max(coalesce(source_opp_file_metric, source_opprx_file_metric)) as c 
						from &c_s_table_name.  
						where execution_date = current_date - &execution_date.
						group by 1					
					
					) b
					where a.source_new_file=b.f
					and a.execution_date = current_date - &execution_date.
					and execution_sftp = 'SUCCESS'
					and upper(source_team) = %tslit(%upcase(&src_team.))
					order by source_team, source_file

				);
				disconnect from tera;
				quit;				
				
				%let pf_total = 0;
				
				data _null_;
				set process_files end=eof;
				ii=left(put(_n_,4.)); 
				call symput('pf'||ii,trim(left(process_file)));
				call symput('pft'||ii,trim(left(source_team)));
				call symput('sfn'||ii,trim(left(source_new_file)));
				call symput('pfa'||ii,trim(left(execution_sftp)));
				call symput('pfm'||ii,trim(left(process_files_metric)));
				if eof then call symput('pf_total',ii);
				run;	
				
				data today;
				x='01MAR18'd; 
				x=today() - &execution_date. ;
				report_date=put(x,yymmddn8.);  
				call symput('report_date',compress(report_date)); 
				run;				
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email report
				+---------------------------------------------------------------------------------------------*/
				proc sql noprint;
				create table report_email as 
				select 	source_team label = 'Source Team',
					process_file label = 'Source File',
					source_new_file label = 'CTT Source File',  
					execution_sftp label = 'Source File - Status',
					process_files_metric label = 'Source File - Counts'	 
				from process_files;
				quit;

				proc format;
				 value csadapt low-0='#ECEDEC '   
					       0<-high='#E5B8D0';
				run; 		

				filename report "%sysfunc(pathname(work))/report_email2.html"; 

				title;
				footnote;

				ods html file=report;
					proc print data=report_email style(header)=[font_face='Arial Narrow'] style(table)={just=l bordercolor=blue} obs label; 
					var	source_team 
						process_file   
						source_new_file      
						execution_sftp  ;	
					var 	process_files_metric / style={BACKGROUND=csadapt.  }; 
					run;
				ods html close;	
		
				%if &c_s_dqi_campaign_id. = 80 %then %do;

					filename xemail email 
					to=(&c_s_email_to.)
					subject="CTT Data Profiling - SUCCESS PBM &src_team. File Processing &c_s_file_date. ***SECUREMAIL*** " 
					attach = (
					"&eoms_report./data_profiling_&src_team._&report_date..zip" 
					CONTENT_TYPE="APPLICATION/HTML" lrecl=32000
					) content_type="text/html";   
				%end;	
				%if &c_s_dqi_campaign_id. = 880 %then %do;
					filename xemail email 
					to=(&c_s_email_to.)
					subject="CTT Data Profiling - SUCCESS PBM &src_team. File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  
				%end; 				

				options noquotelenmax;
				
				
				data _null_;
				%if &pf_total. = 0 %then %do;  
				%end;
				%else %do; 			
					infile report end=eof;
					input;					 
				%end;				
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
					if _n_=1 then do;

						put '<br>'; 
						put "The CTT Data Profiling PBM files processing for &src_team. files was successful. ";
						put '<br>';
						put '<br>'; 
						put "Source Team(s):   &src_team.  ";
						put '<br>';
						put '<br>'; 					

						%if &pf_total. = 0 %then %do; 
							put '<br>';
							put '<b><font color="#13478C">';put '<u>'; put "Targeting PBM Files Information:"; put '</b></font>';put '</u>'; put '<br>';
							put "<ul>"; 	
							put "<br>";					
						%end;	
						%else %do;
							put '<br>';
							put '<b><font color="#13478C">';put '<u>'; put "Targeting PBM Files Information:"; put '</b></font>';put '</u>'; put '<br>'; 
							put "<ul>"; 					 
							put "<br>";
						%end;	

					end;	

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - email report
					+---------------------------------------------------------------------------------------------*/
					%if &pf_total. = 0 %then %do; 
						put "<li> 	Files processed successfully: No Files Available Today "; 		put '</li>'; 
					%end;
					%else %do; 			
						if _infile_ ne '</html>' then put _infile_;
					%end;

 
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - email closing
					+---------------------------------------------------------------------------------------------*/
					%if &pf_total. = 0 %then %do; 
							put "</ul>";
							put "<br>";	
							put "Thank you and have a great week."; put '<br>';
							put '<br>';
							put "Sincerely,"; put '<br>';
							put "EA - Campaign Targeting Team"; put '<br>';
							put " "; put '<br>';						
					%end;
					%else %do; 			
						if eof then do;  
							put "</ul>";
							put "<br>";	
							put "Thank you and have a great week."; put '<br>';
							put '<br>';
							put "Sincerely,"; put '<br>';
							put "EA - Campaign Targeting Team"; put '<br>';
							put " "; put '<br>';		
						end;	
					%end;						

				run;				
				

			%end;  /**<----------------------- do while end **/
			
		%end;  /**<----------------------- do cnt_valid end **/
				    
	%end;  /**<----------------------- do cnt_files_valid end **/



	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - invalid CPL + SFMC opp and disp files
	+---------------------------------------------------------------------------------------------*/	
	%if &cnt_files_invalid. ne 0 %then %do;  /**<----------------------- do cnt_files_invalid start **/
	

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - update invalid opp and disp
		+---------------------------------------------------------------------------------------------*/		
		%let eoms_row_gid = 0;

		proc sql noprint;
		select distinct put(coalesce(eoms_row_gid,0),18.) into: eoms_row_gid separated by ','
		from eoms_output_files2 
		where flag='INVALID';
		quit;

		%put NOTE: eoms_row_gid = &eoms_row_gid. ;

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			update &c_s_table_files.
			set execution_sftp = 'REJECTED'
			where eoms_row_gid in (&eoms_row_gid. )

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;	

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - create data files + trigger files + toucn file - zero records
		+---------------------------------------------------------------------------------------------*/ 	
		data temp_files; 
		format sas_file $100. ;
		set eoms_output_files2 ; 
		where flag = 'INVALID';
		x=trim(left(substr(data_file,1,8)))||'.'||trim(left(substr(data_file,1,8)))||'.'||trim(left(substr(left(suffix),1,6))); 
		if index(source_file,'CNTL') > 0 then cntl='CNTL';
		if index(source_file,'CNTL') > 0 then target_file=compress(scan(source_file,1,'.')||'.TRIGGER.'||x);
		else if index(source_file,'CNTL') = 0 then target_file=compress(scan(source_file,1,'.')||'.'||x);

		/**
		target_file=tranwrd(target_file,'rx_opportunity','opprx');  
		target_file=tranwrd(target_file,'rx_disposition','disprx'); 
		target_file=tranwrd(target_file,'opportunity','opp');  
		target_file=tranwrd(target_file,'disposition','disp'); 
		**/
		
		target_file=tranwrd(target_file,'rx_opportunity','opportunity_rx');  
		target_file=tranwrd(target_file,'rx_disposition','disposition_rx'); 
		target_file=tranwrd(target_file,'opportunity','opportunity');  
		target_file=tranwrd(target_file,'disposition','disposition');

		target_file=upcase(target_file);
		sas_file='load_'||trim(left(source_type))||'_'||trim(left(data_file));
		call symput('sasf'||trim(left(_n_)),trim(left(sas_file)));
		call symput('old'||trim(left(_n_)),trim(left(source_file)));
		call symput('new'||trim(left(_n_)),trim(left(target_file)));
		call symput('cntl'||trim(left(_n_)),trim(left(cntl)));
		call symput('src'||trim(left(_n_)),trim(left(source_type)));
		call symput('sbj'||trim(left(_n_)),trim(left(subject_type)));
		call symput('total',trim(left(_n_)));
		run; 
		
		proc sql noprint; 
		update &c_s_table_files. history
		set source_new_it_sftp_file = ( select target_file
						from temp_files xref
						where history.source_new_file = xref.source_file
						)
		where execution_date = today() - &execution_date. ;
		quit;		


		%macro do_data_files;
			%do i = 1 %to &total;
			
				%if &&cntl&i ne CNTL %then %do;
				
					%put NOTE: old = &&old&i ; 
					
					data _null_;
					file "&eoms_IT.&SYS_DIR.&&new&i" lrecl =3000; 
					run;
					
				%end;			
				%else %if &&cntl&i = CNTL %then %do;
				
					%put NOTE: old = &&old&i ; 

					data y&i;
					length x $5000 ;
					infile "&eoms_directory.&SYS_DIR.&&old&i" lrecl =3000;
					input x ; 
					cnt=scan(x,3,'|'); 
					t1="EA";
					t2="&&old&i";
					t2=scan(t2,2,'.');
					t3="&&new&i"; 
					/**t3=compress(t3,'TRIGGER'); **/
					t3=tranwrd(t3,'TRIGGER','');
					t3=compress(t3,' ');
					t3=tranwrd(t3,'..','.');								
					t4='I';
					t5='0';
					t6='0';
					t7=substr(t2,1,8);
					t8=substr(t2,1,8);
					t9='0';
					t10='0';
					t11='DWV_CAMPGN'; 
					file "&eoms_IT.&SYS_DIR.&&new&i" lrecl =3000;
					put @1 t1 @21 t2 @36 t3 @77 t4 @79 t5 @85 t6 @106 t7 @115 t8 @124 t9 @140 t10 @156 t11  ;
					run;
					
				%end;

			%end;
		%mend do_data_files; 
		%do_data_files;
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - touch files - zero records
		+---------------------------------------------------------------------------------------------*/
		data _null_; 
		set temp_files ; 
		where index(target_file,'TRIGGER') > 0;
		xxx=compress(scan(target_file,1,'.')||'.TCH');  
		call symput('new'||trim(left(_n_)),trim(left(xxx)));
		call symput('total',trim(left(_n_)));
		run;


		%macro do_touch_files;
			%do i = 1 %to &total;

				%put NOTE: old = &&old&i ; 

				data x; 
				file "&eoms_IT.&SYS_DIR.&&new&i" lrecl =3000; 
				run;

			%end;
		%mend do_touch_files; 
		%do_touch_files; 		
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - sftp data files + trigger files + toucn file - zero records
		+---------------------------------------------------------------------------------------------*/
		%if &ecoa_test.	= YES6C %then %do;
		
			%m_ecoa_process(do_tasks=NOABEND);
			
		%end;
		%else %do;
			%if &c_s_it_send. = Y %then %do;

				%m_sftp_put2(get_directory=&get_etl_directory., put_directory=&put_etl_directory., 
					      put_file1=&sftp_put_file1., put_file2=&sftp_put_file2.);

			%end;
		%end;
			    
			    
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - email failure + assessment report
		+---------------------------------------------------------------------------------------------*/
		%let cnt_invalid = 0;
		
		proc sql noprint;
		select count(*) into: cnt_invalid separated by ''
		from temp_files
		where flag = 'INVALID' ;
		quit;	
		
		%put NOTE: cnt_invalid = &cnt_invalid. ;
		
		%if &cnt_invalid. ne 0 %then %do;  /**<----------------------- do cnt_invalid start **/

			%let cnt_invalid = 0;
						
			proc sql noprint;
			select distinct upcase(source_team) into: zzz separated by ' ' 
			from temp_files
			where flag = 'INVALID' ;
			quit;	
			
			%put NOTE: zzz  = &zzz. ; 
		
			%let z = 1 ;
			%let zz = &zzz.;
			%do %while (%length(%sysfunc(scan(&zz, &z, ' '))) ^= 0 ) ;  /**<----------------------- do while start **/

				%let src_team = %sysfunc(scan(&zz, &z, ' '));  
				%let z = %eval(&z. +1);

				proc sql noprint;
				select distinct source_file into: files_invalid separated by ','
				from temp_files
				where flag = 'INVALID'
				and upcase(source_team) = %tslit(%upcase(&src_team.));
				quit;
				
				proc sql noprint;
				select distinct data_file into: sas_invalid separated by ''
				from temp_files
				where flag = 'INVALID'
				and upcase(source_team) = %tslit(%upcase(&src_team.));
				quit;				

				%put NOTE: src_team = &src_team. ;
				%put NOTE: sas_invalid = &sas_invalid. ;
				%put NOTE: files_invalid = &files_invalid. ;
				
				proc sql noprint;
				select count(*) into: cnt_op separated by ''
				from work.load_1_&sas_invalid.
				where rule_total1 = 1;
				quit;
				
				proc sql noprint;
				select count(*) into: cnt_dp separated by ''
				from work.load_3_&sas_invalid.
				where rule_total1 = 1;
				quit;				
				
				%put NOTE: cnt_op = &cnt_op;
				%put NOTE: cnt_dp = &cnt_dp;

				data dqi_eoms_opportunity ;
				%if &cnt_op. = 0 %then %do;
					set work.load_1_&sas_invalid. (obs=0);
				%end;
				%if &cnt_op. ne 0 %then %do;
					set work.load_1_&sas_invalid.;
				%end;				
				run;

				proc sql noprint;
				select count(*) into: cnt_dp separated by ''
				from work.load_3_&sas_invalid.
				where rule_total1 = 1;
				quit;
				
				%put NOTE: cnt_dp = &cnt_dp;
				
				data dqi_eoms_dispositions ; 
				%if &cnt_dp. = 0 %then %do;
					set work.load_3_&sas_invalid. (obs=0);
				%end;
				%if &cnt_dp. ne 0 %then %do;
					set work.load_3_&sas_invalid.;
				%end;					
				run;	
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - keep required data elements
				+---------------------------------------------------------------------------------------------*/
				data template;
				set &c_s_table_opp. (obs=0);
				run;
				
				proc contents data = template out = contents1 noprint;
				run;

				proc sql noprint;
				select name into: keep_variables separated by ' '
				from contents1;
				quit;					
				
				proc sql noprint; 
				create table template as
				select sequence_id, data_attribute_name, rule_id 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (1,2)
				and data_rules_severity = '1' 
				and length(data_rules) > 1 
				order by sequence_id;
				quit; 
				
				data template;
				format rules $20. ;
				set template;
				rules='rule'||left(trim(rule_id));
				run;
				
				proc sql noprint;
				select data_attribute_name into: keep_variables separated by ' '
				from template;
				quit;	
				
				proc sql noprint;
				select rules into: keep_rules separated by ' '
				from template;
				quit;	 		

				data dqi_eoms_opportunity ;
				set dqi_eoms_opportunity ;
				keep n rule_total1 rule_total2 rule_total3 rule_total4 rule_total5
				&keep_rules. execution_triage source_opp_file source_opprx_file &keep_variables. ;
				run;	
				
				proc sql noprint;
				select rules into: keep_rules separated by ','
				from template;
				quit;

				proc sql noprint;
				create table dqi_eoms_opportunity_summ as
				select &keep_rules., count(*) as cnt
				from dqi_eoms_opportunity 
				group by &keep_rules. ;
				quit;
				
				data template;
				set &c_s_table_disp. (obs=0);
				run;
				
				proc sql noprint; 
				select data_attribute_name into: keep_variables separated by ' ' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (3,4)
				and length(data_rules) > 1 ;
				quit;				

				proc contents data = template out = contents1 noprint;
				run;

				proc sql noprint;
				select name into: keep_variables separated by ' '
				from contents1;
				quit;

				data dqi_eoms_dispositions ;
				set dqi_eoms_dispositions ;
				keep n rule: execution_triage source_opp_file source_opprx_file &keep_variables. ;
				run;					
					
				data today;
				x='01MAR18'd; 
				x=today() - &execution_date. ;
				report_date=put(x,yymmddn8.);  
				call symput('report_date',compress(report_date)); 
				run; 

				proc sql noprint;
				create table dqi_eoms_rules as
				select *
				from dss_cea2.dqi_eoms_rules
				order by layout_id, rule_id;
				quit;
				
				data dqi_eoms_rules;
				retain EOMS_ROW_GID RULE_ID RULE_TYPE LAYOUT_ID	LAYOUT_DESCRIPTION SEQUENCE_ID 
				VLOOKUP	DATA_ATTRIBUTE_NAME DESCRIPTION	DATA_RULES DATA_RULES2 DATA_RULES_SEVERITY DATA_REQUIRED RULE_TYPE_DESCRIPTION;
				set dqi_eoms_rules;
				VLOOKUP='rule'||trim(left(RULE_ID));
				run;				

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create email excel report of sample of invalid records
				+---------------------------------------------------------------------------------------------*/
				%macro export_qc(team=, data=, sheetn=);
				
					%if &data. = dqi_eoms_rules or &data. = dqi_eoms_opportunity_summ %then %do;
						data excel2;
						set &data. ;
						run;						
					%end;
					%else %do;
						data excel2;
						set &data. ;
						where rule_total1 = 1;  /**<---------------- create report of severity =1 **/
						run;				

						data excel2;
						set excel2 (obs=2000);
						run;
					%end;
					
					proc export data = excel2
					outfile = "&eoms_report.&SYS_DIR.Data Profiling Report - Rejected Files &team._&report_date..xlsx"  
					dbms=xlsx label replace; 
					sheet=&sheetn.; 
					run; 
					
				%mend export_qc; 


				data temp1;
				set dqi_eoms_opportunity;
				label 
				rule_total1='Primary QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total2='Secondary QC - Warning Data Elements (0=Success 1=Failure)'
				rule_total3='Tertiary QC - Note Data Elements (0=Success 1=Failure)'
				rule_total4='CMCTN History QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total5='Anthem MCS History QC - Critical Data Elements (0=Success 1=Failure)'
				;
				run;

				%export_qc(team=&src_team., data=temp1,                     sheetn='Opportunity - Assessment'); 
				%export_qc(team=&src_team., data=dqi_eoms_opportunity_summ, sheetn='Opportunity - Summary'); 
				
				
				data temp2;
				set dqi_eoms_dispositions;
				label 
				rule_total1='Primary QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total2='Secondary QC - Warning Data Elements (0=Success 1=Failure)'
				rule_total3='Tertiary QC - Note Data Elements (0=Success 1=Failure)'
				rule_total4='CMCTN History QC - Critical Data Elements (0=Success 1=Failure)'
				rule_total5='Anthem MCS History QC - Critical Data Elements (0=Success 1=Failure)'
				;
				run;
				
				%export_qc(team=&src_team., data=temp2,          sheetn='Dispostion - Assessment'); 
				%export_qc(team=&src_team., data=dqi_eoms_rules, sheetn='Data Profiling - Rules');
				
				%**dont_do_report1;
			

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create email variables - opportunity
				+---------------------------------------------------------------------------------------------*/			
				proc sql noprint; 
				select 'rule'||left(trim(put(rule_id,8.))) into: sql_variables separated by ',' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (1,2)
				and data_rules_severity = '1'
				and length(data_rules) > 1 ;
				quit;			

				proc sql noprint;
				create table temp_001 as
				select &sql_variables. , count(*) as cnt
				from temp1
				group by &sql_variables. ;
				quit;

				proc sort data = temp_001;
				by cnt;
				run;				

				proc transpose data = temp_001 out = temp_002;
				by cnt;
				var _all_;
				run;

				data temp_002;
				set temp_002;
				if COL1=1;
				rule_id=substr(_name_,5)*1;
				run; 

				%let rule_id=0;
				
				proc sql noprint; 
				select rule_id into: rule_id separated by ',' 
				from temp_002;
				quit;

				%let email_name1 = no opportunity variables;
				
				proc sql noprint; 
				select data_attribute_name into: email_name1 separated by ', ' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (1,2)
				and data_required = 'Y'
				and length(data_rules) > 1
				and data_rules_severity = '1'
				and rule_id in (&rule_id. );
				quit;

				%put NOTE: email_name1 = &email_name1. ;				
				
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create email variables - disposition
				+---------------------------------------------------------------------------------------------*/			
				proc sql noprint; 
				select 'rule'||left(trim(put(rule_id,8.))) into: sql_variables separated by ',' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (3,4)
				and data_required = 'Y'
				and length(data_rules) > 1 ;
				quit;

				proc sql noprint;
				create table temp_003 as
				select &sql_variables. , count(*) as cnt
				from temp2
				group by &sql_variables. ;
				quit;

				proc sort data = temp_003;
				by cnt;
				run;

				proc transpose data = temp_003 out = temp_004;
				by cnt;
				var _all_;
				run;

				data temp_004;
				set temp_004;
				if COL1=1;
				rule_id=substr(_name_,5)*1;
				run;
				
				%let rule_id=0;
				
				proc sql noprint; 
				select rule_id into: rule_id separated by ',' 
				from temp_004;
				quit;

				%let email_name2 = no disposition variables;
				
				proc sql noprint; 
				select data_attribute_name into: email_name2 separated by ', ' 
				from dss_cea2.dqi_eoms_rules
				where layout_id in (3,4)
				and data_required = 'Y'
				and length(data_rules) > 1 
				and rule_id in (&rule_id. );
				quit;

				%put NOTE: email_name2 = &email_name2. ;	
				
				
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create email content
				+---------------------------------------------------------------------------------------------*/
				proc sql;
				connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
				create table process_files  as 
				select * from connection to tera
				(				
					select source_team, source_file as process_file, source_new_file, c as process_files_metric, execution_sftp
					from &c_s_table_files. a,
					(
						select coalesce(source_opp_file, source_opprx_file) as f, max(coalesce(source_opp_file_metric, source_opprx_file_metric)) as c 
						from &c_s_table_name. 
						where execution_date = current_date - &execution_date.
						group by 1
					
					) b
					where a.source_new_file=b.f
					and a.execution_date = current_date - &execution_date.
					and execution_sftp = 'REJECTED'
					and upper(source_team) = %tslit(%upcase(&src_team.))
					order by source_team, source_file

				);
				disconnect from tera;
				quit; 	

				%let pf_total = 0;
				
				data _null_;
				set process_files end=eof;
				ii=left(put(_n_,4.)); 
				call symput('pf'||ii,trim(left(process_file)));
				call symput('pft'||ii,trim(left(source_team)));
				call symput('sfn'||ii,trim(left(source_new_file)));
				call symput('pfa'||ii,trim(left(execution_sftp)));
				call symput('pfm'||ii,trim(left(process_files_metric)));
				if eof then call symput('pf_total',ii);
				run;	
				
				data today;
				x='01MAR18'd; 
				x=today() - &execution_date. ;
				report_date=put(x,yymmddn8.);  
				call symput('report_date',compress(report_date)); 
				run; 
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email report
				+---------------------------------------------------------------------------------------------*/
				proc sql noprint;
				create table report_email as 
				select 	source_team label = 'Source Team',
					process_file label = 'Source File',
					source_new_file label = 'CTT Source File',  
					execution_sftp label = 'Source File - Status',
					process_files_metric label = 'Source File - Counts'	 
				from process_files;
				quit;

				proc format;
				 value csadapt low-0='#ECEDEC '   
					       0<-high='#E5B8D0';
				run; 		

				filename report "%sysfunc(pathname(work))/report_email2.html"; 

				title;
				footnote;

				ods html file=report;
					proc print data=report_email style(header)=[font_face='Arial Narrow'] style(table)={just=l bordercolor=blue} obs label; 
					var	source_team 
						process_file   
						source_new_file      
						execution_sftp  ;	
					var 	process_files_metric / style={BACKGROUND=csadapt.  }; 
					run;
				ods html close;	
				

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - create zip file of deliverables
				+---------------------------------------------------------------------------------------------*/
				filename zipfile "&eoms_report.&SYS_DIR.data_profiling_&src_team._&report_date..zip";

				data _null_;
				  if (fexist('zipfile')) then rc = fdelete('zipfile');
				run;

				filename zipfile clear; 

				data _null_;
				x cd &eoms_report.;
				x del *.bak;
				run;
				
				data _null_;
				x cd &eoms_report.;
				x zip -r -9 -j "data_profiling_&src_team._&report_date." . -i \Data*Rejected*&src_team.*&report_date..xlsx @ -x \*.bak  @;
				run;
				
				%if &c_s_dqi_campaign_id. = 80 %then %do;

					filename xemail email 
					to=(&c_s_email_to.)
					subject="CTT Data Profiling - FAILURE Rejected Files &src_team. &c_s_file_date. ***SECUREMAIL*** " 
					attach = (
					"&eoms_report./data_profiling_&src_team._&report_date..zip" 
					CONTENT_TYPE="APPLICATION/HTML" lrecl=32000
					) content_type="text/html";   
				%end;

				options noquotelenmax;
				
				data _null_;
				%if &pf_total. = 0 %then %do;  
				%end;
				%else %do; 			
					infile report end=eof;
					input;					 
				%end;
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
					if _n_=1 then do;

						put '<br>'; 
						put "The CTT Data Profiling PBM file processing have failed and the &src_team. files were rejected. ";
						put '<br>';
						put '<br>';
						put "Please resolve the data gaps and resend the data within the subsequent Opportunity or Dispositon files. ";
						put '<br>';
						put '<br>';
						put "Steps to reviewing the Data Profile Report: ";
						put '<br>';
						put '<br>';
						put "<ul>"; 																
							put "<li>    Step 1 - Open excel"; put '</li>';
							put "<li>    Step 2 - Columns A-E: Rule Total1=Primary/Critcal, Rule Total2=Secondary/Warning, Rule Total3=Tertiary/Notes";put '</li>';
							put "<li>    Step 3 - To determine Opportunity columns being validated - go to Data Profiling - Rules and filter on layout_id = 1,2 + data_rules <> blanks";put '</li>';
							put "<li>    Step 4 - To determine Disposition columns being validated - go to Data Profiling - Rules and filter on layout_id = 3,4 + data_rules <> blanks";put '</li>';
							put "<li>    Step 5 - DATA_RULES_SEVERITY: 1=Primary/Critcal, 2=Secondary/Warning, 3=Tertiary/Notes";put '</li>';
							put "<li>    Step 6 - Add Header Column Vlookup to Report";put '</li>';
							put "<li>         6a. insert row at row 1 of Opportunity - Assessment";put '</li>';
							put "<li>         6b. Paste formula in row 1  above the rule1-ruleN columns =VLOOKUP(F2,'Data Profiling - Rules'!$G$2:$H$2000,2,FALSE)"; put '</li>';
							put "<li>         6c. insert row at row 1 of Disposition - Assessment";put '</li>';
							put "<li>         6d. Paste formula in row 1 above the rule1-ruleN columns =VLOOKUP(F2,'Data Profiling - Rules'!$G$2:$H$2000,2,FALSE)"; put '</li>';					
							put "<li>    Step 7 - Opportunity Data Elements Invalid - &email_name1.";put '</li>';
							put "<li>    Step 8 - Disposition Data Elements Invalid - &email_name2.";put '</li>';
						put "</ul>";
						put '<br>'; 
						put '<br>';				

						%if &pf_total. = 0 %then %do; 
							put '<br>';
							put '<b><font color="#13478C">';put '<u>'; put "Files processed successfully: No Files Available Today - &report_date."; put '</b></font>';put '</u>'; put '<br>';
							put "<ul>"; 	
							put "<br>";					
						%end;	
						%else %do;
							put '<br>';
							put '<b><font color="#13478C">';put '<u>'; put "Data profiling of CPL + SFMC rejected files is below for &report_date. ."; put '</b></font>';put '</u>'; put '<br>'; 
							put "<ul>"; 					 
							put "<br>";
						%end;	

					end;	

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - email report
					+---------------------------------------------------------------------------------------------*/
					%if &pf_total. = 0 %then %do; 
						put "<li> 	Files processed successfully: No Files Available Today "; 		put '</li>'; 
					%end;
					%else %do; 			
						if _infile_ ne '</html>' then put _infile_;
					%end;
			
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - email closing
					+---------------------------------------------------------------------------------------------*/
					%if &pf_total. = 0 %then %do; 
							put "</ul>";
							put "<br>";	
							put "Thank you and have a great week."; put '<br>';
							put '<br>';
							put "Sincerely,"; put '<br>';
							put "EA - Campaign Targeting Team"; put '<br>';
							put " "; put '<br>';						
					%end;
					%else %do; 			
						if eof then do;  
							put "</ul>";
							put "<br>";	
							put "Thank you and have a great week."; put '<br>';
							put '<br>';
							put "Sincerely,"; put '<br>';
							put "EA - Campaign Targeting Team"; put '<br>';
							put " "; put '<br>';		
						end;	
					%end;	

				run;					


				

			%end;  /**<----------------------- do while end **/
			
		%end;  /**<----------------------- do cnt_invalid end **/
		
	%end;  /**<----------------------- do cnt_files_invalid end **/


	proc sql;
	connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
	execute(  

		update &c_s_tdtempx..dqi_eoms_files_process
		set execution_triage = 'COMPLETE'
		where execution_date = current_date

	) by tera;
	execute (commit work) by tera;  
	disconnect from tera;
	quit; 	
	

	data working.qc_step6b;
	x=1 ;
	run;	
	
	
	data _null_;				
	x "cd &eoms_IT.";
	x "mv &eoms_IT/*  &eoms_IT./archive";
	run; 	


%mend m_step6_eoms_output;
