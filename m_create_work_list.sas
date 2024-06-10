%macro m_create_work_list(data_out=);

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - process sas datasets from m_intake_form_eoms
	+---------------------------------------------------------------------------------------------*/
	proc datasets lib = work nolist; 
	contents data=_all_ out= temp01 (keep = memname) noprint; 
	quit; 
	run;

	proc sort data = temp01 nodupkey;
	by memname;
	run;

	data temp01;
	format source_type source_team $30. ;
	set temp01;
	if lowcase(scan(memname,1,'_')) in ('sfmc', 'cpl') and scan(memname,2,'_') in ('1','2','3','4');
	if index(memname,'CNTL') = 0;
	data_file=scan(memname,3,'_');
	source_team=upcase(scan(memname,1,'_'));
	source_type=scan(memname,2,'_');
	run;

	proc sort data = temp01 out = temp02 nodupkey;
	by source_team source_type memname;
	run;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - history = loaded for cmctn history + opportunity + disposition 
	+---------------------------------------------------------------------------------------------*/
	proc sql noprint;
	connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
	create table dqi_eoms_cmctn_history as
	select * from connection to tera
	( 
		select distinct  clnt_cd, source_team, source_opp_file as name
		from &c_s_table_name.
		where clm_evnt_gid > 10
		union
		select distinct  clnt_cd, source_team, source_opprx_file as name
		from &c_s_table_name.
		where clm_evnt_gid > 10	
		union			
		select distinct  'DISPOSITIONS' as clnt_cd, source_team, source_opp_file as name
		from &c_s_table_disp.			
		union			
		select distinct  'DISPOSITIONS' as clnt_cd, source_team, source_opprx_file as name
		from &c_s_table_disp.	
		union			
		select distinct  'OPPORTUNITY' as clnt_cd, source_team, source_opp_file as name
		from &c_s_table_opp.			
		union			
		select distinct  'OPPORTUNITY' as clnt_cd, source_team, source_opprx_file as name
		from &c_s_table_opp.		 		
	);
	disconnect FROM tera;
	quit;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - file key = source team + source type + file date
	+---------------------------------------------------------------------------------------------*/
	data dqi_eoms_cmctn_history;
	set dqi_eoms_cmctn_history;
	data_file=upcase(scan(name,2,'.')); 
	if scan(scan(name,1,'.'),2,'_') = 'opportunity' then source_type='1'; 
	if scan(scan(name,1,'.'),2,'_') = 'disposition' then source_type='3';
	if scan(scan(name,1,'.'),2,'_') = 'rx' and scan(scan(name,1,'.'),3,'_') = 'opportunity' then source_type='2';
	if scan(scan(name,1,'.'),2,'_') = 'rx' and scan(scan(name,1,'.'),3,'_') = 'disposition' then source_type='4'; 		
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - all history
	+---------------------------------------------------------------------------------------------*/		
	proc sort data = dqi_eoms_cmctn_history;
	by source_team source_type data_file;
	run;

	proc sort data = temp02;
	by source_team source_type data_file;
	run;

	data &data_out.;
	merge temp02 (in=a) dqi_eoms_cmctn_history (in=b);
	by source_team source_type data_file;
	if a and not b;
	run;

	%let cnt_files_opp = 0;
	%let cnt_files_disp = 0; 

	proc sql noprint;
	select count(*) into: cnt_files_opp separated by ''
	from &data_out.
	where scan(memname,2,'_') in ('1','2') ;
	quit;

	proc sql noprint;
	select count(*) into: cnt_files_disp separated by ''
	from &data_out.
	where scan(memname,2,'_') in ('3','4') ;
	quit;

	%put NOTE: ************************************************************************;
	%put NOTE: data step = &data_out. ;
	%put NOTE: cnt_files_opp = &cnt_files_opp. ;
	%put NOTE: cnt_files_disp = &cnt_files_disp. ;
	%put NOTE: ************************************************************************;

%mend m_create_work_list;