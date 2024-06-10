
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step1_eoms_rename
|
| LOCATION:     ECOA - Enterprise Campaign Opportunity Adapter
| 
| PURPOSE:	Rename to standards for Opportunity + Opportunity RX + Disposition + Disposition RX  
|		1. 8:00 am-2:00 pm - CPL + SFMC + DQI processes and sends ECOA data to ECOA  
|		2. 3:00 pm - ECOA processes and sends ECOA data to IT
|		3. 4:00 pm - IT   processes and sends ECOA data to EDW
|		4. 8:00 pm - EDW  processes ECOA data (or 6:00 pm AZ time)
|
| LOGIC:  
|		step1 = loads 16 files for processing = select * from dss_cea2.dqi_eoms_files where execution_date = current_date
|		
|		note  = send 24 files to IT = 8 TCH + 8 TRIGGER + 4 OPP OPPRX + 4 DISP DISPRX
|
|
|		ECOA Tables:
|		-----------------------------------------------------------------------------
|		select top 100 * from dss_cea2.dqi_eoms_opportunity;       --<-------------------------- cpl + sfmc + dqi opps
|		select top 100 * from dss_cea2.dqi_eoms_dispositions;      --<-------------------------- cpl + sfmc + dqi disps
|		select top 100 * from dss_cea2.dqi_eoms_cmctn_history;     --<-------------------------- cpl + sfmc + dqi cmctn history
|		select top 100 * from dss_cea2.dqi_eoms_files;             --<-------------------------- cpl + sfmc + dqi file cosolidation process history
|		select top 100 * from dss_cea2.dqi_eoms_files_process      --<-------------------------- cpl + sfmc + dqi file processing process   history
|		select top 100 * from dss_cea2.dqi_eoms_file_rule_metrics; --<-------------------------- cpl + sfmc + dqi qc assessment
|
|
|		Other Tables:
|		-----------------------------------------------------------------------------
|		select * from dss_cea2.dqi_eoms_format_sql;                --<-------------------------- sql driven table data formats
|		select * from dss_cea2.dqi_eoms_rules;                     --<-------------------------- sql driven table data rules
|		select * from dss_cea2.dqi_eoms_cfg_dlvry_type;            --<-------------------------- sql driven table eoms config
|		select * from dwv_eoms.cfg_dlvry_type;			   --<-------------------------- sql driven table eoms config
|		select * from dss_cea2.dqi_eoms_layout;                    --<-------------------------- sql driven table eoms layouts
|		select * from dss_cea.t_cim_eoms_keycode_catalog_agg;      --<-------------------------- pbm keycodes 
|		select * from dwv_eoms.opportunity_denorm                  --<-------------------------- pbm IDs
|		select * from dwv_eoms.product where prod_typ_id = 6;      --<-------------------------- channel 1 - channel detail 
|		select * from dwv_eoms.product where prod_typ_id = 18;     --<-------------------------- channel 2 - channel summary 
|		select * from dwu_edw_rb.v_stlnt;                          --<-------------------------- letter templates 
|		select * from dss_cea2.dqi_process_control ;               --<-------------------------- process control prod
|		select * from dss_cea2.dqi_process_control_dev ;           --<-------------------------- process control dev
|		select * from dss_cea2.ea_process_control_surveillance_metadata
|		select * from dss_cea2.ea_process_control_surveillance
|
|
|		Deliverables:
|		-----------------------------------------------------------------------------
| 	        1. code          = /mbr_engmt/reserve/SASControl/PROD/data_integration/eoms_cmctn_history
|		2. logs          = /mbr_engmt/reserve/SASData/PROD/data_integration/eoms_cmctn_history
|		3. surv reports  = /mbr_engmt/reserve/SASControl/PROD/data_integration/surveillance_reporting		
|		4. reports       = /anr_dep/communication_history/eoms_reports 
|		5. send DQI	 = /anr_dep/communication_history/dqi_files
|		6. processing    = /anr_dep/communication_history/eoms_history       <----------------- processing history collection - validation 
|		7. send IT       = /anr_dep/communication_history/eoms_IT            <----------------- sending to IT for dwv_campgn + dwu_edw = 24 files daily
| 		8. send EA       = /anr_dep/communication_history/eoms_files         <----------------- receiving from CPL SFMC from webtransport
|		9. send CPL SMFC = webtransport.caremark.com = /ant591/IVR/incoming  <----------------- SFTP prod files receiving from CPL SFMC 
|	       10. send IT EDW   = webtransport.caremark.com = /ant591/IVR/edw       <----------------- SFTP prod files sending to IT
|	       11. EDW load      = file-> initial land -> land -> stage -> target
|
|
|		File Names:
|		-----------------------------------------------------------------------------
|		1 = 000123 = cpl  opportunity
|		1 = 000456 = sfmc opportunity
|		2 = 000123 = cpl  rx opportunity
|		2 = 000456 = sfmc rx opportunity
|		3 = 000123 = cpl  disposition
|		3 = 000456 = sfmc disposition
|		4 = 000123 = cpl  rx disposition
|		4 = 000456 = sfmc rx disposition
|
|-------------------------------------------------------------------------------------------------------
|
|		Information:
|		-----------------------------------------------------------------------------
|		eoms      = dwv_eoms   = only EOMS
|		eoms2     = dwv_campgn = only CPL + SFMC 
|		reporting = dwu_edw    = only CPL + SFMC + EOMS
|
|		EA Tables:
|		-----------------------------------------------------------------------------
|		dss_cea2.dqi_eoms_opportunity
|		dss_cea2.dqi_eoms_dispositions
|
|		IT Tables:  the IT ETL is executed once a day... so sending ECOA files later in the afternoon will be more live vs morning
|		-----------------------------------------------------------------------------
|		dwv_campgn.opportunity 
|		dwv_campgn.event_message 
|		dwv_campgn.event_party 
|		dwv_campgn.event_party_contact_info                     
|		dwv_campgn.event_drug        
|		dwv_campgn.event_message_extension           
|		dwv_campgn.opportunity_extension          
|		dwv_campgn.ncvs_pharmacy_response                  
|		dwv_campgn.event_rx_fill                     
|		dwv_campgn.billing_info           
|		dwv_campgn.billing_backup_info              
|		dwv_campgn.event_rx_message           
|		dwv_campgn.event_waiver_campaign 
|
|		EOMS Tables:
|		-----------------------------------------------------------------------------
|		dwv_eoms.opportunity
|		dwv_eoms.event_message
|		dwv_eoms.event_party
|		dwv_eoms.event_party_contact_info
|
|		EDW Tables:
|		-----------------------------------------------------------------------------
|		dwu_edw_rb.v_oppty     
|		dwu_edw_rb.v_oppty_bill_agg     
|		dwu_edw_rb.v_oppty_bill_dtl  
|		dwu_edw_rb.v_oppty_clm              
|		dwu_edw_rb.v_oppty_clm_msg 
|		dwu_edw_rb.v_oppty_drug  
|		dwu_edw_rb.v_oppty_dtl  
|		dwu_edw_rb.v_oppty_extn             
|		dwu_edw_rb.v_oppty_msg (event_message_extension + event_message)             
|		dwu_edw_rb.v_oppty_msg_phmcy_rspns  
|		dwu_edw_rb.v_oppty_sprs             
|		dwu_edw_rb.v_oppty_waivr 
|		dwu_edw_rb.v_evnt_msg_extn
|		dwu_edw_rb.v_evnt_pty_cntct_info <------------- member information NA but info in v_oppty_dtl
|
|		Testing:
|		-----------------------------------------------------------------------------
| 		1.  create files TEST_001.sas
| 		2.  files = C:\Users\BStropich1\Documents\Brian\team_AZ\project_stars\CTR000000 - 2019 projects\83_aetna_blue_chips\05_development\eoms_test_sftp
| 		3.  sftp files to webtransport - /ant591/IVR/incoming/sfmc* /ant591/IVR/incoming/cpl*
| 		4.  run pbm adapaters 
|		5.  IT sftp files to webtransport - /ant591/IVR/incoming/SFMC* /ant591/IVR/incoming/CPL*
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step1_eoms_rename(team_id=, layout_id=);


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - list of files to process for today
	+---------------------------------------------------------------------------------------------*/
	data _null_;				
	x "cd &put_linux_directory.";
	x "rm *.has_been_accepted";
	run;
	
	data working.qc_step1a;
	x=1;
	run;	


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create template ID of 16 files
	+---------------------------------------------------------------------------------------------*/	
	data eoms_template;
	format  file_team file_type1 file_type2 file_type3 $30.;
	file_type1 = 'opportunity';file_type2 = 'std';file_type3 = 'cntl';file_team='cpl';output;
	file_type1 = 'opportunity';file_type2 = 'std';file_type3 = 'data';file_team='cpl';output;
	file_type1 = 'opportunity';file_type2 = 'rx';file_type3  = 'cntl';file_team='cpl';output;
	file_type1 = 'opportunity';file_type2 = 'rx';file_type3  = 'data';file_team='cpl';output;
	file_type1 = 'disposition';file_type2 = 'std';file_type3 = 'cntl';file_team='cpl';output;
	file_type1 = 'disposition';file_type2 = 'std';file_type3 = 'data';file_team='cpl';output;
	file_type1 = 'disposition';file_type2 = 'rx';file_type3  = 'cntl';file_team='cpl';output;
	file_type1 = 'disposition';file_type2 = 'rx';file_type3  = 'data';file_team='cpl';output;	 
	file_type1 = 'opportunity';file_type2 = 'std';file_type3 = 'cntl';file_team='sfmc';output;
	file_type1 = 'opportunity';file_type2 = 'std';file_type3 = 'data';file_team='sfmc';output;
	file_type1 = 'opportunity';file_type2 = 'rx';file_type3  = 'cntl';file_team='sfmc';output;
	file_type1 = 'opportunity';file_type2 = 'rx';file_type3  = 'data';file_team='sfmc';output;
	file_type1 = 'disposition';file_type2 = 'std';file_type3 = 'cntl';file_team='sfmc';output;
	file_type1 = 'disposition';file_type2 = 'std';file_type3 = 'data';file_team='sfmc';output;
	file_type1 = 'disposition';file_type2 = 'rx';file_type3  = 'cntl';file_team='sfmc';output;
	file_type1 = 'disposition';file_type2 = 'rx';file_type3  = 'data';file_team='sfmc';output;
	run;

	proc sort data=eoms_template;
	by file_team file_type1 file_type2 file_type3;
	run; 

	data eoms_template;
	format file_macro $10.;
	set eoms_template;
	file_id=_n_;
	file_macro="F"||left(trim(file_id))  ;
	run;	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get the list of CPL + SMFC files for today process
	+---------------------------------------------------------------------------------------------*/
	data eoms_today;
	rc=filename("mydir","&eoms_directory.");
	did=dopen("mydir");
	memcount=dnum(did);
	do i=1 to memcount;
	name=dread(did,i);
	match1=find(name,"cpl","i t",1);
	match2=find(name,"sfmc","i t",1);
	if match1 or match2 then output;
	end;
	rc=dclose(did);
	run; 

	data eoms_today;
	format today $8. file_team file_type1 file_type2 file_type3 $30.;
	set eoms_today;
	year=year(today() - &execution_date. );
	month=put(month(today() - &execution_date. ),z2.);
	day=put(day(today() - &execution_date. ),z2.);
	today=compress(year||month||day);
	/** create teamplate ID ----------------------------------------------**/
	file_team=scan(name,1,'_');
	if index(name,'opp') > 0 then file_type1 = 'opportunity';
	else if index(name,'disp') > 0 then file_type1 = 'disposition';
	if index(name,'rx') > 0 then file_type2 = 'rx';
	else file_type2 = 'std';
	if index(name,'CNTL') > 0 then file_type3 = 'cntl';
	else file_type3 = 'data';
	if index(name,today) > 0;
	run;

	
	%if &ecoa_zero.	= YES %then %do;

		/*** temp solution to remove bad data and process zero record files ***/
		data eoms_today;
		set eoms_today;
		if file_type1 = 'BRIANSTROPICH';
		run;
		
		data _null_;
		x "rm /anr_dep/communication_history/eoms_files/*&c_s_file_date.*";
		run;		

	%end;		


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine what files are missing for today for CPL + SFMC
	+---------------------------------------------------------------------------------------------*/
	proc sort data=eoms_today;
	by file_team file_type1 file_type2 file_type3;
	run; 

	proc sort data=eoms_template;
	by file_team file_type1 file_type2 file_type3;
	run;

	data eoms_today;
	merge eoms_today eoms_template;
	by file_team file_type1 file_type2 file_type3;
	run;

	data eoms_today2;
	set eoms_today;
	if today = '';
	if file_type3 ne 'cntl';
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create missing files for today
	|
	|  The 16 files to be processed each day... 8 data files + 8 cnt files...
	|
	|   1. files to determine missing within eoms_today2 are 8 data files minus 8 cntl files
	|   2. 2 opps + 2 opps rx + 2 disp + 2 disp rx = 8 data files
	|   3. what ever files dont exist for today the process below will create
	|   4. example - cpl sent 1 opp + 1 opp rx + 1 opp cntl + 1 opp rx cntl = 2 data files
	|                sfmc sent nothing
	|                need to create 1 cpl disp + 1 cpl disp rx + 4 data files for smfc = 8 data files
	|		 eoms_today2 which display which of the 8 data files are missing for today
	|
	+---------------------------------------------------------------------------------------------*/	
	%let file_id_cnt = 0; 
	
	data _null_;  
	set eoms_today2 ;
	call symput('file_id'||trim(left(_n_)),trim(left(file_id)));
	call symput('file_id_cnt',trim(left(_n_)));
	put _all_;
	run;
	
	data _null_;
	format today $8. ; 
	year=year(today() - &execution_date. );
	month=put(month(today() - &execution_date. ),z2.);
	day=put(day(today() - &execution_date. ),z2.);
	today=compress(year||month||day);
	call symput('today',left(trim(today)));
	run;		

	%put NOTE: today = &today. ;  
	%put NOTE: file_id_cnt = &file_id_cnt. ;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create empty files DATA + CNTL for CPL + SFMC if none were sftp
	+---------------------------------------------------------------------------------------------*/	
	%do b = 1 %to &file_id_cnt;

		%if &&file_id&b = 16 %then %do;	
			data _null_;
			file "&eoms_directory./sfmc_opportunity.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./sfmc_opportunity.&today.123456.CNTL";
			put "OD|sfmc_opportunity.&today.123456|0|SFMC|I||||"; 
			run;	
		%end;
		%if &&file_id&b = 14 %then %do;
			data _null_;
			file "&eoms_directory./sfmc_rx_opportunity.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./sfmc_rx_opportunity.&today.123456.CNTL";
			put "OD|sfmc_rx_opportunity.&today.123456|0|SFMC|I||||"; 
			run;
		%end;
		%if &&file_id&b = 12 %then %do;		 		
			data _null_;
			file "&eoms_directory./sfmc_disposition.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./sfmc_disposition.&today.123456.CNTL";
			put "OD|sfmc_disposition.&today.123456|0|SFMC|I||||"; 
			run;	
		%end;
		%if &&file_id&b = 10 %then %do;	
			data _null_;
			file "&eoms_directory./sfmc_rx_disposition.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./sfmc_rx_disposition.&today.123456.CNTL";
			put "OD|sfmc_rx_disposition.&today.123456|0|SFMC|I||||"; 
			run;
		%end;
		%if &&file_id&b = 8 %then %do;	
			data _null_;
			file "&eoms_directory./cpl_opportunity.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./cpl_opportunity.&today.123456.CNTL";
			put "OD|cpl_opportunity.&today.123456|0|CPL|I||||"; 
			run;	
		%end;
		%if &&file_id&b = 6 %then %do;	
			data _null_;
			file "&eoms_directory./cpl_rx_opportunity.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./cpl_rx_opportunity.&today.123456.CNTL";
			put "OD|cpl_rx_opportunity.&today.123456|0|CPL|I||||"; 
			run;
		%end;
		%if &&file_id&b = 4 %then %do;	
			data _null_;
			file "&eoms_directory./cpl_disposition.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./cpl_disposition.&today.123456.CNTL";
			put "OD|cpl_disposition.&today.123456|0|CPL|I||||"; 
			run;	
		%end;
		%if &&file_id&b = 2 %then %do;	
			data _null_;
			file "&eoms_directory./cpl_rx_disposition.&today.123456";
			run;

			data _null_;
			file "&eoms_directory./cpl_rx_disposition.&today.123456.CNTL";
			put "OD|cpl_rx_disposition.&today.123456|0|CPL|I||||"; 
			run;
		%end;
		
	%end;	
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - list of the 16 files to process for rename
	+---------------------------------------------------------------------------------------------*/
	data eoms_rename_files;
	rc=filename("mydir","&eoms_directory.");
	did=dopen("mydir");
	memcount=dnum(did);
	do i=1 to memcount;
		name=dread(did,i); 
		match1=find(name,"cpl","i t",1); 
		match2=find(name,"sfmc","i t",1); 	
		if match1 or match2 then output;
	end;
	rc=dclose(did);
	run;	
	
	data eoms_rename_files;
	set eoms_rename_files;
	if index(name, "&c_s_file_date.") > 0 ;
	run;
	
	proc sql noprint;
	create table eoms_rename_files as
	select *
	from eoms_rename_files
	where name not in (select source_file from &c_s_table_files. union select source_new_file from &c_s_table_files.  );
	quit;

	data eoms_rename_files;
	set eoms_rename_files;
	/*** exclude files from a daily re-run ***/
	if index(name,'cpl')  > 0 and index(name,'000123') > 0 then delete;
	if index(name,'sfmc') > 0 and index(name,'000456') > 0 then delete; 
	run;
	
	data _null_;
	set eoms_rename_files;  /**<---------------------------------- should be 16 files every day ***/
	put _all_;
	run;
	
	%let cnt_files = 0;
	
	proc sql noprint; 
	select count(*) into: cnt_files separated by ''
	from eoms_rename_files ;
	quit;	
	
	%put NOTE: ************************************************************************;
	%put NOTE: count files = &cnt_files. ;
	%put NOTE: ************************************************************************;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - QC the DATA + CNTL files = 16 daily files
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_files. ne 16 %then %do;
	
			%put ERROR:  QC - The daily sftp files - &cnt_files. does not equal to the data file 16 daily file count - 8 CPL + 8 SFMC;
			
			proc export data=work.eoms_rename_files
			dbms=xlsx 
			outfile="&c_s_datadir.&SYS_DIR.validate_sftp_files_&tdname..xlsx" 
			replace; 
			sheet="validate_files"; 
			run;

			%m_abend_handler(abend_report=%str(&c_s_datadir./validate_sftp_files_&tdname..xlsx),
					 abend_message=%str(There is an issue with the SFTP files counts for today - 8 CPL + 8 SFMC - There are missing opportunity or disposition files));		
	%end;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - Rename + Dedupe + Update Contents for the DATA + CNTL files = 16 daily files
	+---------------------------------------------------------------------------------------------*/	
	%if &cnt_files. ne 0 %then %do;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - rename the DATA + CNTL files
		+---------------------------------------------------------------------------------------------*/
		data eoms_rename_files2;
		format SOURCE_TEAM SOURCE_FILE SOURCE_NEW_FILE EXECUTION_TRIAGE EXECUTION_SFTP SOURCE_NEW_IT_SFTP_FILE name2 $100. ;
		set eoms_rename_files ;  
		b1=scan(name,1,'.');
		b2=scan(name,2,'.');
		if index(name,'cpl') > 0 then b2b=substr(b2,1,8)||'000123';
		if index(name,'sfmc') > 0 then b2b=substr(b2,1,8)||'000456';
		b3=scan(name,3,'.');
		if index(upcase(name),'CNTL') > 0 then name2=trim(left(b1))||'.'||trim(left(b2b))||'.'||trim(left(upcase(b3)));
		if index(upcase(name),'CNTL') = 0 then name2=trim(left(b1))||'.'||trim(left(b2b));
		EOMS_ROW_GID = 0;
		SOURCE_TEAM = upcase(scan(name,1,'_'));
		SOURCE_FILE = name;
		SOURCE_FILE_METRIC = 0;
		SOURCE_NEW_FILE = name2;
		SOURCE_NEW_FILE_METRIC = 0;
		SOURCE_NEW_IT_SFTP_FILE='X';
		EXECUTION_TRIAGE = 'NONE';
		EXECUTION_SFTP = 'PENDING';
		EXECUTION_DATE = today() - &execution_date. ;
		run;

		data _null_; 
		set eoms_rename_files2 ;  
		call symput('old'||trim(left(_n_)),trim(left(name)));
		call symput('new'||trim(left(_n_)),trim(left(name2)));
		call symput('total',trim(left(_n_)));
		run;


		%macro do_rename;
			%do i = 1 %to &total;

				%put NOTE: old = &&old&i ;
				%put NOTE: new = &&new&i. ;

				%if &SYS_SCP = 1 %then %do;  /** <---------------------- windows **/
					data _null_;				
					x cd "&eoms_directory.";
					x rename "&&old&i"  "&&new&i.";
					run;
				%end;
				%else %do;  /** <--------------------------------------- linux **/
					data _null_;				
					x dir "&eoms_directory.";
					x mv "&eoms_directory./&&old&i"  "&eoms_directory./&&new&i.";
					run;
				%end;

			%end;
		%mend do_rename; 
		%do_rename;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - keep the latest for the CPL dispositions
		|
		|         disposition files contain many disp for same opp + message 
		+---------------------------------------------------------------------------------------------*/
		%macro do_cpl_disposition;
			%do i = 1 %to &total;

				%put NOTE: new = &&new&i. ;

				%if &SYS_SCP = 1 %then %do;  /** <---------------------- windows **/ 
				%end;
				%else %do;  /** <--------------------------------------- linux **/					
					
					%if %index(%upcase(&&&new&i.), RX) = 0 and %index(%upcase(&&&new&i.), CNTL) = 0 
					and %index(%upcase(&&&new&i.), CPL) > 0 and %index(%upcase(&&&new&i.), DISP) > 0 %then %do;

						data version1; 
						length version $1000. ;
						infile "&eoms_directory./&&new&i." firstobs=1 missover pad lrecl=1000 delimiter='=' dsd;
						input version; 
						run;

						data version1;
						format d DATETIME20. ;
						set version1;
						o=scan(version,2,'|');
						m=scan(version,3,'|');
						d1=scan(version,6,'|')*1;
						d2=scan(version,7,'|')*1; 
						d=input(scan(version,8,'|') , ANYDTDTM21.); 
						run;
						
						%global cnt_old_disp cnt_new_disp;
						
						%let cnt_old_disp=0;
						%let cnt_new_disp=0;
						
						proc sql noprint;
						select count(*) into: cnt_old_disp separated by ''
						from version1;
						quit;						

						/* SASDOC --------------------------------------------------------------------------------------
						|  STEP - keep based on the logic of...
						|
						|	+ opportunity 
						|	+ message 
						|	+ most recent date 
						|	+ d1 ascending  = 1=delivered + 3=open + 8=not delivered (lower number better)
						|	+ d2 descending = 79=Email Opened Opt Out + 78=Email Opened Clicked + 77=Email Opened (higher number better)
						|
						|       keep opt outs of 79, 80, 237
						|
						+---------------------------------------------------------------------------------------------*/
						proc sort data = version1;
						by o m descending d d1 descending d2 ;
						run; 

						proc sort data = version1   nodupkey;
						by o m  ;
						run; 
						
						proc sql noprint;
						select count(*) into: cnt_new_disp separated by ''
						from version1;
						quit;
						
						%put NOTE: cnt_old_disp = &cnt_old_disp.;
						%put NOTE: cnt_new_disp = &cnt_new_disp.;
						
						%if &cnt_old_disp. ne &cnt_new_disp. %then %do;

							data _null_;
							set version1;
							file "&eoms_directory./&&new&i."    lrecl=32000 ;
							put version ;
							run;
							
							/* SASDOC --------------------------------------------------------------------------------------
							|  STEP - duplication disposition email notification
							+---------------------------------------------------------------------------------------------*/	

							filename xemail email 
							to=(&c_s_email_to.)
							subject="CTT Data Profiling - DUPLICATION DISPOSITION of ECOA Files ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

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
								put "The ECOA dispostion file has duplicates for the day and ECOA will remove duplicates and keep latest record for processing. ";
								put '<br>';
								put '<br>';								
								put "The Step 1 - ECOA duplication Logic: oppty_evnt_id msg_evnt_id dispostion 1 and dispostion 2 descending ";
								put '<br>';
								put '<br>'; 
								put "The DUPLICATION DISPOSITION File: &&new&i.  ";
								put "The DUPLICATION DISPOSITION Counts - Before: &cnt_old_disp.  ";
								put "The DUPLICATION DISPOSITION Counts - After:  &cnt_new_disp. ";
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
					
				%end;

			%end;
		%mend do_cpl_disposition; 
		%do_cpl_disposition;	
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - keep the latest for the CPL opportunity
		|
		|         opportunity files may contain duplicates for same opp + message 
		+---------------------------------------------------------------------------------------------*/
		%macro do_cpl_opportunity;
			%do i = 1 %to &total;

				%put NOTE: new = &&new&i. ;

				%if &SYS_SCP = 1 %then %do;  /** <---------------------- windows **/ 
				%end;
				%else %do;  /** <--------------------------------------- linux **/					
					
					%if %index(%upcase(&&&new&i.), RX) = 0 and %index(%upcase(&&&new&i.), CNTL) = 0 
					and %index(%upcase(&&&new&i.), CPL) > 0 and %index(%upcase(&&&new&i.), OPP) > 0 %then %do;

						data version1; 
						length version $2000. ;
						infile "&eoms_directory./&&new&i." firstobs=1 missover pad lrecl=2000 delimiter='=' dsd;
						input version; 
						run;

						data version1;
						format d DATETIME20. ;
						set version1;
						o=scan(version,2,'|');
						m=scan(version,12,'|'); 
						d=input(scan(version,13,'|') , ANYDTDTM21.); 
						n=_n_;
						run;
						
						/** get EPH_ID + MBR_ACCT_GID + SPCLT_PTNT_ID + SPCLT_PTNT_GID **/
						data version2;
						length x1-x630 $2000. ;
						infile "&eoms_directory./&&new&i." firstobs=1 missover pad lrecl=2000 delimiter='|' dsd;
						input x1-x630  ; 
						n=_n_;
						keep n x54
						       x60 
						       x67 
						       x328 x330; 
						run;
						
						proc sort data = version1;
						by n;
						run;

						proc sort data = version2;
						by n;
						run;

						data version1;
						merge version1 version2;
						by n; 
						run;						
						
						%global cnt_old_opp cnt_new_opp;
						
						%let cnt_old_opp=0;
						%let cnt_new_opp=0;
						%let cnt_invalid_opp=0;
						
						proc sql noprint;
						select count(*) into: cnt_old_opp separated by ''
						from version1;
						quit;						

						/* SASDOC --------------------------------------------------------------------------------------
						|  STEP - keep based on the logic of...
						|
						|	+ opportunity 
						|	+ message 
						|	+ most recent date  
						|
						+---------------------------------------------------------------------------------------------*/
						proc sort data = version1;
						by o m descending d descending n ;
						run; 

						proc sort data = version1   nodupkey;
						by o m  ;
						run; 

						/* SASDOC --------------------------------------------------------------------------------------
						|  STEP - keep based on the logic of valid member IDs...
						|
						|	+ eph ID 
						|	+ member GID 
						|	+ specialty GID 
						|
						|  STEP - exclude invalid x54 - OPPTY_TYP_PROD_ID
						|
						+---------------------------------------------------------------------------------------------*/						
						data version1;
						set version1; 
						if missing(x60) and missing(x67) and missing(x328) and missing(x330) then delete; 
						run;
						
						proc sql noprint;
						select count(*) into: cnt_invalid_opp separated by ''
						from version1
						where upcase(compress(x54)) = 'NONE' or compress(x54) = '' ; 
						quit;
						
						data version1;
						set version1; 
						if upcase(compress(x54)) = 'NONE' then delete;
						if compress(x54)      = '' then delete; 
						run;						
						
						proc sql noprint;
						select count(*) into: cnt_new_opp separated by ''
						from version1;
						quit;
						
						%put NOTE: cnt_old_opp = &cnt_old_opp.;
						%put NOTE: cnt_new_opp = &cnt_new_opp.;
						%put NOTE: cnt_invalid_opp = &cnt_invalid_opp. ;
						
						%if &cnt_old_opp. ne &cnt_new_opp. %then %do;

							data _null_;
							set version1;
							file "&eoms_directory./&&new&i."    lrecl=32000 ;
							put version ;
							run;
							
							/* SASDOC --------------------------------------------------------------------------------------
							|  STEP - duplication disposition email notification
							+---------------------------------------------------------------------------------------------*/	

							filename xemail email 
							to=(&c_s_email_to.)
							subject="CTT Data Profiling - DUPLICATION OPPORTUNITY of ECOA Files ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

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
								put "The ECOA opportunity file has duplicates for the day and ECOA will remove duplicates and keep latest record for processing. ";
								put '<br>';
								put '<br>'; 
								put '<br>';
								put '<br>'; 
								put "The DUPLICATION OPPORTUNITY File: 			&&new&i.          ";
								put "The DUPLICATION OPPORTUNITY Counts - Before: 	&cnt_old_opp.     ";
								put "The DUPLICATION OPPORTUNITY Counts - After:  	&cnt_new_opp.     ";
								put "The OPPTY_TYP_PROD_ID Invalid Counts - Before:  	&cnt_invalid_opp. "; 
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
					
				%end;

			%end;
		%mend do_cpl_opportunity; 
		%do_cpl_opportunity;		
		
	
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - rename the CNTL contents for file and reassign counts (cpl disposition)
		+---------------------------------------------------------------------------------------------*/
		data eoms_rename_files3;   
		set eoms_rename_files2;   
		if index(name,'.CNTL') > 0;
		run;

		data _null_; 
		set eoms_rename_files3 ;  
		call symput('old'||trim(left(_n_)),trim(left(name)));
		call symput('new'||trim(left(_n_)),trim(left(name2)));
		call symput('oldc'||trim(left(_n_)),trim(left(b2)));
		call symput('newc'||trim(left(_n_)),trim(left(b2b)));
		call symput('total',trim(left(_n_)));
		run;


		%macro do_contents_disp;
			%do i = 1 %to &total;

				%put NOTE: new = &&new&i ; 
				
				%if %index(%upcase(&&&new&i.), RX) = 0 and %index(%upcase(&&&new&i.), CNTL) > 0 
				and %index(%upcase(&&&new&i.), CPL) > 0 and %index(%upcase(&&&new&i.), DISP) > 0 %then %do;
					%if &cnt_old_disp. ne &cnt_new_disp. %then %do;
						%put NOTE: Update CPL Disposition Counts ;
						%put NOTE: cnt_old_disp = &cnt_old_disp.;
						%put NOTE: cnt_new_disp = &cnt_new_disp.;
					%end;
				%end;				

				data x;
				length x $5000 ;
				infile "&eoms_directory.&SYS_DIR.&&new&i" lrecl =3000;
				input x ;
				xx=tranwrd(x,"&&oldc&i.", "&&newc&i."); 
				%if %index(%upcase(&&&new&i.), RX) = 0 and %index(%upcase(&&&new&i.), CNTL) > 0 
				and %index(%upcase(&&&new&i.), CPL) > 0 and %index(%upcase(&&&new&i.), DISP) > 0 %then %do;
					%if &cnt_old_disp. ne &cnt_new_disp. %then %do;
						xx=tranwrd(xx,"&cnt_old_disp.", "&cnt_new_disp.");
					%end;
				%end;				
				file "&eoms_directory.&SYS_DIR.&&new&i" lrecl =3000;
				put xx ;
				run;

			%end;
		%mend do_contents_disp; 
		%do_contents_disp;
		
		
		%macro do_contents_opp;
			%do i = 1 %to &total;

				%put NOTE: new = &&new&i ; 
				
				%if %index(%upcase(&&&new&i.), RX) = 0 and %index(%upcase(&&&new&i.), CNTL) > 0 
				and %index(%upcase(&&&new&i.), CPL) > 0 and %index(%upcase(&&&new&i.), OPP) > 0 %then %do;
					%if &cnt_old_opp. ne &cnt_new_opp. %then %do;
						%put NOTE: Update CPL Opportunity Counts ;
						%put NOTE: cnt_old_opp = &cnt_old_opp.;
						%put NOTE: cnt_new_opp = &cnt_new_opp.;
					%end;
				%end;				

				data x;
				length x $5000 ;
				infile "&eoms_directory.&SYS_DIR.&&new&i" lrecl =3000;
				input x ;
				xx=tranwrd(x,"&&oldc&i.", "&&newc&i."); 
				%if %index(%upcase(&&&new&i.), RX) = 0 and %index(%upcase(&&&new&i.), CNTL) > 0 
				and %index(%upcase(&&&new&i.), CPL) > 0 and %index(%upcase(&&&new&i.), OPP) > 0 %then %do;
					%if &cnt_old_disp. ne &cnt_new_disp. %then %do;
						xx=tranwrd(xx,"&cnt_old_opp.", "&cnt_new_opp.");
					%end;
				%end;				
				file "&eoms_directory.&SYS_DIR.&&new&i" lrecl =3000;
				put xx ;
				run;

			%end;
		%mend do_contents_opp; 
		%do_contents_opp;		


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - load opportunity data files into teradata
		+---------------------------------------------------------------------------------------------*/
		data template;
		set &c_s_table_files. (obs=0);
		run;

		proc contents data = template out = contents1 noprint;
		run;

		proc sql noprint;
		select name into: keep_variables separated by ' '
		from contents1;
		quit;

		data loadt_1_rename_files;
		set eoms_rename_files2;  	 
		keep &keep_variables. ;
		run;

		%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_files. ) x) with no data 

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 
		
		%m_bulkload_sasdata(bulk_data=loadt_1_rename_files, bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id., bulk_option=APPEND); 
		
		%put NOTE: &syserr. ;	

		%if &syserr. > 6 %then %do;
		  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
		  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 1),
				   abend_message=%str(EA ECOA - Proc Append Data failed - step 1));		  
		%end;	
		
		%m_bulkload_sasdata;   

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			insert &c_s_table_files. select * from dss_cea2.dqi_eoms_tera_&mbr_process_id.

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 

		%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

		%m_table_statistics(data_in=&c_s_table_files., index_in=eoms_row_gid);	
		
	%end;
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/			
	%if &ecoa_test.	= YES1B %then %do;

		data working.qc_step1b;
		x=1;
		run;	
		
		%m_ecoa_process(do_tasks=ALL);

	%end;	
	%else %do;

		data working.qc_step1b;
		x=1;
		run;			

	%end;	


%mend m_step1_eoms_rename;
