
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    	m_step4_eoms_dataprofiling
|
| LOCATION:  
| 
| PURPOSE: 	Apply data profiling rules to data and calculate assessment by 
|		severity levels 1 = required fields
|		severity levels 2 = optional fields
|		severity levels 3 = non-required fields
|		severity levels 4 = required cmctn history fields
|		severity levels 5 = required mcs fields
|
| LOGIC:   
|		step4 = profile data within SAS
|
|		select * from dss_cea2.dqi_eoms_format_sql;
|		select * from dss_cea2.dqi_eoms_rules; 
|		select * from dwv_eoms.cfg_dlvry_type;
|
|
|		why data profiling?
|		Data profiling, the act of monitoring, troubleshoot, and cleansing data.
|
|		Data profiling helps you discover, understand and organize your data.
|
|		1. Data profiling helps cover the basics with your data, verifying that the information in your 
|		tables matches the descriptions. Then it can help you better understand your data by revealing the 
|		relationships that span different databases, source applications or tables.
|		2.  data profiling helps you ensure that your data is up to standard statistical 
|		measures, as well as business rules specific to your company.
|		3. Data profiling would uncover this inconsistency and inform the creation of a standardization 
|		rule that could make them all consistent, two-letter codes 
|
|		What are some data profiling techniques?
|		
|		There are four general methods by which data profiling tools help accomplish better data quality: column profiling, 
|		cross-column profiling, cross-table profiling and data rule validation.
|		
|		Column profiling scans through a table and counts the number of times each value shows up within each column. 
|		This method can be useful to find frequency distribution and patterns within a column of data.
|		
|		Cross-column profiling is made up of two processes: key analysis and dependency analysis. Key analysis examines 
|		collections of attribute values by scouting for a possible primary key. Dependency analysis is a more complex 
|		process that determines whether there are relationships or structures embedded in a data set. Both techniques 
|		help analyze dependencies among data attributes within the same table.
|		
|		Cross-table profiling uses foreign key analysis, which is the identification of orphaned records and determination 
|		of semantic and syntactic differences, to examine the relationships of column sets in different tables. This can 
|		help cut down on redundancy but also identify data value sets that could be mapped together.
|		
|		Finally, data rule validation uses data profiling in a proactive manner to verify that data instances and data sets 
|		conform with predefined rules. This process helps find ways to improve data quality and can be achieved either 
|		through batch validation or an ongoing validation service.
|
|
|		select top 100 * from dss_cea2.dqi_eoms_opportunity;
|		select top 100 * from dss_cea2.dqi_eoms_dispositions;
|		select * from dss_cea2.dqi_eoms_format_sql;
|		select * from dss_cea2.dqi_eoms_rules;  
|
| INPUT: 	Opportunity + Opportunity RX + Disposition + Disposition RX
|
| OUTPUT:  	Opportunity Load + Disposition Load
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step4_eoms_dataprofiling(layout_id=,eoms_file=);

	/**---------------------------------------------------------------------------------
		MCS - Required Fields    
			CommID
			MemberSourceID
			MemberSource
			ClientCode
			CommSourceSystem
			CommName
			CommDeliveryChannel
			CommContactInfo
			CommDeliveryDate
			CommDeliveryStatus
			TemplateID
			CommDocInd

		CMCTN - Required Fields  
			Program Id
			Application Communication Id
			Receiver Id
			
		EOMS  - Required Fields  
			EOMS_OPPTY_EVNT_ID
			EOMS_MSG_EVNT_ID
			RX_FILL_EVNT_ID (depending on targeting)
			LVL1_DISP
			LVL2_DISP
			COMM_HIST_PGM_ID
			MSG_TMPLT_ID
			Member Info
			Client Info
			
	---------------------------------------------------------------------------------**/

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine files to process
	+---------------------------------------------------------------------------------------------*/
	%m_create_work_list(data_out=temp01_rules); 		

	data working.qc_step4a;
	x=1;
	run;	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get format definitions
	|
	|  LOGIC - create data sets or referenece data sets to generate validation formats
	+---------------------------------------------------------------------------------------------*/			

	%macro do_format_defintions(data_file_type=, condition_values=N);

		
		proc sql noprint;
		connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
		create table dqi_eoms_format_sql as
		select * from connection to tera
		( 
			select *
			from dss_cea2.dqi_eoms_format_sql 
			where DATA_FILE_TYPE=%tslit(%upcase(&data_file_type.)) 
			order by rule_id
		);
		disconnect FROM tera;
		quit;
			

		data _null_;
		set dqi_eoms_format_sql; 
		n=trim(left(_n_));
		call symput('fmt_srce'||n,trim(left(data_source_in)));
		call symput('fmt_din'||n,trim(left(data_name_in)));
		call symput('fmt_dout'||n,trim(left(data_name_out))); 
		call symput('fmt_vout'||n,trim(left(data_variable_out))); 
		call symput('fmt_name'||n,trim(left(format_name))); 
		call symput('fmt_sql'||n,trim(left(sql_rules))); 
		call symput('qc_sql'||n,trim(left(qc_rules))); 
		call symput('fmt_total',n);
		run;			

		%do b = 1 %to &fmt_total. ;

			%if &&fmt_srce&b. = EDW %then %do;
				proc sql noprint;
				connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
				create table &&fmt_din&b. as
				select * from connection to tera
				( 
					&&fmt_sql&b.
				);
				disconnect FROM tera;
				quit;
			%end;
			%else %if &&fmt_srce&b. = SAS %then %do;
				proc sql noprint; 
				create table &&fmt_din&b. as 
				&&fmt_sql&b.
				quit;	
			%end;

			%if &&fmt_din&b. = _stntfmt %then %do;
				data _stntfmt;
				set _stntfmt;
				pgm_id=put(left(p),20.);
				run; 
			%end;

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create SAS definition formats
			+---------------------------------------------------------------------------------------------*/
			%macro create_formats(datain=, dataout=, variable=);
			
				data &datain.; 
				set &datain.; 
				if &variable in ('OTHER','other') then do;
				&variable = 'OTHE';
				end;
				if substr(&variable ,1,4) in ('HIGH') and length(trim(&variable )) = 5 then do;
				&variable = 'XYZ12';
				end;				
				run;

				data &dataout. (keep = start label type fmtname );
				   length fmtname $8. type $1 label $20. start $40.;
				   set &datain.;  
				   retain fmtname "&dataout."  type 'C';
				   if &variable. ne "" then do;
				     start = upcase(&variable);
				     label = '.';
				     output;
				   end;
				   if _n_ = 1 then do;
				     start = "other";
				     label = '1';
				     output;
				   end;
				run; 

				proc sort data = &dataout. nodupkey;
				  by start;
				run;

				proc format cntlin = &dataout. ;
				run; 

			%mend create_formats; 
			
			%macro create_formats2(datain=, dataout=, variable=);
			
				data &datain.; 
				set &datain.; 
				if &variable in ('OTHER','other') then do;
				&variable = 'OTHE';
				end;
				run;			

				data &dataout. (keep = start label type fmtname );
				   length fmtname $8. type $1 label $20. start $40.;
				   set &datain.;  
				   retain fmtname "&dataout."  type 'C';
				   if &variable. ne "BRIAN_STROPICH" then do;
				     start = upcase(&variable);
				     label = '1';
				     output;
				   end;
				   if _n_ = 1 then do;
				     start = "other";
				     label = '.';
				     output;
				   end;
				run; 

				proc sort data = &dataout. nodupkey;
				  by start;
				run;

				proc format cntlin = &dataout. ;
				run; 

			%mend create_formats2; 			
			
			%if &condition_values = N %then %do;
				%create_formats(datain=&&fmt_din&b., dataout=&&fmt_dout&b.,  variable=&&fmt_vout&b.); 
			%end;
			%else %if &condition_values = X %then %do;
				%create_formats2(datain=&&fmt_din&b., dataout=&&fmt_dout&b.,  variable=&&fmt_vout&b.); 
			%end;			
			%else %do;
			
				data test;
				set &&fmt_din&b. ;
				%if &&qc_sql&b. = channel_id %then %do;
				if &&qc_sql&b. * 1 = . ;
				%end;
				%else %do;
				if &&qc_sql&b. * 1 > 0 ;
				%end;
				run;
				
				%let qc_sql_cnt=0;
				
				proc sql noprint;
				select count(*) into: qc_sql_cnt separated by ''
				from test;
				quit;
				
				%if &qc_sql_cnt = 0 %then %do;
				%end;
				%else %do;
				
				  	%put WARNING: There are values within dss_cea2.dqi_eoms_format_sql that are not being resolved ;
				  
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - send out start email
					|   
					+---------------------------------------------------------------------------------------------*/
					%if &c_s_dqi_campaign_id. = 80 %then %do;
						filename xemail email 
						to=(&c_s_email_to.)
						subject="CTT Data Profiling - WARNING PBM File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000; 
					%end; 	

					options noquotelenmax;

					data _null_;
					file xemail;
						put " "; 
						put "The CTT Data Profiling PBM warning - There are values within dss_cea2.dqi_eoms_format_sql that are not being resolved.";
						put " ";
						put "Thanks,";
						put " ";
						put "EA - Campaign Targeting Team";
						put " ";
					run;	

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - non-mapped channels set to placebo value 
					+---------------------------------------------------------------------------------------------*/					
					data &&fmt_din&b. ;
					set &&fmt_din&b. ;
					if substr(BRAND_COMPLIANCE,1,1) in ('1','2','3','4','5','6','7','8','9','0') then BRAND_COMPLIANCE='N';
					if substr(CHANNEL_CATEGORY,1,1) in ('1','2','3','4','5','6','7','8','9','0') then CHANNEL_CATEGORY='DIGITAL';
					run;
				
				%end;

				proc sql noprint;
				select distinct upcase(&&fmt_dout&b.) into: data_loop separated by ' ' 
				from &&fmt_din&b. ;
				quit;	

				%put NOTE: data_loop = &data_loop. ;

				%let z = 1 ; 
				%do %while (%length(%sysfunc(scan(&data_loop, &z, ' '))) ^= 0 ) ;  /**<----------------------- do while start **/

					%let data_category = %sysfunc(scan(&data_loop, &z, ' ')); 
					%let z = %eval(&z. +1);
					%global &data_category.;

					proc sql noprint;
					select "'"||trim(&&fmt_vout&b.)||"'" into: &data_category. separated by ','
					from &&fmt_din&b.
					where &&fmt_dout&b. = "&data_category.";
					quit;

					%put NOTE: &data_category. = &&&data_category.;

				%end;
			
			%end;
		%end;

	%mend do_format_defintions;	


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through opportunity files
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_files_opp. ne 0 %then %do;  /** <----------------------------------------------------- start - cnt_files **/

		proc sort data = temp01_rules (where = (scan(memname,2,'_') in ('1','2'))) out = temp01b_rules nodupkey;
		by source_team data_file;
		run;
		
		data _null_;
		set temp01b_rules; 
		where scan(memname,2,'_') in ('1','2');
		call symput('eoms_name'||left(_n_),trim(left(data_file)));
		call symput('source_team'||left(_n_),trim(left(source_team))); 
		call symput('eoms_total',trim(left(_n_)));
		run;

		%do ii = 1 %to &eoms_total;  /** <----------------------------------------------------- start - eoms_total **/
		

			proc sql noprint;
			create table dqi_eoms_rules_opp as
			select *
			from dss_cea2.dqi_eoms_rules
			where layout_id in (1,2)
			and length(data_rules) > 1
			and rule_id ne 999
			order by layout_id, rule_id;
			quit;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - get client level 1 definitions
			+---------------------------------------------------------------------------------------------*/
			%macro do_edw_definitions(variable=, variable2=, definition=, source=, target=, select=, where_var= );
			
				%m_table_drop_passthrough(data_in=dss_cea2.dqi_&variable._&mbr_process_id.);

				%if &definition = C %then %do;
					proc sql;
					connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
					execute(  

						CREATE MULTISET TABLE dss_cea2.dqi_&variable._&mbr_process_id. ,FALLBACK ,
						     NO BEFORE JOURNAL,
						     NO AFTER JOURNAL,
						     CHECKSUM = DEFAULT,
						     DEFAULT MERGEBLOCKRATIO,
						     MAP = TD_MAP1
						     ( 
								&variable. VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC

							 )
						PRIMARY INDEX ( &variable. )

					) by tera;
					execute (commit work) by tera;  
					disconnect from tera;
					quit; 
				%end;
				%if &definition = N %then %do;
					proc sql;
					connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
					execute(  

						CREATE MULTISET TABLE dss_cea2.dqi_&variable._&mbr_process_id. ,FALLBACK ,
						     NO BEFORE JOURNAL,
						     NO AFTER JOURNAL,
						     CHECKSUM = DEFAULT,
						     DEFAULT MERGEBLOCKRATIO,
						     MAP = TD_MAP1
						     ( 
								&variable. DECIMAL(15,0) 

							 )
						PRIMARY INDEX ( &variable. )

					) by tera;
					execute (commit work) by tera;  
					disconnect from tera;
					quit; 
				%end;			

				proc sql noprint;
				create table &variable2. as 
				select distinct &source. as &variable.
				from load_1_&&eoms_name&ii.;
				quit;	
				
				
				%if &definition = N %then %do;
					data x1;
					&variable.=1;output;
					run;

					data &variable2. (rename=(b=&variable.));
					set &variable2. ;
					b=&variable.*1;
					drop &variable.;
					run;					

					data &variable2.;
					set &variable2. x1;
					run;				
				%end;
				%if &definition = C %then %do;
					data x1;
					&variable.='7363';output; 
					run;

					data &variable2.;
					set &variable2. x1;
					run;				
				%end;
				%if &definition = X %then %do;
					data x1;
					&variable.='1336205285';output; 
					&variable.='1932245438';output; 
					run;

					data &variable2.;
					set &variable2. x1;
					run;				
				%end;				
				
				%m_bulkload_sasdata(bulk_data=&variable2. , bulk_base=dss_cea2.dqi_&variable._&mbr_process_id. , bulk_option=APPEND, copy_data=N); 
				
				%put NOTE: &syserr. ;	

				%if &syserr. > 6 %then %do;
				  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
				  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 4),
						   abend_message=%str(EA ECOA - Proc Append Data failed - step 4));		  
				%end;				

				%m_table_statistics(data_in=dss_cea2.dqi_&variable._&mbr_process_id., index_in=&variable.);
				
				%m_bulkload_sasdata;  


				proc sql noprint;
				connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
				create table &variable2. as
				select * from connection to tera
				( 

					select distinct &select.
					from &c_s_schema..&target. a,
					     dss_cea2.dqi_&variable._&mbr_process_id. b 
					where  &where_var.=b.&variable.   
					and &where_var. is not null 
					and &where_var. > ''

				);
				disconnect FROM tera;
				quit;
	
			%mend do_edw_definitions;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create reference data for validation
			|
			|  LOGIC 1 - create format data sets to generate validation formats from EDW which are reference for data_file_type=OPPORTUNITY 			
			|
			|	     example:
			|	     lvl1_acct_id = select distinct lvl1_acct_id from dwu_edw.v_clnt_acct_denorm where lvl1_acct_id in (lvl1_acct_id)
			|	     lvl2_acct_id = select distinct lvl2_acct_id from dwu_edw.v_clnt_acct_denorm where lvl1_acct_id in (lvl1_acct_id)
			|	     lvl3_acct_id = select distinct lvl3_acct_id from dwu_edw.v_clnt_acct_denorm where lvl1_acct_id in (lvl1_acct_id)
			|
			+---------------------------------------------------------------------------------------------*/
			/****
			proc datasets memtype=catalog;
			delete formats;
			run;
			quit; 	
			****/
			
			%do_edw_definitions(variable=lvl1_acct_id, variable2=lvl1_acct_id, definition=C, source=prsn_ssk_1, target= v_clnt_acct_denorm, select=%str(a.lvl1_acct_id), where_var = a.lvl1_acct_id); 
			%do_edw_definitions(variable=lvl1_acct_id, variable2=lvl2_acct_id, definition=C, source=prsn_ssk_1, target= v_clnt_acct_denorm, select=%str(a.lvl2_acct_id), where_var = a.lvl1_acct_id);
			%**do_edw_definitions(variable=lvl1_acct_id, variable2=lvl3_acct_id, definition=C, source=prsn_ssk_1, target= v_clnt_acct_denorm, select=%str(a.lvl3_acct_id), where_var = a.lvl1_acct_id);
						
			%do_edw_definitions(variable=phcy_id, variable2=phcy_id,        definition=X, source=phcy_id, target= v_phmcy_denorm, select=%str(a.npi_id),                   where_var = a.npi_id);
			%do_edw_definitions(variable=phcy_id, variable2=ncpdp_prvdr_id, definition=X, source=phcy_id, target= v_phmcy_denorm, select=%str(a.ncpdp_prvdr_id as npi_id), where_var = a.ncpdp_prvdr_id);
			data phcy_id; set phcy_id ncpdp_prvdr_id; run;
			
			%do_edw_definitions(variable=prsc_id, variable2=prsc_id,   definition=X, source=prsc_id, target= v_prscbr_denorm, select=%str(a.npi_id),              where_var = a.npi_id);
			%do_edw_definitions(variable=prsc_id, variable2=prscbr_id, definition=X, source=phcy_id, target= v_prscbr_denorm, select=%str(a.prscbr_id as npi_id), where_var = a.prscbr_id);
			data prsc_id; set prsc_id prscbr_id; run;
			
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build formats
			|
			|  LOGIC 2 - create format data sets to generate validation formats from code within dqi_eoms_format_sql
			|            validation - CAG stellents channels prescriber pharmacy
			|
			|	     example:
			|	     pgm_id		_pgmfmt		program ID code		OPPORTUNITY	?	select distinct pgm_id as p from dwu_edw.v_stlnt
			|	     lvl3_acct_id	_lvl3fmt	client level 3 code	OPPORTUNITY	?	select distinct lvl3_acct_id from lvl3_acct_id
			|	     chnl_cd		_chnfmt		channel codes		OPPORTUNITY	?	select distinct dsc_shrt_tx as chnl_cd from dwv_eoms.product where prod_typ_id = 6 
			|
			+---------------------------------------------------------------------------------------------*/			
			%do_format_defintions(data_file_type=OPPORTUNITY);
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build formats
			|
			|  LOGIC 3 - create format data sets to generate validation formats from code within dqi_eoms_format_sql 
			|
			|	     1. CHANNEL_CATEGORY logic... each channel_category creates a macro variable of the channels  
			|	     for that category (mail, email, phone, sms, fax) and validates event_party_contact_info such as
			|	     mailing address, phone, and email
			|
			|	     2. BRAND_COMPLIANCE logic... if BRAND_COMPLIANCE then opportunity should have valid stellent 
			|	     PGM_ID and MSG_PRDCT_ALT_ID
			|
			|	     example:
			|	     rule100 = PTNT_ADDR_LINE1_TX = if EVNT_PTY_ROLE_CD = "PTNT" and CHNL_CD in (mail.) then do;if missing(PTNT_ADDR_LINE1_TX) then
			|	     rule113 = PTNT_EMAIL_ADDR_TX = if EVNT_PTY_ROLE_CD = "PTNT" and CHNL_CD in (email.) then do;if missing(PTNT_EMAIL_ADDR_TX) then
			|	     rule109 = PTNT_PHONE1_NBR    = if EVNT_PTY_ROLE_CD = "PTNT" and CHNL_CD in (phone.) then do;if missing(PTNT_PHONE1_NBR) then
			|
			+---------------------------------------------------------------------------------------------*/			
			%do_format_defintions(data_file_type=OTHER, condition_values=Y);
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create reference data for validation of primary keys
			|
			|  LOGIC 4 - create format data sets to generate validation formats from code within dqi_eoms_format_sql
			|            validation - primary keys of rx_fill_evnt_id msg_evnt_id oppty_evnt_id are not being re-used for other opps
			|
			|  example:
			|  rx_fill_evnt_id	_id3fmt	primary key 3	DWV_CAMPGN	dwv_campgn_3	select top 1000 rx_fill_evnt_id from dwv_eoms.event_rx_fill where year(rec_crte_ts) = 2021 and  month(rec_crte_ts) = 3 
			|  msg_evnt_id		_id2fmt	primary key 2	DWV_CAMPGN	dwv_campgn_2	select top 1000 msg_evnt_id from dwv_eoms.event_message where year(rec_crte_ts) = 2021 and  month(rec_crte_ts) = 3 
			|  oppty_evnt_id	_id1fmt	primary key 1	DWV_CAMPGN	dwv_campgn_1	select top 1000 oppty_evnt_id from dwv_eoms.opportunity where year(rec_crte_ts) = 2021 and  month(rec_crte_ts) = 3 
			
			+---------------------------------------------------------------------------------------------*/			
			%do_format_defintions(data_file_type=DWV_CAMPGN, condition_values=X);				
			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_lvl1_acct_id_&mbr_process_id.);	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - cross variable qc - unique member for opportunity IDs
			|
			+---------------------------------------------------------------------------------------------*/			
			%macro qcx_001;			
			
				%global qcx_001;
				
				%let qcx_001 = 10;

				proc sql noprint;
				create table qc_001 as
				select oppty_evnt_id, count(distinct prsn_ssk_4) as cnt
				from load_1_&&eoms_name&ii.
				group by oppty_evnt_id
				having cnt > 1;
				quit;

				proc sql noprint;
				select count(*) into: qcx_001 separated by ''
				from qc_001;
				quit;

				%put NOTE: qcx_001 = &qcx_001. ;
				
			%mend qcx_001;
			%qcx_001;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - sample data for members
			|
			+---------------------------------------------------------------------------------------------*/			
			%macro edw_sample_members;
			
				%global cnt_mgids cnt_mids cnt_eids;
				
				%let cnt_members = 0;
				%let cnt_mgids   = 10;
				%let cnt_mids    = 10;
				%let cnt_eids    = 10;
				
				proc sql noprint;
				select count(*) into: cnt_members
				from load_1_&&eoms_name&ii. 
				where SPCLT_PTNT_SRC_CD = "";
				quit;
				
				%put NOTE: cnt_members = &cnt_members. ;
				
				%if &cnt_members. > 10 %then %do;
				
					data sample_members;
					set load_1_&&eoms_name&ii. ;
					where SPCLT_PTNT_SRC_CD = "";
					run;
					
					data sample_members;
					set sample_members;
					if _n_ > 10 then stop;
					run;
					
					proc sql noprint;
					select "'"||trim(left(PRSN_SSK_4))||"'" into: sample_member_ids separated by ','
					from sample_members;
					quit;
					
					proc sql noprint;
					select trim(left(EDW_MBR_GID))  into: sample_member_gids separated by ','
					from sample_members;
					quit;
					
					proc sql noprint;
					select trim(left(EPH_LINK_ID))  into: sample_eph_ids separated by ','
					from sample_members;
					quit;					
			
					proc sql noprint;
					connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
					create table sample_member_ids as
					select * from connection to tera
					( 
						select distinct mbr_acct_id
						from &c_s_schema..v_mbr_acct_denorm
						where mbr_acct_id in (&sample_member_ids.)
					);
					disconnect FROM tera;
					quit;
				
					proc sql noprint;
					connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
					create table sample_member_gids as
					select * from connection to tera
					( 
						select distinct mbr_acct_gid
						from &c_s_schema..v_mbr_acct_denorm
						where mbr_acct_gid in (&sample_member_gids.)
					);
					disconnect FROM tera;
					quit;	
					
					proc sql noprint;
					connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
					create table sample_eph_ids as
					select * from connection to tera
					( 
						select distinct to_char(eph_link_id) as eph_link_id
						from &c_s_schema..v_mbr_acct_denorm
						where eph_link_id in (&sample_eph_ids.)
					);
					disconnect FROM tera;
					quit;						
					
					proc sql noprint;
					select count(*) into: cnt_mids
					from sample_member_ids ;
					quit;

					proc sql noprint;
					select count(*) into: cnt_mgids
					from sample_member_gids ;
					quit;	
					
					proc sql noprint;
					select count(*) into: cnt_eids
					from sample_eph_ids ;
					quit;					
				
				%end;
				
				%put NOTE: cnt_mids  = &cnt_mids. ;
				%put NOTE: cnt_mgids = &cnt_mgids. ;
				%put NOTE: cnt_eids  = &cnt_eids. ;
				
				
			%mend edw_sample_members;
			%edw_sample_members;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - Validate opportunity message IDs 
			|
			|  LOGIC 5 - validate if IDs are being reused for opportunity or exist for disposition
			|
			|	1.  QC if IDs are being re-used for opportunity
			|	2.  QC if IDs exist for disposition updates
			|
			+---------------------------------------------------------------------------------------------*/				
			%macro edw_sample_opp(data_type=, data_in=, data_in2=NONE);
			
				%global cnt_edw_oids cnt_edw_oids1 cnt_edw_oids2 cnt_edw_oids3 
				        cnt_edw_mids cnt_edw_mids1 cnt_edw_mids2 cnt_edw_mids3
				        cnt_edw_rids;
				
				%let cnt_edw_opps  = 0;
				%let cnt_edw_oids  = 0;
				%let cnt_edw_oids1 = 0;
				%let cnt_edw_oids2 = 0;
				%let cnt_edw_oids3 = 0;
				%let cnt_edw_mids  = 0;
				%let cnt_edw_mids1 = 0;
				%let cnt_edw_mids2 = 0;
				%let cnt_edw_mids3 = 0;
				%let cnt_edw_rids  = 0;
				
				proc sql noprint;
				select count(*) into: cnt_edw_opps separated by ''
				from &data_in.  ;
				quit;
				
				%put NOTE: cnt_edw_opps = &cnt_edw_opps. ;
				
				%if &cnt_edw_opps. > 1 %then %do;


					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - create data files 
					+---------------------------------------------------------------------------------------------*/				
					data sample_data;   /** <------------------- integer values to compare to past database opp files **/
					set &data_in. (rename=(OPPTY_EVNT_ID=OPPTY_EVNT_ID2 MSG_EVNT_ID=MSG_EVNT_ID2 RX_FILL_EVNT_ID=RX_FILL_EVNT_ID2));  
					if OPPTY_EVNT_ID2 ne '' then OPPTY_EVNT_ID=OPPTY_EVNT_ID2*1;  else OPPTY_EVNT_ID=1;
					if MSG_EVNT_ID2 ne '' then MSG_EVNT_ID=MSG_EVNT_ID2*1;	else MSG_EVNT_ID=1;
					if RX_FILL_EVNT_ID2 ne '' then RX_FILL_EVNT_ID=RX_FILL_EVNT_ID2*1;  else RX_FILL_EVNT_ID=1;
					if OPPTY_ACTION_IND = '' then OPPTY_ACTION_IND='A';
					%if &data_type. = opportunity %then %do;
					if OPPTY_ACTION_IND = 'A';
					%end;
					keep OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID ; 
					run;
					
					data sample_data2;  /** <------------------- character values to compare to todays sas opp file **/
					set &data_in.  ;   
					%if &data_type. = opportunity %then %do;
					if OPPTY_ACTION_IND = 'A';
					%end;
					keep OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID ;
					run;					


					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - load data files into teradata
					+---------------------------------------------------------------------------------------------*/			
					%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

					proc sql;
					connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
					execute(   

						CREATE MULTISET TABLE dss_cea2.dqi_eoms_tera_&mbr_process_id. ,FALLBACK ,
						     NO BEFORE JOURNAL,
						     NO AFTER JOURNAL,
						     CHECKSUM = DEFAULT,
						     DEFAULT MERGEBLOCKRATIO,
						     MAP = TD_MAP2
						     ( 
						      OPPTY_EVNT_ID DECIMAL(17,0),
						      MSG_EVNT_ID DECIMAL(17,0),
						      RX_FILL_EVNT_ID DECIMAL(17,0) 
							 )
						PRIMARY INDEX ( OPPTY_EVNT_ID )

					) by tera;
					execute (commit work) by tera;  
					disconnect from tera;
					quit; 
					
					%m_bulkload_sasdata(bulk_data=sample_data, bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id. , bulk_option=APPEND, copy_data=N); 
					
					%put NOTE: &syserr. ;	

					%if &syserr. > 6 %then %do;
					  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
					  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 4),
							   abend_message=%str(EA ECOA - Proc Append Data failed - step 4));		  
					%end;	
					
					%m_bulkload_sasdata;  
					
					%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., index_in=OPPTY_EVNT_ID);
					%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., column_in=MSG_EVNT_ID);
					%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., column_in=RX_FILL_EVNT_ID);
					
					
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - create template data in case of teradata deadlock
					+---------------------------------------------------------------------------------------------*/					
					data cnt_edw_oids1;cnt_edw_oids=0;run;
					data cnt_edw_oids2;cnt_edw_oids=0;run;
					data cnt_edw_oids3;cnt_edw_oids=0;run;
					data cnt_edw_mids1;cnt_edw_mids=0;run;
					data cnt_edw_mids2;cnt_edw_mids=0;run;
					data cnt_edw_mids3;cnt_edw_mids=0;run;
					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - check if same opp ID + message ID exist DWV_CAMPGN + EDW
					|
					|  NOTE - the opp ID could exist but files can contain subsequent messages for the same opp
					+---------------------------------------------------------------------------------------------*/					
					proc sql;
					connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
					create table cnt_edw_oids1  as 
					select * from connection to tera
					(			 
						select count(*) as cnt_edw_oids 
						from dwv_campgn.event_message a,
						     dss_cea2.dqi_eoms_tera_&mbr_process_id. b
						where a.oppty_evnt_id=b.oppty_evnt_id
						and a.msg_evnt_id=b.msg_evnt_id
						and year(a.rec_crte_ts) >= 2021 
						and a.oppty_evnt_id > 4000000000000000;
						
 
					);
					disconnect from tera;
					quit; 
					
					
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - check if same message ID exist DSS_CEA2 (data might not exist yet in DWV_CAMPGN + EDW)
					+---------------------------------------------------------------------------------------------*/					
					proc sql;
					connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
					create table cnt_edw_oids2  as 
					select * from connection to tera
					(			 
						select count(*) as cnt_edw_oids 
						from dss_cea2.dqi_eoms_opportunity a,
						     dss_cea2.dqi_eoms_tera_&mbr_process_id. b
						where to_number(a.oppty_evnt_id)=b.oppty_evnt_id 
						and to_number(a.msg_evnt_id)=b.msg_evnt_id
						and a.execution_date <> current_date - &execution_date.
						and substr(a.execution_triage,1,1) = '0';
 
					);
					disconnect from tera;
					quit;
					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - check if same message ID exist DWV_CAMPGN + EDW
					+---------------------------------------------------------------------------------------------*/
					proc sql;
					connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
					create table cnt_edw_mids1  as 
					select * from connection to tera
					(			 
						select count(*) as cnt_edw_mids 
						from dwv_campgn.event_message a,
						     dss_cea2.dqi_eoms_tera_&mbr_process_id. b
						where a.msg_evnt_id=b.msg_evnt_id 
						and year(a.rec_crte_ts) >= 2021 
						and a.msg_evnt_id > 4000000000000000;
 
					);
					disconnect from tera;
					quit; 


					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - check if same message ID exist DSS_CEA2 (data might not exist yet in DWV_CAMPGN + EDW)
					+---------------------------------------------------------------------------------------------*/					
					proc sql;
					connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
					create table cnt_edw_mids2  as 
					select * from connection to tera
					(			 
						select count(*) as cnt_edw_mids 
						from dss_cea2.dqi_eoms_opportunity a,
						     dss_cea2.dqi_eoms_tera_&mbr_process_id. b
						where to_number(a.msg_evnt_id)=b.msg_evnt_id 
						and a.execution_date <> current_date - &execution_date.
						and substr(a.execution_triage,1,1) = '0';
 
					);
					disconnect from tera;
					quit;	




					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - retain disposition if MSG_DISPN_CD = 1 and MSG_DISPN_SUB_CD =  79 80 237
					|
					|  1    Delivered
					|
					|  79	Email Opened Opt Out
					|  80	Email Opened Clicked Spam
					|  237	Patient Opt-out SMS
					|
					}  NOTE - Channel code for dwv_campgn is numeric and channel code for cpl is character
					|
					+---------------------------------------------------------------------------------------------*/
					%if &data_type. = disposition %then %do;
					
						proc sql;
						connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
						create table disposition_retain  as 
						select * from connection to tera
						(			 
							select to_char(a.msg_evnt_id) as msg_evnt_id,  
							       a.msg_dispn_sub_cd as msg_dispn_sub_cd_updt 
							from dwv_campgn.event_message                a,
							     dss_cea2.dqi_eoms_tera_&mbr_process_id. b
							where a.msg_evnt_id=b.msg_evnt_id 
							and a.msg_src_cd = 'CPL'
							and a.msg_dispn_cd in ('1')
							and a.msg_dispn_sub_cd in ('79','80','237');
						);
						disconnect from tera;
						quit; 
						
						%let cnt_disposition_retain = 0;
						
						proc sql noprint;
						select count(*) into: cnt_disposition_retain separated by ''
						from disposition_retain  ;
						quit;	
						
						%put NOTE: cnt_disposition_retain = &cnt_disposition_retain. ;
						
						data _null_;
						set &data_in. (obs=10) ;
						put msg_evnt_id chnl_cd msg_dispn_sub_cd;
						run;
						
						%if &cnt_disposition_retain. = 0 %then %do;
							%put NOTE: There are no disposition retains for message IDs;
						%end;
						%else %do;
						
							%let cnt_disposition_before = 0;
							
							proc sql noprint;
							select count(*) into: cnt_disposition_before separated by ''
							from &data_in. 
							where msg_dispn_sub_cd in ('79','80','237');
							quit;
							
							%put NOTE:  cnt_disposition_before = &cnt_disposition_before. ;

							proc sort data = &data_in. ;
							by msg_evnt_id ;
							run;

							proc sort data = disposition_retain;
							by msg_evnt_id ;
							run;

							data &data_in.;
							merge &data_in. (in=a) disposition_retain (in=b);
							if a;
							by msg_evnt_id ;
							if a and b then do;
							   msg_dispn_sub_cd=msg_dispn_sub_cd_updt;
							end;
							drop msg_dispn_sub_cd_updt;
							run;
							
							%let cnt_disposition_after = 0;
							
							proc sql noprint;
							select count(*) into: cnt_disposition_after separated by ''
							from &data_in. 
							where msg_dispn_sub_cd in ('79','80','237');
							quit;
							
							%put NOTE:  cnt_disposition_after = &cnt_disposition_after. ;							
							
						%end;
					
					%end;
					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - check if same message ID exist SAS 
					|
					|	load_1_20220101000456 = SFMC 
					|	load_1_20220101000123 = CPL
					+---------------------------------------------------------------------------------------------*/
					%if &data_type. = disposition %then %do;
					
						%if &data_in2. = NONE %then %do;
						%end;
						%else %do; 

							proc sql noprint;
							select count(*) into: cnt_edw_mids3 separated by ''
							from sample_data2 a,
							     &data_in2.   b
							where a.oppty_evnt_id=b.oppty_evnt_id
							and   a.msg_evnt_id=b.msg_evnt_id
							and   length(a.oppty_evnt_id) > 5;
							quit;

							%let cnt_edw_oids3 = &cnt_edw_mids3. ;

							%put NOTE: cnt_edw_oids3 = &cnt_edw_oids3. ; 
							%put NOTE: cnt_edw_mids3 = &cnt_edw_mids3. ;

						%end;
						
					%end;
					%else %do;
					
						%let cnt_edw_oids3 = 0 ;
						%let cnt_edw_mids3 = 0 ;
						
					%end;
					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - assess counts from dwv_campgn + dss_cea2
					+---------------------------------------------------------------------------------------------*/				
					proc sql noprint;
					select cnt_edw_oids into: cnt_edw_oids1 separated by ''
					from cnt_edw_oids1 ;
					quit;
					
					proc sql noprint;
					select cnt_edw_oids into: cnt_edw_oids2 separated by ''
					from cnt_edw_oids2 ;
					quit;					

					proc sql noprint;
					select cnt_edw_mids into: cnt_edw_mids1 separated by ''
					from cnt_edw_mids1 ;
					quit;
					
					proc sql noprint;
					select cnt_edw_mids into: cnt_edw_mids2 separated by ''
					from cnt_edw_mids2 ;
					quit;					
				
				%end;
				
				%let cnt_edw_oids=%eval(&cnt_edw_oids1. + &cnt_edw_oids2. + &cnt_edw_oids3.);
				%let cnt_edw_mids=%eval(&cnt_edw_mids1. + &cnt_edw_mids2. + &cnt_edw_mids3.);


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - metrics from dwv_campgn + dss_cea2
				+---------------------------------------------------------------------------------------------*/				
				%put NOTE: data type     = &data_type. ;
				%put NOTE: cnt_edw_opps  = &cnt_edw_opps. ;
				
				%put NOTE: cnt_edw_oids1 = &cnt_edw_oids1. ;
				%put NOTE: cnt_edw_oids2 = &cnt_edw_oids2. ;
				%put NOTE: cnt_edw_oids3 = &cnt_edw_oids3. ;
				%put NOTE: cnt_edw_mids1 = &cnt_edw_mids1. ;
				%put NOTE: cnt_edw_mids2 = &cnt_edw_mids2. ;
				%put NOTE: cnt_edw_mids3 = &cnt_edw_mids3. ;
				

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - determine if issue with opp ID + message ID or message ID from opportunity file
				|
				|	  1.  evaluate cnt_edw_oids = if opportunity IDs dont exist within EDW
				|	  2.  evaluate cnt_edw_mids = if message     IDs dont exist within EDW
				|
				+---------------------------------------------------------------------------------------------*/
				
				%if &cnt_edw_opps. > 1 %then %do;
				
					%if &data_type. = opportunity %then %do;

						%let cnt_edw_oids=0;
						%let cnt_edw_mids=0;

						%if &cnt_edw_oids. ne 0 %then %do;    /** <------------- opportunity data should not exist in EDW **/

							%put NOTE: Opportunity ID EXIST within EDW - BAD;

							%let c_s_email_subject = EA ECOA - Step 4 Failure Opportunity ID EXIST within EDW for Opportunity;
							%m_abend_handler(abend_report=%str(EA ECOA - Step 4 Failure Opportunity ID Exist within EDW),
							abend_message=%str(EA ECOA - Step 4 Failure Opportunity ID EXIST within EDW));	

						%end;
						%else %if &cnt_edw_mids. ne 0 %then %do;    /** <------------- opportunity data should not exist in EDW **/

							%put NOTE: Message IDs DNE within EDW - BAD;

							%let c_s_email_subject = EA ECOA - Step 4 Failure Message ID EXIST within EDW for Opportunity;
							%m_abend_handler(abend_report=%str(EA ECOA - Step 4 Failure Message ID DNE within EDW),
							abend_message=%str(EA ECOA - Step 4 Failure Message ID EXIST within EDW));		

						%end;
						%else %do;
							%put NOTE: Opportunity ID DNE within EDW - GOOD;
							
							%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
						%end;					

					%end;

					%if &data_type. = disposition %then %do;
					
						%let cnt_edw_oids=10;
						%let cnt_edw_mids=10;
					
						%if &cnt_edw_mids. = 0 %then %do;  /** <------------- disposition data should exist in EDW **/

							%put NOTE: Message IDs DNE within EDW - BAD;

							%let c_s_email_subject = EA ECOA - Step 4 Failure Message ID DNE within EDW for Dispositon;
							%m_abend_handler(abend_report=%str(EA ECOA - Step 4 Failure Message ID DNE within EDW),
							abend_message=%str(EA ECOA - Step 4 Failure Message ID DNE within EDW));		

						%end;
						%else %if &cnt_edw_oids. = 0 %then %do;  /** <------------- opportunity data should exist in EDW **/

							%put NOTE: Opportunity IDs DNE within EDW - BAD;

							%let c_s_email_subject = EA ECOA - Step 4 Failure Opportunity ID DNE within EDW for Dispositon;
							%m_abend_handler(abend_report=%str(EA ECOA - Step 4 Failure Opportunity ID DNE within EDW),
							abend_message=%str(EA ECOA - Step 4 Failure Opportunity ID DNE within EDW));		

						%end;									
						%else %do;
							%put NOTE: Opportunity + Message IDs EXIST within EDW - GOOD;
							
							%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
						%end;				

					%end;	
				
				%end;
				
			%mend edw_sample_opp;
			%edw_sample_opp(data_type=opportunity, data_in=load_1_&&eoms_name&ii.);			
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build logic for QC rules
			|
			|  LOGIC 6 - validate data based on table rules
			|
			+---------------------------------------------------------------------------------------------*/
			data dqi_eoms_rules_opp;
			format rule_sas $500. ;
			set dqi_eoms_rules_opp;
			n=put(left(_n_),8.);
			eoms_file="&eoms_file.";
			eoms_name=scan(eoms_file,2,'.');
			rule_id_name="rule"||trim(left(rule_id));
			if rule_type = 1 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||" = 1;";
			end;
			else if rule_type = 2 then do;
			rule_sas='if '||trim(left(data_attribute_name))||" ne '' then "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules))||");";
			end;
			else if rule_type = 3 then do;
			rule_sas='if '||trim(left(data_attribute_name))||" * 1 = . then "||trim(left(rule_id_name))||"=1;";
			end;
			else if rule_type = 4 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||" = 1;";
			end;
			else if rule_type = 5 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||" = 1; end;";
			end;
			else if rule_type = 6 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules2))||"); end;";
			end;
			else if rule_type = 7 then do;
			rule_sas='if '||trim(left(data_attribute_name))||" ne 'CRITICAL_QC' then "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules))||");";
			end;
			else if rule_type = 8 then do;
			rule_sas='if missing('||trim(left(data_attribute_name))||') or '||trim(left(data_attribute_name))||'= "" then '||trim(left(rule_id_name))||" = 1;";
			end;
			else if rule_type = 9 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules2))||"); end;";
			end;			
			call symput('rule'||n,trim(left(rule_sas)));
			call symput('rule_id'||n,trim(left(rule_id_name)));
			call symput('rule_severity'||n,trim(left(data_rules_severity)));
			call symput('name_layout',left(trim(eoms_name))); 
			call symput('rule_total',n);
			run;
						

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - appy QC rules
			+---------------------------------------------------------------------------------------------*/
			data load_1_&&eoms_name&ii.; 
			set load_1_&&eoms_name&ii.;
			CHNL_CD=CHNL_CD_ORIGINAL;
			run;			
			
			
			data load_1_&&eoms_name&ii.;
			format %do j=1 %to &rule_total.; &&rule_id&j %end; 8.;
			set load_1_&&eoms_name&ii.;
			%do m=1 %to &rule_total.; &&rule&m %end; 
			run;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build override QC rules based on eoms configuration
			|
			|  LOGIC 7 - override rules			
			|
			|  rule5000 - Do not send to CMCTN History based on dwv_eoms.cfg_dlvry_type.comm_hist_upd_ind ne Y
			|
			|             1. dwv_eoms.cfg_dlvry_type - defines what opportunity subtype ID + channels for cmctn history
			|	      2. Reset stellents as acceptable if should not go to cmctn history (doesnt matter if valid or not within PBM)
			|
			|	     example:
			|	      1. 14 - Letter (Fiserv)			CHLTRFSRV      	14	Y      	CHLTRFSRV	BRAND_COMPLIANCE	MAIL
			|	      2. 17 - eMail (eDialog)			CHEMLEDLG      	17	Y      	CHEMLEDLG	BRAND_COMPLIANCE	EMAIL
			|	      3. 16 - Outbound Call Center (Carenet)	CHOBCCCRNT      16	Y      	CHOBCCCRNT	BRAND_COMPLIANCE	PHONE
			|			
			+---------------------------------------------------------------------------------------------*/ 
			
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table cfg_dlvry_type as
			select * from connection to tera
			( 
				select *
				from dwv_eoms.cfg_dlvry_type
			);
			disconnect FROM tera;
			quit;
			

			data cfg_dlvry_type;
			set cfg_dlvry_type;
			if index(DLVRY_CODE,'_OLD') = 0;  
			run;

			data cfg_dlvry_type_history;
			format CHNL_CD OPPORTUNITY_SUB_TYPE_PROD_ID $5000. ;
			set cfg_dlvry_type; 
			**if DLVRY_CODE = 'RXCONNECT';
			OPPORTUNITY_SUB_TYPE_PROD_ID='"'||trim(left(DLVRY_SUBTYPES_INCL))||'"';
			OPPORTUNITY_SUB_TYPE_PROD_ID=tranwrd(OPPORTUNITY_SUB_TYPE_PROD_ID,',','","');
			CHNL_CD='"'||trim(left(DLVRY_CHNL_CD_INCL))||'"';
			CHNL_CD=tranwrd(CHNL_CD,',','","');
			if COMM_HIST_UPD_IND = 'N';  /** <-------------------------------------- chnl_cd + opportunity_sub_type_prod_id not required for CMCTN History **/
			keep DLVRY_CODE DLVRY_DSC COMM_HIST_UPD_IND CHNL_CD OPPORTUNITY_SUB_TYPE_PROD_ID; 
			run;			

			proc sort data = cfg_dlvry_type_history nodupkey;
			by CHNL_CD OPPORTUNITY_SUB_TYPE_PROD_ID COMM_HIST_UPD_IND;
			run; 					

			data cfg_dlvry_type_history;
			format rule_sas  $5000. ;
			set cfg_dlvry_type_history  end=eof;
			n=put(left(_n_),8.); 
			rule_id='5000';
			rule_type=1;
			rule_id_name="rule"||trim(left(rule_id)); 

			/**---- do not load to cmctn history but reset stellent rule to acceptable and load to PBM  ----***/
			if n = '1' then do;
				if length(OPPORTUNITY_SUB_TYPE_PROD_ID) > 3 then do;
				rule_sas='if CHNL_CD in ('||trim(left(CHNL_CD))||') and OPPTY_TYP_PROD_ID in ('||trim(left(OPPORTUNITY_SUB_TYPE_PROD_ID))||") then do; rule7=.;rule35=.;rule5000=1; end;";
				end;
				else do;
				rule_sas='if CHNL_CD in ('||trim(left(CHNL_CD))||') then do; rule7=.;rule35=.;rule5000=1; end;';
				end;
				output;
			end;  
			else do;
				if length(OPPORTUNITY_SUB_TYPE_PROD_ID) > 3 then do;
				rule_sas='else if CHNL_CD in ('||trim(left(CHNL_CD))||') and OPPTY_TYP_PROD_ID in ('||trim(left(OPPORTUNITY_SUB_TYPE_PROD_ID))||") then do; rule7=.;rule35=.;rule5000=1; end;";
				end;
				else do;
				rule_sas='else if CHNL_CD in ('||trim(left(CHNL_CD))||') then do; rule7=.;rule35=.;rule5000=1; end;';
				end;
				output;
			end;
			if eof then do;  
				rule_sas="else if CHNL_CD ne '' and OPPTY_TYP_PROD_ID ne '' then do; if rule7 ne 1 and rule35 ne 1 then rule5000=.; else rule5000=1; end;";
				output;
			end;  
			run;	

			data _null_;	
			set cfg_dlvry_type_history;
			n=put(left(_n_),8.);
			call symput('ruleov'||n,trim(left(rule_sas))); 
			call symput('ruleov_total',n);
			run;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - apply override rules based on eoms configuration
			+---------------------------------------------------------------------------------------------*/
			data load_1_&&eoms_name&ii.; 
			set load_1_&&eoms_name&ii.;
			%do n=1 %to &ruleov_total.; &&ruleov&n %end;  
			run;	
			


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build validation rules based on eoms configuration
			|
			|  LOGIC 8 - validation rules			
			|
			|  rule6000 - Do not send to IT based on dwv_eoms.cfg_dlvry_type.comm_hist_upd_ind opportunity subtype ID + channels
			|
			|             1. dwv_eoms.cfg_dlvry_type - defines what opportunity subtype ID + channels for every pbm campaign
			|	      2. every pbm campaign should be registered within dwv_eoms.cfg_dlvry_type - if not then improper setup
			|	      3. if channel and opportunity subtype dont match configuration then set as invalid
			|			
			+---------------------------------------------------------------------------------------------*/ 
			
			data cfg_dlvry_type_xref;
			format CHNL_CD OPPORTUNITY_SUB_TYPE_PROD_ID $5000. ;
			set cfg_dlvry_type; 
			**if DLVRY_CODE = 'RXCONNECT';
			OPPORTUNITY_SUB_TYPE_PROD_ID='"'||trim(left(DLVRY_SUBTYPES_INCL))||'"';
			OPPORTUNITY_SUB_TYPE_PROD_ID=tranwrd(OPPORTUNITY_SUB_TYPE_PROD_ID,',','","');
			CHNL_CD='"'||trim(left(DLVRY_CHNL_CD_INCL))||'"';
			CHNL_CD=tranwrd(CHNL_CD,',','","');
			if length(DLVRY_CHNL_CD_INCL) > 3 and length(DLVRY_SUBTYPES_INCL) > 3; /** <--------------------- filter out valid chnl_cd + opportunity_sub_type_prod_id **/  
			keep DLVRY_CODE DLVRY_DSC COMM_HIST_UPD_IND CHNL_CD OPPORTUNITY_SUB_TYPE_PROD_ID; 
			run;			

			proc sort data = cfg_dlvry_type_xref nodupkey;
			by CHNL_CD OPPORTUNITY_SUB_TYPE_PROD_ID COMM_HIST_UPD_IND;
			run; 	

			data cfg_dlvry_type_xref;
			format rule_sas  $5000. ;
			set cfg_dlvry_type_xref  end=eof;
			n=put(left(_n_),8.); 
			rule_id='6000';
			rule_type=1;
			rule_id_name="rule"||trim(left(rule_id)); 

			/**---- do not load to PBM  = missing is valid + 1 is invalid ----***/
			if n = '1' then do;
				rule_sas='if CHNL_CD in ('||trim(left(CHNL_CD))||') and OPPTY_TYP_PROD_ID in ('||trim(left(OPPORTUNITY_SUB_TYPE_PROD_ID))||") then do; rule6000=.; end;";
				output;
			end;  
			else do;
				rule_sas='else if CHNL_CD in ('||trim(left(CHNL_CD))||') and OPPTY_TYP_PROD_ID in ('||trim(left(OPPORTUNITY_SUB_TYPE_PROD_ID))||") then do; rule6000=.; end;";
				output;
			end;
			if eof then do;  
				rule_sas="else do; rule6000=1; end;";
				output;
			end;  
			run;

			data _null_;	
			set cfg_dlvry_type_xref;
			n=put(left(_n_),8.);
			call symput('rulexr'||n,trim(left(rule_sas))); 
			call symput('rulexr_total',n);
			run;	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - apply override rules based on eoms configuration
			+---------------------------------------------------------------------------------------------*/
			data load_1_&&eoms_name&ii.; 
			set load_1_&&eoms_name&ii.;
			%do n=1 %to &rulexr_total.; &&rulexr&n %end;  
			run;
	
	
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - reset specialty rules based on specialty campaign targeting non-pbm data
			|
			|  LOGIC 9 - specialty rules			
			|			
			|
			|         qc rules for IT will reject the data if 98s are missing values
			|         refer to CTT-35 bteq_E_SFMC_OPPORTUNITY.bteq.txt for all rules
			+---------------------------------------------------------------------------------------------*/			
			proc sql noprint;
			create table dqi_eoms_rules_specialty as
			select *
			from dss_cea2.dqi_eoms_rules
			where data_rules_severity_specialty in ('99','98')
			order by layout_id, rule_id;
			quit;	

			data dqi_eoms_rules_specialty; 
			set dqi_eoms_rules_specialty;
			n=put(left(_n_),8.); 
			rule_id_name="rule"||trim(left(rule_id)); 
			call symput('srule_id'||n,trim(left(rule_id_name)));  
			call symput('srule_total',n); 	
			run;

			data load_1_&&eoms_name&ii.; 
			set load_1_&&eoms_name&ii.;
			if SPCLT_PTNT_SRC_CD ne '' then do;
			%do n=1 %to &srule_total.; &&srule_id&n = . ; %end;  
			end;
			run;	
			
			proc sql noprint;
			create table dqi_eoms_rules_specialty2 as
			select *
			from dss_cea2.dqi_eoms_rules
			where data_rules_severity_specialty in ('98')
			order by layout_id, rule_id;
			quit;	

			data dqi_eoms_rules_specialty2; 
			set dqi_eoms_rules_specialty2;
			n=put(left(_n_),8.); 
			rule_id_name="rule"||trim(left(rule_id)); 
			call symput('ssrule_id'||n,trim(left(data_attribute_name)));  
			call symput('ssrule_total',n); 	
			run;

			data load_1_&&eoms_name&ii.; 
			set load_1_&&eoms_name&ii.;
			if SPCLT_PTNT_SRC_CD ne '' then do;
			%do n=1 %to &ssrule_total.; if &&ssrule_id&n = '' then &&ssrule_id&n = '1' ; %end;  
			end;
			run;	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - test
			|
			+---------------------------------------------------------------------------------------------*/			
			%if &ecoa_test.	= YES4A %then %do;

				data working.qc_step4a_&&source_team&ii.;
				set load_1_&&eoms_name&ii. ;
				run;	
				
				%m_ecoa_process(do_tasks=ALL);	

			%end;			
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - determine severity of data profiling
			|
			|  LOGIC 10 - assess rules	
			|
			|  rule_total4 = cmctn history - turn off rule35 stellent suffix 2022.09.21
			|			
			+---------------------------------------------------------------------------------------------*/			
			data load_1_&&eoms_name&ii.;
			format rule_total1 rule_total2 rule_total3 rule_total4 rule_total5  8.;
			set load_1_&&eoms_name&ii.;
			rule_total1=coalesce(%do n=1 %to &rule_total.; %if &&rule_severity&n = 1 %then %do; &&rule_id&n , %end; %end; 0); /** <----------------- EOMS Primary   **/
			rule_total2=coalesce(%do n=1 %to &rule_total.; %if &&rule_severity&n = 2 %then %do; &&rule_id&n , %end; %end; 0); /** <----------------- EOMS Secondary **/
			rule_total3=coalesce(%do n=1 %to &rule_total.; %if &&rule_severity&n = 3 %then %do; &&rule_id&n , %end; %end; 0); /** <----------------- EOMS Tertiary  **/
			rule_total4=coalesce(rule7,rule66,rule329,rule5000,0);  /** <------ CMCTN History  - 7=Stellents prefix 35=Stellents suffix 66=QL bene ID 329=specialty 5000=configuration **/ 
			rule_total5=coalesce(rule7,rule66,rule5000,0);          /** <------ MCS History    - 7=Stellents prefix 35=Stellents suffix 66=QL bene ID 5000=configuration  **/		
			run;	
	

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - opportunity data elements - convert character channel (cpl-sfmc) to numeric channel  (edw)
			+---------------------------------------------------------------------------------------------*/	 
			data load_1_&&eoms_name&ii.;
			set load_1_&&eoms_name&ii.;
			chnl_cd=put(chnl_cd,_chnlfmt.);	
			run;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - Validate opportunity QC - CTT-35 bteq_E_SFMC_OPPORTUNITY.bteq.txt
			|
			|  LOGIC 11 - bteq rules are the rules that the IT team applies for loading into dwv_campgn			
			|			
			|
			+---------------------------------------------------------------------------------------------*/			 		
			%let cnt_bteq = 0;
			
			proc sql noprint;
			select count(*) into: cnt_bteq separated by ''
			from load_1_&&eoms_name&ii. ;
			quit;
			
			%put NOTE: cnt_bteq = &cnt_bteq. ;
			
			%if &cnt_bteq. > 1 %then %do;
			
				data qc_bteq;
				format err_desc $1000.;
				set load_1_&&eoms_name&ii.;
				err_desc=''; 
				run;

				proc sql noprint;
				update qc_bteq
				set err_desc =
					CASE WHEN SVC_TRANS_ID IS NULL OR TRIM(SVC_TRANS_ID) = '' THEN 'SVC_TRANS_ID IS NULL OR BLANK.' ELSE '' END ||
					CASE WHEN REC_CRTE_TRANS_ID IS NULL OR TRIM(REC_CRTE_TRANS_ID) = '' THEN 'REC_CRTE_TRANS_ID IS NULL OR BLANK.'  ELSE '' END ||
					CASE WHEN CLT_LEVEL_1 IS NULL OR TRIM(CLT_LEVEL_1) = '' THEN 'CLT_LEVEL_1 IS NULL OR BLANK.' 	     ELSE '' END ||
					CASE WHEN CLT_LEVEL_2 IS NULL OR TRIM(CLT_LEVEL_2) = '' THEN 'CLT_LEVEL_2 IS NULL OR BLANK.' 	     ELSE '' END ||
					CASE WHEN CLT_LEVEL_3 IS NULL OR TRIM(CLT_LEVEL_3) = '' THEN 'CLT_LEVEL_3 IS NULL OR BLANK.' 	     ELSE '' END ||
					CASE WHEN OPPTY_EVNT_ID IS NULL OR TRIM(OPPTY_EVNT_ID) = '' THEN 'OPPTY_EVNT_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(OPPTY_EVNT_ID,20.) = . THEN 'OPPTY_EVNT_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PGM_ID IS NULL OR TRIM(PGM_ID) = '' THEN ''
					ELSE CASE WHEN input(PGM_ID,20.) = . THEN 'PGM_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN OPPTY_CMPGN_ID IS NULL OR TRIM(OPPTY_CMPGN_ID) = '' THEN ''
					ELSE CASE WHEN input(OPPTY_CMPGN_ID,20.) = . THEN 'OPPTY_CMPGN_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN TTL_MBR_SVNGS IS NULL OR TRIM(TTL_MBR_SVNGS) = '' THEN ''
					ELSE CASE WHEN input(TTL_MBR_SVNGS,20.) = . THEN 'TTL_MBR_SVNGS IS INVALID.'  
					ELSE '' END END||
					CASE WHEN TTL_CLT_SVNGS IS NULL OR TRIM(TTL_CLT_SVNGS) = '' THEN ''
					ELSE CASE WHEN input(TTL_CLT_SVNGS,20.) = . THEN 'TTL_CLT_SVNGS IS INVALID.'  
					ELSE '' END END||
					CASE WHEN TTL_MBR_TIME_SVNGS IS NULL OR TRIM(TTL_MBR_TIME_SVNGS) = '' THEN ''
					ELSE CASE WHEN input(TTL_MBR_TIME_SVNGS,20.) = . THEN 'TTL_MBR_TIME_SVNGS IS INVALID.'  
					ELSE '' END END||
					CASE WHEN MSG_EVNT_ID IS NULL OR TRIM(MSG_EVNT_ID) = '' THEN 'MSG_EVNT_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(MSG_EVNT_ID,20.) = . THEN 'MSG_EVNT_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN CHNL_CD IS NULL OR TRIM(CHNL_CD) = '' THEN ''
					ELSE CASE WHEN input(CHNL_CD,20.) = . THEN 'CHNL_CD IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PRTY_NBR IS NULL OR TRIM(PRTY_NBR) = '' THEN ''
					ELSE CASE WHEN input(PRTY_NBR,20.) = . THEN 'PRTY_NBR IS INVALID.'  
					ELSE '' END END||
					CASE WHEN OPPTY_TYP_PROD_ID IS NULL OR TRIM(OPPTY_TYP_PROD_ID) = '' THEN ''
					ELSE CASE WHEN input(OPPTY_TYP_PROD_ID,20.) = . THEN 'OPPTY_TYP_PROD_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN EDW_MBR_GID IS NULL OR TRIM(EDW_MBR_GID) = '' THEN ''
					ELSE CASE WHEN input(EDW_MBR_GID,20.) = . THEN 'EDW_MBR_GID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PCT_OF_COPAY IS NULL OR TRIM(PCT_OF_COPAY) = '' THEN ''
					ELSE CASE WHEN input(PCT_OF_COPAY,20.) = . THEN 'PCT_OF_COPAY IS INVALID.'  
					ELSE '' END END||
					CASE WHEN COPAY_WAIVR_FILL_NBR IS NULL OR TRIM(COPAY_WAIVR_FILL_NBR) = '' THEN ''
					ELSE CASE WHEN input(COPAY_WAIVR_FILL_NBR,20.) = . THEN 'COPAY_WAIVR_FILL_NBR IS INVALID.'  
					ELSE '' END END||
					CASE WHEN COPAY_WAIVR_AMT IS NULL OR TRIM(COPAY_WAIVR_AMT) = '' THEN ''
					ELSE CASE WHEN input(COPAY_WAIVR_AMT,20.) = . THEN 'COPAY_WAIVR_AMT IS INVALID.'  
					ELSE '' END END||
					CASE WHEN DAY_SPLY_QTY IS NULL OR TRIM(DAY_SPLY_QTY) = '' THEN ''
					ELSE CASE WHEN input(DAY_SPLY_QTY,20.) = . THEN 'DAY_SPLY_QTY IS INVALID.'  
					ELSE '' END END||
					CASE WHEN WAIVR_AMT IS NULL OR TRIM(WAIVR_AMT) = '' THEN ''
					ELSE CASE WHEN input(WAIVR_AMT,20.) = . THEN 'WAIVR_AMT IS INVALID.'  
					ELSE '' END END||
					CASE WHEN NBR_OF_FILL_CNT IS NULL OR TRIM(NBR_OF_FILL_CNT) = '' THEN ''
					ELSE CASE WHEN input(NBR_OF_FILL_CNT,20.) = . THEN 'NBR_OF_FILL_CNT IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PCT_MIN_WAIVR_AMT IS NULL OR TRIM(PCT_MIN_WAIVR_AMT) = '' THEN ''
					ELSE CASE WHEN input(PCT_MIN_WAIVR_AMT,20.) = . THEN 'PCT_MIN_WAIVR_AMT IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PCT_MAX_WAIVR_AMT IS NULL OR TRIM(PCT_MAX_WAIVR_AMT) = '' THEN ''
					ELSE CASE WHEN input(PCT_MAX_WAIVR_AMT,20.) = . THEN 'PCT_MAX_WAIVR_AMT IS INVALID.'  
					ELSE '' END END||
					CASE WHEN WAIVR_DRTN_DAY IS NULL OR TRIM(WAIVR_DRTN_DAY) = '' THEN ''
					ELSE CASE WHEN input(WAIVR_DRTN_DAY,20.) = . THEN 'WAIVR_DRTN_DAY IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PGM_PROD_ID IS NULL OR TRIM(PGM_PROD_ID) = '' THEN ''
					ELSE CASE WHEN input(PGM_PROD_ID,20.) = . THEN 'PGM_PROD_ID IS INVALID.'  
					ELSE '' END END||
					/**CASE WHEN AGNT_PTY_ID_OPPTY IS NULL OR TRIM(AGNT_PTY_ID_OPPTY) = '' THEN ''
					ELSE CASE WHEN input(AGNT_PTY_ID_OPPTY,20.) = . THEN 'AGNT_PTY_ID_OPPTY IS INVALID.'  
					ELSE '' END END||**/
					/**CASE WHEN NOTE_EVNT_ID_OPPTY IS NULL OR TRIM(NOTE_EVNT_ID_OPPTY) = '' THEN ''
					ELSE CASE WHEN input(NOTE_EVNT_ID_OPPTY,20.) = . THEN 'NOTE_EVNT_ID_OPPTY IS INVALID.'  
					ELSE '' END END||**/
					CASE WHEN PRSCRPT_FILL_ID IS NULL OR TRIM(PRSCRPT_FILL_ID) = '' THEN ''
					ELSE CASE WHEN input(PRSCRPT_FILL_ID,20.) = . THEN 'PRSCRPT_FILL_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN MSG_TYP_PROD_ID IS NULL OR TRIM(MSG_TYP_PROD_ID) = '' THEN ''
					ELSE CASE WHEN input(MSG_TYP_PROD_ID,20.) = . THEN 'MSG_TYP_PROD_ID IS INVALID.'  
					ELSE '' END END||
					/**CASE WHEN AGNT_PTY_ID_MSG IS NULL OR TRIM(AGNT_PTY_ID_MSG) = '' THEN ''
					ELSE CASE WHEN input(AGNT_PTY_ID_MSG,20.) = . THEN 'AGNT_PTY_ID_MSG IS INVALID.'  
					ELSE '' END END||**/
					/**CASE WHEN NOTE_EVNT_ID_MSG IS NULL OR TRIM(NOTE_EVNT_ID_MSG) = '' THEN ''
					ELSE CASE WHEN input(NOTE_EVNT_ID_MSG,20.) = . THEN 'NOTE_EVNT_ID_MSG IS INVALID.'  
					ELSE '' END END||**/
					CASE WHEN EVNT_PTY_ID IS NULL OR TRIM(EVNT_PTY_ID) = '' THEN 'EVNT_PTY_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(EVNT_PTY_ID,20.) = . THEN 'EVNT_PTY_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PTY_ID IS NULL OR TRIM(PTY_ID) = '' THEN ''
					ELSE CASE WHEN input(PTY_ID,20.) = . THEN 'PTY_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN ACCT_ID IS NULL OR TRIM(ACCT_ID) = '' THEN ''
					ELSE CASE WHEN input(ACCT_ID,20.) = . THEN 'ACCT_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN CLT_AGGR_LIST_ID IS NULL OR TRIM(CLT_AGGR_LIST_ID) = '' THEN ''
					ELSE CASE WHEN input(CLT_AGGR_LIST_ID,20.) = . THEN 'CLT_AGGR_LIST_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN EVNT_CNTCT_INFO_ID IS NULL OR TRIM(EVNT_CNTCT_INFO_ID) = '' THEN ''
					ELSE CASE WHEN input(EVNT_CNTCT_INFO_ID,20.) = . THEN 'EVNT_CNTCT_INFO_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PTNT_ID IS NULL OR TRIM(PTNT_ID) = '' THEN ''
					ELSE CASE WHEN input(PTNT_ID,20.) = . THEN 'PTNT_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN SPCLT_PTNT_GID IS NULL OR TRIM(SPCLT_PTNT_GID) = '' THEN ''
					ELSE CASE WHEN input(SPCLT_PTNT_GID,20.) = . THEN 'SPCLT_PTNT_GID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN EVNT_PTY_CNTCT_ID IS NULL OR TRIM(EVNT_PTY_CNTCT_ID) = '' THEN 'EVNT_PTY_CNTCT_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(EVNT_PTY_CNTCT_ID,20.) = . THEN 'EVNT_PTY_CNTCT_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN VNDR_CASE_ID IS NULL OR TRIM(VNDR_CASE_ID) = '' THEN 'VNDR_CASE_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(VNDR_CASE_ID,20.) = . THEN 'VNDR_CASE_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN OPPTY_PHCY_PYMT IS NULL OR TRIM(OPPTY_PHCY_PYMT) = '' THEN ''
					ELSE CASE WHEN input(OPPTY_PHCY_PYMT,20.) = . THEN 'OPPTY_PHCY_PYMT IS INVALID.'  
					ELSE '' END END||
					CASE WHEN CAMPAIGN_ID IS NULL OR TRIM(CAMPAIGN_ID) = '' THEN 'CAMPAIGN_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(CAMPAIGN_ID,20.) = . THEN 'CAMPAIGN_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN QUANTITY IS NULL OR TRIM(QUANTITY) = '' THEN ''
					ELSE CASE WHEN input(QUANTITY,20.) = . THEN 'QUANTITY IS INVALID.'  
					ELSE '' END END||
					CASE WHEN RATE IS NULL OR TRIM(RATE) = '' THEN ''
					ELSE CASE WHEN input(RATE,20.) = . THEN 'RATE IS INVALID.'  
					ELSE '' END END||
					CASE WHEN TOTAL_BILLAMT IS NULL OR TRIM(TOTAL_BILLAMT) = '' THEN ''
					ELSE CASE WHEN input(TOTAL_BILLAMT,20.) = . THEN 'TOTAL_BILLAMT IS INVALID.'  
					ELSE '' END END||
					/**CASE WHEN ERROR_CD_BILL_INFO IS NULL OR TRIM(ERROR_CD_BILL_INFO) = '' THEN ''
					ELSE CASE WHEN input(ERROR_CD_BILL_INFO,20.) = . THEN 'ERROR_CD_BILL_INFO IS INVALID.'  
					ELSE '' END END||**/
					CASE WHEN OPPTY_SUBTYP_ID IS NULL OR TRIM(OPPTY_SUBTYP_ID) = '' THEN ''
					ELSE CASE WHEN input(OPPTY_SUBTYP_ID,20.) = . THEN 'OPPTY_SUBTYP_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN RATE_BILLED IS NULL OR TRIM(RATE_BILLED) = '' THEN ''
					ELSE CASE WHEN input(RATE_BILLED,20.) = . THEN 'RATE_BILLED IS INVALID.'  
					ELSE '' END END||
					CASE WHEN EVNT_REL_ID IS NULL OR TRIM(EVNT_REL_ID) = '' THEN 'EVNT_REL_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(EVNT_REL_ID,20.) = . THEN 'EVNT_REL_ID IS INVALID.'  
					ELSE '' END END||
					/**CASE WHEN ERROR_CD_BILL_BKUP_INFO IS NULL OR TRIM(ERROR_CD_BILL_BKUP_INFO) = '' THEN ''
					ELSE CASE WHEN input(ERROR_CD_BILL_BKUP_INFO,20.) = . THEN 'ERROR_CD_BILL_BKUP_INFO IS INVALID.'  
					ELSE '' END END||**/
					CASE WHEN EVNT_PROD_ID IS NULL OR TRIM(EVNT_PROD_ID) = '' THEN 'EVNT_PROD_ID IS NULL OR BLANK.'
					ELSE CASE WHEN input(EVNT_PROD_ID,20.) = . THEN 'EVNT_PROD_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN PROD_ID IS NULL OR TRIM(PROD_ID) = '' THEN ''
					ELSE CASE WHEN input(PROD_ID,20.) = . THEN 'PROD_ID IS INVALID.'  
					ELSE '' END END||
					CASE WHEN WAIVR_CLM_SRC_XREF_ID IS NULL OR TRIM(WAIVR_CLM_SRC_XREF_ID) = '' THEN ''
					ELSE CASE WHEN input(WAIVR_CLM_SRC_XREF_ID,20.) = . THEN 'WAIVR_CLM_SRC_XREF_ID IS INVALID.'  
					ELSE '' END END||		          
					CASE WHEN MSG_EFF_DT IS NULL OR TRIM(MSG_EFF_DT) = '' THEN ''
					ELSE CASE WHEN MSG_EFF_DT IS NOT NULL AND LENGTH(MSG_EFF_DT) = 19 AND index(MSG_EFF_DT,'/')=0 THEN '' 
					ELSE 'MSG_EFF_DT IS INVALID.' END END||			          		          
					CASE WHEN MSG_EXPRN_DT IS NULL OR TRIM(MSG_EXPRN_DT) = '' THEN ''
					ELSE CASE WHEN MSG_EXPRN_DT  IS NOT NULL AND LENGTH(MSG_EXPRN_DT) = 19 AND index(MSG_EXPRN_DT,'/')=0 THEN '' 
					ELSE 'MSG_EXPRN_DT IS INVALID.' END END||
					CASE WHEN WAIVR_EFF_DT IS NULL OR TRIM(WAIVR_EFF_DT) = '' THEN ''
					ELSE CASE WHEN WAIVR_EFF_DT  IS NOT NULL AND LENGTH(WAIVR_EFF_DT) = 19 AND index(WAIVR_EFF_DT,'/')=0 THEN '' 
					ELSE 'WAIVR_EFF_DT IS INVALID.' END END||
					CASE WHEN WAIVR_EXPRN_DT IS NULL OR TRIM(WAIVR_EXPRN_DT) = '' THEN ''
					ELSE CASE WHEN WAIVR_EXPRN_DT  IS NOT NULL AND LENGTH(WAIVR_EXPRN_DT) = 19 AND index(WAIVR_EXPRN_DT,'/')=0 THEN '' 
					ELSE 'WAIVR_EXPRN_DT IS INVALID.' END END||
					CASE WHEN OPPTY_EFF_DT IS NULL OR TRIM(OPPTY_EFF_DT) = '' THEN ''
					ELSE CASE WHEN OPPTY_EFF_DT  IS NOT NULL AND LENGTH(OPPTY_EFF_DT) = 19 AND index(OPPTY_EFF_DT,'/')=0 THEN '' 
					ELSE 'OPPTY_EFF_DT IS INVALID.' END END||
					CASE WHEN OPPTY_EXPRN_DT IS NULL OR TRIM(OPPTY_EXPRN_DT) = '' THEN ''
					ELSE CASE WHEN OPPTY_EXPRN_DT  IS NOT NULL AND LENGTH(OPPTY_EXPRN_DT) = 19 AND index(OPPTY_EXPRN_DT,'/')=0 THEN '' 
					ELSE 'OPPTY_EXPRN_DT IS INVALID.' END END||
					CASE WHEN EFF_DT IS NULL OR TRIM(EFF_DT) = '' THEN ''
					ELSE CASE WHEN EFF_DT  IS NOT NULL AND LENGTH(EFF_DT) = 19 AND index(EFF_DT,'/')=0 THEN '' 
					ELSE 'EFF_DT IS INVALID.' END END||
					CASE WHEN EXPRN_DT IS NULL OR TRIM(EXPRN_DT) = '' THEN ''
					ELSE CASE WHEN EXPRN_DT  IS NOT NULL AND LENGTH(EXPRN_DT) = 19 AND index(EXPRN_DT,'/')=0 THEN '' 
					ELSE 'EXPRN_DT IS INVALID.' END END||
					CASE WHEN MBR_BIRTH_DT IS NULL OR TRIM(MBR_BIRTH_DT) = '' THEN ''
					ELSE CASE WHEN MBR_BIRTH_DT  IS NOT NULL AND LENGTH(MBR_BIRTH_DT) = 19  AND index(MBR_BIRTH_DT,'/')=0 THEN '' 
					ELSE 'MBR_BIRTH_DT IS INVALID.' END END||
					CASE WHEN MBR_ELIG_EFF_DT IS NULL OR TRIM(MBR_ELIG_EFF_DT) = '' THEN ''
					ELSE CASE WHEN MBR_ELIG_EFF_DT IS NOT NULL AND LENGTH(MBR_ELIG_EFF_DT) = 10 AND index(MBR_ELIG_EFF_DT,'/')=0 THEN '' 
					ELSE 'MBR_ELIG_EFF_DT IS INVALID.' END END||
					CASE WHEN MBR_ELIG_END_DT IS NULL OR TRIM(MBR_ELIG_END_DT) = '' THEN ''
					ELSE CASE WHEN MBR_ELIG_END_DT IS NOT NULL AND LENGTH(MBR_ELIG_END_DT) = 10 AND index(MBR_ELIG_END_DT,'/')=0 THEN '' 
					ELSE 'MBR_ELIG_END_DT IS INVALID.' END END||
					CASE WHEN PTNT_BRTH_DT IS NULL OR TRIM(PTNT_BRTH_DT) = '' THEN ''
					ELSE CASE WHEN PTNT_BRTH_DT  IS NOT NULL AND LENGTH(PTNT_BRTH_DT) = 19  AND index(PTNT_BRTH_DT,'/')=0 THEN '' 
					ELSE 'PTNT_BRTH_DT IS INVALID.' END END||
					CASE WHEN MGRTN_TS IS NULL OR TRIM(MGRTN_TS) = '' THEN ''
					ELSE CASE WHEN MGRTN_TS  IS NOT NULL AND LENGTH(MGRTN_TS) = 19 AND index(MGRTN_TS,'/')=0 THEN '' 
					ELSE 'MGRTN_TS IS INVALID.' END END||
					CASE WHEN BILL_PROCESS_DT IS NULL OR TRIM(BILL_PROCESS_DT) = '' THEN ''
					ELSE CASE WHEN BILL_PROCESS_DT  IS NOT NULL AND LENGTH(BILL_PROCESS_DT) = 19 AND index(BILL_PROCESS_DT,'/')=0 THEN '' 
					ELSE 'BILL_PROCESS_DT IS INVALID.' END END||		 
					CASE WHEN BILL_DT IS NULL OR TRIM(BILL_DT) = '' THEN ''
					ELSE CASE WHEN BILL_DT  IS NOT NULL AND LENGTH(BILL_DT) = 19 AND index(BILL_DT,'/')=0 THEN '' 
					ELSE 'BILL_DT IS INVALID.' END END||			 
					CASE WHEN CHNL_REC_CREATED_DT IS NULL OR TRIM(CHNL_REC_CREATED_DT) = '' THEN ''
					ELSE CASE WHEN CHNL_REC_CREATED_DT  IS NOT NULL AND LENGTH(CHNL_REC_CREATED_DT) = 19 AND index(CHNL_REC_CREATED_DT,'/')=0 THEN '' 
					ELSE 'CHNL_REC_CREATED_DT IS INVALID.' END END||
					CASE WHEN CHNL_REC_DLVRY_DT IS NULL OR TRIM(CHNL_REC_DLVRY_DT) = '' THEN ''
					ELSE CASE WHEN CHNL_REC_DLVRY_DT  IS NOT NULL AND LENGTH(CHNL_REC_DLVRY_DT) = 19 AND index(CHNL_REC_DLVRY_DT,'/')=0 THEN '' 
					ELSE 'CHNL_REC_DLVRY_DT IS INVALID.' END END||
					CASE WHEN CHNL_REC_DISPOSITION_DT IS NULL OR TRIM(CHNL_REC_DISPOSITION_DT) = '' THEN ''
					ELSE CASE WHEN CHNL_REC_DISPOSITION_DT  IS NOT NULL AND LENGTH(CHNL_REC_DISPOSITION_DT) = 19 AND index(CHNL_REC_DISPOSITION_DT,'/')=0 THEN '' 
					ELSE 'CHNL_REC_DISPOSITION_DT IS INVALID.' END END||		 
					CASE WHEN WAIVR_CMPGN_EFF_DT IS NULL OR TRIM(WAIVR_CMPGN_EFF_DT) = '' THEN ''
					ELSE CASE WHEN WAIVR_CMPGN_EFF_DT  IS NOT NULL AND LENGTH(WAIVR_CMPGN_EFF_DT) = 19 AND index(WAIVR_CMPGN_EFF_DT,'/')=0 THEN '' 
					ELSE 'WAIVR_CMPGN_EFF_DT IS INVALID.' END END||
					CASE WHEN WAIVR_CMPGN_EXP_DT IS NULL OR TRIM(WAIVR_CMPGN_EXP_DT) = '' THEN ''
					ELSE CASE WHEN WAIVR_CMPGN_EXP_DT  IS NOT NULL AND LENGTH(WAIVR_CMPGN_EXP_DT) = 19 AND index(WAIVR_CMPGN_EXP_DT,'/')=0 THEN '' 
					ELSE 'WAIVR_CMPGN_EXP_DT IS INVALID.' END END||
					CASE WHEN REC_CRTE_TS IS NULL OR TRIM(REC_CRTE_TS) = '' THEN ''
					ELSE CASE WHEN REC_CRTE_TS  IS NOT NULL AND LENGTH(REC_CRTE_TS) = 19 AND index(REC_CRTE_TS,'/')=0 THEN '' 
					ELSE 'REC_CRTE_TS IS INVALID.' END END||
					CASE WHEN REC_UPD_TS IS NULL OR TRIM(REC_UPD_TS) = '' THEN ''
					ELSE CASE WHEN REC_UPD_TS  IS NOT NULL AND LENGTH(REC_UPD_TS) = 19 AND index(REC_UPD_TS,'/')=0 THEN '' 
					ELSE 'REC_UPD_TS IS INVALID.' END END

				;
				quit;			
				
				%let cnt_qc_bteq=0;
				
				proc sql noprint; 
				select count(*) into: cnt_qc_bteq separated by ''
				from qc_bteq
				where length(err_desc) > 5 ;
				quit;
				
				%put NOTE: cnt_qc_bteq = &cnt_qc_bteq. ;
				
				%if &cnt_qc_bteq. = 0 %then %do;
					%put NOTE: No BTEQ issues with load_1_&&eoms_name&ii. ;
				%end;
				%else %do;
				
					data working.qc_step4b_bteq_&&source_team&ii.;
					set qc_bteq;
					run;				
				
					%let c_s_email_subject = EA ECOA Opportunity QC BTEQ Failure;
					%m_abend_handler(abend_report=%str(EA ECOA - QC BTEQ Failure- step 4),
						   abend_message=%str(EA ECOA - QC BTEQ Failure - step 4 check sas dataset working.qc_bteq for specific issuses));				
				
				%end;
				
			%end;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load opportunity data files into teradata
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
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - assign cntl counts - opportunity = 1 + opportunity rx = 2 
			+---------------------------------------------------------------------------------------------*/			
			%let SOURCE_OPP_FILE_METRIC=0;
			%let SOURCE_OPPRX_FILE_METRIC=0;
			
			%if &&source_team&ii. = SFMC %then %do;
			
				%if %sysfunc(exist(work.&&source_team&ii.._1_cntl_&&eoms_name&ii.)) %then %do;
				
					proc sql noprint;
					select eoms_file_count into: SOURCE_OPP_FILE_METRIC separated by ''
					from &&source_team&ii.._1_cntl_&&eoms_name&ii. ;
					quit;
				
				%end;
				%else %do;
				
					%let c_s_email_subject = EA ECOA SFMC Opportunity Missing Failure;
					%m_abend_handler(abend_report=%str(EA ECOA - Missing Opportunity - step 4),
						   abend_message=%str(EA ECOA - Missing Opportunty - step 4 check tables for past dates for files));
						   
				%end;
							
			%end;
			%if &&source_team&ii. = CPL %then %do;
			
				%if %sysfunc(exist(work.&&source_team&ii.._1_cntl_&&eoms_name&ii.)) %then %do;
				
					proc sql noprint;
					select eoms_file_count into: SOURCE_OPP_FILE_METRIC separated by ''
					from &&source_team&ii.._1_cntl_&&eoms_name&ii. ;
					quit;

				%end;
				%else %do;
				
					%let c_s_email_subject = EA ECOA CPL Opportunity Missing Failure;
					%m_abend_handler(abend_report=%str(EA ECOA - Missing Opportunity - step 4),
						   abend_message=%str(EA ECOA - Missing Opportunty - step 4 check tables for past dates for files));
						   
				%end;								
				
			%end;
			
			%put NOTE: SOURCE_OPP_FILE_METRIC = &SOURCE_OPP_FILE_METRIC. ;
			%put NOTE: SOURCE_OPPRX_FILE_METRIC = &SOURCE_OPPRX_FILE_METRIC. ;
					


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load opportunity file
			+---------------------------------------------------------------------------------------------*/			
			data load_1_&&eoms_name&ii.;
			set load_1_&&eoms_name&ii.;
			EOMS_ROW_GID=0;
			n=trim(left(_n_));
			SOURCE_TEAM = upcase(scan(source_opp_file,1,'_'));
			SOURCE_OPP_FILE = source_opp_file;
			SOURCE_OPP_FILE_METRIC = &SOURCE_OPP_FILE_METRIC.;
			SOURCE_OPPRX_FILE = source_opprx_file;
			SOURCE_OPPRX_FILE_METRIC = &SOURCE_OPPRX_FILE_METRIC.;  
			EXECUTION_TRIAGE = trim(left(rule_total1))||'_'||trim(left(rule_total2))||'_'||trim(left(rule_total3))||'_'||trim(left(rule_total4))||'_'||trim(left(rule_total5));
			EXECUTION_DATE = today() - &execution_date. ; 		
			if missing(oppty_evnt_id) and eoms_record ne 0 then delete;
			if SOURCE_OPP_FILE_METRIC = 0 and SOURCE_OPPRX_FILE_METRIC = 0 then EXECUTION_TRIAGE = '0_0_0_0_0';
			if SOURCE_OPP_FILE_METRIC = 0 and SOURCE_OPPRX_FILE_METRIC = 0 then rule_total1 = '0';
			run;			

			data loadt_1_&&eoms_name&ii.;
			set load_1_&&eoms_name&ii.;
			keep n &keep_variables. ;
			run;	
						
			data working.qc_step4b_load1_&&source_team&ii. ;
			set load_1_&&eoms_name&ii. (obs=100);
			run;	


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load opportunity data rule metrics into teradata
			+---------------------------------------------------------------------------------------------*/			
			data template;
			set &c_s_table_rules. (obs=0);
			run;

			proc contents data = template out = contents1 noprint;
			run;

			proc sql noprint;
			select name into: keep_variables separated by ' '
			from contents1;
			quit;

			data loadr_1_&&eoms_name&ii. ;
			set template load_1_&&eoms_name&ii. ;
			keep &keep_variables. ;
			run;	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load data files into teradata
			+---------------------------------------------------------------------------------------------*/			
			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_rules. ) x) with no data 

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_bulkload_sasdata(bulk_data=loadr_1_&&eoms_name&ii. , bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id. , bulk_option=APPEND, copy_data=N); 
			
			%put NOTE: &syserr. ;	

			%if &syserr. > 6 %then %do;
			  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
			  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 4),
					   abend_message=%str(EA ECOA - Proc Append Data failed - step 4));		  
			%end;	
			
			%m_bulkload_sasdata;  

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				insert &c_s_table_rules. select * from dss_cea2.dqi_eoms_tera_&mbr_process_id.

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_table_statistics(data_in=&c_s_table_name., index_in=eoms_row_gid);

			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);				
		 
			
		
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - process only QC rules = 0
			+---------------------------------------------------------------------------------------------*/
			%let cnt_qcload = 0;
			
			proc sql noprint;
			select count(*) into: cnt_qcload separated by ''
			from load_1_&&eoms_name&ii.
			where rule_total1 ne 0;
			quit;
			
			%put NOTE: cnt_qcload = &cnt_qcload. ;
			
			%if &cnt_qcload. = 0 %then %do;  /** <----------------------------------------------------- start - cnt_qcload **/
				%put NOTE: There are no primary QC rules failure. ;
			%end;
			%else %do;
				
				%put WARNING: There are primary QC rules failure. ;
				
				data working.qc_step4b_failure_&&source_team&ii.;
				set load_1_&&eoms_name&ii.;
				where rule_total1 ne 0;
				run; 				
				
			
			%end;  /** <----------------------------------------------------- end - cnt_qcload **/


		%end;  /** <----------------------------------------------------- end - cnt_files  **/
		

	%end;  /** <----------------------------------------------------- end - eoms_total **/



	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through disposition files
	+---------------------------------------------------------------------------------------------*/	
	%if &cnt_files_disp. ne 0 %then %do;  /** <----------------------------------------------------- start - cnt_files **/	

		proc sort data = temp01_rules (where = (scan(memname,2,'_') in ('3','4'))) out = temp01b_rules nodupkey;
		by source_team data_file;
		run;
		
		data _null_;
		set temp01b_rules; 
		where scan(memname,2,'_') in ('3','4');
		call symput('eoms_name'||left(_n_),trim(left(data_file)));
		call symput('source_team'||left(_n_),trim(left(source_team))); 
		call symput('eoms_total',trim(left(_n_)));
		run;

		%do iii = 1 %to &eoms_total;  /** <----------------------------------------------------- start - eoms_total **/	

			proc sql noprint;
			create table dqi_eoms_rules_disp as
			select *
			from dss_cea2.dqi_eoms_rules
			where layout_id in (3,4)
			and length(data_rules) > 1
			order by layout_id, rule_id;
			quit;
			
			%do_format_defintions(data_file_type=DISPOSITION);
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - format data rules
			+---------------------------------------------------------------------------------------------*/
			data dqi_eoms_rules_disp;
			format rule_sas $500. ;
			set dqi_eoms_rules_disp;
			n=put(left(_n_),8.);
			eoms_file="&eoms_file.";
			eoms_name=scan(eoms_file,2,'.');
			rule_id_name="rule"||trim(left(rule_id));
			if rule_type = 1 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||" = 1;";
			end;
			else if rule_type = 2 then do;
			rule_sas='if '||trim(left(data_attribute_name))||" ne '' then "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules))||");";
			end;
			else if rule_type = 3 then do;
			rule_sas='if '||trim(left(data_attribute_name))||" * 1 = . then "||trim(left(rule_id_name))||"=1;";
			end;
			else if rule_type = 4 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||" = 1;";
			end;
			else if rule_type = 5 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||" = 1; end;";
			end;
			else if rule_type = 6 then do;
			rule_sas=trim(left(data_rules))||" "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules2))||");";
			end;
			else if rule_type = 7 then do;
			rule_sas='if '||trim(left(data_attribute_name))||" ne 'CRITICAL_QC' then "||trim(left(rule_id_name))||"=put(upcase("||trim(left(data_attribute_name))||"),"||left(trim(data_rules))||");";
			end;
			call symput('rule'||n,trim(left(rule_sas)));
			call symput('rule_id'||n,trim(left(rule_id_name)));
			call symput('rule_severity'||n,trim(left(data_rules_severity)));
			call symput('name_layout',left(trim(eoms_name))); 
			call symput('rule_total',n);
			run;	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - sample data for opportunity message and rx fill IDs from dwu_edw - qc if IDs exist or... could they send opps + disps the same day?
			|
			+---------------------------------------------------------------------------------------------*/			
			%edw_sample_opp(data_type=disposition, data_in=load_3_&&eoms_name&iii., data_in2=load_1_&&eoms_name&iii.); 
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - appy rules
			+---------------------------------------------------------------------------------------------*/
			data load_3_&&eoms_name&iii.; 
			set load_3_&&eoms_name&iii.;
			CHNL_CD=CHNL_CD_ORIGINAL;
			CHNL_CD2=CHNL_CD_ORIGINAL2;
			run;
			
			
			data load_3_&&eoms_name&iii.;
			format rule_total1 rule_total2 rule_total3 rule_total4 rule_total5  %do j=1 %to &rule_total.; &&rule_id&j %end; 8.;
			set load_3_&&eoms_name&iii.;
			%do m=1 %to &rule_total.; &&rule&m %end; 
			rule_total1=coalesce(%do n=1 %to &rule_total.; %if &&rule_severity&n = 1 %then %do; &&rule_id&n , %end; %end; 0); /** <----------------- EOMS Primary    **/
			rule_total2=coalesce(%do n=1 %to &rule_total.; %if &&rule_severity&n = 2 %then %do; &&rule_id&n , %end; %end; 0); /** <----------------- EOMS Secondary  **/
			rule_total3=coalesce(%do n=1 %to &rule_total.; %if &&rule_severity&n = 3 %then %do; &&rule_id&n , %end; %end; 0); /** <----------------- EOMS Tertiary   **/
			rule_total4=0;
			rule_total5=0;			
			run;	


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - assign cntl counts - disposition = 3 + disposition rx = 4
			+---------------------------------------------------------------------------------------------*/
			%let SOURCE_OPP_FILE_METRIC=0;
			%let SOURCE_OPPRX_FILE_METRIC=0;
			
			%if &&source_team&iii. = SFMC %then %do;
			
				%if %sysfunc(exist(work.&&source_team&iii.._3_cntl_&&eoms_name&iii.)) %then %do;
			
					proc sql noprint;
					select eoms_file_count into: SOURCE_OPP_FILE_METRIC separated by ''
					from &&source_team&iii.._3_cntl_&&eoms_name&iii. ;
					quit;
				
				%end;
				%else %do;
				
					%let c_s_email_subject = EA ECOA SFMC Disposition Missing Failure;
					%m_abend_handler(abend_report=%str(EA ECOA - Missing Disposition - step 4),
						   abend_message=%str(EA ECOA - Missing Disposition - step 4 check tables for past dates for files));
						   
				%end;		 
				
			%end;
			%if &&source_team&iii. = CPL %then %do;
			
				%if %sysfunc(exist(work.&&source_team&iii.._3_cntl_&&eoms_name&iii.)) %then %do;
				
					proc sql noprint;
					select eoms_file_count into: SOURCE_OPP_FILE_METRIC separated by ''
					from &&source_team&iii.._3_cntl_&&eoms_name&iii. ;
					quit;
				
				%end;
				%else %do;
				
					%let c_s_email_subject = EA ECOA CPL Disposition Missing Failure;
					%m_abend_handler(abend_report=%str(EA ECOA - Missing Disposition - step 4),
						   abend_message=%str(EA ECOA - Missing Disposition - step 4 check tables for past dates for files));
						   
				%end;		 				
				
			%end;
			
			%put NOTE: SOURCE_OPP_FILE_METRIC = &SOURCE_OPP_FILE_METRIC. ;
			%put NOTE: SOURCE_OPPRX_FILE_METRIC = &SOURCE_OPPRX_FILE_METRIC. ;			


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load disposition file
			+---------------------------------------------------------------------------------------------*/			
			data load_3_&&eoms_name&iii.;
			set load_3_&&eoms_name&iii.;
			EOMS_ROW_GID=0;
			n=trim(left(_n_));
			SOURCE_TEAM = upcase(scan(source_opp_file,1,'_'));
			SOURCE_OPP_FILE = source_opp_file;
			SOURCE_OPP_FILE_METRIC = &SOURCE_OPP_FILE_METRIC.;
			SOURCE_OPPRX_FILE = source_opprx_file;
			SOURCE_OPPRX_FILE_METRIC = &SOURCE_OPPRX_FILE_METRIC.;  
			EXECUTION_TRIAGE = trim(left(rule_total1))||'_'||trim(left(rule_total2))||'_'||trim(left(rule_total3))||'_'||trim(left(rule_total4))||'_'||trim(left(rule_total5));
			EXECUTION_DATE = today() - &execution_date. ; 	
			chnl_cd=put(chnl_cd,_chnlfmt.);
			chnl_cd2=put(chnl_cd2,_chnlfmt.);
			if missing(oppty_evnt_id) and eoms_record ne 0 then delete;
			if SOURCE_OPP_FILE_METRIC = 0 and SOURCE_OPPRX_FILE_METRIC = 0 then EXECUTION_TRIAGE = '0_0_0_0_0';
			if SOURCE_OPP_FILE_METRIC = 0 and SOURCE_OPPRX_FILE_METRIC = 0 then rule_total1 = '0';
			run;					

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load disposition data files into teradata
			+---------------------------------------------------------------------------------------------*/
			data template;
			set &c_s_table_disp. (obs=0);
			run;

			proc contents data = template out = contents1 noprint;
			run;

			proc sql noprint;
			select name into: keep_variables separated by ' '
			from contents1;
			quit;

			data loadt_3_&&eoms_name&iii.;
			set load_3_&&eoms_name&iii.;
			keep n &keep_variables. ;
			run;	

			data working.qc_step4b_load3_&&source_team&iii. ;
			set load_3_&&eoms_name&iii. (obs=100);
			run;	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load disposition data rule metrics into teradata
			+---------------------------------------------------------------------------------------------*/			
			data template;
			set &c_s_table_rules. (obs=0);
			run;

			proc contents data = template out = contents1 noprint;
			run;

			proc sql noprint;
			select name into: keep_variables separated by ' '
			from contents1;
			quit;

			data loadr_3_&&eoms_name&iii. ;
			set template load_3_&&eoms_name&iii. ;
			keep &keep_variables. ;
			run;	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load disposition data files into teradata
			+---------------------------------------------------------------------------------------------*/			
			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_rules. ) x) with no data 

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 
			
			%m_bulkload_sasdata(bulk_data=loadr_3_&&eoms_name&iii. , bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id. , bulk_option=APPEND, copy_data=N); 
			
			%put NOTE: &syserr. ;	

			%if &syserr. > 6 %then %do;
			  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
			  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 4),
					   abend_message=%str(EA ECOA - Proc Append Data failed - step 4));		  
			%end;	
			
			%m_bulkload_sasdata;  

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				insert &c_s_table_rules. select * from dss_cea2.dqi_eoms_tera_&mbr_process_id.

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_table_statistics(data_in=&c_s_table_name., index_in=eoms_row_gid);

			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);			
			
		
		%end;  /** <----------------------------------------------------- end - cnt_files  **/
	
	%end;  /** <----------------------------------------------------- end - eoms_total **/


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/			
	%if &ecoa_test.	= YES4B %then %do;

		data working.qc_step4b_cpl_opp;
		set load_1_&c_s_file_date.000123 ;
		run;

		data working.qc_step4b_cpl_disp;
		set load_3_&c_s_file_date.000123 ;
		run;
		
		data working.qc_step4b_sfmc_opp;
		set load_1_&c_s_file_date.000456 ;
		run;

		data working.qc_step4b_sfmc_disp;
		set load_3_&c_s_file_date.000456 ;
		run;
		
		data working.qc_step4b;
		x=1;
		run;
		
		%m_ecoa_process(do_tasks=ALL);	

	%end;		
	%else %do;
	
		data working.qc_step4b_cpl_opp;
		set load_1_&c_s_file_date.000123 (obs = 10000);
		run;

		data working.qc_step4b_cpl_disp;
		set load_3_&c_s_file_date.000123 (obs = 10000);
		run;
		
		data working.qc_step4b_sfmc_opp;
		set load_1_&c_s_file_date.000456 (obs = 10000);
		run;

		data working.qc_step4b_sfmc_disp;
		set load_3_&c_s_file_date.000456 (obs = 10000);
		run;

		data working.qc_step4b;
		x=1;
		run;			

	%end;	
	
	
%mend m_step4_eoms_dataprofiling;
%**m_step4_eoms_dataprofiling(layout_id=1);
