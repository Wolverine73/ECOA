
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step3_eoms_transform
|
| LOCATION:  
| 
| PURPOSE:	Transform and Data Element Assignment by values and tables 
|
| LOGIC: 
|		step3 = transform data within SAS
|
|		select top 100 * from dss_cea2.dqi_eoms_files;
|		select top 100 * from dss_cea2.dqi_eoms_layout;
|		dmv_eoms - tables
|		dwu_edw  - tables 
|
| INPUT:     
|
| OUTPUT:    
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201001 - DQI Team
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_step3_eoms_transform;

	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine files to process
	+---------------------------------------------------------------------------------------------*/	
	%m_create_work_list(data_out=temp01_transform);
	
	data working.qc_step3a;
	x=1;
	run;	
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc = after step2 there should be 16 files available 
	|
	|  		cpl_disposition.20220101000123
	|  		cpl_disposition.20220101000123.CNTL
	|  		cpl_opportunity.20220101000123
	|  		cpl_opportunity.20220101000123.CNTL
	|  		cpl_rx_disposition.20220101000123
	|  		cpl_rx_disposition.20220101000123.CNTL
	|  		cpl_rx_opportunity.20220101000123
	|  		cpl_rx_opportunity.20220101000123.CNTL
	|
	|  		sfmc_disposition.20220101000456
	|  		sfmc_disposition.20220101000456.CNTL
	|  		sfmc_opportunity.20220101000456
	|  		sfmc_opportunity.20220101000456.CNTL
	|  		sfmc_rx_disposition.20220101000456
	|  		sfmc_rx_disposition.20220101000456.CNTL
	|  		sfmc_rx_opportunity.20220101000456
	|  		sfmc_rx_opportunity.20220101000456.CNTL
	|	
	+---------------------------------------------------------------------------------------------*/
	proc sql noprint;
	connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
	create table qc_step3 as
	select * from connection to tera
	( 			
		select distinct clm_evnt_gid, clnt_cd, a.source_team, coalesce(source_opp_file,source_opprx_file) as name, source_new_file, source_file
		from &c_s_table_name.  a inner join
		     &c_s_table_files. b
		on coalesce(a.source_opp_file,a.source_opprx_file) = b.source_new_file
		where clm_evnt_gid < 5	
		and b.source_new_file like %tslit(&c_s_file_date_delete.)
	);
	disconnect FROM tera;
	quit;
	
	%let qc_step3=0;
	
	proc sql noprint;
	select count(*) into: qc_step3 separated by ''
	from qc_step3;
	quit;
	
	%put NOTE: qc_step3 = &qc_step3. ;
	
	%if &qc_step3 = 16 %then %do;
	%end;
	%else %do;
	
		%let c_s_email_subject = EA ECOA CPL SMFC Missing 16 Daily Files Failure;
		%m_abend_handler(abend_report=%str(EA ECOA - Missing 16 Daily Files - step 3),
			   abend_message=%str(EA ECOA - Missing 16 Daily Files - step 3 check files within dss_cea2.dqi_eoms_cmctn_history + dss_cea2.dqi_eoms_files ));
	
	%end;	
		
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/			
	%if &ecoa_test.	= YES3A %then %do;

		data working.qc_step3b;
		x=1;
		run;
		
		%m_ecoa_process(do_tasks=ALL);	

	%end;		


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through files - opportunity = 1s + 2s
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_files_opp. ne 0 %then %do;  /** <----------------------------------------------------- begin - cnt_files **/

		proc sort data = temp01_transform (where = (scan(memname,2,'_') in ('1','2'))) out = temp01b_transform nodupkey;
		by source_team data_file;
		run;
		
		data _null_;
		set temp01b_transform;  
		where scan(memname,2,'_') in ('1','2');
		call symput('eoms_name'||left(_n_),trim(left(data_file)));
		call symput('source_team'||left(_n_),trim(left(source_team))); 
		call symput('eoms_total',trim(left(_n_)));
		run;

		%do i = 1 %to &eoms_total;  /** <----------------------------------------------------- begin - eoms_total **/

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - combine opp + opp rx
			|
			+---------------------------------------------------------------------------------------------*/
			proc sort data = &&source_team&i.._1_&&eoms_name&i.;
			by oppty_evnt_id msg_evnt_id;
			run;

			%if %sysfunc(exist(work.&&source_team&i.._2_&&eoms_name&i.)) %then %do;
				proc sort data = &&source_team&i.._2_&&eoms_name&i.;
				by oppty_evnt_id msg_evnt_id;
				run;
			%end; 
			%else %do;	
				proc sql noprint;
				create table dqi_eoms_layout as
				select *
				from dss_cea2.dqi_eoms_layout
				where layout_id = 2
				order by layout_id, sequence_id;
				quit;

				data _null_;
				set dqi_eoms_layout; 
				n=trim(left(_n_));
				call symput('name_var'||n,trim(left(data_attribute_name))); 
				call symput('name_var_total',n);
				run;
				
				data &&source_team&i.._2_&&eoms_name&i.;
				length %do k=1 %to &name_var_total; &&name_var&k %end; $100. ;
				run;

				data &&source_team&i.._2_&&eoms_name&i.;
				format eoms_file $100. eoms_record 8.;
				set &&source_team&i.._2_&&eoms_name&i.;
				run;			
			%end;

			data load_1_&&eoms_name&i.;
			merge &&source_team&i.._1_&&eoms_name&i. (in=a rename=(eoms_file=source_opp_file))
			      &&source_team&i.._2_&&eoms_name&i. (in=b rename=(eoms_file=source_opprx_file));
			by oppty_evnt_id msg_evnt_id; 
			if missing(source_opprx_file) then source_opprx_file=left(trim(scan(source_opp_file,1,'_')))||'_rx_disposition.'||left(trim(scan(source_opp_file,2,'.')));
			run;

			data load_1_&&eoms_name&i.;
			set load_1_&&eoms_name&i.;
			if missing(oppty_evnt_id) and eoms_record ne 0 then delete;	 
			run;
			
			proc sql noprint;
			select count(*) into: cnt_load1 separated by ''
			from load_1_&&eoms_name&i. ;
			quit;
			
			%put NOTE: cnt_load1 = &cnt_load1. ;
			
			%if &cnt_load1. > 1 %then %do;
			  data load_1_&&eoms_name&i.;
			  set load_1_&&eoms_name&i.;
			  if eoms_record = 0 then delete;
			  run;
			%end;	
			
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - Validate opportunity data elements - populate non-specialty members 
			|
			+---------------------------------------------------------------------------------------------*/							
			data qc_populate;
			set load_1_&&eoms_name&i.; 
			if (length(EDW_MBR_GID) > 0 ) and (missing(SPCLT_PTNT_ID)) and (missing(SPCLT_PTNT_GID)) ; 
			if eoms_record ne 0;
			run;
			
			data qc_populate;
			set qc_populate;

				if missing(EPH_LINK_ID)      or  
				missing(PRTC_ID)             or
				missing(PRSN_SSK_1)          or
				missing(PRSN_SSK_2)          or
				missing(PRSN_SSK_3)          or
				missing(PRSN_SSK_4)          or
				missing(CLT_NM)              or
				missing(PTNT_FIRST_NM)       or 
				missing(PTNT_LAST_NM)        or 
				missing(PTNT_ADDR_LINE1_TX)  or
				missing(PTNT_CITY_TX)        or
				missing(PTNT_STATE_CD)       or
				missing(PTNT_ZIP_CD)         or				
				missing(PTNT_PHONE1_NBR)     or
				missing(PTNT_BRTH_DT)        or
				missing(PTNT_GNDR_CD)  ;
			run;
 			
			%let qc_populate = 0;
			
			proc sql noprint;
			select count(*) into: qc_populate separated by ''
			from qc_populate;
			quit;
			
			%put NOTE: qc_populate = &qc_populate. ;
			
			%if &qc_populate. = 0 %then %do;
				%put NOTE: All data elements are populated;
			%end;
			%else %do;
			
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - lookup EDW member attributes by member GID 
				|
				+---------------------------------------------------------------------------------------------*/			
				%put NOTE: Missing data elements are populated - fix with EDW_MBR_GID;				
				
				data working.qc_step3b_mag;
				set qc_populate (obs=1000);  /** <-------------------------------------------- sample of data **/
				run;
				
				data qc_populate;
				set qc_populate (keep = EDW_MBR_GID rename=(EDW_MBR_GID=mag));
				mbr_acct_gid=mag*1;
				if mbr_acct_gid > 0;
				keep mbr_acct_gid; 
				run;
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - load member GIDs into teradata
				+---------------------------------------------------------------------------------------------*/			
				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_eph_&mbr_process_id.);

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					CREATE MULTISET TABLE &c_s_tdtempx..dqi_eoms_eph_&mbr_process_id. ,FALLBACK ,
					     NO BEFORE JOURNAL,
					     NO AFTER JOURNAL,
					     CHECKSUM = DEFAULT,
					     DEFAULT MERGEBLOCKRATIO,
					     MAP = TD_MAP1
					     ( 
							mbr_acct_gid DECIMAL(15,0) 
					     )
					PRIMARY INDEX ( mbr_acct_gid )

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 			

				%m_bulkload_sasdata(bulk_data=qc_populate , bulk_base=&c_s_tdtempx..dqi_eoms_eph_&mbr_process_id. , bulk_option=APPEND, copy_data=N); 	
				
				%m_table_statistics(data_in=&c_s_tdtempx..dqi_eoms_eph_&mbr_process_id., index_in=mbr_acct_gid);

				%m_bulkload_sasdata;			

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - get EDW member attributes from EDW
				+---------------------------------------------------------------------------------------------*/
				proc sql; 
				connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw");
				create table _members as
				select * from connection to tera(

					select 
					to_char(mbr_acct_gid) 	as EDW_MBR_GID,  
					to_char(eph_id)       	as CTT_EPH_LINK_ID,
					ql_bnfcy_id  		as CTT_PRTC_ID, 
					lvl1_acct_id 		as CTT_PRSN_SSK_1, 
					lvl2_acct_id 		as CTT_PRSN_SSK_2, 
					lvl3_acct_id 		as CTT_PRSN_SSK_3,
					mbr_acct_id  		as CTT_PRSN_SSK_4, 
					mbr_brth_dt  		as CTT_PTNT_BRTH_DT, 	  
					mbr_frst_nm  		as CTT_PTNT_FIRST_NM, 
					mbr_last_nm  		as CTT_PTNT_LAST_NM,
					addr_line1   		as CTT_PTNT_ADDR_LINE1_TX, 
					addr_city_nm 		as CTT_PTNT_CITY_TX, 
					addr_st_abbr_cd 	as CTT_PTNT_STATE_CD, 
					addr_zip5_cd 		as CTT_PTNT_ZIP_CD, 
					phone_nbr    		as CTT_PTNT_PHONE1_NBR,
					gndr_cd      		as CTT_PTNT_GNDR_CD,
					lvl1_acct_nm 		as CTT_CLT_NM, 
					lvl1_acct_id 		as CTT_CLT_LEVEL_1, 
					lvl2_acct_id 		as CTT_CLT_LEVEL_2, 
					lvl3_acct_id 		as CTT_CLT_LEVEL_3
					from &c_s_schema..v_mbr_acct_denorm a,
					     &c_s_schema..v_clnt_acct_denorm b
					where a.lvl3_acct_gid = b.lvl3_acct_gid
					and mbr_acct_gid in (select mbr_acct_gid from &c_s_tdtempx..dqi_eoms_eph_&mbr_process_id. )
					
				) ;
				quit;

				proc sort data = _members nodupkey;
				by EDW_MBR_GID;
				run;
 	
				proc datasets library=work;
				 modify load_1_&&eoms_name&i.;
				 index create EDW_MBR_GID; 
				 run;
				quit;				

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - match member GIDs to update member attributes
				+---------------------------------------------------------------------------------------------*/
				data load_1_&&eoms_name&i.;
				merge load_1_&&eoms_name&i. (in=a) _members (in=b);
				by EDW_MBR_GID; 
				
				if ( length(EDW_MBR_GID) > 0 ) and (missing(SPCLT_PTNT_ID)) and (missing(SPCLT_PTNT_GID)) then do;

					if missing(EPH_LINK_ID)         then EPH_LINK_ID=CTT_EPH_LINK_ID;  
					if missing(PRTC_ID)             then PRTC_ID=CTT_PRTC_ID;
					if missing(PRSN_SSK_1)          then PRSN_SSK_1=CTT_PRSN_SSK_1;
					if missing(PRSN_SSK_2)          then PRSN_SSK_2=CTT_PRSN_SSK_2; 
					if missing(PRSN_SSK_3)          then PRSN_SSK_3=CTT_PRSN_SSK_3; 
					if missing(PRSN_SSK_4)          then PRSN_SSK_4=CTT_PRSN_SSK_4;
					if missing(CLT_NM)              then CLT_NM=CTT_CLT_NM;
					if missing(PTNT_FIRST_NM)       then PTNT_FIRST_NM=CTT_PTNT_FIRST_NM;   
					if missing(PTNT_LAST_NM)        then PTNT_LAST_NM=CTT_PTNT_LAST_NM;  
					if missing(PTNT_ADDR_LINE1_TX)  then PTNT_ADDR_LINE1_TX=CTT_PTNT_ADDR_LINE1_TX; 
					if missing(PTNT_CITY_TX)        then PTNT_CITY_TX=CTT_PTNT_CITY_TX;
					if missing(PTNT_STATE_CD)       then PTNT_STATE_CD=substr(CTT_PTNT_STATE_CD,1,2);
					if missing(PTNT_ZIP_CD)         then PTNT_ZIP_CD=substr(CTT_PTNT_ZIP_CD,1,5);				
					if missing(PTNT_PHONE1_NBR)     then PTNT_PHONE1_NBR=CTT_PTNT_PHONE1_NBR;
					if missing(PTNT_GNDR_CD)        then PTNT_GNDR_CD=substr(CTT_PTNT_GNDR_CD,1,1); 
					if missing(PTNT_BRTH_DT) or PTNT_BRTH_DT = 'xxxxxxxx' then PTNT_BRTH_DT=put(CTT_PTNT_BRTH_DT,yymmdd10.);
					CLT_LEVEL_1=PRSN_SSK_1; 
					CLT_LEVEL_2=PRSN_SSK_2;
					CLT_LEVEL_3=PRSN_SSK_3;  
			   		
			   	end;
			   		
				drop  CTT_: ;
				run;  
				
			%end;			
				

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create drug formats 
			+---------------------------------------------------------------------------------------------*/			
			data dummy;
			drug_id='59762018102';
			run;
			
			data drug_id;
			set load_1_&&eoms_name&i.;
			drug_id=TD1_VAL; 
			if drug_id ne '';
			keep drug_id;
			run;
			
			data drug_id;
			set drug_id dummy;
			run;

			proc sort data = drug_id nodupkey; by drug_id; run;
			
			%let cnt_drug = 0;
			
			proc sql noprint;
			select count(*) into: cnt_drug separated by ''
			from drug_id;
			quit;
			
			%put NOTE: cnt_drug = &cnt_drug. ;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create sample of drug formats if no drugs
			+---------------------------------------------------------------------------------------------*/
			%if &cnt_drug. ne 0 %then %do;
			
				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_drug_&mbr_process_id.);

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					CREATE MULTISET TABLE dss_cea2.dqi_eoms_drug_&mbr_process_id. ,FALLBACK ,
					     NO BEFORE JOURNAL,
					     NO AFTER JOURNAL,
					     CHECKSUM = DEFAULT,
					     DEFAULT MERGEBLOCKRATIO,
					     MAP = TD_MAP1
					     ( 
							drug_id VARCHAR(11) 
					     )
					PRIMARY INDEX ( drug_id )

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 
				
				%m_bulkload_sasdata(bulk_data=drug_id, bulk_base=dss_cea2.dqi_eoms_drug_&mbr_process_id., bulk_option=APPEND); 	 
				
				%put NOTE: &syserr. ;	

				%if &syserr. > 6 %then %do;
				  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
				  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 3),
						   abend_message=%str(EA ECOA - Proc Append Data failed - step 3));		  
				%end;					

				%m_table_statistics(data_in=dss_cea2.dqi_eoms_drug_&mbr_process_id., index_in=drug_id);	
				
				%m_bulkload_sasdata; 
				
				
				proc sql noprint;
				connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
				create table v_drug_denorm as
				select * from connection to tera
				( 
					select distinct a.drug_id, brnd_gnrc_cd, drug_lbl_nm, drug_multi_src_cd, drug_prod_gid, drug_strgh_nm, 
					dsg_form_cd, gcn_seq_nbr, gpi_cd, gpi_cls_nm, maint_drug_ind, 'N' as drug_list_nm, 
					'RX' as drug_legl_stus_cd, pkg_uom, mddb_tot_pkg_unts_qty as pkg_size, spclt_drug_ind, gc3_thcls_cd, gcn_nbr
					from &c_s_schema..v_drug_denorm a, 
					     dss_cea2.dqi_eoms_drug_&mbr_process_id. c
					where  a.drug_id=c.drug_id	   
				);
				disconnect FROM tera;
				quit;	
				
				data v_drug_denorm;
				set v_drug_denorm;
				brnd_gnrc_cd=substr(brnd_gnrc_cd,1,1);
				if DRUG_MULTI_SRC_CD = 'SINGLE' then DRUG_MULTI_SRC_CD='1'; else DRUG_MULTI_SRC_CD='2';
				run;				
				
			%end;			


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create member formats 
			+---------------------------------------------------------------------------------------------*/ 
			data mbr_acct_gid;
			set load_1_&&eoms_name&i.;
			mbr_acct_gid=edw_mbr_gid*1;  
			if not missing(edw_mbr_gid);
			keep mbr_acct_gid;
			run;
			
			proc sort data = mbr_acct_gid nodupkey; by mbr_acct_gid; run;
			
			data lvl1_acct_id;
			set load_1_&&eoms_name&i.;
			lvl1_acct_id=CLT_LEVEL_1; 
			keep lvl1_acct_id;
			run;
			
			proc sort data = lvl1_acct_id nodupkey; by lvl1_acct_id; run;
			
			%let cnt_mbr = 0;
			
			proc sql noprint;
			select count(*) into: cnt_mbr separated by ''
			from mbr_acct_gid;
			quit;
			
			%put NOTE: cnt_mbr = &cnt_mbr. ;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create sample of member formats if no members
			+---------------------------------------------------------------------------------------------*/			
			%if &cnt_mbr. = 0 %then %do;
			
				proc sql noprint;
				connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
				create table mbr_acct_gid as
				select * from connection to tera
				( 
					select top 10 mbr_acct_gid
					from &c_s_schema..v_mbr_acct_denorm    
				);
				disconnect FROM tera;
				quit;
				
				proc sql noprint;
				select mbr_acct_gid into: mbr_acct_gid separated by ','
				from mbr_acct_gid;
				quit;				
				
				
				proc sql noprint;
				connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
				create table lvl1_acct_id as
				select * from connection to tera
				( 
					select distinct c.lvl1_acct_id, lvl1_acct_nm
					from &c_s_schema..v_mbr_acct_denorm a, 
					     &c_s_schema..v_clnt_acct_lvl1 c
					where  a.mbr_acct_gid in (&mbr_acct_gid. )
					  and  a.lvl1_acct_gid=c.lvl1_acct_gid	   
				);
				disconnect FROM tera;
				quit;				
				
			%end;
							

			proc sort data =mbr_acct_gid nodupkey;
			by mbr_acct_gid;
			run;

			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_ql_&mbr_process_id.);

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				CREATE MULTISET TABLE dss_cea2.dqi_eoms_ql_&mbr_process_id. ,FALLBACK ,
				     NO BEFORE JOURNAL,
				     NO AFTER JOURNAL,
				     CHECKSUM = DEFAULT,
				     DEFAULT MERGEBLOCKRATIO,
				     MAP = TD_MAP1
				     ( 
						mbr_acct_gid DECIMAL(15,0) 
				     )
				PRIMARY INDEX ( mbr_acct_gid )

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 
			 
			%m_bulkload_sasdata(bulk_data=mbr_acct_gid, bulk_base=dss_cea2.dqi_eoms_ql_&mbr_process_id. , bulk_option=APPEND); 
			
			%put NOTE: &syserr. ;	

			%if &syserr. > 6 %then %do;
				%let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
				%m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 3),
				abend_message=%str(EA ECOA - Proc Append Data failed - step 3));		  
			%end;			
			
			%m_table_statistics(data_in=dss_cea2.dqi_eoms_ql_&mbr_process_id., index_in=mbr_acct_gid);
			
			%m_bulkload_sasdata;  


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create member formats 
			+---------------------------------------------------------------------------------------------*/			
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table v_mbr_acct_denorm as
			select * from connection to tera
			( 
				select distinct to_char(a.eph_link_id) as eph_link_id, a.mbr_acct_gid, a.mbr_acct_id, a.ql_bnfcy_id
				from &c_s_schema..v_mbr_acct_denorm a,
				     dss_cea2.dqi_eoms_ql_&mbr_process_id. b 
				where  a.mbr_acct_gid=b.mbr_acct_gid   
			);
			disconnect FROM tera;
			quit;

			data v_mbr_acct_denorm;
			set v_mbr_acct_denorm;
			if ql_bnfcy_id = '' then ql_bnfcy_id = '1234567890';
			run; 
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create client formats
			+---------------------------------------------------------------------------------------------*/ 				
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table v_clnt_acct_lvl1 as
			select * from connection to tera
			( 
				select distinct c.lvl1_acct_id, c.lvl1_acct_nm
				from &c_s_schema..v_clnt_acct_lvl1 c
				where  c.lvl1_acct_id in (
								select distinct client.lvl1_acct_id
								from &c_s_schema..v_clnt_acct_lvl1 client, 
								     &c_s_schema..v_mbr_acct_hrchy hrchy 
								where client.lvl1_acct_gid  = hrchy.lvl1_acct_gid
								and hrchy.mbr_acct_gid in (select mbr_acct_gid from dss_cea2.dqi_eoms_ql_&mbr_process_id. )
								
							)			  
			);
			disconnect FROM tera;
			quit;
			
			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_ql_&mbr_process_id.);


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create stellent formats
			+---------------------------------------------------------------------------------------------*/ 			
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table _tmplfmt as
			select * from connection to tera
			( 
				select distinct pgm_id as p, appl_cmnct_id, tmplt_id
				from &c_s_schema..v_stlnt a
				where exprn_dt > current_date
				qualify rank() over (partition by a.tmplt_id order by a.ver_id desc, a.src_add_ts desc) = 1 
			);
			disconnect FROM tera;
			quit;			
			
			data _tmplfmt;
			set _tmplfmt;
			pgm_id=put(left(p),20.);
			run;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create pbm formats
			+---------------------------------------------------------------------------------------------*/ 			
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table _oppdfmt as
			select * from connection to tera
			( 
				select *
				from dwv_eoms.opportunity_denorm 
			);
			disconnect FROM tera;
			quit;	


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create channel formats
			+---------------------------------------------------------------------------------------------*/ 			
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table _chnlfmt as
			select * from connection to tera
			( 
				select prdct_altid, dsc_shrt_tx
				from dwv_eoms.product p  
				where prod_typ_id = 6
			);
			disconnect FROM tera;
			quit;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create oppty_typ_prod_id formats (for oppty_typ_prod_id = none)
			+---------------------------------------------------------------------------------------------*/ 			
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table _pbmfmt as
			select * from connection to tera
			( 
				select distinct pgm_id||oppty_cmpgn_id as pbm_id, oppty_typ_prod_id
				from dss_cea2.dqi_eoms_opportunity
				where oppty_src_cd = 'CPL'
				and pgm_id <> '106'
				and substr(execution_triage,1,1) = '0' 
				and oppty_evnt_id not in (
					select oppty_evnt_id 
					from dss_cea2.dqi_eoms_opportunity 
					where oppty_src_cd = 'CPL' 
					and substr(oppty_cmpgn_id,1,3) = '911' 
					and oppty_typ_prod_id = '2317084573'
				)
			);
			disconnect FROM tera;
			quit;			

			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build formats for opportunity data elements 
			|
			+---------------------------------------------------------------------------------------------*/
			%macro create_formats1(datain=, dataout=, variable=, variable2=ignore);

				%if &variable2. = ignore %then %do;
					data &dataout. (keep = start label type fmtname );
					   length fmtname $8. type $1 label $20. start $40.;
					   set &datain.;  
					   retain fmtname "&dataout."  type 'C';
					   if &variable. ne "" then do;
					     start = upcase(&variable);
					     label = 'INGENIORX';
					     output;
					   end;
					   if _n_ = 1 then do;
					     start = "other";
					     label = 'NON-INGENIORX';
					     output;
					   end;
					run; 		
				%end;
				%else %do;
					data &dataout. (keep = start label type fmtname );
					   length fmtname $8. type $1 label $20. start $40.;
					   set &datain.;  
					   retain fmtname "&dataout."  type 'C';
					   if &variable. ne "" then do;
					     start = upcase(trim(left(&variable.)));
					     label = trim(left(&variable2.));
					     output;
					   end;
					   %if &datain. ne _tmplfmt %then %do;
						if _n_ = 1 then do;
						start = "other";
						%if &datain. = _tmplfmt %then %do;
						label = 'NA';
						%end;
						%else %do;
						label = '';
						%end;
						output;
						end;
					   %end;
					   %else %do;
						if _n_ = 1 then do;
						start = "1234567890123456789012345678901234567890"; 
						label = '1234567890123456789012345678901234567890';
						output;
						end;
					   %end;
					run; 
				%end;


				proc sort data = &dataout. nodupkey;
				  by start;
				run;

				proc format cntlin = &dataout. ;
				run; 

			%mend create_formats1; 	
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - build transform formats
			+---------------------------------------------------------------------------------------------*/			
			%create_formats1(datain=v_mbr_acct_denorm, dataout=_benefmt,  variable=mbr_acct_gid, variable2=ql_bnfcy_id);
			%create_formats1(datain=v_mbr_acct_denorm, dataout=_ephfmt,   variable=mbr_acct_gid, variable2=eph_link_id);
			%create_formats1(datain=v_clnt_acct_lvl1,  dataout=_clntfmt,  variable=lvl1_acct_id, variable2=lvl1_acct_nm); 			
			%create_formats1(datain=_tmplfmt, dataout=_tmppfmt,      variable=appl_cmnct_id, variable2=pgm_id); 
			%create_formats1(datain=_tmplfmt, dataout=_tmpafmt,      variable=appl_cmnct_id, variable2=appl_cmnct_id);			
			%create_formats1(datain=_oppdfmt, dataout=_opp1fmt,      variable=opportunity_sub_type_prod_id, variable2=opportunity_sub_type_prod_id);
			%create_formats1(datain=_oppdfmt, dataout=_opp2fmt,      variable=opportunity_sub_type_prod_id, variable2=opportunity_type_cd);
			%create_formats1(datain=_oppdfmt, dataout=_opp3fmt,      variable=opportunity_sub_type_prod_id, variable2=opportunity_sub_type_cd); 
			%create_formats1(datain=_oppdfmt, dataout=_opp4fmt,      variable=opportunity_sub_type_prod_id, variable2=program_cd);			
			%create_formats1(datain=_chnlfmt, dataout=_chnlfmt,      variable=dsc_shrt_tx, variable2=prdct_altid); 			
			%create_formats1(datain=v_drug_denorm, dataout=_d1fmt,   variable=drug_id, variable2=brnd_gnrc_cd);
			%create_formats1(datain=v_drug_denorm, dataout=_d2fmt,   variable=drug_id, variable2=drug_lbl_nm);
			%create_formats1(datain=v_drug_denorm, dataout=_d3fmt,   variable=drug_id, variable2=drug_multi_src_cd);
			%create_formats1(datain=v_drug_denorm, dataout=_d4fmt,   variable=drug_id, variable2=drug_prod_gid);
			%create_formats1(datain=v_drug_denorm, dataout=_d5fmt,   variable=drug_id, variable2=drug_strgh_nm);
			%create_formats1(datain=v_drug_denorm, dataout=_d6fmt,   variable=drug_id, variable2=dsg_form_cd);
			%create_formats1(datain=v_drug_denorm, dataout=_d7fmt,   variable=drug_id, variable2=gcn_seq_nbr);
			%create_formats1(datain=v_drug_denorm, dataout=_d8fmt,   variable=drug_id, variable2=gpi_cd);
			%create_formats1(datain=v_drug_denorm, dataout=_d9fmt,   variable=drug_id, variable2=gpi_cls_nm);
			%create_formats1(datain=v_drug_denorm, dataout=_d10fmt,  variable=drug_id, variable2=maint_drug_ind); 
			%create_formats1(datain=v_drug_denorm, dataout=_d11fmt,  variable=drug_id, variable2=drug_list_nm); 
			%create_formats1(datain=v_drug_denorm, dataout=_d12fmt,  variable=drug_id, variable2=drug_legl_stus_cd); 
			%create_formats1(datain=v_drug_denorm, dataout=_d13fmt,  variable=drug_id, variable2=pkg_uom); 
			%create_formats1(datain=v_drug_denorm, dataout=_d14fmt,  variable=drug_id, variable2=pkg_size);
			%create_formats1(datain=v_drug_denorm, dataout=_d15fmt,  variable=drug_id, variable2=spclt_drug_ind); 
			%create_formats1(datain=v_drug_denorm, dataout=_d16fmt,  variable=drug_id, variable2=gc3_thcls_cd);
			%create_formats1(datain=v_drug_denorm, dataout=_d17fmt,  variable=drug_id, variable2=gcn_nbr);
			%create_formats1(datain=_pbmfmt,       dataout=_pbm1fmt, variable=pbm_id,  variable2=oppty_typ_prod_id);		


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - transform and clean data elements for opportunity
			|   
			+---------------------------------------------------------------------------------------------*/
			%let cnt_OPPTY_TYP_PROD_ID=0;
			
			proc sql noprint;
			select count(*) into: cnt_OPPTY_TYP_PROD_ID separated by ''
			from load_1_&&eoms_name&i.
			where upcase(OPPTY_TYP_PROD_ID) = 'NONE';
			quit;
			
			%put NOTE: cnt_OPPTY_TYP_PROD_ID = &cnt_OPPTY_TYP_PROD_ID. ;
			
			
			data load_1_&&eoms_name&i.;
			   set load_1_&&eoms_name&i.;
			    			   
			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - DWV_EOMS.OPPORTUNITY_DENORM.OPPTY_TYP_PROD_ID                                        --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/		   
			   if missing(PRTC_ID) then PRTC_ID=put(EDW_MBR_GID,_benefmt.); /**<-------------------------- QL beneficiary ID  **/ 
			   if PRTC_ID = '' then PRTC_ID='1234567890';                   /**<-------------------------- QL beneficiary ID missing in EDW **/   
			   **EPH_LINK_ID=put(EDW_MBR_GID,_ephfmt.);                     /**<-------------------------- EPH ID clean and reassignment  **/ 		   
			   if missing(OPPTY_SRC_CD) then OPPTY_SRC_CD = scan(upcase(source_opp_file),1,'_');
			   if missing(ALT_OPPTY_SUBTYP_CD) then ALT_OPPTY_SUBTYP_CD = '';
			   if missing(ALT_OPPTY_SUBTYP_SRC_CD) then ALT_OPPTY_SUBTYP_SRC_CD = OPPTY_SRC_CD;
			   if missing(OPPTY_ACTION_IND) then OPPTY_ACTION_IND = 'A';
			   if OPPTY_ACTION_IND not in ('A','C','U') then OPPTY_ACTION_IND = 'A'; 
			   if missing(OPPTY_STUS_CD) then OPPTY_STUS_CD = '6';
			   if OPPTY_STUS_CD not in ('2','3','4','6','13','14','15') then OPPTY_STUS_CD = '6'; 
			   if missing(MSG_STUS_CD) then MSG_STUS_CD = '6';
			   if missing(MSG_STUS_RSN_CD) then MSG_STUS_RSN_CD = '10';
			   if missing(OPPTY_ALTID) then OPPTY_ALTID = '';


			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- CPL data with NONE as OPPTY_TYP_PROD_ID                                                                          --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	 
			   /**if  upcase(OPPTY_TYP_PROD_ID) = 'NONE' and OPPTY_SRC_CD = 'CPL' then OPPTY_TYP_PROD_ID='2317084573'; **/ /**<-------- OLD LOGIC = incorrect = ngTDC CPL Intervention **/
			   
			   if upcase(OPPTY_TYP_PROD_ID) = 'NONE' then OPPTY_TYP_PROD_ID=put(left(trim(PGM_ID))||left(trim(OPPTY_CMPGN_ID)),_pbm1fmt.);


			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- PBM IDs = dwv_eoms.opportunity_denorm - validate 5 data element always should have valid OPPTY_TYP_PROD_ID       --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	 			   
			   if missing(OPPTY_TYP_PROD_ID)       then OPPTY_TYP_PROD_ID='MISSING';      /**<--------------------------------------------------------- driver ID **/
			   if missing(OPPORTUNITY_TYPE_CD)     then OPPORTUNITY_TYPE_CD=put(OPPTY_TYP_PROD_ID,_opp2fmt.);  
			   if missing(OPPTY_ALTID_TYP_CD)      then OPPTY_ALTID_TYP_CD=put(OPPTY_TYP_PROD_ID,_opp2fmt.); 
			   if missing(OPPORTUNITY_SUB_TYPE_CD) then OPPORTUNITY_SUB_TYPE_CD=put(OPPTY_TYP_PROD_ID,_opp3fmt.);
			   OPPORTUNITY_SUB_TYPE_CD=substr(OPPORTUNITY_SUB_TYPE_CD,1,15);  /** <---------- DQI values exceed 15 might fail on IT loads **/
			   if missing(PGM_TYP_CD)              then PGM_TYP_CD=put(OPPTY_TYP_PROD_ID,_opp3fmt.);
			   if missing(PGM_TYP_CD)              then PGM_TYP_CD=put(OPPTY_TYP_PROD_ID,_opp4fmt.);
			   PGM_TYP_CD='';                      /**<------------------------------------------------------------------------------------------------ comment out - PGM_TYP_CD is null in the EDW **/


			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - application campaign information                                                     --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   if missing(OPPTY_CMPGN_REQUEST_ID) then OPPTY_CMPGN_REQUEST_ID=compress(year(today() - &execution_date. )||put(month(today() - &execution_date. ),z2.)||put(day(today() - &execution_date. ),z2.));
			   EXTNL_SYS_OPPTY_ID=compress(EXTNL_SYS_OPPTY_ID,"'-");
			   ALT_OPPTY_SUBTYP_CD=compress(ALT_OPPTY_SUBTYP_CD,"'-");
			   ALT_OPPTY_SUBTYP_CD=substr(ALT_OPPTY_SUBTYP_CD,1,15);
 			   OPPTY_CMPGN_ID=compress(OPPTY_CMPGN_ID,"'-");
			   if missing(OPPTY_CMPGN_ID) and OPPTY_SRC_CD = 'SFMC' then OPPTY_CMPGN_ID='1';
			   else if missing(OPPTY_CMPGN_ID) and OPPTY_SRC_CD = 'CPL' then OPPTY_CMPGN_ID='2';
			   else if missing(OPPTY_CMPGN_ID) and OPPTY_SRC_CD = 'DQI' then OPPTY_CMPGN_ID='3';
			   else if missing(OPPTY_CMPGN_ID) and OPPTY_SRC_CD = 'STARS' then OPPTY_CMPGN_ID='4';
			   else if missing(OPPTY_CMPGN_ID) then OPPTY_CMPGN_ID='5'; 
			   
			   if CHNL_CD='99' then CHNL_CD='CHALLCHNLS';  /** <-------------------- for control groups from CPL **/
			   CHNL_CD_ORIGINAL=CHNL_CD;  
			   

			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - dates                                                                                --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	
			   %m_edw_datetime(data_element=COMM_GENERATE_DT,     	data_type=1, default=2);
			   %m_edw_datetime(data_element=MSG_EFF_DT,     	data_type=1);
			   %m_edw_datetime(data_element=MSG_EXPRN_DT,   	data_type=2);
			   %m_edw_datetime(data_element=WAIVR_EFF_DT,   	data_type=3);
			   %m_edw_datetime(data_element=WAIVR_EXPRN_DT, 	data_type=2);
			   %m_edw_datetime(data_element=OPPTY_EFF_DT,   	data_type=3);
			   %m_edw_datetime(data_element=OPPTY_EXPRN_DT, 	data_type=3);
			   %m_edw_datetime(data_element=EFF_DT,         	data_type=1);
			   %m_edw_datetime(data_element=EXPRN_DT,       	data_type=2);		   
			   %m_edw_datetime(data_element=RX_PKUP_DT,     	data_type=3);
			   %m_edw_datetime(data_element=FILL_DT,        	data_type=3);
			   %m_edw_datetime(data_element=DSPND_DT,       	data_type=3);
			   %m_edw_datetime(data_element=RX_PKUP_DT,     	data_type=3);
			   %m_edw_datetime(data_element=RX_EXPRN_DT,    	data_type=3);
			   %m_edw_datetime(data_element=RX_DISPN_DT,    	data_type=3);
			   
			   if PTNT_BRTH_DT=''         then PTNT_BRTH_DT='1/1/2022 12:12:00 AM';
			   if PTNT_BRTH_DT='xxxxxxxx' then PTNT_BRTH_DT='1/1/2022 12:12:00 AM'; 
			   %m_edw_datetime(data_element=PTNT_BRTH_DT,   	data_type=3);
			   MBR_BIRTH_DT=PTNT_BRTH_DT;
			   
			   RX_ARDD_DT='';

			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - specialty members  dss_cea2.dqi_eoms_rules where DATA_RULES_SEVERITY_BTEQ = '1'      --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	
			   if SPCLT_PTNT_SRC_CD ne '' then do; 
				   if missing(CLT_LEVEL_1) then CLT_LEVEL_1='UNKWN';
				   if missing(CLT_LEVEL_2) then CLT_LEVEL_2='UNKWN';
				   if missing(CLT_LEVEL_3) then CLT_LEVEL_3='UNKWN';
			   end;			   
			   

			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - client                                                                               --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   if missing(QL_CLT_ID)   then QL_CLT_ID='0'; 
			   if missing(CLT_TYP)     then CLT_TYP='PBM';
			   if missing(CLT_NM)      then CLT_NM=put(PRSN_SSK_1,_clntfmt.);
			   if missing(CLT_LEVEL_1) then CLT_LEVEL_1=PRSN_SSK_1;
			   if missing(CLT_LEVEL_2) then CLT_LEVEL_2=PRSN_SSK_2;
			   if missing(CLT_LEVEL_3) then CLT_LEVEL_3=PRSN_SSK_3;
			   if missing(PRSN_SSK_1)  then PRSN_SSK_1=CLT_LEVEL_1;
			   if missing(PRSN_SSK_2)  then PRSN_SSK_2=CLT_LEVEL_2;
			   if missing(PRSN_SSK_3)  then PRSN_SSK_3=CLT_LEVEL_3;	
			  
			  
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - zero byte files                                                                      --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	
			   if eoms_record = 0 then PRSN_SSK_1='3784';
			   if eoms_record = 0 then CLT_LEVEL_1='3784';
			   if eoms_record = 0 then PHCY_ID='1295752566';
			   if eoms_record = 0 then PRSC_ID='1891266797';
			   
			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - identity columns                                                                     --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   TRGT_SCOPE_CD='P';
			   PRTY_NBR='';
			   PTNT_DOMESTIC_ADDR_IND='Y';
			   if missing(OPPTY_CTGRY) then OPPTY_CTGRY='ALLSEGMENTS'; 
			   PRSN_SRC_CD='D';
			   EXTNL_IND='N';
			   CLT_SRC_CD='D';
			   OPPTY_STUS_RSN_CD='1';
			   OPPTY_DISPN_CD='1';
			   OPPTY_DISPN_SUB_CD='10';
			   PTNT_COUNTRY_CD='USA';
			   PTNT_PHONE1_TYP_CD='Primary';
			   PTNT_EMAIL_TYP_CD='Primary';
			   PHCY_EMAIL_TYP_CD='Primary';
			   PHCY_PHONE1_TYP_CD='Primary';
			   PRSC_PHONE1_TYP_CD='Primary';
			   PRSC_EMAIL_TYP_CD='Primary'; 	   
			   OPP_COL_START='OPTY';
			   MSG_COL_START='MSG';
			   PRSN_COL_START='PRSN';
			   PRSC_COL_START='PRSC';
			   PHCY_COL_START='PHCY';
			   WAIVR_COL_START='WAIVR';
			   OTHER_COL_START='ADD';
			   DRG_COL_START='DRG'; 


			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - identity columns                                                                     --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   EVNT_PTY_ID=MSG_EVNT_ID;
			   PTY_ID=MSG_EVNT_ID;
			   ACCT_ID=MSG_EVNT_ID;
			   EVNT_CNTCT_INFO_ID=MSG_EVNT_ID;
			   EVNT_PTY_CNTCT_ID=MSG_EVNT_ID;
			   SVC_TRANS_ID=MSG_EVNT_ID;   


			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity rx data elements - identity columns (dual data elements)                                             --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   EVNT_REL_ID=MSG_EVNT_ID;
			   EVNT_PROD_ID=MSG_EVNT_ID;
			   PROD_ID=MSG_EVNT_ID;
			   EVNT_REL_ID2=RX_FILL_EVNT_ID; 
			   EVNT_REL_ID3=RX_FILL_EVNT_ID; 
			   EVNT_PROD_ID2=RX_FILL_EVNT_ID;
			   PROD_ID2=RX_FILL_EVNT_ID;
			   

			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - housekeeping + identity columns                                                      --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   REC_CRTE_PROC_ID=OPPTY_SRC_CD;
			   REC_CRTE_TRANS_ID=compress(put(today() - &execution_date. ,yymmdd10.),'-');
			   REC_CRTE_TS=put(today() - &execution_date. ,yymmdd10.)||' 00.00.00';
			   REC_CRTE_USER_ID=OPPTY_SRC_CD;
			   REC_SRC_FILE_NM=scan(source_opp_file,2,'.');
			   REC_UPD_PROC_ID=OPPTY_SRC_CD;
			   REC_UPD_TRANS_ID=compress(put(today() - &execution_date. ,yymmdd10.),'-');
			   REC_UPD_TS=put(today() - &execution_date. ,yymmdd10.)||' 00.00.00';
			   REC_UPD_USER_ID=OPPTY_SRC_CD;
			   REC_UPD_USER_ID_2=OPPTY_SRC_CD;
			   

			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - subjects                                                                             --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   if missing(EVNT_PTY_ROLE_CD)      and (EPH_LINK_ID ne '' or PRSN_SSK_4 ne '' or PRTC_ID ne '' ) then EVNT_PTY_ROLE_CD='PTNT'; 
			   else if missing(EVNT_PTY_ROLE_CD) and PRSC_ID ne '' then EVNT_PTY_ROLE_CD='PRSC';
			   else if missing(EVNT_PTY_ROLE_CD) and PHCY_ID ne '' then EVNT_PTY_ROLE_CD='PHCY'; 
			   else if missing(EVNT_PTY_ROLE_CD) then EVNT_PTY_ROLE_CD='PTNT';
			   
			   if EVNT_PTY_ROLE_CD not in ('PTNT','PRSC','PHCY') then EVNT_PTY_ROLE_CD = 'PTNT';	
			   
			   if missing(PTY_ALTID_TYP_CD)      and EVNT_PTY_ROLE_CD='PTNT' then PTY_ALTID_TYP_CD='EPH';
			   else if missing(PTY_ALTID_TYP_CD) and EVNT_PTY_ROLE_CD='PRSC' then PTY_ALTID_TYP_CD='NPI';
			   if PTY_ALTID_TYP_CD not in ('EPH','NPI') then PTY_ALTID_TYP_CD = 'EPH';
			   
			   if missing(EXTNL_MBR_ID) then EXTNL_MBR_ID=PRSN_SSK_4;			   
			   
			   PTNT_FIRST_NM=upcase(PTNT_FIRST_NM);
			   if EVNT_PTY_ROLE_CD = 'PTNT' and missing(PTNT_FIRST_NM) then PTNT_FIRST_NM='UNKNOWN';
			   PTNT_MIDDLE_NM=upcase(PTNT_MIDDLE_NM);
			   PTNT_LAST_NM=upcase(PTNT_LAST_NM);
			   if EVNT_PTY_ROLE_CD = 'PTNT' and missing(PTNT_LAST_NM) then PTNT_LAST_NM='UNKNOWN';
			   PTNT_GNDR_CD=upcase(PTNT_GNDR_CD);
			   PTNT_ADDR_LINE1_TX=upcase(PTNT_ADDR_LINE1_TX);
			   if EVNT_PTY_ROLE_CD = 'PTNT' and missing(PTNT_ADDR_LINE1_TX) then PTNT_ADDR_LINE1_TX='UNKNOWN';
			   PTNT_ADDR_LINE2_TX=upcase(PTNT_ADDR_LINE2_TX);
			   PTNT_CITY_TX=upcase(PTNT_CITY_TX);
			   if EVNT_PTY_ROLE_CD = 'PTNT' and missing(PTNT_CITY_TX) then PTNT_CITY_TX='UNKNOWN';
			   PTNT_STATE_CD=upcase(PTNT_STATE_CD);
			   if EVNT_PTY_ROLE_CD = 'PTNT' and missing(PTNT_STATE_CD) then PTNT_STATE_CD='IL';
			   if EVNT_PTY_ROLE_CD = 'PTNT' and missing(PTNT_ZIP_CD) then PTNT_ZIP_CD='12345';
			   
			   PRSC_FIRST_NM=upcase(PRSC_FIRST_NM);
			   PRSC_MIDDLE_NM=upcase(PRSC_MIDDLE_NM);
			   PRSC_LAST_NM=upcase(PRSC_LAST_NM);
			   PRSC_ADDR_LINE1_TX=upcase(PRSC_ADDR_LINE1_TX);
			   PRSC_ADDR_LINE2_TX=upcase(PRSC_ADDR_LINE2_TX);
			   PRSC_CITY_TX=upcase(PRSC_CITY_TX);
			   PRSC_STATE_CD=upcase(PRSC_STATE_CD);
			   
			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - added for CPL sending member GID only                                                --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/			   
			   if length(KEY_CODE) > 20 then KEY_CODE=substr(KEY_CODE,1,20);
			   if missing(PTNT_PHONE1_NBR) then PTNT_PHONE1_NBR = '1234567890';
			   if missing(PTNT_EMAIL_ADDR_TX) then PTNT_EMAIL_ADDR_TX = 'missing_email@gmail.com';			   


			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - gender code                                                                          --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	
			   if length(PTNT_GNDR_CD) > 1 then PTNT_GNDR_CD=upcase(substr(PTNT_GNDR_CD,1,1));
			   if missing(PTNT_GNDR_CD) then PTNT_GNDR_CD='U';   			   
			   if PTNT_GNDR_CD not in ('M','F') then PTNT_GNDR_CD='U';
			   			   
			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - language code                                                                        --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/	
			   if PTNT_LANG_CD='ENGL' then PTNT_LANG_CD = 'EN';
			   else if PTNT_LANG_CD='FREN' then PTNT_LANG_CD = 'FR';
			   else if PTNT_LANG_CD='SPAN' then PTNT_LANG_CD = 'SP';
			   else if PTNT_LANG_CD='CHIN' then PTNT_LANG_CD = 'CH';
			   else if PTNT_LANG_CD='GERM' then PTNT_LANG_CD = 'GE';
			   else if PTNT_LANG_CD='VIET' then PTNT_LANG_CD = 'VI';
			   else if PTNT_LANG_CD='TAGA' then PTNT_LANG_CD = 'TA';
			   else if PTNT_LANG_CD='PORT' then PTNT_LANG_CD = 'PG';
			   else if PTNT_LANG_CD='ILOC' then PTNT_LANG_CD = 'IL';
			   else if PTNT_LANG_CD='KORE' then PTNT_LANG_CD = 'KO';
			   else if PTNT_LANG_CD='ARAB' then PTNT_LANG_CD = 'AR';
			   else if PTNT_LANG_CD='CAMB' then PTNT_LANG_CD = 'CA';
			   else if PTNT_LANG_CD='ARME' then PTNT_LANG_CD = 'AM';
			   else if PTNT_LANG_CD='FARS' then PTNT_LANG_CD = 'FA';
			   else if PTNT_LANG_CD='HMON' then PTNT_LANG_CD = 'HM';
			   else if PTNT_LANG_CD='ITAL' then PTNT_LANG_CD = 'IT';
			   else if PTNT_LANG_CD='JAPA' then PTNT_LANG_CD = 'JA';
			   else if PTNT_LANG_CD='CANT' then PTNT_LANG_CD = 'CN';
			   else if PTNT_LANG_CD='MAND' then PTNT_LANG_CD = 'MA';
			   else if PTNT_LANG_CD='HIND' then PTNT_LANG_CD = 'HI';
			   else if PTNT_LANG_CD='CREO' then PTNT_LANG_CD = 'CR';
			   else if PTNT_LANG_CD='POLI' then PTNT_LANG_CD = 'PO';
			   else if PTNT_LANG_CD='HAIT' then PTNT_LANG_CD = 'HA';
			   else if PTNT_LANG_CD='RUSS' then PTNT_LANG_CD = 'RU';
			   else if PTNT_LANG_CD='SOMA' then PTNT_LANG_CD = 'SO';
			   else if PTNT_LANG_CD='YIDD' then PTNT_LANG_CD = 'YI'; 
			   else if PTNT_LANG_CD='OTHE' then PTNT_LANG_CD = 'OT'; 
			   else if PTNT_LANG_CD='UNKW' then PTNT_LANG_CD = 'UN'; 
			   else PTNT_LANG_CD=substr(PTNT_LANG_CD,1,2);	
			   

			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - missing from QA                                                                      --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   SRC_CD=PRSN_SRC_CD;
			   SRC_CD2=PRSN_SRC_CD;
			   SRC_CD3=PRSN_SRC_CD;
			   CLM_ADJ_SRC_CD=PRSN_SRC_CD;
			   if EVNT_PTY_ROLE_CD = 'PTNT' then EVNT_TYP_CD='OPTY';
			   else if EVNT_PTY_ROLE_CD = 'PRSC' then EVNT_TYP_CD='COMM';
			   else if EVNT_PTY_ROLE_CD = 'PHCY' then EVNT_TYP_CD='COMM';
			   else EVNT_TYP_CD='OPTY';
			   PTY_ROLE_CD=EVNT_PTY_ROLE_CD;
			   PRSTN_IND='Y';			   
			   OPPTY_SUBTYP_CD=OPPTY_ALTID_TYP_CD;
			   OPPTY_SUBTYP_CD2=OPPTY_ALTID_TYP_CD;
			   OPPTY_SUBTYP_ID=OPPTY_TYP_PROD_ID;
	
			   if missing(RX_DISPN_SUB_CD) then RX_DISPN_SUB_CD='0';
			   if missing(RX_DISPN_SUB_CD2) then RX_DISPN_SUB_CD2='0';
			   if missing(EVNT_PROD_ROLE_CD) then EVNT_PROD_ROLE_CD='DP';	
			   
			   MSG_TYP_PROD_ID='1';
			   CMNCT_TYP_CD='MSSG'; 
			   
			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - other                                                                                --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   CAMPAIGN_ID=OPPTY_TYP_PROD_ID;                  
			   VNDR_CASE_ID=MSG_EVNT_ID;

			   
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity data elements - stellents                                                                            --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/
			   if PGM_ID = MSG_PRDCT_ALT_ID then PGM_ID='106';  
			   if PGM_ID*1 < 0 then PGM_ID='106';
			   if missing(MSG_PRDCT_ALT_ID) then MSG_PRDCT_ALT_ID='NA';
	

			   /**----------------------------------------------------------------------------------------------------------------------**/
			   /**-- opportunity rx data elements - drugs                                                                             --**/
			   /**----------------------------------------------------------------------------------------------------------------------**/				   
			   if length(TD1_VAL) = 11 then TD1_TYP_CD='N';
			   else if length(TD1_VAL) > 1 then TD1_TYP_CD='GP';
			   else if missing(TD1_VAL) then TD1_TYP_CD='N';
			   
			   if missing(DRUG_TYP_CD) then DRUG_TYP_CD=TD1_TYP_CD;	  
			   if TD1_DAY_SPLY_QTY*1 < 0 then TD1_DAY_SPLY_QTY=DAY_SPLY_QTY;
			   if missing(DAY_SPLY_QTY) then DAY_SPLY_QTY=TD1_DAY_SPLY_QTY;

			   if not missing(TD1_VAL) then DRUG_NDC_NBR=TD1_VAL;
			   if not missing(TD1_VAL) then DRUG_NDC_NBR2=TD1_VAL;
			   if not missing(TD1_VAL) then TD1_NM_TX=put(TD1_VAL,_d2fmt.);  
			   if not missing(TD1_VAL) then DRUG_NM_TX=put(TD1_VAL,_d2fmt.);
			   if not missing(TD1_VAL) then TD1_MAINT_DRUG_IND=put(TD1_VAL,_d10fmt.); 
			   if not missing(TD1_VAL) then TD1_SPCLT_DRUG_IND=put(TD1_VAL,_d15fmt.); 
			   if not missing(TD1_VAL) then TD1_BRAND_GNRC_CD=put(TD1_VAL,_d1fmt.); 
			   if not missing(TD1_VAL) then TD1_MULT_SRC_CD=put(TD1_VAL,_d3fmt.);
			   if not missing(TD1_VAL) then TD1_DRUG_DSG_FORM=put(TD1_VAL,_d6fmt.); 
			   if not missing(TD1_VAL) then TD1_DRUG_NM_TX=put(TD1_VAL,_d6fmt.); 
			   if not missing(TD1_VAL) then TD1_PKG_SIZE=put(TD1_VAL,_d14fmt.); 
			   if not missing(TD1_VAL) then GPI_CD=put(TD1_VAL,_d8fmt.);
			   if not missing(TD1_VAL) then GCN_SEQ_NBR=put(TD1_VAL,_d7fmt.); 
			   if not missing(TD1_VAL) then DRUG_CLASS_NM=put(TD1_VAL,_d9fmt.); 
			   if not missing(TD1_VAL) then DRUG_STRG=put(TD1_VAL,_d5fmt.); 
			   if not missing(TD1_VAL) then MAINT_DRUG_IND=put(TD1_VAL,_d10fmt.); 
			   if not missing(TD1_VAL) then SPCLT_DRUG_IND=put(TD1_VAL,_d15fmt.); 
			   if not missing(TD1_VAL) then BRND_GNRC_CD=put(TD1_VAL,_d1fmt.); 
			   if not missing(TD1_VAL) then DRUG_MULTSRC_CD=put(TD1_VAL,_d3fmt.); 
			   DRUG_LIST_ID=''; /**<---------------- leave as null else data wont load **/
			   if not missing(TD1_VAL) then DRUG_DSG_FORM=put(TD1_VAL,_d14fmt.); 
			   if not missing(TD1_VAL) then PKG_SIZE=put(TD1_VAL,_d14fmt.); 
			   if not missing(TD1_VAL) then UOM_CD=put(TD1_VAL,_d6fmt.); 
			   if not missing(TD1_VAL) then DRUG_SHRT_NM=put(TD1_VAL,_d2fmt.);
			   if not missing(TD1_VAL) then DRUG_PROD_GID=put(TD1_VAL,_d4fmt.); 
			   if not missing(TD1_VAL) then CVS_THCLS_CD=put(TD1_VAL,_d16fmt.); 
			   if not missing(TD1_VAL) then GCN_NBR=put(TD1_VAL,_d17fmt.);
			   if not missing(TD1_VAL) then DRUG_LEGL_STUS_CD=put(TD1_VAL,_d12fmt.);
			   
			run;	
			
			
			%let cnt_update_action_ind = 0;

			proc sql noprint;
			  select count(*) into: cnt_update_action_ind separated by ''
			  from load_1_&&eoms_name&i.
			  where OPPTY_ACTION_IND = 'U';
			quit;
			
			%put NOTE: cnt_update_action_ind = &cnt_update_action_ind. ;
			
			%if &cnt_update_action_ind. ne 0 %then %do;


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - update opportunity email notification
				+---------------------------------------------------------------------------------------------*/	

				filename xemail email 
				to=(&c_s_email_to.)
				subject="CTT Data Profiling - UPDATES OPPORTUNITY of ECOA Files ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

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
					put "The ECOA opportunity file has records of OPPTY_ACTION_IND = U and ECOA will send the updates to the EDW for processing. ";
					put '<br>';
					put '<br>';								
					put "The Step 1 - OPPTY_ACTION_IND = U = update an existing opportunity record ";
					put '<br>'; 
					put "The UPDATES OPPORTUNITY File:            load_1_&&eoms_name&i. ";
					put "The UPDATES OPPORTUNITY Counts - Update: &cnt_update_action_ind.  "; 
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
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - validate missing opportunity data elements
			+---------------------------------------------------------------------------------------------*/ 	
			data qc_populate2;
			set load_1_&&eoms_name&i.; 
			if ( missing(EPH_LINK_ID) or missing(PRSN_SSK_4) or missing(EDW_MBR_GID) or missing(CLT_LEVEL_1) ) and (missing(SPCLT_PTNT_ID)) and (missing(SPCLT_PTNT_GID)) ; 
			if eoms_record ne 0;
			run;	
			
			data qc_populate2;
			set qc_populate2;

			if missing(EPH_LINK_ID)      or  
			missing(PRTC_ID)             or
			missing(PRSN_SSK_1)          or
			missing(PRSN_SSK_2)          or
			missing(PRSN_SSK_3)          or
			missing(PRSN_SSK_4)          or
			missing(CLT_NM)              or
			missing(PTNT_FIRST_NM)       or 
			missing(PTNT_LAST_NM)        or 
			missing(PTNT_ADDR_LINE1_TX)  or
			missing(PTNT_CITY_TX)        or
			missing(PTNT_STATE_CD)       or
			missing(PTNT_ZIP_CD)         or				
			missing(PTNT_PHONE1_NBR)     or
			missing(PTNT_BRTH_DT)        or
			missing(PTNT_GNDR_CD)  ;
			run;			

			%let qc_populate2 = 0;
			
			proc sql noprint;
			select count(*) into: qc_populate2 separated by ''
			from qc_populate2;
			quit;
			
			%put NOTE: qc_populate2 = &qc_populate2. ;
			
			%if &qc_populate2. = 0 %then %do;
				%put NOTE: All data elements are populated;
				
				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_eph_&mbr_process_id.);	
			%end;
			%else %do;
			
				data working.qc_step3b_missing_&&eoms_name&i.;
				set qc_populate2;
				run;				
			
				%let c_s_email_subject = EA ECOA CPL SMFC Missing Data Elements Daily Files Failure;
				%m_abend_handler(abend_report=%str(EA ECOA - Missing Data Elements Daily Files - step 3),
				abend_message=%str(EA ECOA - Missing Data Elements Daily Files));
			   
			%end;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  QC - validate length of values of opportunity vs eoms layout
			|
			|  NOTE - if lengths exceed the requirements within opp specifications then will fail on IT loads
			|
			+---------------------------------------------------------------------------------------------*/
			%macro do_qc_length;

				proc sql; 
				connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw");
				create table dqi_eoms_rules as
				select * from connection to tera(

					select *
					from dss_cea2.dqi_eoms_rules  
					where layout_id = 1
					and sequence_id < 631
					and data_type = 'VARCHAR'
					and data_length > 0
					and data_attribute_name not like '%DT%'
					order by layout_id, rule_id	
				) ;
				quit;

				proc sort data = dqi_eoms_rules ;
				by rule_id;
				run;

				%let rule_total    = 0;
				%let cnt_rule_file = 0;
				
				proc sql noprint;
				select count(*) into: cnt_rule_file separated by ''
				from load_1_&&eoms_name&i. ;
				quit;
				
				data _null_;
				set dqi_eoms_rules end=eof;
				ii=left(put(_n_,4.));
				call symput('rule_var'||ii,trim(left(DATA_ATTRIBUTE_NAME)));
				call symput('rule_length'||ii,trim(left(data_length)));
				call symput('rule_id'||ii,trim(left(rule_id)));
				if eof then call symput('rule_total',ii);
				run;
				
				%put NOTE: rule_total    = &rule_total. ;
				%put NOTE: cnt_rule_file = &cnt_rule_file. ;
				
				%if &rule_total. = 0 or cnt_rule_file. < 2 %then %do;
				%end;
				%else %do;

					data qc1 ;
					set load_1_&&eoms_name&i. ;
					%do d = 1 %to &rule_total. ;
					if length( &&rule_var&d) > &&rule_length&d then rule&&rule_id&d = "1";
					else rule&&rule_id&d = "0";
					%end;
					run;

					data qc2;
					set qc1;
					keep rule: ;
					run;

					proc sort data = qc2 nodupkey;
					by %do d = 1 %to &rule_total. ;rule&&rule_id&d 	%end;;
					run;

					data qc3;
					set qc2;
					%do d = 1 %to &rule_total. ;
					if rule&&rule_id&d = "1" then output;
					%end;
					run;
					
					%let cnt_qc1    = 0; 

					proc sql noprint;
					select count(*) into: cnt_qc1 separated by ' '
					from qc3 ;
					quit;
									
					%if &cnt_qc1 > 0 %then %do;
					
						data working.qc3;
						set load_1_&&eoms_name&i.;
						run;

						proc export data=work.qc3
						dbms=xlsx 
						outfile="&c_s_datadir.&SYS_DIR.validate_lengths_opportunity_&tdname..xlsx" 
						replace; 
						sheet="validate_lengths"; 
						run;

						%m_abend_handler(abend_report=%str(&c_s_datadir./validate_lengths_opportunity_&tdname..xlsx),
								 abend_message=%str(There is an issue with the imported intake opporunity - There are lengths that exceed DDLs for dwv_campgn ));			

					%end;					
				
				%end;

			%mend do_qc_length;
			%do_qc_length;			
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  QC - validate unique record IDs	 
			+---------------------------------------------------------------------------------------------*/
			proc sort data=load_1_&&eoms_name&i.  dupout=duplicate_recs nodupkey;  
			by OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID ; 
			run;	

			%let dsid=%sysfunc(open(duplicate_recs));
			%let cntddp=%sysfunc(attrn(&dsid,nlobs));
			%let rc=%sysfunc(close(&dsid));

			%if &cntddp > 0 %then %do;

				proc export data=work.duplicate_recs
				dbms=xlsx 
				outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_opportunity_&tdname..xlsx" 
				replace; 
				sheet="validate_duplicates"; 
				run;

				%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_opportunity_&tdname..xlsx),
						 abend_message=%str(There is an issue with the imported ecoa opportunity files - There are duplicate IDs));			

			%end;			
			
	
		%end;  /** <----------------------------------------------------- end - eoms_total **/


	%end;  /** <----------------------------------------------------- end - cnt_files **/
	
	
	
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through files - disposition = 3s + 4s
	+---------------------------------------------------------------------------------------------*/	
	%if &cnt_files_disp. ne 0 %then %do;  /** <----------------------------------------------------- begin - cnt_files **/

		proc sort data = temp01_transform (where = (scan(memname,2,'_') in ('3','4'))) out = temp01b_transform nodupkey;
		by source_team data_file;
		run;
		
		data _null_;
		set temp01b_transform;  
		where scan(memname,2,'_') in ('3','4');
		call symput('eoms_name'||left(_n_),trim(left(data_file)));
		call symput('source_team'||left(_n_),trim(left(source_team))); 
		call symput('eoms_total',trim(left(_n_)));
		run;		

		%do w = 1 %to &eoms_total;  /** <----------------------------------------------------- begin - eoms_total **/

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - combine disp + disp rx
			|
			+---------------------------------------------------------------------------------------------*/
			proc sort data = &&source_team&w.._3_&&eoms_name&w.;
			by oppty_src_cd oppty_evnt_id msg_evnt_id ;
			run;

			%if %sysfunc(exist(work.&&source_team&w.._4_&&eoms_name&w.)) %then %do;
			
				proc contents data = work.&&source_team&w.._4_&&eoms_name&w. out = cnt_contents noprint;
				run;
				
				%let cnt_contents = 0;
				
				proc sql noprint;
				select count(*) into: cnt_contents separated by ''
				from cnt_contents
				where upcase(name) in ('OPPTY_EVNT_ID') ;
				quit;
				
				%if &cnt_contents. ne 0 %then %do;
					proc sort data = &&source_team&w.._4_&&eoms_name&w.;
					by oppty_src_cd oppty_evnt_id msg_evnt_id ;
					run;
				%end; 
				%else %do;	
					proc sql noprint;
					create table dqi_eoms_layout as
					select *
					from dss_cea2.dqi_eoms_layout
					where layout_id = 4
					order by layout_id, sequence_id;
					quit;

					data _null_;
					set dqi_eoms_layout; 
					n=trim(left(_n_));
					call symput('name_var'||n,trim(left(data_attribute_name))); 
					call symput('name_var_total',n);
					run;

					data &&source_team&w.._4_&&eoms_name&w.;
					length %do k=1 %to &name_var_total; &&name_var&k %end; $100. ;
					run;

					data &&source_team&w.._4_&&eoms_name&w.;
					format eoms_file $100. eoms_record 8.;
					set &&source_team&w.._4_&&eoms_name&w.;
					eoms_record=0;
					run;			
				%end;
			%end;
			%else %do;	
				proc sql noprint;
				create table dqi_eoms_layout as
				select *
				from dss_cea2.dqi_eoms_layout
				where layout_id = 4
				order by layout_id, sequence_id;
				quit;

				data _null_;
				set dqi_eoms_layout; 
				n=trim(left(_n_));
				call symput('name_var'||n,trim(left(data_attribute_name))); 
				call symput('name_var_total',n);
				run;

				data &&source_team&w.._4_&&eoms_name&w.;
				length %do k=1 %to &name_var_total; &&name_var&k %end; $100. ;
				run;

				data &&source_team&w.._4_&&eoms_name&w.;
				format eoms_file $100. eoms_record 8.;
				set &&source_team&w.._4_&&eoms_name&w.;
				run;			
			%end;			


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - transform and clean data elements for disposition
			|   
			+---------------------------------------------------------------------------------------------*/
			data load_3_&&eoms_name&w.;
			merge &&source_team&w.._3_&&eoms_name&w. (in=a rename=(eoms_file=source_opp_file))
			      &&source_team&w.._4_&&eoms_name&w. (in=b rename=(eoms_file=source_opprx_file));
			by oppty_src_cd oppty_evnt_id msg_evnt_id ;
			
				/**----------------------------------------------------------------------------------------------------------------------**/
				/**-- dispostion data elements - dates                                                                                 --**/
				/**----------------------------------------------------------------------------------------------------------------------**/
				%m_edw_datetime(data_element=PRSTN_DT,       data_type=1);
				%m_edw_datetime(data_element=PRSTN_DT2,      data_type=1); 

				/**----------------------------------------------------------------------------------------------------------------------**/
				/**-- dispostion data elements - dispostion level 1 + 2  = default when missing values                                 --**/
				/**-- level 1 = 1  = delivered                                                                                         --**/
				/**-- level 2 = 10 = new opportunity                                                                                   --**/
				/**----------------------------------------------------------------------------------------------------------------------**/				
				if missing(MSG_DISPN_CD) then MSG_DISPN_CD = '1';
				if missing(MSG_DISPN_SUB_CD) then MSG_DISPN_SUB_CD = '10';  

				/**----------------------------------------------------------------------------------------------------------------------**/
				/**-- dispostion data elements - housekeeping + identity columns                                                       --**/
				/**----------------------------------------------------------------------------------------------------------------------**/				   
				REC_CRTE_PROC_ID=OPPTY_SRC_CD;
				REC_CRTE_TRANS_ID='';
				REC_CRTE_TS=put(today() - &execution_date. ,yymmdd10.)||' 00.00.00';
				REC_CRTE_USER_ID=OPPTY_SRC_CD;
				REC_SRC_FILE_NM=scan(source_opp_file,2,'.');
				REC_UPD_PROC_ID=OPPTY_SRC_CD;
				REC_UPD_TRANS_ID='';
				REC_UPD_TS=put(today() - &execution_date. ,yymmdd10.)||' 00.00.00';
				REC_UPD_USER_ID=OPPTY_SRC_CD;
				REC_UPD_USER_ID_2=OPPTY_SRC_CD;
				
				if CHNL_CD='99' then CHNL_CD='CHALLCHNLS';
				if CHNL_CD2='99' then CHNL_CD2='CHALLCHNLS';
				
				CHNL_CD_ORIGINAL=CHNL_CD;
				CHNL_CD_ORIGINAL2=CHNL_CD2;

				EXTNL_SYS_OPPTY_ID=compress(EXTNL_SYS_OPPTY_ID,"'-");

				if missing(source_opprx_file) then source_opprx_file=left(trim(scan(source_opp_file,1,'_')))||'_rx_disposition.'||left(trim(scan(source_opp_file,2,'.')));
				if missing(oppty_evnt_id) and eoms_record ne 0 then delete;
				
			run;
			
			proc sql noprint;
			select count(*) into: cnt_load3 separated by ''
			from load_3_&&eoms_name&w. ;
			quit;
			
			%put NOTE: cnt_load3 = &cnt_load3. ;
			
			%if &cnt_load3. > 1 %then %do;
			  data load_3_&&eoms_name&w.;
			  set load_3_&&eoms_name&w.;
			  if eoms_record = 0 then delete;
			  run;
			%end;
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  QC - validate unique record IDs
			|
			+---------------------------------------------------------------------------------------------*/
			proc sort data=load_3_&&eoms_name&w.  dupout=duplicate_recs nodupkey;  
			by  OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID ; 
			run;	

			%let dsid=%sysfunc(open(duplicate_recs));
			%let cntddp=%sysfunc(attrn(&dsid,nlobs));
			%let rc=%sysfunc(close(&dsid));

			%if &cntddp > 0 %then %do;

				proc export data=work.duplicate_recs
				dbms=xlsx 
				outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_dispositions_&tdname..xlsx" 
				replace; 
				sheet="validate_duplicates"; 
				run;

				%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_dispositions_&tdname..xlsx),
						 abend_message=%str(There is an issue with the imported ecoa disposition files - There are duplicate IDs));			

			%end;				
		

		%end;  /** <----------------------------------------------------- end - eoms_total **/


	%end;  /** <----------------------------------------------------- end - cnt_files **/	
		
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/			
	%if &ecoa_test.	= YES3B %then %do;
		
		data working.qc_step3b_cpl_opp;
		set load_1_&c_s_file_date.000123 ;
		run;
		
		data working.qc_step3b_cpl_disp;
		set load_3_&c_s_file_date.000123 ;
		run;		
		
		data working.qc_step3b_sfmc_opp;
		set load_1_&c_s_file_date.000456 ;
		run;	
		
		data working.qc_step3b_sfmc_disp;
		set load_3_&c_s_file_date.000456 ;
		run;	
		
		data working.qc_step3b;
		x=1;
		run;		
		
		%m_ecoa_process(do_tasks=ALL);

	%end;	
	%else %do;
		
		data working.qc_step3b_cpl_opp;
		set load_1_&c_s_file_date.000123 (obs = 10000);
		run;
		
		data working.qc_step3b_cpl_disp;
		set load_3_&c_s_file_date.000123 (obs = 10000);
		run;		
		
		data working.qc_step3b_sfmc_opp;
		set load_1_&c_s_file_date.000456 (obs = 10000);
		run;	
		
		data working.qc_step3b_sfmc_disp;
		set load_3_&c_s_file_date.000456 (obs = 10000);
		run;	

		data working.qc_step3b;
		x=1;
		run;			

	%end;	


%mend m_step3_eoms_transform;
