
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step5_eoms_load
|
| LOCATION:  
| 
| PURPOSE: 	Load EOMS + CMCTN History + MCS data into EA Environment 
|
| LOGIC: 
|		step5 = sends data to EA Team  
|
|			select * from dss_cea2.dqi_eoms_opportunity  where execution_date = current_date
|			select * from dss_cea2.dqi_eoms_dispositions where execution_date = current_date
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

%macro m_step5_eoms_load;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine files to process
	+---------------------------------------------------------------------------------------------*/
	%m_create_work_list(data_out=temp01_load); 

	data working.qc_step5a;
	x=1;
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through files
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_files_opp. ne 0 %then %do;  /** <----------------------------------------------------- start - cnt_files **/

		proc sort data = temp01_load (where = (scan(memname,2,'_') in ('1','2'))) out = temp01b_load nodupkey;
		by source_team data_file;
		run;
		
		data _null_;
		set temp01b_load;  
		where scan(memname,2,'_') in ('1','2');
		call symput('eoms_name'||left(_n_),trim(left(data_file)));
		call symput('source_team'||left(_n_),trim(left(source_team))); 
		call symput('eoms_total',trim(left(_n_)));
		run;


		%do i = 1 %to &eoms_total;  /** <----------------------------------------------------- start - eoms_total **/			
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - get Anthem carriers IDs
			+---------------------------------------------------------------------------------------------*/
			proc sql noprint;
			connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
			create table v_clnt_acct_denorm as
			select * from connection to tera
			( 
				select distinct 'INGENIORX' as clnt_cd, a.lvl1_acct_id 
				from &c_s_schema..v_clnt_acct_denorm a,
				     &c_s_schema..v_usrvw_clnt_lst   b 
				where a.lvl1_acct_gid=b.lvl1_acct_gid
				  /**and upper(clnt_typ_cd) = 'INGENIORX'**/	
				  and a.lvl0_acct_id = 'S-468'
			);
			disconnect FROM tera;
			quit;

			%create_formats1(datain=v_clnt_acct_denorm, dataout=_acctfmt,  variable=lvl1_acct_id);	
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - do not process file if any QC rules not 0
			+---------------------------------------------------------------------------------------------*/
			%let cnt_tload = 0;
			
			proc sql noprint;
			select count(*) into: cnt_tload separated by ''
			from load_1_&&eoms_name&i.
			where rule_total1 > 0;
			quit;
			
			%put NOTE: cnt_tload = &cnt_tload. ;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - if any eoms gaps within the files then reject entire file and no load of cmctn history + mcs data
			|
			|  EXECUTION_TRIAGE
			|	1. SUCCESS     - initial load of files from step 2 (metadata of cntl + opp + disp)
			|	2. FAILURE     - if eoms triage  fails from step 4 (metadata of cntl + opp + disp)
			|	3. 0_0_0_0_0   - 1st 3 zeros = primary secondary and tertiary QC
			|	4. 0_0_0_0_0   - 4th   zero  = cmctn history QC
			|	5. 0_0_0_0_0   - 5th   zero  = mcs   history QC
			|
			+---------------------------------------------------------------------------------------------*/			
			%if &cnt_tload. ne 0 %then %do;    /** <----------------------------------------------------- start - cnt_tload **/
			
				%put NOTE:  No valid data to load to teradata;
				
				%let updt_oppfile   = %str('X');
				%let updt_opprxfile = %str('Y');
				
				proc sql noprint;
				select distinct("'"||trim(left(source_opp_file))||"'") into: updt_oppfile separated by ','
				from load_1_&&eoms_name&i.
				where rule_total1 ne 0;
				quit;
				
				proc sql noprint;
				select distinct("'"||trim(left(source_opprx_file))||"'") into: updt_opprxfile separated by ','
				from load_1_&&eoms_name&i.
				where rule_total1 ne 0;
				quit;				

				%put NOTE: updt_oppfile = &updt_oppfile. ;	
				%put NOTE: updt_opprxfile = &updt_opprxfile. ;	
				
				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					update &c_s_table_name. 
					set EXECUTION_TRIAGE='FAILURE'
					where source_opp_file in ( &updt_oppfile.)
					and clm_evnt_gid < 99

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 
				
				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					update &c_s_table_name. 
					set EXECUTION_TRIAGE='FAILURE'
					where source_opprx_file in (&updt_opprxfile.)
					and clm_evnt_gid < 99

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 	
								
			%end;
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - if no  eoms gaps within the files then load the cmctn history + mcs data  
			+---------------------------------------------------------------------------------------------*/			
			%else %do;			

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - format columns based on template
				+---------------------------------------------------------------------------------------------*/
				data template;
				set &c_s_table_name. (obs=0);
				run;

				proc contents data = template out = contents1 noprint;
				run;

				proc sql noprint;
				select name into: keep_variables separated by ' '
				from contents1;
				quit;

				data loadz_1_&&eoms_name&i. ;
				set template load_1_&&eoms_name&i. (rename = (fill_dt=fill_dt2 clm_evnt_gid=clm_evnt_gid2 OPPTY_EVNT_ID=OPPTY_EVNT_ID2 MSG_EVNT_ID=MSG_EVNT_ID2));
				run;

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - cmctn history and mcs mapping + rule_total1 = 0 (valid)
				+---------------------------------------------------------------------------------------------*/
				data loadz_1_&&eoms_name&i. loadtqc_1_&&eoms_name&i.;
				set loadz_1_&&eoms_name&i.;
				if rule_total1 = 0 ;
				
					EOMS_ROW_GID=0;
					CLM_EVNT_GID=clm_evnt_gid2*1;  if missing(CLM_EVNT_GID) then CLM_EVNT_GID=99;
					MBR_ACCT_GID = EDW_MBR_GID*1;
					OPPTY_EVNT_ID=OPPTY_EVNT_ID2*1;
					MSG_EVNT_ID=MSG_EVNT_ID2*1;
					QL_BNFCY_ID=PRTC_ID; 
					PRSCBR_PTY_GID = 1;
					QL_PRSCR_ID = PRSC_ID;
					CLNT_CD = 'NON-INGENIORX';
					if SPCLT_PTNT_SRC_CD ne '' then CLNT_CD = 'SPECIALTY - '||left(trim(SPCLT_PTNT_SRC_CD));
					LVL1_ACCT_ID = PRSN_SSK_1;  if missing(LVL1_ACCT_ID) then LVL1_ACCT_ID=CLT_LEVEL_1;
					LVL2_ACCT_ID = PRSN_SSK_2;  if missing(LVL2_ACCT_ID) then LVL2_ACCT_ID=CLT_LEVEL_2;
					LVL3_ACCT_ID = PRSN_SSK_3;  if missing(LVL3_ACCT_ID) then LVL3_ACCT_ID=CLT_LEVEL_3;
					ORACLE_ID = left(trim(PGM_ID))||"-"||left(trim(MSG_PRDCT_ALT_ID));
					MBR_FRST_NM = PTNT_FIRST_NM;
					MBR_MDL_NM = PTNT_MIDDLE_NM;
					MBR_LAST_NM = PTNT_LAST_NM;
					MBR_ADDRESS1 = PTNT_ADDR_LINE1_TX;
					MBR_ADDRESS2 = PTNT_ADDR_LINE2_TX;
					MBR_CITY = PTNT_CITY_TX;
					MBR_STATE = PTNT_STATE_CD;
					MBR_ZIP = PTNT_ZIP_CD;
					MBR_PHONE = PTNT_PHONE1_NBR;
					MBR_EMAIL = PTNT_EMAIL_ADDR_TX;
					PRSCBR_FRST_NM = PRSC_FIRST_NM;
					PRSCBR_MDL_NM = PRSC_MIDDLE_NM;
					PRSCBR_LAST_NM = PRSC_LAST_NM;
					PRSCBR_ADDRESS1 = PRSC_ADDR_LINE1_TX;
					PRSCBR_ADDRESS2 = PRSC_ADDR_LINE2_TX;
					PRSCBR_CITY = PRSC_CITY_TX;
					PRSCBR_STATE = PRSC_STATE_CD;
					PRSCBR_ZIP = PRSC_ZIP_CD;
					PRSCBR_PHONE = PRSC_PHONE1_NBR;
					PRSCBR_FAX = PRSC_FAX_NB;
					PRSCBR_EMAIL = PRSC_EMAIL_ADDR_TX;
					DRUG_ID = TD1_VAL;
					RX_NBR = RX_NBR;
					FILL_DT = input(substr(DSPND_DT,1,8),anydtdte.); 
					PHMCY_NPI_ID = PHCY_ID;
					NEXT_REFILL_DUE_DATE = input(substr(RX_ARDD_DT,1,8),anydtdte.);
					PHMCY_NM = RX_PHCY_NM;	
					if EVNT_PTY_ROLE_CD = 'PTNT' then RVR_CMCTN_ROLE_CD = '1';
					else if EVNT_PTY_ROLE_CD = 'PRSC' then RVR_CMCTN_ROLE_CD = '2';
					else if EVNT_PTY_ROLE_CD = 'PHCY' then RVR_CMCTN_ROLE_CD = '4'; 
					else RVR_CMCTN_ROLE_CD = '1'; 
					if chnl_cd='CHLTRHRCLS' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOBIVRNNC' then DSTRBTN_CD = '15';
					else if chnl_cd='CHALLCHNLS' then DSTRBTN_CD = '1';
					else if chnl_cd='CHCAREPSFE' then DSTRBTN_CD = '4';
					else if chnl_cd='CHIBIVRPBM' then DSTRBTN_CD = '15';
					else if chnl_cd='CHRXCONNCT' then DSTRBTN_CD = '4';
					else if chnl_cd='CHOBIVRWST' then DSTRBTN_CD = '15';
					else if chnl_cd='CHOBIVRSLK' then DSTRBTN_CD = '15';
					else if chnl_cd='CHOBIVRRCC' then DSTRBTN_CD = '15';
					else if chnl_cd='CHCMKPORTAL' then DSTRBTN_CD = '4'; 
					else if chnl_cd='CHCMPEMAIL' then DSTRBTN_CD = '4';
					else if chnl_cd='CHPHARMADV' then DSTRBTN_CD = '1';
					else if chnl_cd='CHRXDW' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRFSRV' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRPRSA' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOBCCCRNT' then DSTRBTN_CD = '3';
					else if chnl_cd='CHEMLEDLG' then DSTRBTN_CD = '4';
					else if chnl_cd='CHFAXBIZFAX' then DSTRBTN_CD = '2';
					else if chnl_cd='CHEPRSCBR' then DSTRBTN_CD = '4';
					else if chnl_cd='CHOUTBOX' then DSTRBTN_CD = '4'; 
					else if chnl_cd='CHMINCLINIC' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRVER' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRUGRPH' then DSTRBTN_CD = '1';
					else if chnl_cd='CHMIRIXA' then DSTRBTN_CD = '1';
					else if chnl_cd='CHDFTWLMRT' then DSTRBTN_CD = '1';
					else if chnl_cd='CHPRSCBRECM' then DSTRBTN_CD = '4';
					else if chnl_cd='CHLTRUNVWLD' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOMG' then DSTRBTN_CD = '1';
					else if chnl_cd='CHIDA' then DSTRBTN_CD = '17';
					else if chnl_cd='CHCSS' then DSTRBTN_CD = '1';  
					else if chnl_cd='CHOUTBOXCFST' then DSTRBTN_CD = '4';
					else if chnl_cd='CHLTRKIRKWOOD' then DSTRBTN_CD = '1';
					else if chnl_cd='CHRTLMBL' then DSTRBTN_CD = '17';
					else if chnl_cd='CHPRDMEPIC' then DSTRBTN_CD = '1';
					else if chnl_cd='CHGLOOKO' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOBLIVONGO' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOBTELCARE' then DSTRBTN_CD = '1';
					else if chnl_cd='CHCAREOB' then DSTRBTN_CD = '3';
					else if chnl_cd='CHDGTLICE' then DSTRBTN_CD = '4';
					else if chnl_cd='CHRTLPORCH' then DSTRBTN_CD = '4';
					else if chnl_cd='CHRTLCLGPFM' then DSTRBTN_CD = '3';
					else if chnl_cd='CHOMSLINKS' then DSTRBTN_CD = '1';
					else if chnl_cd='CHSPCLCC' then DSTRBTN_CD = '3';
					else if chnl_cd='CHOBVIDA' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOBVIVIFY' then DSTRBTN_CD = '1';
					else if chnl_cd='CHCTTMANDEL' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRUPC' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOUTCOMESMTM' then DSTRBTN_CD = '1';
					else if chnl_cd='CHOBCCFYICRNT' then DSTRBTN_CD = '3';
					else if chnl_cd='CHRTLRPCCI' then DSTRBTN_CD = '3';  
					else if chnl_cd='CHSFMC' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRALD' then DSTRBTN_CD = '1';
					else if chnl_cd='CHLTRSPR' then DSTRBTN_CD = '1';
					else if chnl_cd='CHEMLDEP' then DSTRBTN_CD = '4';
					else if chnl_cd='CHSMSDEP' then DSTRBTN_CD = '17';
					else if chnl_cd='CHIVRDEP' then DSTRBTN_CD = '15';
					else if chnl_cd='CHOBCCCONTINUUM' then DSTRBTN_CD = '3';
					else if chnl_cd='CHSPCLCCFYI' then DSTRBTN_CD = '3';
					else if chnl_cd='CHSPCLFYISPARCS' then DSTRBTN_CD = '3';
					else DSTRBTN_CD = '1';
					SBJ_COM_RLE_CDE = '1';
					CMCTN_STAT_CD = '2';  
					DRUG_EXPL_DESC_CD = '3';  
					DRUG_NHU_TYP_CD = '1';  
					SOURCE_TEAM = upcase(scan(source_opp_file,1,'_'));
					SOURCE_OPP_FILE = source_opp_file;
					SOURCE_OPP_FILE_METRIC = 0;
					SOURCE_OPPRX_FILE = source_opprx_file;
					SOURCE_OPPRX_FILE_METRIC = 0;
					CMCTN_HISTORY_FILE = 'PENDING';
					CMCTN_HISTORY_FILE_METRIC = 0;
					MCS_HISTORY_FILE = '';
					MCS_HISTORY_FILE_METRIC = 0;
					EXECUTION_DATE = today() - &execution_date. ; 			
					if missing(COMM_GENERATE_DT) then COMM_GENERATE_DT=compress(put(today() - &execution_date. ,yymmdd10.),'-');  
					DELIVERY_DATE=input(substr(COMM_GENERATE_DT,1,8),anydtdte.);   
					EXECUTION_TRIAGE = trim(left(rule_total1))||'_'||trim(left(rule_total2))||'_'||trim(left(rule_total3))||'_'||trim(left(rule_total4))||'_'||trim(left(rule_total5));
					MSG_DISPN_CD='0';
				keep &keep_variables. ;
				run;
				
			
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - apply anthem format
				+---------------------------------------------------------------------------------------------*/
				data loadz_1_&&eoms_name&i.;
				set loadz_1_&&eoms_name&i.;
				if index(CLNT_CD,'SPECIALTY') = 0 then do;
					CLNT_CD = put(upcase(LVL1_ACCT_ID), _acctfmt.);
				end;
				run;


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - load data files into teradata
				+---------------------------------------------------------------------------------------------*/			
				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_name. ) x) with no data 

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 

				%m_bulkload_sasdata(bulk_data=loadz_1_&&eoms_name&i. , bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id. , bulk_option=APPEND); 
				
				%put NOTE: &syserr. ;	

				%if &syserr. > 6 %then %do;
				  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
				  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 5),
						   abend_message=%str(EA ECOA - Proc Append Data failed - step 5));		  
				%end;	
				
				%m_bulkload_sasdata; 

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					insert &c_s_table_name. select * from dss_cea2.dqi_eoms_tera_&mbr_process_id.

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 

				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
				
				%m_table_statistics(data_in=&c_s_table_name., index_in=eoms_row_gid);
				
				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
				
				
				
				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - update level 1 disposition based on disposition date with 90 look back
				|
				|         HEE does not send to CMCTN History if the letter or email does not get a disposition of the L1 of 1.
				+---------------------------------------------------------------------------------------------*/
				%if &c_s_level1_disposition. = Y %then %do;
							

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - check EDW at a opportunity + message level and disposition code = 1 = delivery 
					|         for ECOA history not sent to cmctn history PeopleSafe
					|
					|  NOTE - an opp could have multiple messages so determine at oppty_evnt_id + msg_evnt_id level
					|         and not oppty_evnt_id level
					+---------------------------------------------------------------------------------------------*/				
					proc sql;
					connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
					execute(  

						create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (
							select top 200000 *
							from 
							(
																
								
								select 
								distinct oppty_altid as oppty_evnt_id, oppty_msg_altid as msg_evnt_id, dispn_cd, 
								dispn_sub_cd, chnl_cd, msg_prod_alt_id, src_pty_id, rec_add_ts, rec_upd_ts 
								from &c_s_schema..v_oppty_msg a 
								where  a.dispn_cd = '1' 
								and a.rec_upd_ts > current_timestamp - interval '90' day  /**<------------- logic for EDW  **/
								and a.src_pty_id <> 'DQI'   
								and a.oppty_msg_altid in (	
												select msg_evnt_id 
												from &c_s_table_name.  b
												where b.execution_date > current_date - interval '90' day  /**<------------- logic for ECOA **/
												and b.execution_date < current_date 
												and b.msg_dispn_cd <> 1
												and substring (b.execution_triage FROM 1 FOR 1) = '0'
												and substring (b.execution_triage FROM 7 FOR 1) = '0'
												and clnt_cd not like '%SPECIALTY%'
												and substr(b.cmctn_history_file,1,3) <> 'ext'
											)									
							) x

						) with data 

					) by tera;
					execute (commit work) by tera;  
					disconnect from tera;
					quit; 


					%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., column_in=oppty_evnt_id);
					%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., column_in=msg_evnt_id);
					
					data _null_;
					set dss_cea2.dqi_eoms_tera_&mbr_process_id. ;
					run;

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - update ECOA history if a opportunity + message dispositon code   
					|         for opportunities to be process and sent to cmctn history PeopleSafe
					+---------------------------------------------------------------------------------------------*/
					proc sql;
					connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
					execute(  

						update opps 
						from &c_s_table_name. opps,
						     dss_cea2.dqi_eoms_tera_&mbr_process_id. reset
						set msg_dispn_cd = reset.dispn_cd,
						    cmctn_history_file = 'IN QUEUE'
						where opps.oppty_evnt_id = reset.oppty_evnt_id 
						and opps.msg_evnt_id = reset.msg_evnt_id 
						and substr(opps.cmctn_history_file,1,3) <> 'ext'

					) by tera;
					execute (commit work) by tera;  
					disconnect from tera;
					quit; 
					

					%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
				 
				%end;				
				

			%end;  /** <----------------------------------------------------- end - cnt_tload **/


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - if no  gaps within the files then load the eoms data 
			|         if any gaps within the files then load the eoms data
			+---------------------------------------------------------------------------------------------*/			
			%if &cnt_tload. ne 0 or &cnt_tload. = 0 %then %do;    /** <----------------------------------------------------- start - cnt_tload **/			
			

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

				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_opp. ) x) with no data 

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 
				
				%m_bulkload_sasdata(bulk_data=loadt_1_&&eoms_name&i. , bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id. , bulk_option=APPEND); 
				
				%put NOTE: &syserr. ;	

				%if &syserr. > 6 %then %do;
				  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
				  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 5),
						   abend_message=%str(EA ECOA - Proc Append Data failed - step 5));		  
				%end;	
				
				%m_bulkload_sasdata; 

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					insert &c_s_table_opp. select * from dss_cea2.dqi_eoms_tera_&mbr_process_id.

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 

				%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);	
				
				%m_table_statistics(data_in=&c_s_table_opp., index_in=eoms_row_gid);
				

			%end;  /** <----------------------------------------------------- end - cnt_tload **/

			

		%end;  /** <----------------------------------------------------- end - eoms_total **/


	%end;  /** <----------------------------------------------------- end - cnt_files **/



	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through files
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_files_disp. ne 0 %then %do;  /** <----------------------------------------------------- start - cnt_files **/

		proc sort data = temp01_load (where = (scan(memname,2,'_') in ('3','4'))) out = temp01b_load nodupkey;
		by source_team data_file;
		run;
		
		data _null_;
		set temp01b_load; 
		where scan(memname,2,'_') in ('3','4');
		call symput('eoms_name'||left(_n_),trim(left(data_file)));
		call symput('source_team'||left(_n_),trim(left(source_team))); 
		call symput('eoms_total',trim(left(_n_)));
		run;


		%do w = 1 %to &eoms_total;  /** <----------------------------------------------------- start - eoms_total **/	


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
			
			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (select * from (select * from  &c_s_table_disp. ) x) with no data 

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 
 
			%m_bulkload_sasdata(bulk_data=loadt_3_&&eoms_name&w. , bulk_base=dss_cea2.dqi_eoms_tera_&mbr_process_id. , bulk_option=APPEND); 
			
			%put NOTE: &syserr. ;	

			%if &syserr. > 6 %then %do;
			  %let c_s_email_subject = EA ECOA Stars SQL Phone Data Failure;
			  %m_abend_handler(abend_report=%str(EA ECOA - Proc Append Data failed - step 5),
					   abend_message=%str(EA ECOA - Proc Append Data failed - step 5));		  
			%end;	
			
			%m_bulkload_sasdata;  

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				insert &c_s_table_disp. select * from dss_cea2.dqi_eoms_tera_&mbr_process_id.

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);
			
			%m_table_statistics(data_in=&c_s_table_disp., index_in=eoms_row_gid);	

		
		%end;
		
	%end;
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/			
	%if &ecoa_test.	= YES5B %then %do;

		data working.qc_step5b;
		set x=1;
		run;	
		
		%m_ecoa_process(do_tasks=ALL);

	%end;	
	%else %do;

		data working.qc_step5b;
		x=1;
		run;			

	%end;	


%mend m_step5_eoms_load;
