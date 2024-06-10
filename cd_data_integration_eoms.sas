
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    cd_data_integration_eoms
|
| LOCATION:  
| 
| PURPOSE:  
|
| LOGIC:     
|
| INPUT:     
|
| OUTPUT:    
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20190201 - Clinical Stars Team - Original (version 2019.02.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/



/* SASDOC --------------------------------------------------------------------------------------(dashes)
| Communication History Variables - H2R2 SOP Communication History External Feeds v0_09.doc
|
| 	rvr_cmctn_role_cd 
|         1 = Participant
|	  2 = Prescriber
|	  3 = Client
|	  4 = Other
|	  5 = Cardholder
|  
| 	dstrbtn_cd:
| 	  1  = letter
| 	  2  = fax
| 	  3  = phone call
| 	  4  = email
| 	  6  = televox
| 	  15 = ivr
| 	  16 = pda
| 	  17 = text message
|
|	cmctn_stat_cd:
| 	  1 = participant
| 	  2 = prescriber
| 	  3 = client
| 	  4 = other
| 	  5 = cardholder
| 
| 	drug_expl_desc_cd:
| 	  1 = brand with generic available
| 	  2 = non-formulary with formulary available
| 	  3 = target drug
| 	  4 = conflicting drug
| 	  5 = controlled substance
| 	  6 = denied prescription
|
| 	drug_nhu_typ_cd:
| 	  1 = ndc - national drug code
| 	  2 = hri - health related item
| 	  3 = upc - universal product code
| 	  4 = dmr - dmr dummy ndc code
| 	  5 = ncpdp - ncpdp dummy ndc code
| 	  6 = mfr - manufacturer drug code
|
|
+(dashes)--------------------------------------------------------------------------------------SASDOC*/

/* SASDOC --------------------------------------------------------------------------------------
|  STEP - campaign environment
+---------------------------------------------------------------------------------------------*/
%m_environment;


%macro cd_data_integration_eoms(di_type=, rcrc=, cs_label=, pharmacy_fax_flag=N);


 	/* SASDOC --------------------------------------------------------------------------------------
 	|  STEP - parameters
 	|   
	+---------------------------------------------------------------------------------------------*/
	options nosymbolgen nomlogic spool;

	%global m_cmctn_history_file m_cmctn_history_filecnt bad_ph_fax cnt_files_opp cnt_files_disp 
		myfilerf eoms_report eoms_IT get_sftp_directory put_linux_directory eoms_directory
		c_s_table_files c_s_table_rules c_s_table_opp c_s_table_disp c_s_table_oppdnorm c_s_table_name SYS_DIR 
		c_s_fastload cnt_bulkload c_s_schema;

	%let m_cmctn_history_file	= no_files.txt;
	%let m_cmctn_history_filecnt	= 0; 
	%let bad_ph_fax      		= %str('1111111111','2222222222','3333333333','4444444444','5555555555','6666666666','7777777777','8888888888','9999999999');
	%let c_s_fastload		= YES;
	%let cnt_bulkload		= 10000;
	%let c_s_schema         	= %str(dwu_edw_rb);
	
	libname eoms "&c_s_datadir.";	
		
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine operating system 
	+---------------------------------------------------------------------------------------------*/
	%let SYS_SCP = %index(%sysfunc(getoption(work)), C: );
	%if &SYS_SCP = 1 %then %let SYS_DIR=%str(\); %else %let SYS_DIR=%str(/);
	%put NOTE: SYS_SCP = &SYS_SCP;
	%put NOTE: SYS_DIR = &SYS_DIR;	
	 


	%if &c_s_dqi_production. = Y %then %do;
		%let c_s_table_oppdnorm = %str(dss_cea2.dqi_eoms_opportunity_denorm);
		%let c_s_table_files    = %str(dss_cea2.dqi_eoms_files);
		%let c_s_table_process  = %str(dss_cea2.dqi_eoms_files_process);
		%let c_s_table_history  = %str(dss_cea2.dqi_eoms_files_history);
		%let c_s_table_rules    = %str(dss_cea2.dqi_eoms_file_rule_metrics);
		%let c_s_table_opp	= %str(dss_cea2.dqi_eoms_opportunity);
		%let c_s_table_disp	= %str(dss_cea2.dqi_eoms_dispositions);	
		%let c_s_table_name 	= %str(dss_cea2.dqi_eoms_cmctn_history); 		/** <--------------------- location of HCE table raw data files **/
		
	
		%let myfilerf    	= %str(/anr_dep/communication_history/files/);      	/** <--------------------- location of HCE raw data files **/
		%let eoms_report	= %str(/anr_dep/communication_history/eoms_reports);    /** <--------------------- location of CTT reports of raw data files **/
		%let eoms_IT		= %str(/anr_dep/communication_history/eoms_IT);         /** <--------------------- location of IT raw data files **/
		%let get_sftp_directory	= %str(/ant591/IVR/incoming);				/** <--------------------- location of SFTP raw data files **/
		%let put_linux_directory= %str(/anr_dep/communication_history/eoms_files);      /** <--------------------- location of LINUX raw data files **/
		%let eoms_directory	= &put_linux_directory.;                                /** <--------------------- location of CTT raw data files **/
		%let eoms_history	= %str(/anr_dep/communication_history/eoms_history);    /** <--------------------- location of history of data files **/
		%let get_etl_directory	= %str(/anr_dep/communication_history/eoms_IT);	
		%let put_etl_directory  = %str(/appl/edw/incoming/campgn);
		%let put_etl_directory  = %str(/ant591/IVR/edw);
		%let eoms_consolidate   = %str(/anr_dep/communication_history/eoms_files_consolidate);
	%end;
	%else %do;
		%let c_s_table_oppdnorm = %str(dss_cea2.dqi_eoms_opportunity_denorm);
		%let c_s_table_files    = %str(dss_cea2.dqi_eoms_files_test);
		%let c_s_table_process  = %str(dss_cea2.dqi_eoms_files_process_test);
		%let c_s_table_history  = %str(dss_cea2.dqi_eoms_files_history_test);
		%let c_s_table_rules    = %str(dss_cea2.dqi_eoms_file_rule_metrics_test);
		%let c_s_table_opp	= %str(dss_cea2.dqi_eoms_opportunity_test);
		%let c_s_table_disp	= %str(dss_cea2.dqi_eoms_dispositions_test);	
		%let c_s_table_name 	= %str(dss_cea2.dqi_eoms_cmctn_history_test); 		/** <--------------------- location of HCE table raw data files **/
		
		%let myfilerf    	= %str(/anr_dep/communication_history/files_test/);     /** <--------------------- location of HCE raw data files **/
		%let eoms_report	= %str(/anr_dep/communication_history/eoms_reports_test);/** <--------------------- location of CTT reports of raw data files **/ 
		%let eoms_IT		= %str(/anr_dep/communication_history/eoms_IT_test);	/** <--------------------- location of IT raw data files **/		
		%let get_sftp_directory	= %str(/ant591/IVR/incoming);				/** <--------------------- location of SFTP raw data files **/
		%let put_linux_directory= %str(/anr_dep/communication_history/eoms_files_test); /** <--------------------- location of LINUX raw data files **/
		%let eoms_directory	= &put_linux_directory.;                                /** <--------------------- location of CTT raw data files **/
		%let eoms_history	= %str(/anr_dep/communication_history/eoms_history);    /** <--------------------- location of history of data files **/
		%let get_etl_directory	= %str(/anr_dep/communication_history/eoms_IT_test);	
		%let put_etl_directory  = %str(/appl/edw/incoming/campgn);
		%let put_etl_directory  = %str(/ant591/IVR/edw); 
		%let eoms_consolidate   = %str(/anr_dep/communication_history/eoms_files_consolidate_test);
	%end;


	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - opportunity + disposition data - begin
	|   
	+---------------------------------------------------------------------------------------------*/ 
	%if &di_type. = 0 %then %do; 
	
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - clean up a prior run and files for that day
		|   
		+---------------------------------------------------------------------------------------------*/ 	
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			delete &c_s_table_name.  where execution_date   = current_date - &execution_date. ;
			delete &c_s_table_files. where execution_date   = current_date - &execution_date. ;
			delete &c_s_table_opp.   where execution_date   = current_date - &execution_date. ;
			delete &c_s_table_disp.  where execution_date   = current_date - &execution_date. ;
			delete &c_s_table_rules. where execution_date   = current_date - &execution_date. ;
			delete &c_s_table_process. where execution_date = current_date - &execution_date. ;
			delete &c_s_table_history. where execution_date = current_date - &execution_date. ;
			
			delete &c_s_table_name.  where source_opp_file   like %tslit(&c_s_file_date_delete.);
			delete &c_s_table_name.  where source_opprx_file like %tslit(&c_s_file_date_delete.);
			delete &c_s_table_files. where source_file       like %tslit(&c_s_file_date_delete.);
			delete &c_s_table_opp.   where source_opp_file   like %tslit(&c_s_file_date_delete.);
			delete &c_s_table_disp.  where source_opp_file   like %tslit(&c_s_file_date_delete.);
			delete &c_s_table_rules. where source_opp_file   like %tslit(&c_s_file_date_delete.);	
			delete &c_s_table_process. where source_file     like %tslit(&c_s_file_date_delete.);
			delete &c_s_table_history. where source_file     like %tslit(&c_s_file_date_delete.);

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit; 	
		
		data _null_;
		x "rm /anr_dep/communication_history/eoms_files/&c_s_file_date_linux.";
		run;		

		data _null_; 
		x "cd &put_linux_directory.";
		x "rm &put_linux_directory./&sftp_get_file1. "; 
		x "rm &put_linux_directory./&sftp_get_file2. "; 
		run;

		%m_dqi_process_control(id=1, step=%str(DI - DATA INTGRATION BEGIN));
	%end;		
		

 	/* SASDOC --------------------------------------------------------------------------------------
 	|  STEP - opportunity + disposition data - CTT team
 	|   
	+---------------------------------------------------------------------------------------------*/
	%if &di_type. = 1 and  &c_s_files_raw. = Y %then %do;  /**<----------------------------------------------- CTT eoms files **/ 
		
		%m_sftp_put(put_cleanup=YES);
		%m_dqi_process_control(id=2, step=%str(DI - DATA INTGRATION OPPORTUNITY AND DISPOSITION FILES BEGIN));
		%m_sftp_get(get_directory=&get_sftp_directory., put_directory=&put_linux_directory., get_file1=&sftp_get_file1., get_file2=&sftp_get_file2.);	
		
		%m_step0_eoms_process;          /** <--------------- collects daily history of ecoa processing of files **/
		%m_step0_non_processed_files;   /** <--------------- validates if any files missing for the last 7 days and reprocesses them **/
		
		%m_step0_eoms_consolidate(team_id=cpl);
		%m_step1_eoms_rename;
		
		data working.qc_step2a;
		x=1;
		run;
	
		%m_step2_eoms_extract(team_id=cpl,  layout_id=1);
		%m_step2_eoms_extract(team_id=cpl,  layout_id=2);
		%m_step2_eoms_extract(team_id=sfmc, layout_id=1);
		%m_step2_eoms_extract(team_id=sfmc, layout_id=2);
		%m_step2_eoms_extract(team_id=cpl,  layout_id=3);
		%m_step2_eoms_extract(team_id=cpl,  layout_id=4);
		%m_step2_eoms_extract(team_id=sfmc, layout_id=3);
		%m_step2_eoms_extract(team_id=sfmc, layout_id=4);
		
		data working.qc_step2b;
		x=1;
		run;
		
		%m_step3_eoms_transform;
		%m_step4_eoms_dataprofiling;			
		%m_step5_eoms_load;
		%m_dqi_process_control(id=3, step=%str(DI - DATA INTGRATION OPPORTUNITY AND DISPOSITION FILES COOMPLETE)); 

	%end;
	

 	/* SASDOC --------------------------------------------------------------------------------------
 	|  STEP - opportunity + disposition data - CMCTN History team
 	|   
	+---------------------------------------------------------------------------------------------*/
	%if &di_type. = 2 and &c_s_cmctn_send. = Y %then %do;  /**<----------------------------------------------- cmcntn history **/ 
	
	
		%m_dqi_process_control(id=4, step=%str(DI - DATA INTGRATION CMCTN HISTORY BEGIN));	

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - extract cmctn history transaction data
		|   
		+---------------------------------------------------------------------------------------------*/
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table comm_hist_temp  as 
		select * from connection to tera
		(			
			select distinct 
			  EOMS_ROW_GID as cs_gid
			, CLM_EVNT_GID as claim_gid
			, PRSCBR_PTY_GID as provider_gid
			, QL_PRSCR_ID as provider_id
			, MBR_ACCT_GID as mbr_gid
			, trim(QL_BNFCY_ID) as beneficiary_id
			, ORACLE_ID as oracleid
			%if &rcrc. = 1 %then %do;
				, MBR_FRST_NM as name_first
				, MBR_MDL_NM as name_middle
				, MBR_LAST_NM as name_last
				, MBR_ADDRESS1 as recipientaddress
				, MBR_ADDRESS2 as recipientaddress2
				, MBR_CITY as recipientcity
				, MBR_STATE as recipientstate
				, MBR_ZIP as recipientzip
				, MBR_PHONE as recipientphone
			%end;
			%else %if &rcrc. = 2 %then %do;
				, MBR_FRST_NM as name_first
				, MBR_MDL_NM as name_middle
				, MBR_LAST_NM as name_last
				, MBR_ADDRESS1 as recipientaddress
				, MBR_ADDRESS2 as recipientaddress2
				, MBR_CITY as recipientcity
				, MBR_STATE as recipientstate
				, MBR_ZIP as recipientzip
				, MBR_PHONE as recipientphone
			%end;			
			%else %do;
				, MBR_FRST_NM as name_first
				, MBR_MDL_NM as name_middle
				, MBR_LAST_NM as name_last
				, MBR_ADDRESS1 as recipientaddress
				, MBR_ADDRESS2 as recipientaddress2
				, MBR_CITY as recipientcity
				, MBR_STATE as recipientstate
				, MBR_ZIP as recipientzip
				, MBR_PHONE as recipientphone		
			%end;
			, DRUG_ID as ndc_code
			, RX_NBR as rx_number
			, FILL_DT as dspnd_date
			, PHMCY_NPI_ID as pharmacy_npi_id
			, NEXT_REFILL_DUE_DATE as next_refill_due_date
			, PHMCY_NM as pharmacy_name
			, RVR_CMCTN_ROLE_CD 
			, DSTRBTN_CD 
			, SBJ_COM_RLE_CDE
			, CMCTN_STAT_CD 
			, DRUG_EXPL_DESC_CD 
			, DRUG_NHU_TYP_CD
			, MSG_EFF_DT

			from &c_s_table_name.  history,	
			(	select oppty_evnt_id, msg_evnt_id, SUBSTR(prstn_dt2,1,10) as msg_eff_dt
				from &c_s_table_disp. m 
				qualify rank() over (partition by m.msg_evnt_id order by m.execution_date desc) = 1		
			) d			
			 
			where TO_CHAR(history.oppty_evnt_id)=d.oppty_evnt_id
			and TO_CHAR(history.msg_evnt_id)=d.msg_evnt_id			
			and execution_date <= current_date
			and rvr_cmctn_role_cd = %tslit(&rcrc.)	                   /** <--------- rvr_cmctn_role_cd = 1 = Participant  **/
			and cmctn_history_file = 'IN QUEUE'			   /** <--------- cmctn_history_file = ext = delivered to PeopleSafe  **/
			and source_team <> 'DQI'				   /** <--------- DQI already delivered to PeopleSafe within DQI **/
			and CLNT_CD not like '%SPECIALTY%'                         /** <--------- Specialty is non-PBM data **/
			%if &c_s_level1_disposition. = Y %then %do;
			and msg_dispn_cd = 1                                       /** <--------- msg_dispn_cd = 1 = Delivered **/
			%end;
			and substring (execution_triage FROM 1 FOR 1) = '0'        /** <--------- ecoa file failure **/		
			/**and substring (execution_triage FROM 7 FOR 1) = '0'**/  /** <--------- cmctn history failure = rule7=program ID + rule35=tempalte + rule66 = QL bene ID **/
		);
		disconnect from tera;
		quit; 
		
		/*****************************************************
		HEE does not send to CMCTN History if the letter or email doesnt get a disposition of the L1 of 1.   
		
		select top 100 oppty_evnt_id, msg_evnt_id, msg_dispn_cd, rec_crte_ts
		from dwv_eoms.event_message
		where msg_dispn_cd = '1'
		and year(rec_crte_ts) > 2020 ;
		
		
		select oppty_altid as oppty_evnt_id, oppty_msg_altid as msg_evnt_id, dispn_cd, rec_add_ts
		from &c_s_schema..v_oppty_msg
		where dispn_cd = '1'
		and oppty_altid in 
			(
			select oppty_evnt_id 
			from dss_cea2.dqi_eoms_cmctn_history  
			where cmctn_history_file is null
			and execution_date < current_date
			and msg_dispn_cd <> 1                                
			and substring (execution_triage FROM 1 FOR 1) = '0'   
			and substring (execution_triage FROM 7 FOR 1) = '0'   						
			)  ;
							
		*****************************************************/


		%let cnt_cmctn_history = 0;

		proc sql noprint;
		select count(*) into: cnt_cmctn_history separated by ''
		from comm_hist_temp;
		quit;

		%put NOTE: cnt_cmctn_history = &cnt_cmctn_history. ;

		%if &cnt_cmctn_history. ne 0 %then %do;	 
		
			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - STARTED PBM CMCTN History Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

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
				put "The PBM File CMCTN History Processing has started. ";
				put '<br>';
				put '<br>'; 
				put "The PBM File CMCTN History Processing counts: &cnt_cmctn_history. ";
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
			|  STEP - clean the oracle ID
			|   
			+---------------------------------------------------------------------------------------------*/
			data npschars (drop = i);  
			length npschars $2;
			retain npschars;
			if _n_=1 then do; 
				do i=0, 160;
				  if i=0 then npschars=byte(i);
				  else npschars=trim(npschars)||byte(i);
				end; 
			end; 
			run; 

			data comm_hist_temp (drop = npschars oracleid);
			if _n_=1 then set npschars;
			set comm_hist_temp;
			oracle_id=compress(oracleid, npschars); 
			oracle_id=upcase(oracle_id);
			run; 
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create cmctn history transaction data
			|   
			+---------------------------------------------------------------------------------------------*/
			data comm_hist_temp2 (keep=recipient_gid subject_gid recipient_id subject_id address1_tx
						address2_tx city_tx state zip_cd zip_suffix_cd address3_tx address4_tx
						intl_postal_cd cntry_cd email_address tlphn_nb tlphn_area_cd
						tlphn_ext drug_ndc_id trans_id program_id stellent1 rvr_full_nm 
						rvr_cmctn_role_cd dstrbtn_cd sbj_com_rle_cde cmctn_stat_cd drug_expl_desc_cd drug_nhu_typ_cd
						msg_eff_dt
						%if &pharmacy_fax_flag. = Y %then %do;
						rx_nb fill_dt orig_phm_npi_id next_pick_up_dt ltr_reference_id
						%end;);
			set  comm_hist_temp;

			if rvr_cmctn_role_cd = '1' then do;
				recipient_gid = mbr_gid;
				recipient_id  = beneficiary_id;  	/**<-------------------------------------------------- QL beneficiary ID **/
				subject_gid   = mbr_gid;
				subject_id    = beneficiary_id;
			end;
			else if rvr_cmctn_role_cd = '2' then do;
				if vtype(provider_id)='N' then do;
				  recipient_id  = put(left(provider_id),30.);
				end;
				else do;
				  recipient_id  = provider_id;         /**<--------------------------------------------------- QL prescriber ID **/
				end;		
				recipient_gid = provider_gid; 
				subject_gid   = mbr_gid;
				subject_id    = beneficiary_id;
			end;	
			else if rvr_cmctn_role_cd = '4' then do;
				recipient_gid = '';          
				recipient_id  = '';			/**<-------------------------------------------------- rvr_cmctn_role_cd = 4 then must be null **/
				subject_gid   = mbr_gid;
				subject_id    = beneficiary_id;		/**<-------------------------------------------------- pharmacy fax = QL beneficiary ID **/
			end;	
			else do;
				recipient_gid = mbr_gid;
				recipient_id  = beneficiary_id;  	/**<-------------------------------------------------- QL beneficiary ID **/
				subject_gid   = mbr_gid;
				subject_id    = beneficiary_id;
			end;			

			trans_id = '00000000';
			program_id = scan(oracle_id,1,'-');
			stellent1  = scan(oracle_id,2,'-');
			%if &pharmacy_fax_flag. = Y %then %do;
				rvr_full_nm = 'PHARMACY FAX: '||trim(left(pharmacy_name));       						
			%end;
			%else %do;
				rvr_full_nm = trim(name_first) || ' ' || trim(name_last);  
			%end;
			address1_tx = recipientaddress;			/**<-------------------------------------------------- address where cmctn was sent - pharmacy **/
			address2_tx = recipientaddress2;
			city_tx = recipientcity;
			state = recipientstate;
			zip_cd = recipientzip;
			zip_suffix_cd = ' ';
			address3_tx = '  ';
			address4_tx = '  ';
			intl_postal_cd = '  ';
			cntry_cd = '  ';
			email_address = '  ';
			recipientphone=compress(recipientphone,'-)(');
			if length(recipientphone) < 10 or (length(recipientphone) >= 3 and substr(recipientphone,1,3) = '000') or recipientphone in (&bad_ph_fax.) then do;
				tlphn_nb = '0000000';
				tlphn_area_cd = '000';
			end;
			else do;
				tlphn_nb = substr(recipientphone,4,7);
				tlphn_area_cd = substr(recipientphone,1,3);
			end;
			tlphn_ext = '';
			drug_ndc_id = ndc_code;

			%if &pharmacy_fax_flag. = Y %then %do;
				rx_nb=rx_number;				/**<-------------------------------------------------- 5 additional elements for peoplesafe drug grid **/
				fill_dt=dspnd_date;
				orig_phm_npi_id=pharmacy_npi_id;		
				next_pick_up_dt=next_refill_due_date;
				ltr_reference_id='';
			%end;

			if recipient_id ne '' then output;
			run;

			data _null_;
			set comm_hist_temp2 (obs=1);
			call symput('rvr_cmctn_role_cd',trim(left(rvr_cmctn_role_cd)));
			call symput('dstrbtn_cd',trim(left(dstrbtn_cd)));
			call symput('sbj_com_rle_cde',trim(left(sbj_com_rle_cde)));
			call symput('cmctn_stat_cd',trim(left(cmctn_stat_cd)));
			call symput('drug_expl_desc_cd',trim(left(drug_expl_desc_cd)));
			call symput('drug_nhu_typ_cd',trim(left(drug_nhu_typ_cd))); 
			run;		

			data _null_;
			today=today() - &execution_date. ;
			call symput('cnct_dt',put(today,yymmddn8.));
			run;

			%put NOTE: cnct_dt = &cnct_dt.;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create and sftp cmctn history transaction files
			|   
			+---------------------------------------------------------------------------------------------*/
			%m_create_cmctn_history_eoms(&cnct_dt,comm_hist_temp2,&rvr_cmctn_role_cd,&dstrbtn_cd,&sbj_com_rle_cde,&cmctn_stat_cd,&drug_expl_desc_cd,&drug_nhu_typ_cd);


			%if &c_s_dqi_production. = Y %then %do;
			  %if %sysfunc(fileexist(&myfilerf.&m_cmctn_history_file.)) %then %do; 
				data _null_;
				x "cp &myfilerf.&m_cmctn_history_file. /anr_dep/hercules/&m_cmctn_history_file. ";
				x chmod 777 "/anr_dep/hercules/&m_cmctn_history_file." ;
				run;
			  %end;
			  %else %do;
			    %put WARNING: external DEV transaction file DNE - &m_cmctn_history_file.;
			  %end;
			%end;
			%else %if &c_s_dqi_production. = N %then %do;
			  %if %sysfunc(fileexist(&myfilerf.&m_cmctn_history_file.)) %then %do;
				data _null_;
				x "cp &myfilerf.&m_cmctn_history_file. &c_s_datadir./&m_cmctn_history_file. ";
				x chmod 777 "&c_s_datadir./&m_cmctn_history_file." ;
				run;
			  %end;
			  %else %do;
			    %put WARNING: external DEV transaction file DNE - &m_cmctn_history_file.;
			  %end;			
			%end;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - collect metrics
			|   
			+---------------------------------------------------------------------------------------------*/
			%m_table_drop(data_in=&c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id.);

			data &c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id. (bulkload=yes tenacity=2 sessions=4 dbcreate_table_opts='primary index(cs_gid)');
			set comm_hist_temp ;
			cmctn_history_file="&m_cmctn_history_file.";
			cmctn_history_file_metric = &m_cmctn_history_filecnt. ;
			keep cs_gid cmctn_history_file cmctn_history_file_metric;
			run; 
			
			%m_table_statistics(data_in=&c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id., index_in=cs_gid); 
			
			 
			proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  
					update history
					from &c_s_table_name.		 	      	          history, 
					     &c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id.  infile

					set cmctn_history_file  = infile.cmctn_history_file,
					    cmctn_history_file_metric = infile.cmctn_history_file_metric
					where history.eoms_row_gid = infile.cs_gid  
					and  msg_dispn_cd = 1
				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
			quit;  
			
			%m_table_drop(data_in=&c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id.);
			
			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - COMPLETED PBM CMCTN History Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  

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
				put "The PBM File CMCTN History Processing has completed. ";
				put '<br>';
				put '<br>'; 
				put "The PBM File CMCTN History Processing counts: &m_cmctn_history_filecnt. ";
				put '<br>';
				put '<br>'; 
				put "The PBM File CMCTN History Processing External CMCTN History File: &m_cmctn_history_file. ";
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

		%m_dqi_process_control(id=5, step=%str(DI - DATA INTGRATION CMCTN HISTORY COMPLETE));

	%end;
 
 
 
 	/* SASDOC --------------------------------------------------------------------------------------
 	|  STEP - opportunity + disposition data - MCS team
 	|   
	+---------------------------------------------------------------------------------------------*/
 	%if &di_type. = 3 and &c_s_mcs_send. = Y %then %do;  /**<----------------------------------------------- anthem mcs **/
 	

		%m_dqi_process_control(id=6, step=%str(DI - DATA INTGRATION MCS HISTORY BEGIN));
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - extract mcs history transaction data
		|   
		+---------------------------------------------------------------------------------------------*/
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table comm_hist_temp  as 
		select * from connection to tera
		(			 
			select distinct 
			  EOMS_ROW_GID as cs_gid
			, EOMS_ROW_GID as CommID
			, 'RxCLAIM' as MemberSource 
			, trim(QL_BNFCY_ID) as MemberSourceID
			, CLNT_CD as ClientCode
			, '' as ClientID
			, LVL1_ACCT_ID as CarrierID
			, LVL2_ACCT_ID as AccountID
			, LVL3_ACCT_ID as GroupID
			, trim(QL_BNFCY_ID) as MemberID
			, '' as ContractID
			, '' as PBPID
			, '' as LOB
			, 'PRNTVND6' as CommSourceSystem
			, 'CPL SFMC' as CommName
			, (CASE
				WHEN DSTRBTN_CD = 1  THEN 'Letter'
				WHEN DSTRBTN_CD = 2  THEN 'Fax'
				WHEN DSTRBTN_CD = 3  THEN 'Secure Message'
				WHEN DSTRBTN_CD = 4  THEN 'Email'
				WHEN DSTRBTN_CD = 6  THEN 'Secure Message' 
				WHEN DSTRBTN_CD = 15 THEN 'IVR'
				WHEN DSTRBTN_CD = 16 THEN 'Secure Message'
				WHEN DSTRBTN_CD = 17 THEN 'SMS'
				ELSE 'Other'
			END) AS CommDeliveryChannel
			, (CASE
				WHEN DSTRBTN_CD = 1  THEN MBR_ADDRESS1||','||MBR_ADDRESS2||','||MBR_CITY||','||MBR_STATE||','||MBR_ZIP
				WHEN DSTRBTN_CD = 2  THEN 'Fax'
				WHEN DSTRBTN_CD = 3  THEN MBR_PHONE
				WHEN DSTRBTN_CD = 4  THEN MBR_EMAIL
				WHEN DSTRBTN_CD = 6  THEN 'Secure Message' 
				WHEN DSTRBTN_CD = 15 THEN 'IVR'
				WHEN DSTRBTN_CD = 16 THEN 'Secure Message'
				WHEN DSTRBTN_CD = 17 THEN 'SMS'
				ELSE 'Other'
			END) AS CommContactInfo		
			, '' as CommSenderInfo
			, '' as CommSubject
			, DELIVERY_DATE  /** as CommDeliveryDate  **/
			, (CASE
				WHEN DSTRBTN_CD = 1 THEN 'Mailed' 
				ELSE 'Sent'
			END) AS CommDeliveryStatus	
			, '' as CommOutcome
			, '' as CommFirstAttempt
			, '' as CommFinalAttempt
			, ORACLE_ID as TemplateID
			, '' as CommDocName
			, 'N' as CommDocInd
			, '' as CltPlatformCode
			, '' as CltDivision
			, '' as CltMasterGrp
			, '' as CltPlanType
			, '' as CltGroup
			, '' as CltFamilyID
			, '' as CltDependentCode
			, '' as CltMemberDOB            

			from &c_s_table_name.  history			
			where EXECUTION_DATE <= current_date 
			and CLNT_CD in ('INGENIORX') 
			and DSTRBTN_CD = 1                                   /** <--------------------------------------------- print channel only **/
			and MCS_HISTORY_FILE is null
			and substring (EXECUTION_TRIAGE FROM 1 FOR 1) = '0'  /** <--------------------------------------------- eoms triage        = 0 = valid    1 = invalid**/
			and substring (EXECUTION_TRIAGE FROM 9 FOR 1) = '0'  /** <--------------------------------------------- mcs history triage = 0 = valid    1 = invalid**/
		);
		disconnect from tera;
		quit; 
		
		data comm_hist_temp;
		set comm_hist_temp;
		DELIVERY_DATE2=put(DELIVERY_DATE,yymmdd10.);
		CommDeliveryDate=left(trim(DELIVERY_DATE2))||' 10:50:34 AM';
		drop DELIVERY_DATE DELIVERY_DATE2;
		run;

		%let cnt_cmctn_history = 0;

		proc sql noprint;
		select count(*) into: cnt_cmctn_history separated by ''
		from comm_hist_temp;
		quit;

		%put NOTE: cnt_cmctn_history = &cnt_cmctn_history. ;


		%if &cnt_cmctn_history. ne 0 %then %do;  
		
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - clean the oracle ID
			|   
			+---------------------------------------------------------------------------------------------*/  		
			data npschars (drop = i);  
			length npschars $2;
			retain npschars;
			if _n_=1 then do; 
				do i=0, 160;
				  if i=0 then npschars=byte(i);
				  else npschars=trim(npschars)||byte(i);
				end; 
			end; 
			run; 

			data comm_hist_temp (drop = npschars oracleid);
			if _n_=1 then set npschars;
			set comm_hist_temp;
			oracle_id=compress(oracleid, npschars); 
			oracle_id=upcase(oracle_id);
			run;		

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create anthem mcs transaction data
			|   
			+---------------------------------------------------------------------------------------------*/  	
			data comm_hist_temp2;
			set  comm_hist_temp;
			run;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create and sftp anthem mcs transaction files
			|   
			+---------------------------------------------------------------------------------------------*/
			%m_create_anthem_mcs_eoms(comm_hist_temp2 );


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - collect metrics
			|   
			+---------------------------------------------------------------------------------------------*/		
			%m_table_drop(data_in=&c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id.);

			data &c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id. (bulkload=yes tenacity=2 sessions=4 dbcreate_table_opts='primary index(cs_gid)');
			set comm_hist_temp ;
			cmctn_history_file="&m_cmctn_history_file.";
			keep cs_gid cmctn_history_file;
			run;

			%m_table_statistics(data_in=&c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id., index_in=%str(cs_gid));

			proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  
					update history
					from &c_s_table_name.		 	      	      history, 
					     &c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id.  infile

					set mcs_history_file  = infile.cmctn_history_file,
					    mcs_history_file_metric = &m_cmctn_history_filecnt.
					where history.eoms_row_gid = infile.cs_gid  
				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
			quit; 

			%m_table_drop(data_in=&c_s_tdtempx..update_teradata_&c_s_dqi_campaign_id.);		

		  %end;
		  
		  %m_dqi_process_control(id=7, step=%str(DI - DATA INTGRATION MCS HISTORY COMPLETE)); 

	%end;	



	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - opportunity + disposition data - IT team
	|   
	+---------------------------------------------------------------------------------------------*/	
	%if &di_type. = 4  %then %do;  

		%m_dqi_process_control(id=8, step=%str(DI - DATA INTGRATION IT PBM BEGIN)); 
		%m_step6_eoms_output;
		%m_dqi_process_control(id=9, step=%str(DI - DATA INTGRATION IT PBM COMPLETE)); 
		
	%end;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - opportunity + disposition data - complete
	|   
	+---------------------------------------------------------------------------------------------*/
	%if &di_type. = 9 %then %do;   
	
		%m_dqi_process_control(id=10, step=%str(DI - DATA INTGRATION COMPLETE));
		%m_ecoa_process(do_tasks=DQI);
		  
	%end;		  

%mend cd_data_integration_eoms;

%macro main_process;
	
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - send out start email
		|   
		+---------------------------------------------------------------------------------------------*/
		%if &c_s_dqi_campaign_id. = 80 %then %do;
			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - STARTED PBM File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  
		%end; 	

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
			put "The CTT Data Profiling PBM file processing has started. ";
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
		|  STEP - process files 
		|   
		+---------------------------------------------------------------------------------------------*/	
		%cd_data_integration_eoms(di_type=0, rcrc=0, cs_label=Process Conrol);
		%cd_data_integration_eoms(di_type=1, rcrc=0, cs_label=CPL and SFMC Raw Data);
		%cd_data_integration_eoms(di_type=2, rcrc=1, cs_label=Enterprise CMCTN History - Member);
		%cd_data_integration_eoms(di_type=2, rcrc=2, cs_label=Enterprise CMCTN History - Prescriber);		
		%cd_data_integration_eoms(di_type=3, rcrc=0, cs_label=Enterprise MCS History);		
		%cd_data_integration_eoms(di_type=4, rcrc=1, cs_label=Enterprise PBM History);
		%cd_data_integration_eoms(di_type=9, rcrc=1, cs_label=Process Conrol);


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - determine processed files
		|   
		+---------------------------------------------------------------------------------------------*/
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table process_files  as 
		select * from connection to tera
		(				
			
			select  
			source_team, 
			source_file as process_file, 
			source_new_file, coalesce(cmctn_history_file, 'NA') as cmctn_history_file, 
			coalesce(mcs_history_file, 'NA') as mcs_history_file, 
			c as process_files_metric, 
			execution_sftp
			
			from &c_s_table_files. a, 
			(
				select coalesce(source_opp_file, 
				source_opprx_file) as f, 
				max(cmctn_history_file) cmctn_history_file, 
				max(mcs_history_file) mcs_history_file,
				max(coalesce(source_opp_file_metric, 
				source_opprx_file_metric)) as c 
				from &c_s_table_name.  
				where execution_date = current_date - &execution_date.
				group by 1					
			) b						
			where a.source_new_file=b.f
			and a.execution_date = current_date - &execution_date.
			order by source_team, source_file
		);
		disconnect from tera;
		quit; 	
			
		%let pf_total = 0;
		
		data _null_;
		set process_files end=eof;
		ii=left(put(_n_,4.)); 
		call symput('pf'||ii,trim(left(process_file)));
		call symput('cf'||ii,trim(left(cmctn_history_file)));
		call symput('mf'||ii,trim(left(mcs_history_file)));
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
			cmctn_history_file label = 'CMCTN History File', 
			mcs_history_file label = 'MCS History File',  
			execution_sftp label = 'Source File - Status',
			process_files_metric label = 'Source File - Counts'	 
		from process_files;
		quit;
		
		proc format;
		 value csadapt low-0='#ECEDEC '   
		               0<-high='#E5B8D0';
		run; 		

		filename report "%sysfunc(pathname(work))/report_email.html"; 
		
		title;
		footnote;
				
		ods html file=report;
			proc print data=report_email style(header)=[font_face='Arial Narrow'] style(table)={just=l bordercolor=blue} obs label; 
			var	source_team 
				process_file   
				source_new_file  
				cmctn_history_file   
				mcs_history_file    
				execution_sftp  ;	
			var 	process_files_metric / style={BACKGROUND=csadapt.  }; 
			run;
		ods html close;		


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - send out complete email 
		|   
		+---------------------------------------------------------------------------------------------*/	
		%if %symexist(c_s_log) %then %do; 
		  %m_qc_scan_log(infile=&c_s_maindir.&c_s_log., logcheck=ALL);
		%end; 
	
		%if &c_s_dqi_campaign_id. = 80 %then %do;
			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - COMPLETED PBM File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html"; 
		%end; 	 

		options noquotelenmax;

		data _null_;
		infile report;
		input;
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
				put "The CTT Data Profiling PBM file processing has completed. ";
				put '<br>';
				put '<b>';
				put '<br>';
				put '<b><font color="#13478C">'; put '<u>'; put "Targeting information: "; put '</b></font>'; put '</u>';
				put '</b>';
				put '<br>';	
				put "<ul>";
					put "<li> 	PBM Adapter + Profiling Framework"; 		put '</li>'; 
					put "<li> 	program: cd_data_integration_eoms.sas"; 	put '</li>'; 
					put "<li> 	campaign ID:  &c_s_dqi_campaign_id."; 		put '</li>'; 
					put "<li> 	user: &_clientusername. "; 			put '</li>';  
					put "<li> 	campaign directory:  &c_s_filedir."; 		put '</li>'; 
					put "<li> 	log file:  &c_s_log."; 				put '</li>'; 
				put "</ul>";
				put "<br>";			


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email body 6 - custom logic
				+---------------------------------------------------------------------------------------------*/	
				%if %symexist(c_s_log)  %then %do;
					put '<br>';
					put '<b><font color="#13478C">';put '<u>'; put "Log Scan information:"; put '</b></font>';put '</u>'; put '<br>'; 
					put "<ul>";
						put "<li>     Number of errors within the log file: &toterr. ERRORS FOUND"; put '</li>'; 
						put "<li>     Number of warnings within the log file: &totwarn. WARNINGS FOUND"; put '</li>'; 
						put "<li>     Number of uninitialized variables within the log file: &totunin. UNINITIALIZED VARIABLES FOUND"; put '</li>'; 
						put "<li>     Number of teradata commit work failed issues within the log file: &tottera. COMMIT FAILED FOUND"; put '</li>'; 

						%if &toterr. ne NO %then %do;
							put "<li>     Message of last error detected within the log file: &syserrortext."; put '</li>';
						%end;
						%if &totwarn. ne NO %then %do;
							put "<li>     Message of last warning detected within the log file: &totwarn2.  "; put '</li>';
						%end;						
					put "</ul>";
					put "<br>";
				%end; 					
			
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
			else do; 
				put "</ul>";
				put "<br>";	
				put "Thank you and have a great week."; put '<br>';
				put '<br>';
				put "Sincerely,"; put '<br>';
				put "EA - Campaign Targeting Team"; put '<br>';
				put " "; put '<br>';		
			end;	
			
		run;		

		
		data working.qc_step7b;
		x=1;
		run;		
	
	
%mend main_process;
%main_process;



