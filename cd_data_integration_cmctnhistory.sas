
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    cd_data_integration_cmctnhistory
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

/* SASDOC --------------------------------------------------------------------------------------
|  STEP - campaign environment
+---------------------------------------------------------------------------------------------*/
%m_environment;

%macro cd_data_integration_cmctnhistory(di_type=, rcrc=, cs_label=, pharmacy_fax_flag=N);


 	/* SASDOC --------------------------------------------------------------------------------------
 	|  STEP - parameters
 	|   
	+---------------------------------------------------------------------------------------------*/
	options nosymbolgen nomlogic;

	%global m_cmctn_history_file m_cmctn_history_filecnt bad_ph_fax cnt_files_opp cnt_files_disp 
		myfilerf eoms_report eoms_IT get_sftp_directory put_linux_directory eoms_directory
		c_s_table_files c_s_table_rules c_s_table_opp c_s_table_disp c_s_table_oppdnorm c_s_table_name SYS_DIR;

	%let m_cmctn_history_file	= no_files.txt;
	%let m_cmctn_history_filecnt	= 0; 
	%let bad_ph_fax      		= %str('1111111111','2222222222','3333333333','4444444444','5555555555','6666666666','7777777777','8888888888','9999999999');

	
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
		%let get_etl_directory	= %str(/anr_dep/communication_history/eoms_IT);	
		%let put_etl_directory  = %str(/appl/edw/incoming/campgn);
		%let put_etl_directory  = %str(/ant591/IVR/edw);
		%let eoms_consolidate   = %str(/anr_dep/communication_history/eoms_files_consolidate);
	%end;
	%else %do;
		%let c_s_table_oppdnorm = %str(dss_cea2.dqi_eoms_opportunity_denorm);
		%let c_s_table_files    = %str(dss_cea2.dqi_eoms_files_test);
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
		%let get_etl_directory	= %str(/anr_dep/communication_history/eoms_IT_test);	
		%let put_etl_directory  = %str(/appl/edw/incoming/campgn);
		%let put_etl_directory  = %str(/ant591/IVR/edw); 
		%let eoms_consolidate   = %str(/anr_dep/communication_history/eoms_files_consolidate_test);
	%end;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - opportunity + disposition data - begin
	|   
	+---------------------------------------------------------------------------------------------*/ 
	%m_dqi_process_control(id=1, step=%str(DI - DATA INTGRATION BEGIN)); 	
		


 	/* SASDOC --------------------------------------------------------------------------------------
 	|  STEP - opportunity + disposition data - CMCTN History team
 	|   
	+---------------------------------------------------------------------------------------------*/
	%if &di_type. = 2 and &c_s_cmctn_send. = Y %then %do;  /**<----------------------------------------------- cmcntn history **/ 
	
	
		%m_dqi_process_control(id=4, step=%str(DI - DATA INTGRATION CMCTN HISTORY BEGIN));	
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - update level 1 disposition based on disposition date
		|
		|         HEE does not send to CMCTN History if the letter or email does not get a disposition of the L1 of 1.
		+---------------------------------------------------------------------------------------------*/
		%if &c_s_level1_disposition. = Y %then %do;

			proc sql;
			connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
			execute(  

				create multiset table dss_cea2.dqi_eoms_tera_&mbr_process_id. as (

					select top 800000 oppty_altid as oppty_evnt_id, oppty_msg_altid as msg_evnt_id, dispn_cd, rec_add_ts
					from &c_s_schema..v_oppty_msg
					where dispn_cd = '1'
					and oppty_altid in (
						select oppty_evnt_id 
						from &c_s_table_name. 
						where cmctn_history_file = 'NA'
						and execution_date < current_date
						and msg_dispn_cd <> 1                                
						and substring (execution_triage FROM 1 FOR 1) = '0'   
						and substring (execution_triage FROM 7 FOR 1) = '0'   						
					)  

				) with data 

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 

			%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., column_in=oppty_evnt_id);
			%m_table_statistics(data_in=dss_cea2.dqi_eoms_tera_&mbr_process_id., column_in=msg_evnt_id);


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

			) by tera;
			execute (commit work) by tera;  
			disconnect from tera;
			quit; 


			%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_eoms_tera_&mbr_process_id.);

		%end;			

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

			from &c_s_table_name.  history			
			where execution_date <= current_date
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
			proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  
					update &c_s_table_name.		 	      	      
					set cmctn_history_file        = %tslit(&m_cmctn_history_file.) ,
					    cmctn_history_file_metric = &m_cmctn_history_filecnt.
					where cmctn_history_file = 'X' 
					and MSG_DISPN_CD = 1
				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
			quit;		

		%end;

		%m_dqi_process_control(id=5, step=%str(DI - DATA INTGRATION CMCTN HISTORY COMPLETE));

	%end;
 

	%m_dqi_process_control(id=10, step=%str(DI - DATA INTGRATION COMPLETE)); 		  

%mend cd_data_integration_cmctnhistory; 
	
%cd_data_integration_cmctnhistory(di_type=2, rcrc=1, cs_label=Enterprise CMCTN History - Member);  



