
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    cd_data_integration_stars
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
| HISTORY:  201220901 - Clinical Stars Team - Original (version 2022.09.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/


/* SASDOC --------------------------------------------------------------------------------------
|  STEP - campaign environment
+---------------------------------------------------------------------------------------------*/
%m_environment;


/* SASDOC --------------------------------------------------------------------------------------
|  STEP - ecoa directory for ecoa templates + output for stars opportunity + disposition 
+---------------------------------------------------------------------------------------------*/
%let dir_in        = %str(/anr_dep/communication_history/eoms_files);
%let dir_out       = %str(/anr_dep/communication_history/stars_files);
%let template_opp  = %str(cpl_opportunity.20220618000123);
%let template_disp = %str(cpl_disposition.20220714000123);

%let cnt_stars_opps  = 0;
%let cnt_stars_disps = 0;
%let opp_cnt         = 0;
%let disp_cnt        = 0;
%let SYS_DIR 	     = %str(/);


%macro cd_data_integration_stars;

	options nomlogic nosymbolgen;
	
	%if &c_s_dqi_campaign_id. = 86 %then %do;
	
		filename xemail email 
		to=(&c_s_email_to.)
		subject="CTT Data Profiling - STARTED STARS ECOA File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  
		
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
			put "The CTT Data Profiling STARS file processing has started. ";
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

	%m_dqi_process_control(id=1, step=%str(STARS - DATA INTGRATION BEGIN));
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - clean up PROD SFTP server DQI files - EA PROD
	+---------------------------------------------------------------------------------------------*/	
	%let get_sftp_directory	= %str(/ant591/IVR/incoming);				/** <--------------------- location of SFTP raw data files **/
	%let put_linux_directory= %str(/anr_dep/communication_history/eoms_files);      /** <--------------------- location of LINUX raw data files **/

	data _null_;
	file "&put_linux_directory./put.txt";
	put "cd &get_sftp_directory."; 
	put "rm &get_sftp_directory./&sftp_get_file2.";
	put "exit";
	run; 

	data _null_;
	rc = system("cd &put_linux_directory."); 
	rc = system("sftp &sftp_eoms_id.@webtransport.caremark.com < &put_linux_directory./put.txt"); 
	if   _error_=0  then do;
	call symput('file_sftp_flg','1');
	end;
	else  call symput('file_sftp','0');
	run;	
	
	data _null_;
	x "rm &dir_out./&c_s_file_date_linux.";
	run;	


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get stars opportunity + disposition data 
	|
	+---------------------------------------------------------------------------------------------*/
	
	%let c_s_table_prodwc=sb_adhnc.t_unlock_wc_ltf_otf_2022;			  /**  CAMPAIGN 5  - WELLTOK  - WELLCARE LATE-TO-FILL (LTF) + 
											       CAMPAIGN 6  - CARENET  - WELLCARE REFILL REMINDER (RR)  **/
	%let c_s_table_prodadhloc=sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022_loc;		  /**  CAMPAIGN 7  - CONDUENT - WELLCARE 30-90 FAX  **/
	%let c_s_table_prodadh=sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022;			  /**  CAMPAIGN 7  - WELLTOK  - WELLCARE 30-90 FAX   **/		
	%let c_s_table_prodsupd=sb_adhnc.supd_fax_hix_tgt_data_2022;			  /**  CAMPAIGN 8  - WELLTOK  - WELLCARE SUPD   **/
	%let c_s_table_prodsupdloc=sb_adhnc.supd_fax_hix_tgt_data_2022_loc;               /**  CAMPAIGN 8  - CONDUENT - WELLCARE SUPD  **/ 	
	%let c_s_table_prodadhpharm=sb_adhnc.t_unlock_adh_pharm_mbr_tgt_hist_2022;        /**  CAMPAIGN 12 - WELLTOK  - WELLCARE PHARMACY 30-90 FAX  **/
	%let c_s_table_prodcnc=sb_adhnc.t_unlock_cnc_ltf_otf_2022;                        /**  CAMPAIGN 13 - WELLTOK  - CENTENE LATE-TO-FILL (LTF) + 
											       CAMPAIGN 14 - CARENET  - CENTENE REFILL REMINDER (RR) **/
	%let c_s_table_prodadhcncloc=sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022_loc;	  /**  CAMPAIGN 15 - CONDUENT - CENTENE 30-90 FAX  **/													       
	%let c_s_table_prodadhcnc=sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022;             /**  CAMPAIGN 15 - WELLTOK  - CENTENE 30-90 FAX   **/ 
	%let c_s_table_prodcncsupd=sb_adhnc.cnc_supd_fax_hix_tgt_data_2022;               /**  CAMPAIGN 16 - WELLTOK  - CENTENE SUPD   **/
	%let c_s_table_prodcncsupdloc=sb_adhnc.cnc_supd_fax_hix_tgt_data_2022_loc;        /**  CAMPAIGN 16 - CONDUENT - CENTENE SUPD  **/
	%let c_s_table_prodadhcncpharm=sb_adhnc.t_unlock_adhcnc_pharm_mbr_tgt_hist_2022;  /**  CAMPAIGN 17 - WELLTOK  - CENTENE PHARMACY 30-90 FAX  **/
	%let c_s_table_prodadh=sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022;			  /**  CAMPAIGN 18 - WELLTOK  - WELLCARE LATE-TO-FILL (LTF) PHARMACY FAX **/
	%let c_s_table_prodadhcncloc=sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022_loc;	  /**  CAMPAIGN 19 - WELLTOK  - CENTENE LATE-TO-FILL (LTF) PHARMACY FAX  **/
	%let c_s_table_prodwc=sb_adhnc.t_unlock_wc_ltf_otf_2022;			  /**  CAMPAIGN 20 - CARENET  - WELLCARE FIRST FILL (FF) + 
											       CAMPAIGN 22 - WELLTOK  - WELLCARE FIRST FILL RR (FF) **/		  
	%let c_s_table_prodcnc=sb_adhnc.t_unlock_cnc_ltf_otf_2022;                        /**  CAMPAIGN 21 - CARENET  - WELLCARE FIRST FILL (FF) + 
											       CAMPAIGN 23 - WELLTOK  - WELLCARE FIRST FILL RR (FF) **/
	%let c_s_table_prodcnc=sb_adhnc.t_unlock_wc_ltf_otf_2022;                         /**  CAMPAIGN 27 - CARENET  - WELLCARE AT RISK RETAINED (ARR) **/
	%let c_s_table_prodcnc=sb_adhnc.t_unlock_cnc_ltf_otf_2022;                        /**  CAMPAIGN 28 - CARENET  - CENTENE AT RISK RETAINED (ARR)  **/


	%macro get_stars_opportunity(audience=);


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - campaign stars - members
		|
		|	1. smart_id      = campaign ID + request ID + opp ID (opp ID = carenet or mbr_acct_gid = welltok) + 0
		|	2. smart_id disp = campaign ID + request ID + opp ID (opp ID = carenet or mbr_acct_gid = welltok) + message ID
		|
		+---------------------------------------------------------------------------------------------*/
		%if &audience = PTNT %then %do;

			proc sql;
			connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
			create table _stars_members  as  
			select * from connection to tera
			(
				 select top 200000 *
				 from (	/**----------- step 3 - get ecoa data elements -----------**/ 
					select distinct  
					'STARS' as program_sub_id,
					'PTNT'  as audience_sub_id,
					a.cs_campaign_id as dqi_campaign_id,
					trim(extract(year from execution_date) (format '9999'))||trim(extract(month from execution_date) (format '99'))||trim(extract(day from execution_date) (format '99'))||trim(a.cs_campaign_id) as ticket,				
					a.oppt_event_id,					
					cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||'0' as varchar(100)) as smart_id,
					cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||trim(strtok(oracle_id,'-',2)) as varchar(100))  as smart_id_disp,					
					substr(oracle_id, 1, POSITION('-' IN oracle_id)-1) as program_id,
					substr(oracle_id,    POSITION('-' IN oracle_id)+1) as apn_cmctn_id, 
					execution_date as effective_dt,
					execution_date as release_dt,
					
 					mbr_id                           as mbr_acct_id,
 					trim(beneficiary_id)             as ql_bnfcy_id,
					mbr_gid                          as mbr_acct_gid,
					carrier                          as clnt_lvl1, 
					acct                             as clnt_lvl2,
					grp                              as clnt_lvl3,	
					first_name                       as mbr_name_first, 
					last_name                        as mbr_name_last,
					address_1                        as mbr_address1,
					address_2                        as mbr_address2,
					city                             as mbr_city,
					state                            as mbr_state,
					zip_code                         as mbr_zip,
					phone_number                     as mbr_phone,
					
					channel                          as stars_channel,
					upper(adh_pgm_vendor)            as stars_vendor,
					upper(b.cs_campaign_description) as dqi_product_category
					
					from 
					(       /**----------- step 1 - get stars carenet + welltok opportunity -----------**/ 
						select  
						cast(trim(to_char(coalesce(oppt_event_id,mbr_gid))) as varchar(100)) as oppt_event_id,
						execution_date,oracle_id, cs_campaign_id, carrier_mbr_tbl as carrier,account_code as acct,group_code as grp,mbr_id,beneficiary_id,mbr_gid, 
						channel, first_name,last_name, address_1, address_2, city, state, zip_code, phone_number, min(adh_pgm_vendor) as adh_pgm_vendor
						from sb_adhnc.t_unlock_wc_ltf_otf_2022 /** &c_s_table_prodwc. **/     
						where cs_campaign_id in (5,6,20,22,27)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
						
						union
						
						select 
						cast(trim(to_char(coalesce(oppt_event_id,mbr_gid))) as varchar(100)) as oppt_event_id,
						execution_date,oracle_id, cs_campaign_id, carrier_mbr_tbl as carrier,account_code as acct,group_code as grp,mbr_id,beneficiary_id,mbr_gid, 
						channel, first_name,last_name, address_1, address_2, city, state, zip_code, phone_number, min(adh_pgm_vendor) as adh_pgm_vendor
						from sb_adhnc.t_unlock_cnc_ltf_otf_2022 /** &c_s_table_prodcnc. **/  
						where cs_campaign_id in (13,14,21,23,28)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 
						
					) a  left join	
					
					dss_cea2.cs_campaigns b on a.cs_campaign_id = b.cs_campaign_id left join

					(       /**----------- step 2 - get ecoa opportunity -----------**/ 
						select distinct extnl_sys_oppty_id
						from dss_cea2.dqi_eoms_opportunity    
						where substr(execution_triage,1,1) = '0'
						and oppty_evnt_id is not null
						and extnl_sys_oppty_id is not null
						and source_team = 'DQI'
						and alt_oppty_subtyp_src_cd = 'STARS'

					) o on smart_id = o.extnl_sys_oppty_id  

					where execution_date <= current_date
					and execution_date >= cast(extract(year from current_date - 0)||'0101' as date format 'yyyymmdd')  	 			
					and a.mbr_gid is not null 
					and o.extnl_sys_oppty_id is null              /** <-------------------------- exclusion = ecoa opportunity already processed **/ 

				) x
				
				order by release_dt 
				
			);
			disconnect from tera;
			quit; 

		%end;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - campaign stars - prescribers
		|
		|	1. smart_id      = campaign ID + request ID + opp ID (claim GID) + 0
		|	2. smart_id disp = campaign ID + request ID + opp ID (claim GID) + message ID
		|
		+---------------------------------------------------------------------------------------------*/		
		%if &audience = PRSC %then %do;

			proc sql;
			connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
			create table _stars_prescribers as 
			select * from connection to tera
			(
				 select top 200000 *
				 from (	/**----------- step 4 - get ecoa data elements -----------**/ 
					select distinct  
					'STARS' as program_sub_id,
					'PRSC' as audience_sub_id,
					a.cs_campaign_id as dqi_campaign_id,
					trim(extract(year from execution_date) (format '9999'))||trim(extract(month from execution_date) (format '99'))||trim(extract(day from execution_date) (format '99'))||trim(a.cs_campaign_id) as ticket,				
					a.oppt_event_id,
					cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||'0' as varchar(100))  as smart_id,
					cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||trim(strtok(oracle_id,'-',2)) as varchar(100))  as smart_id_disp,
					substr(oracle_id, 1, POSITION('-' IN oracle_id)-1) AS program_id,
					substr(oracle_id,    POSITION('-' IN oracle_id)+1) AS apn_cmctn_id, 
					execution_date as effective_dt,
					execution_date as release_dt,
					
 					mbr_id                                as mbr_acct_id,
 					trim(beneficiary_id)                  as ql_bnfcy_id,
					mbr_gid                               as mbr_acct_gid,
					carrier                               as clnt_lvl1, 
					acct                                  as clnt_lvl2,
					grp                                   as clnt_lvl3,	
					first_name                            as mbr_name_first, 
					last_name                             as mbr_name_last,
					address_1                             as mbr_address1,
					address_2                             as mbr_address2,
					city                                  as mbr_city,
					state                                 as mbr_state,
					zip_code                              as mbr_zip, 
					cast(oreplace(a.phone_number,'-','')  as varchar(20) ) as mbr_phone,
					
					a.prsc_npi                            as prsc_npi,   
					trim(to_char(a.prsc_ql))              as prsc_ql,
					trim(to_char(a.prsc_gid))             as prsc_gid,
					a.prsc_dea                            as prsc_dea, 
					a.first_namep                         as prsc_name_first, 
					a.last_namep                          as prsc_name_last,
					a.address_1p                          as prsc_address1,
					a.address_2p                          as prsc_address2,
					a.cityp                               as prsc_city,
					a.statep                              as prsc_state,
					a.zip_codep                           as prsc_zip,
					cast(oreplace(a.phone_numberp,'-','') as varchar(20) ) as prsc_phone,
					cast(oreplace(a.fax_numberp  ,'-','') as varchar(20) ) as prsc_fax,
					
					channel                               as stars_channel,
					upper(adh_pgm_vendor)                 as stars_vendor, 
					upper(b.cs_campaign_description)      as dqi_product_category 
 
					from
					(       /**----------- step 1 - get stars welltok opportunity -----------**/ 						
						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, pilot_group as channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022 /**&c_s_table_prodadh.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid  			
						where a.cs_campaign_id in (7,18)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
						
						union

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.supd_fax_hix_tgt_data_2022 /**&c_s_table_prodsupdloc.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid   			
						where a.cs_campaign_id in (8)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33						
						
						union

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, pilot_group as channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.t_unlock_adh_pharm_mbr_tgt_hist_2022 /**&c_s_table_prodadhpharm.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid   			
						where a.cs_campaign_id in (12)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33

						union				

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, pilot_group as channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022 /**&c_s_table_prodadhcnc.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid  			
						where a.cs_campaign_id in (15,19)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33

						union 

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.cnc_supd_fax_hix_tgt_data_2022 /**&c_s_table_prodcncsupdloc.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid  				
						where a.cs_campaign_id in (16)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33	
						
						union

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, pilot_group as channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.t_unlock_adhcnc_pharm_mbr_tgt_hist_2022 /**&c_s_table_prodadhcncpharm.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid  			
						where a.cs_campaign_id in (17)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
						
						union
						
						
						/**----------- step 2 - get stars conduent opportunity -----------**/ 						
						select  
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id,
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, pilot_group as channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022_loc /**&c_s_table_prodadhloc.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid   			
						where a.cs_campaign_id in (7)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
						
						union

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.supd_fax_hix_tgt_data_2022_loc /**&c_s_table_prodsupdloc.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid   			
						where a.cs_campaign_id in (8)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33	
						
						union

						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, pilot_group as channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022_loc /**&c_s_table_prodadhcncloc.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid  			
						where a.cs_campaign_id in (15)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33 
						
						union
						
						select 
						cast(to_char(claim_gid) as varchar(100))  as oppt_event_id, 
						execution_date, oracleid as oracle_id, a.cs_campaign_id, extnl_lvl_id1 as carrier,client_level_2 as acct,client_level_3 as grp,mbr_id, 
						ql_bnfcy_id as beneficiary_id,mbr_gid, channel, adh_pgm_vendor, 
						namefirst as first_name, namelast as last_name, recipientaddress as address_1, recipientaddress2 address_2, recipientcity as city, recipientstate as state, zip as zip_code, recipientphone as phone_number,
						a.providerid as prsc_npi,a.prctr_gid as prsc_gid,pr.dea_id as prsc_dea, pr.ql_prscbr_id as prsc_ql,
						providerfirstname as first_namep, providerlastname as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
						a.providerphone as phone_numberp, a.providerfax as fax_numberp
						from sb_adhnc.cnc_supd_fax_hix_tgt_data_2022_loc /**&c_s_table_prodcncsupd.**/ a left join 
						dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid left join
						dwu_edw_rb.v_phmcy_denorm ph on a.phmcy_gid = ph.phmcy_pty_gid  			
						where a.cs_campaign_id in (16)
						group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33						

					) a left join

					dss_cea2.cs_campaigns b on a.cs_campaign_id = b.cs_campaign_id left join

					(       /**----------- step 3 - get ecoa opportunity -----------**/ 
						select distinct extnl_sys_oppty_id
						from dss_cea2.dqi_eoms_opportunity    
						where substr(execution_triage,1,1) = '0'
						and oppty_evnt_id is not null
						and extnl_sys_oppty_id is not null
						and source_team = 'DQI'
						and alt_oppty_subtyp_src_cd = 'STARS'

					) o on smart_id = o.extnl_sys_oppty_id  

					where execution_date <= current_date
					and execution_date >= cast(extract(year from current_date - 0)||'0101' as date format 'yyyymmdd')  	 			
					and a.mbr_gid is not null 
					and a.cs_campaign_id > 0 
					and o.extnl_sys_oppty_id is null              /** <-------------------------- exclusion = ecoa opportunity already processed **/ 
							
				) x
				
				order by release_dt 
				
			);
			disconnect from tera;
			quit;  

		%end;
 
 
	%mend get_stars_opportunity;
	
	%get_stars_opportunity(audience=PRSC); 	
	%get_stars_opportunity(audience=PTNT); 
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - consolidate members + prescribers 
	|
	+---------------------------------------------------------------------------------------------*/	
	data _stars;
	set _stars_members _stars_prescribers;  
	run;	
	
	proc sql noprint;
	create table qc_009 as 
	select program_sub_id, dqi_campaign_id, dqi_product_category, stars_vendor, count(*) as cnt
	from _stars
	group by 1,2,3,4;
	quit;

	data _null_;
	set qc_009;
	put @1 _n_ @5 program_sub_id @15 dqi_campaign_id @15 dqi_product_category @70 stars_vendor @80 cnt;
	run;
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc to validate one member x ticket - all counts should = 1
	|
	+---------------------------------------------------------------------------------------------*/	
	%let cntopp = 0;
	
	proc sql noprint;
	create table duplicate_recs as 
	select ticket, oppt_event_id, apn_cmctn_id, count(distinct dqi_campaign_id) as cnt
	from _stars
	group 1,2,3 
	having cnt > 1;
	quit; 

	%let dsid=%sysfunc(open(duplicate_recs));
	%let cntopp=%sysfunc(attrn(&dsid,nlobs));
	%let rc=%sysfunc(close(&dsid));
	
	%put NOTE: cntopp = &cntopp. ;

	%if &cntopp > 0 %then %do;

		proc export data=work.duplicate_recs
		dbms=xlsx 
		outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_opporunity_DQI.xlsx" 
		replace; 
		sheet="validate_duplicates"; 
		run;

		%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_opporunity_DQI.xlsx),
				 abend_message=%str(There is an issue with the creation of the opporunity DQI data - There are duplicate IDs));			

	%end;	
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc to validate the smart ID is unique for each opportunity
	|
	+---------------------------------------------------------------------------------------------*/	
	%let cntopp = 0;

	proc sort data=_stars dupout=duplicate_recs nodupkey;
	by smart_id smart_id_disp;
	run;					
					
	%let dsid=%sysfunc(open(duplicate_recs));
	%let cntopp=%sysfunc(attrn(&dsid,nlobs));
	%let rc=%sysfunc(close(&dsid));
	
	%put NOTE: cntopp = &cntopp. ;

	%if &cntopp > 0 %then %do;

		proc export data=work.duplicate_recs
		dbms=xlsx 
		outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_opporunity_STARS.xlsx" 
		replace; 
		sheet="validate_duplicates"; 
		run;

		%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_opporunity_STARS.xlsx),
				 abend_message=%str(There is an issue with the creation of the opporunity STARS data - There are duplicate IDs));			

	%end;
	 
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - deterimine if there are any opportunity to process
	|
	+---------------------------------------------------------------------------------------------*/
	%let cnt_stars_opps = 0;

	proc sql noprint;
	select count(*) into: cnt_stars_opps separated by ''
	from _stars;
	quit;

	%put NOTE: cnt_stars_opps = &cnt_stars_opps. ; 
	

	%if &cnt_stars_opps ne 0 %then %do;  /**<------------------------------- start - create opportunity  **/
	
	
		%m_dqi_process_control(id=4, step=%str(STARS - DATA INTGRATION CMCTN HISTORY BEGIN));


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get max ID for oppty_evnt_id + msg_evnt_id creation for DQI or STARS 
		|
		+---------------------------------------------------------------------------------------------*/
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			delete &c_s_tdtempx..dqi_eoms_queue
			where execution_source = 'STARS'
			and execution_date = current_date

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;
		
		
		proc sql; 
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		create table _ecoa as
		select * from connection to tera(

			select max(max_id) as max_id
			from (
				select max(oppty_evnt_id) as max_id
				from dss_cea2.dqi_eoms_opportunity 
				where oppty_src_cd = 'DQI'
				union
				select max(msg_evnt_id) as max_id
				from dss_cea2.dqi_eoms_opportunity 
				where oppty_src_cd = 'DQI'
				union				
				select  max(oppty_evnt_id) as max_id
				from dss_cea2.dqi_eoms_dispositions 
				where oppty_src_cd = 'DQI'				
				union				
				select  max(msg_evnt_id) as max_id
				from dss_cea2.dqi_eoms_dispositions 
				where oppty_src_cd = 'DQI'
				union
				select max(oppty_evnt_id) as max_id 			
				from dss_cea2.dqi_eoms_queue	
				where execution_date = current_date
				union
				select max(msg_evnt_id) as max_id 			
				from dss_cea2.dqi_eoms_queue
				where execution_date = current_date
			) x	
		) ;
		quit;	

		data x;
		set _ecoa;
		call symput('max_id',compress(put(max_id,20.))); 
		run;

		%put NOTE: max_id = &max_id;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get opportunity template - 630 variables
		|
		+---------------------------------------------------------------------------------------------*/
		proc sql noprint;
		create table variables_opp as 
		select * 
		from &c_s_tdtempx..dqi_eoms_layout 
		where layout_id = 1 
		order by layout_id, sequence_id;
		quit;

		data _null_;
		set variables_opp;
		n=trim(left(_n_));
		call symput('name_var'||n,trim(left(data_attribute_name)));
		call symput('name_var_total',n);
		run;

		data template_opp;
		length %do k=1 %to &name_var_total; &&name_var&k %end; $100. ;
		infile "&dir_in./&template_opp." firstobs=1 missover pad lrecl=32000 delimiter='|' dsd;
		input %do j=1 %to &name_var_total; &&name_var&j %end; ; 
		run;

		data template_opp;
		set template_opp (obs=1);
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - qc smart IDs
		|
		+---------------------------------------------------------------------------------------------*/		
		data qc001;
		set _stars;
		wc=count(smart_id,'.'); /** <-------------------------------------- should always be 3 = smart ID = campaign ID + request ID + opp ID + disp ID  **/
		run;			 
		
		proc sort data = qc001 nodupkey;
		by wc;
		run;
		
		%let cntopp = 0;

		proc sql noprint;
		select count(*) into: cntopp separated by ''
		from qc001;
		quit;
		
		%put NOTE: cntopp = &cntopp. ;

		%if &cntopp. ne 1 %then %do;

			proc export data=work.qc001
			dbms=xlsx 
			outfile="&c_s_datadir.&SYS_DIR.validate_Smart_IDS_STARS.xlsx" 
			replace; 
			sheet="validate_duplicates"; 
			run;

			%m_abend_handler(abend_report=%str(&c_s_datadir./validate_Smart_IDS_STARS.xlsx),
					 abend_message=%str(There is an issue with the creation of the smart ID in the STARS data - There are invalid IDs));			

		%end;		


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - mapping of the ecoa stars opportunity file = 30 data elements
		|
		|  NOTE - unique ID for traceability tracking = smart ID
		|
		+---------------------------------------------------------------------------------------------*/
		data stars_opporunity;
		if _n_ = 1 then set template_opp;
		set _stars;

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of opportunity
			|
			+---------------------------------------------------------------------------------------------*/
			OPPTY_EVNT_ID=left(put(&max_id+_n_,20.));       /**<--------------------------------- Opp ID for DQI 9000000000000000+     **/ 
			OPPTY_SRC_CD='DQI';                             /**<--------------------------------- DQI Team Source                      **/  
			EXTNL_SYS_OPPTY_ID=smart_id;                    /**<--------------------------------- smart ID                             **/
			PGM_ID=left(put(program_id,20.));               /**<--------------------------------- Stellent Prefix                      **/
			MSG_PRDCT_ALT_ID=apn_cmctn_id;                  /**<--------------------------------- Stellent Suffix                      **/
			OPPTY_ACTION_IND='A';                           /**<--------------------------------- A - add/create as a new opportunity  **/
			OPPTY_STUS_CD='3';                              /**<--------------------------------- 3 - Open                             **/
			ALT_OPPTY_SUBTYP_SRC_CD=program_sub_id;         /**<--------------------------------- level 1 - Team ID                    **/
			OPPTY_CMPGN_ID=left(put(dqi_campaign_id,20.));  /**<--------------------------------- level 2 - Campaign ID                **/
			PGM_TYPE_CD=left(trim(ticket));                 /**<--------------------------------- level 3 - Request ID                 **/
			ALT_OPPTY_SUBTYP_CD=oppt_event_id;              /**<--------------------------------- level 4 - Opportunity ID             **/
			KEY_CODE='STARS'||left(trim(ticket));           /**<--------------------------------- Key Code for request                 **/
			MSG_EVNT_ID='';                                 /**<--------------------------------- assignment in subsequent step below  **/
			MSG_EFF_DT=put(effective_dt,yymmdd10.);         /**<--------------------------------- message effective                    **/
			MSG_EXPRN_DT=MSG_EFF_DT;                        /**<--------------------------------- message expiration                   **/
			MSG_STUS_CD='3';                                /**<--------------------------------- 3 - Open                             **/ 
			MSG_STUS_RSN_CD='1';                            /**<--------------------------------- 1 - New Opportunity                  **/
			EVNT_PTY_ROLE_CD=audience_sub_id;               /**<--------------------------------- PTNT - member PRSC = prescriber      **/ 
			OPPTY_CMPGN_REQUEST_ID=ticket;                  /**<--------------------------------- DQI request ID                       **/
			COMM_GENERATE_DT=put(effective_dt,yymmdd10.);   /**<--------------------------------- communication date                   **/
			PRSN_SRC_CD='D';                                /**<--------------------------------- D - RxClaim                          **/
			EXTNL_IND='N';                                  /**<--------------------------------- N - internal to CVSH                 **/
			EPH_LINK_ID='';                                 /**<--------------------------------- assigment in m_step3_eoms_transform  **/
			PRSN_SSK_1=clnt_lvl1;                           /**<--------------------------------- carrier ID                           **/
			PRSN_SSK_2=clnt_lvl2;                           /**<--------------------------------- account ID                           **/
			PRSN_SSK_3=clnt_lvl3;                           /**<--------------------------------- group ID                             **/
			PRSN_SSK_4=mbr_acct_id;                         /**<--------------------------------- member acccount ID                   **/
			PRSN_SSK_5='';                                  /**<--------------------------------- NA                                   **/
			PRTC_ID=ql_bnfcy_id;                            /**<--------------------------------- assigment in m_step3_eoms_transform  **/
			EDW_MBR_GID=left(put(mbr_acct_gid,20.));        /**<--------------------------------- member account GID                   **/
			CLT_TYP='PBM';                                  /**<--------------------------------- PBM - pharmacy benefit management    **/
			
			OPTY_PASS_THRU_FLR1_VAL=smart_id_disp;          /**<--------------------------------- smart ID disposition                 **/
			OPTY_PASS_THRU_FLR2_VAL=dqi_product_category;   /**<--------------------------------- DQI product                          **/
			OPTY_PASS_THRU_FLR3_VAL='';			/**<--------------------------------- Aprimo product                       **/
			OPTY_PASS_THRU_FLR4_VAL='';			/**<--------------------------------- Aprimo sub-product                   **/
			OPTY_PASS_THRU_FLR5_VAL='';			/**<--------------------------------- VT consolidated product              **/
			OPTY_PASS_THRU_FLR6_VAL='';			/**<--------------------------------- VT consolidated sub-product          **/
			OPTY_PASS_THRU_FLR7_VAL='';
			OPTY_PASS_THRU_FLR8_VAL='';
			OPTY_PASS_THRU_FLR9_VAL=''; 

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of clients - set as null and ecoa will populate
			|
			+---------------------------------------------------------------------------------------------*/			
			CLT_NM='';
			CLT_LEVEL_1='';
			CLT_LEVEL_2='';
			CLT_LEVEL_3=''; 


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of members
			|
			+---------------------------------------------------------------------------------------------*/			
			PTNT_FIRST_NM=mbr_name_first;
			PTNT_MIDDLE_NM='';
			PTNT_LAST_NM=mbr_name_last;
			PTNT_BRTH_DT='';
			PTNT_GNDR_CD='';
			PTNT_ADDR_LINE1_TX=mbr_address1;
			PTNT_ADDR_LINE2_TX=mbr_address2;
			PTNT_CITY_TX=mbr_city;
			PTNT_STATE_CD=mbr_state;
			PTNT_ZIP_CD=mbr_zip;
			PTNT_PHONE1_NBR=mbr_phone;	
			PTNT_EMAIL_ADDR_TX='';
			PTNT_EMAIL_TYP_CD='';
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of presribers
			|
			+---------------------------------------------------------------------------------------------*/
			if audience_sub_id = 'PRSC' then do;
			
				PRSC_ID=prsc_gid;
				PRSC_NPI_ID=prsc_npi;
				PRSC_DEA_NBR=prsc_dea;
				PRSC_FIRST_NM=prsc_name_first;
				PRSC_MIDDLE_NM='';
				PRSC_LAST_NM=prsc_name_last;
				PRSC_DOMESTIC_ADDR_IND='Y';
				PRSC_ADDR_LINE1_TX=prsc_address1;
				PRSC_ADDR_LINE2_TX=prsc_address2;
				PRSC_CITY_TX=prsc_city;
				PRSC_STATE_CD=prsc_state;
				PRSC_ZIP_CD=prsc_zip;
				PRSC_ZIP_SFX_CD='';
				PRSC_COUNTRY_CD='USA';
				PRSC_POSTAL_CD='';
				PRSC_TZ='';
				PRSC_PHONE1_NBR=prsc_phone;
				PRSC_PHONE1_TYP_CD='Primary';
				PRSC_PHONE2_NBR='';
				PRSC_PHONE2_TYP_CD='';
				PRSC_EMAIL_ADDR_TX='';
				PRSC_EMAIL_TYP_CD='';
				PRSC_FAX_NB=prsc_phone;
				PRSC_LANG_CD='EN';
				QL_PRSC_ID=prsc_ql;
				
			end;
			
			
			%macro eoms_prsc_mapping;

				------------------ PRSCBR_NPI_ID + QL_PRSC_ID   are loaded
				------------------ PRSC_ID       + PRSC_DEA_NBR are not loaded

				select top 100 *  
				from dwv_eoms.opportunity 
				where oppty_evnt_id = 5377270848;	

				select top 100 *
				from dwv_eoms.event_message
				where oppty_evnt_id = 5377270848;	

				select top 100 *  
				from dwu_edw_rb.v_oppty_msg    ------------------------------ prescriber GID or PRSCBR_PTY_GID in this view but empty
				where oppty_altid = 5377270848; 

				select top 100 *  
				from dwv_eoms.event_party      ------------------------------ PRSCBR_NPI_ID + QL_PRSC_ID in this view but empty
				where evnt_pty_role_cd = 'PRSC'
				--and cast(rec_crte_ts AS DATE FORMAT 'yyyy-mm-dd') > current_date - 10 
				and evnt_pty_id in (5377270848);

				select top 100 *
				from dwv_eoms.event_party_contact_info
				where pty_role_cd = 'PRSC' 
				and evnt_pty_cntct_id in (select evnt_cntct_info_id from dwv_eoms.event_party where evnt_pty_id in (5377270848));

				select top 100 *
				from dwu_edw_rb.V_PRSCBR_DENORM
				where ql_prscbr_id in (select ql_prsc_id from dwv_eoms.event_party where evnt_pty_id in (5377270848));

			%mend eoms_prsc_mapping;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of pharmacy
			|
			+---------------------------------------------------------------------------------------------*/
			%macro dont_do;
				if audience_sub_id = 'PHCY' then do;

					PHCY_TYPE='NCPDP';
					PHCY_ID=nabp_code;
					PHCY_NM=phmcy_nm;
					PHCY_DOMESTIC_ADDR_IND='';
					PHCY_ADDR_LINE1_TX=phmcy_addr1;
					PHCY_ADDR_LINE2_TX=phmcy_addr2;
					PHCY_CITY_TX=phmcy_city;
					PHCY_STATE_CD=phmcy_state;
					PHCY_ZIP_CD=phmcy_zip;
					PHCY_ZIP_SFX_CD=phmcy_zip_plus;
					PHCY_COUNTRY_CD='USA';
					PHCY_PHONE1_NBR='';
					PHCY_PHONE1_TYP_CD='Primary'; 
					PHCY_FAX_NB=phmcy_fax_nb; 
					EVNT_PTY_ROLE_CD='PHCY';

				end;
			%mend dont_do;
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of channels 
			|
			|  SQL  - select prdct_altid, dsc_shrt_tx, dsc_tx from dwv_eoms.product where prod_typ_id = 6
			|
			|	CHOBCCCRNT      = 16 = Outbound Call Center (Carenet) = Carenet LOC
			|	CHOBIVRSLK      = 8  = Outbound IVR (SilverLink) = Welltok IVR = Silverlink = Welltok = Virgin Pulse
			|       CHOBCCCONTINUUM = 59 = Specialty Call Center (Continuum) = Continuum (aka Conduent) LOC
			| 
			+---------------------------------------------------------------------------------------------*/
			if stars_vendor      = 'CARENET'  then CHNL_CD='CHOBCCCRNT'; 
			else if stars_vendor = 'WELLTOK'  then CHNL_CD='CHOBIVRSLK';
			else if stars_vendor = 'CONDUENT' then CHNL_CD='CHOBCCCONTINUUM';
			else CHNL_CD="CHUNKNOWN";                 
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of opportunity subtype product ID 
			|
			|  SQL  - select * from dwv_eoms.opportunity_denorm where program like '%STARS%'
			|
			+---------------------------------------------------------------------------------------------*/			
			if      dqi_campaign_id      =  5  then OPPTY_TYP_PROD_ID='2840481640';    
			else if dqi_campaign_id      =  6  then OPPTY_TYP_PROD_ID='2841025412';  
			else if dqi_campaign_id      =  7  then OPPTY_TYP_PROD_ID='2841025413';  
			else if dqi_campaign_id      =  8  then OPPTY_TYP_PROD_ID='2841025415';   
			else if dqi_campaign_id      =  12 then OPPTY_TYP_PROD_ID='2841025417';  
			else if dqi_campaign_id      =  13 then OPPTY_TYP_PROD_ID='2840481640';    
			else if dqi_campaign_id      =  14 then OPPTY_TYP_PROD_ID='2841025412';
			else if dqi_campaign_id      =  15 then OPPTY_TYP_PROD_ID='2841025413';
			else if dqi_campaign_id      =  16 then OPPTY_TYP_PROD_ID='2841025415';
			else if dqi_campaign_id      =  17 then OPPTY_TYP_PROD_ID='2841025417';
			else if dqi_campaign_id      =  18 then OPPTY_TYP_PROD_ID='1425443987';
			else if dqi_campaign_id      =  19 then OPPTY_TYP_PROD_ID='1425443987';
			else if dqi_campaign_id      =  20 then OPPTY_TYP_PROD_ID='2841025418';
			else if dqi_campaign_id      =  21 then OPPTY_TYP_PROD_ID='2841025418';
			else if dqi_campaign_id      =  22 then OPPTY_TYP_PROD_ID='2841025419';
			else if dqi_campaign_id      =  23 then OPPTY_TYP_PROD_ID='2841025419'; 
			else if dqi_campaign_id      =  27 then OPPTY_TYP_PROD_ID='2841025420';
			else if dqi_campaign_id      =  28 then OPPTY_TYP_PROD_ID='2841025420';
			else OPPTY_TYP_PROD_ID='1425443987'; 
			

		keep %do j=1 %to &name_var_total; &&name_var&j %end; ;
		run;		


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - max opp ID to create unique msg ID 
		|
		+---------------------------------------------------------------------------------------------*/
		%let oppty_evnt_id = 0;
		
		proc sql noprint; 
		select max(oppty_evnt_id) into: max_id_new separated by ''
		from stars_opporunity;
		quit;

		%put NOTE: max_id_new = &max_id_new;

		data stars_opporunity;
		set stars_opporunity;
		MSG_EVNT_ID=left(put(&max_id_new +_n_,20.));
		run;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - qc opp + msg IDs
		|
		+---------------------------------------------------------------------------------------------*/		
		%let cntddp = 0;
		
		proc sort data=stars_opporunity dupout=duplicate_recs nodupkey;
		by OPPTY_EVNT_ID MSG_EVNT_ID ;
		run;

		%let dsid=%sysfunc(open(duplicate_recs));
		%let cntddp=%sysfunc(attrn(&dsid,nlobs));
		%let rc=%sysfunc(close(&dsid));
		
		%put NOTE: cntddp = &cntddp. ;

		%if &cntddp > 0 %then %do;

			proc export data=work.duplicate_recs
			dbms=xlsx 
			outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_opportunity_STARS.xlsx" 
			replace; 
			sheet="validate_duplicates"; 
			run;

			%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_opportunity_STARS.xlsx),
					 abend_message=%str(There is an issue with the creation of the dispostion STAS data - There are duplicate IDs));			

		%end;			
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - create ecoa opportunity files - 880123 = stars
		|
		+---------------------------------------------------------------------------------------------*/
		proc export data = stars_opporunity
		outfile = "&dir_out./sfmc_opportunity.&c_s_file_date.880123"
		dbms = DLM Replace;
		delimiter = '|';
		putnames=NO;
		run;
		
		%let opp_cnt=0;

		proc sql noprint;
		select count(*) into: opp_cnt separated by '' 
		from stars_opporunity;
		quit;

		data _null_;
		format x $100. ;
		file "&dir_out./sfmc_opportunity.&c_s_file_date.880123.CNTL";
		x="OD|sfmc_opportunity.&c_s_file_date.880123|"||"&opp_cnt."||"|SFMC|I||||";
		x=compress(x);
		put x ;
		run;

		data _null_;
		file "&dir_out./sfmc_rx_opportunity.&c_s_file_date.880123";
		run;

		data _null_;
		file "&dir_out./sfmc_rx_opportunity.&c_s_file_date.880123.CNTL";
		put "OD|sfmc_rx_opportunity.&c_s_file_date.880123|0|SFMC|I||||";
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - collect queue IDs
		|
		+---------------------------------------------------------------------------------------------*/ 		
		proc sql noprint;
		select "'"||trim(max(oppty_evnt_id))||"'"   into: opp_max separated by '' 
		from stars_opporunity;
		quit;
		
		proc sql noprint;
		select "'"||trim(max(msg_evnt_id))||"'"   into: msg_max separated by '' 
		from stars_opporunity;
		quit;		

		%put NOTE: opp_max = &opp_max. ;
		%put NOTE: msg_max = &msg_max. ;

				
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			insert &c_s_tdtempx..dqi_eoms_queue
			select 0, &opp_max. , &msg_max. , 'STARS' , current_date 

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;	
		
		
	%end;  /**<------------------------------- end - create opportunity  **/
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - deterimine if there are any disposition to process
	|
	+---------------------------------------------------------------------------------------------*/
	%let cnt_stars_disps = 0;
	
	proc sql; 
	connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
	create table cnt_stars_disps as
	select * from connection to tera(

		select count(*) as cnt_stars_disps
		from dss_cea2.dqi_eoms_opportunity 
		where alt_oppty_subtyp_src_cd = 'STARS'  
	) ;
	quit;
		
	proc sql noprint;
	select cnt_stars_disps into: cnt_stars_disps separated by ''
	from cnt_stars_disps;
	quit;	

	%put NOTE: cnt_stars_disps = &cnt_stars_disps. ; 
	

	%if &cnt_stars_disps ne 0 %then %do;  /**<------------------------------- start - create disposition  **/	


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get disposition template - 13 variables
		|
		+---------------------------------------------------------------------------------------------*/       
		proc sql noprint;
		create table variables_disp as 
		select * 
		from &c_s_tdtempx..dqi_eoms_layout 
		where layout_id = 3
		and subject_area = 'DISPOSITIONS'
		order by layout_id, sequence_id;
		quit;
		
		proc sql noprint;
		select data_attribute_name into: variables_disp separated by ' '
		from variables_disp;
		quit;
		       
		data template_disp;
		length  &variables_disp. $100. ;
		infile "&dir_in./&template_disp." firstobs=1 missover pad lrecl=32000 delimiter='|' dsd;
		input &variables_disp.  ;
		run;

		data template_disp;
		set template_disp (obs=1);
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - determine if opportunities have dispostions sent back from vendors
		|
		+---------------------------------------------------------------------------------------------*/
		%m_table_statistics(data_in=&c_s_tdtempx..dqi_eoms_opportunity, column_in=extnl_sys_oppty_id);
		%m_table_statistics(data_in=sb_adhnc.t_unlock_wc_ltf_otf_2022,  column_in=oppt_event_id);
		%m_table_statistics(data_in=sb_adhnc.wellcare_2022_otf_disps,   column_in=eoms_oppty_evnt_id);


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - build carenet dispositions members - uniqueness = campaign + opp ID
		|
		+---------------------------------------------------------------------------------------------*/
		%m_table_drop_passthrough(data_in=dss_cea2.dqi_stars_carenet_disp);

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			create multiset table dss_cea2.dqi_stars_carenet_disp as (

				/**----------- step 1 - get stars carenet dispostion -----------**/ 
				select 
					6 as cs_campaign_id, 
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id, 
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description   
				from sb_adhnc.wellcare_2022_otf_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					14 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id, 
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description   
				from sb_adhnc.centene_2022_otf_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					20 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id, 
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description  
				from sb_adhnc.wellcare_2022_ntt_otf_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					21 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id,  
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description  
				from sb_adhnc.centene_2022_ntt_otf_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					22 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id, 
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description  
				from sb_adhnc.wellcare_2022_nttr_otf_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					23 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id, 
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description  
				from sb_adhnc.centene_2022_nttr_otf_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					27 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id,  
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description  
				from sb_adhnc.wellcare_2022_arr_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1
				union
				select
					28 as cs_campaign_id,
					cast(trim(to_char(b.eoms_oppty_evnt_id)) as varchar(100))  as oppt_event_id,  
					b.prstn_dt as outreach_date, 
					cast(trim(to_char(b.lvl1_disp)) as varchar(5))   as lvl1_disp,
					cast(upper(b.lvl1_description)  as varchar(100)) as lvl1_description,  
					cast(trim(to_char(b.lvl2_disp)) as varchar(5))   as lvl2_disp,
					cast(upper(b.lvl2_description)  as varchar(100)) as lvl2_description  
				from sb_adhnc.centene_2022_arr_disps b  
				qualify rank() over (partition by cs_campaign_id, b.eoms_oppty_evnt_id order by b.prstn_dt desc, b.lvl2_disp desc) = 1

			) with data primary index(cs_campaign_id, oppt_event_id)

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;
		
		
		%macro qc_001;

			select cs_campaign_id, cs_campaign_description, 
			lvl1_disp, lvl1_description, 
			lvl2_disp, lvl2_description, 
			count(*) as cnt
			from dss_cea2.clinical_stars_eoy_report
			group by 1,2,3,4,5,6
			order by 1,2,3,4,5,6

			select dispn_cd, dispn_sub_cd, count(*) as cnt
			from dwu_edw_rb.v_oppty_msg
			where dispn_sub_cd = '26'
			group by 1,2	

				DISPN_CD	DISPN_SUB_CD	cnt
			1	1		26		19,421,370
			2	8		26		1,455,955


		%mend qc_001;
			
		
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			update dss_cea2.dqi_stars_carenet_disp
			set lvl1_disp = '1',
			    lvl1_description = 'DELIVERED'
			where lvl2_disp = '26'

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;			

		%m_table_statistics(data_in=dss_cea2.dqi_stars_carenet_disp, index_in=%str(cs_campaign_id, oppt_event_id)); 


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - match carenet dispositions members to sent ECOA opportunities
		|
		+---------------------------------------------------------------------------------------------*/		
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table _stars_disps_carenet_loc  as 
		select * from connection to tera
		(
			 select top 200000 *
			 from (/**----------- step 6 - match ecoa opportunity to stars opportunity disposition -----------**/
				select distinct		
				o.oppty_evnt_id,
				o.msg_evnt_id, 
				d.smart_id,
				d.smart_id_disp,
				d.stars_vendor,
				d.stars_channel,
				d.outreach_date,
				d.lvl1_disp,
				d.lvl1_description, 
				d.lvl2_disp,
				d.lvl2_description 				

				from dss_cea2.dqi_eoms_opportunity o inner join	  	
				
				(	/**----------- step 3 - match stars opportunity to stars disposition -----------**/
					select 
						trim(extract(year from execution_date) (format '9999'))||trim(extract(month from execution_date) (format '99'))||
						trim(extract(day from execution_date) (format '99'))||trim(a.cs_campaign_id) as ticket,
						cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||'0'  as varchar(100)) as smart_id,
						cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||trim(a.queue_id) as varchar(100)) as smart_id_disp, 						
						upper(a.adh_pgm_vendor) as stars_vendor,
						upper(a.channel)        as stars_channel,
						b.outreach_date,
						b.lvl1_disp,
						b.lvl1_description,
						b.lvl2_disp,
						b.lvl2_description
					from 	
					(	/**----------- step 1 - get stars carenet opportunity -----------**/
						select distinct cs_campaign_id, execution_date, cast(trim(to_char(oppt_event_id)) as varchar(100))  as oppt_event_id, 
						strtok(oracle_id,'-',2) as queue_id, adh_pgm_vendor, channel   
						from sb_adhnc.t_unlock_wc_ltf_otf_2022   
						where cs_campaign_id in (6,20,22,27)
						union
						select distinct cs_campaign_id, execution_date, cast(trim(to_char(oppt_event_id)) as varchar(100))  as oppt_event_id, 
						strtok(oracle_id,'-',2) as queue_id, adh_pgm_vendor, channel   
						from sb_adhnc.t_unlock_cnc_ltf_otf_2022  
						where cs_campaign_id in (14,21,23,28)
					) a inner join	
					
					/**----------- step 2 - get stars carenet disposition -----------**/
					dss_cea2.dqi_stars_carenet_disp b          
					on a.cs_campaign_id = b.cs_campaign_id and a.oppt_event_id = b.oppt_event_id   /**<--------- oppt_event_id = claim + date **/
					
				) d on o.extnl_sys_oppty_id = d.smart_id  left join
				
				(       /**----------- step 4 - exclude existing ecoa disposition -----------**/					
					select d.contact_data_value as smart_id_disp, cast(d.prstn_dt AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date
					from dss_cea2.dqi_eoms_dispositions d,
					     dss_cea2.dqi_eoms_opportunity  o 
					where d.oppty_evnt_id=o.oppty_evnt_id
					and d.msg_evnt_id=o.msg_evnt_id 
					and o.alt_oppty_subtyp_src_cd = 'STARS'  		 	
				) e on d.smart_id_disp = e.smart_id_disp and d.outreach_date = e.outreach_date left join 
				
				(       /**----------- step 5 - include existing IT disposition -----------**/
					select d.oppty_evnt_id, d.msg_evnt_id
					from dwv_campgn.event_message d,
					     dwv_campgn.opportunity  o 
					where d.oppty_evnt_id=o.oppty_evnt_id  
					and o.alt_oppty_subtyp_src_cd = 'STARS'	
				) it on o.oppty_evnt_id = it.oppty_evnt_id and o.msg_evnt_id = it.msg_evnt_id  
				
				where o.alt_oppty_subtyp_src_cd = 'STARS'
				and e.smart_id_disp is null             /** <--------------------------------- exclusion = disps already processed by ecoa  **/
				and it.msg_evnt_id is not null		/** <--------------------------------- inclusion = disps need availabiliy in IT to update **/

			) x

		);
		disconnect from tera;
		quit; 	
		
		 

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - build welltok dispositions members - uniqueness = campaign + claim + date
		|
		+---------------------------------------------------------------------------------------------*/
		%m_table_drop_passthrough(data_in=dss_cea2.dqi_stars_welltok_disp);

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			create multiset table dss_cea2.dqi_stars_welltok_disp as (
			
				select 
					b.cs_campaign_id,		                     			
					b.claim_gid,
					b.execution_date, 	
					b.queue_id,
					b.outreach_date,
					case 
						when upper(l1_desc) like '%COMPLETE%' then cast('1' as varchar(5)) 
						when upper(l1_desc) like '%PROGRESS%' then cast('8' as varchar(5)) 
						else '8'
					end as lvl1_disp,
					cast(upper(l1_desc) as varchar(100))  as lvl1_description,
					case 
						when upper(l2_desc) like '%MACHINE%'       then cast('26' as varchar(5)) 
						when upper(l2_desc) like '%WRONG%'         then cast('20' as varchar(5))
						when upper(l2_desc) like '%MESSAGE%'       then cast('26' as varchar(5))
						when upper(l2_desc) like '%AUTHENTICATED%' then cast('17' as varchar(5))
						when upper(l2_desc) like '%ABANDONED%'     then cast('56' as varchar(5))
						when upper(l2_desc) like '%PRIMARY NO%'    then cast('18' as varchar(5))
						when upper(l2_desc) like '%PRIMARY YES%'   then cast('55' as varchar(5))
						when upper(l2_desc) like '%NON-RESPONS%'   then cast('18' as varchar(5))
						when upper(l2_desc) like '%UNCERTAIN%'     then cast('55' as varchar(5))
						when upper(l2_desc)  =   'NO ANSWER'       then cast('31' as varchar(5))
						when upper(l2_desc)  =   'BUSY'            then cast('30' as varchar(5))
						when upper(l2_desc)  =   'UNKNOWN'         then cast('55' as varchar(5))
						else '55'
					end as lvl2_disp, 
					cast(upper(l2_desc) as varchar(100))  as lvl2_description
				from 
				(       /**----------- step 1 - get stars welltok dispostion -----------**/ 
					select 
					case 
						when x.carrier is not null then 5     
						else                            13
					end as cs_campaign_id, strtok(oracle_id,'-',2) as queue_id,	 
					claim_gid, d.queueid, d.effectivedate as execution_date, processtime as outreach_date, callrecipientstatus as l1_desc, status as l2_desc
					from sb_adhnc.ivr_2022_ltr_refills d left join 
					     sb_adhnc.table_wc_rr_comm x on d.carrier = x.carrier
				) b
				
				where b.claim_gid > 0
				qualify rank() over (partition by b.cs_campaign_id, b.claim_gid, b.execution_date 
				                     order by b.outreach_date desc, b.l2_desc desc) = 1 /** <-------------------------------------------- lastest and greatest **/ 

			) with data primary index(cs_campaign_id, claim_gid, execution_date)

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;	
		
		%m_table_statistics(data_in=dss_cea2.dqi_stars_welltok_disp, index_in=%str(cs_campaign_id, claim_gid, execution_date)); 		
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - match welltok dispositions members to sent ECOA opportunities
		|
		|  NOTE - use queueid instead of claim gid since it will let you know which record in disp was used		
		|
		+---------------------------------------------------------------------------------------------*/		
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table _stars_disps_welltok_ivr  as 
		select * from connection to tera
		(
			 select top 200000 *
			 from (/**----------- step 6 - match ecoa opportunity to stars opportunity disposition -----------**/
				select distinct		
				o.oppty_evnt_id,
				o.msg_evnt_id,
				d.smart_id,
				d.smart_id_disp,
				d.stars_vendor,
				d.stars_channel,
				d.outreach_date,
				d.lvl1_disp,
				d.lvl1_description,
				d.lvl2_disp,	
				d.lvl2_description

				from dss_cea2.dqi_eoms_opportunity o inner join 
				
				(       /**----------- step 3 - match stars opportunity to stars disposition -----------**/
					select 
						trim(extract(year from a.execution_date) (format '9999'))||trim(extract(month from a.execution_date) (format '99'))||
						trim(extract(day from a.execution_date) (format '99'))||trim(a.cs_campaign_id) as ticket,
						cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(to_char(a.oppt_event_id))||'.'||'0' as varchar(100)) as smart_id, 
						cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(to_char(a.oppt_event_id))||'.'||trim(b.queue_id) as varchar(100)) as  smart_id_disp, 						
						upper(a.adh_pgm_vendor) as stars_vendor,
						upper(a.channel)        as stars_channel,
						b.outreach_date,
						b.lvl1_disp,
						b.lvl1_description,
						b.lvl2_disp,
						b.lvl2_description
					from 
					
					(       /**----------- step 1 - get stars welltok opportunity -----------**/
						select cs_campaign_id, claim_gid, execution_date, coalesce(oppt_event_id,mbr_gid) as oppt_event_id, adh_pgm_vendor, channel  
						from sb_adhnc.t_unlock_wc_ltf_otf_2022   
						where cs_campaign_id in (5)
						union
						select cs_campaign_id, claim_gid, execution_date, coalesce(oppt_event_id,mbr_gid) as oppt_event_id, adh_pgm_vendor, channel 
						from sb_adhnc.t_unlock_cnc_ltf_otf_2022  
						where cs_campaign_id in (13)
					) a inner join 
					
					/**----------- step 2 - get stars welltok disposition -----------**/ 
					dss_cea2.dqi_stars_welltok_disp b          
					on a.cs_campaign_id=b.cs_campaign_id and a.claim_gid = b.claim_gid and a.execution_date = b.execution_date
					
				) d on o.extnl_sys_oppty_id = d.smart_id  left join  
				
				(       /**----------- step 4 - exclude prior ecoa disposition -----------**/
					select d.contact_data_value as smart_id_disp, cast(d.prstn_dt AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date
					from dss_cea2.dqi_eoms_dispositions d,
					     dss_cea2.dqi_eoms_opportunity o 
					where d.oppty_evnt_id=o.oppty_evnt_id
					and d.msg_evnt_id=o.msg_evnt_id 
					and o.alt_oppty_subtyp_src_cd = 'STARS'  				
				) e on d.smart_id_disp = e.smart_id_disp and d.outreach_date = e.outreach_date left join 
				
				(       /**----------- step 5 - include only available IT opportunity + disposition -----------**/				
					select d.oppty_evnt_id, d.msg_evnt_id
					from dwv_campgn.event_message d,
					     dwv_campgn.opportunity  o 
					where d.oppty_evnt_id=o.oppty_evnt_id  
					and o.alt_oppty_subtyp_src_cd = 'STARS'					
				) it on o.oppty_evnt_id = it.oppty_evnt_id and o.msg_evnt_id = it.msg_evnt_id  
				
				where o.alt_oppty_subtyp_src_cd = 'STARS'
				and e.smart_id_disp is null             /** <--------------------------------- exclusion = disps are already available in ecoa  **/
				and it.msg_evnt_id is not null		/** <--------------------------------- inclusion = disps need availabiliy in IT to update **/
					
 			) x

		);
		disconnect from tera;
		quit; 	
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - build welltok dispositions prescribers - uniqueness = campaign + claim + date
		|
		+---------------------------------------------------------------------------------------------*/
		%m_table_drop_passthrough(data_in=dss_cea2.dqi_stars_welltok_disp_fax);                                                                                                			

		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			create multiset table dss_cea2.dqi_stars_welltok_disp_fax as (

				select  
					b.cs_campaign_id,                          
					b.claim_gid,  
					b.execution_date, 
					b.queue_id,
					b.outreach_date,
					case 
						when upper(l1_desc) like '%SUCCESS%'  then cast('1' as varchar(5))
						when upper(l1_desc) like '%FAX SENT%' then cast('1' as varchar(5))
						when upper(l1_desc) like '%CONDUENT%' then cast('1' as varchar(5))
						else '8'
					end as lvl1_disp,
					case 
						when upper(l1_desc) like '%SUCCESS%'  then cast('DELIVERED' as varchar(100)) 
						when upper(l1_desc) like '%FAX SENT%' then cast('DELIVERED' as varchar(100)) 
						when upper(l1_desc) like '%CONDUENT%' then cast('DELIVERED' as varchar(100)) 
						else 'NOT DELIVERED'
					end as lvl1_description,					 
					case 
						when upper(l1_desc) like '%SUCCESS%'       		then cast('201' as varchar(5))
						when upper(l1_desc) like '%FAX SENT%'       		then cast('201' as varchar(5))
						when upper(l1_desc) like '%CONDUENT%'       		then cast('10'  as varchar(5))
						when upper(l1_desc) like '%UNKNOWN%'       		then cast('55'  as varchar(5))
						when upper(l1_desc) like '%INVALID FAXNUMBER%'		then cast('20'  as varchar(5))
						when upper(l1_desc) like '%MACHINE BUSY%'          	then cast('30'  as varchar(5))
						when upper(l1_desc) like '%VOICE ANSWER%'          	then cast('25'  as varchar(5))
						when upper(l1_desc) like '%LOST COMMUNICATION%'        	then cast('32'  as varchar(5))
						when upper(l1_desc) like '%NO ANSWER%'        		then cast('31'  as varchar(5))
						when upper(l1_desc) like '%EARLY DISCONNECT%'        	then cast('75'  as varchar(5))
						when upper(l1_desc) like '%DONOTFAX%'        		then cast('34'  as varchar(5))
						when upper(l1_desc) like '%HANGUP%'        		then cast('56'  as varchar(5))
						when upper(l1_desc) like '%DISCONNECT%'        		then cast('75'  as varchar(5))
						when upper(l1_desc) like '%NETWORK BUSY%'      		then cast('30'  as varchar(5))
						when upper(l1_desc) like '%BUSY%'      			then cast('30'  as varchar(5))
						when upper(l1_desc) like '%JOB BLOCKED%'      		then cast('262' as varchar(5))
						when upper(l1_desc) like '%NO DIAL TONE%'      		then cast('262' as varchar(5))
						when upper(l1_desc) like '%NETWORK RESPONSE%'      	then cast('262' as varchar(5))
						when upper(l1_desc) like '%ERROR%'      		then cast('262' as varchar(5))
						when upper(l1_desc) like '%REJECT%'      		then cast('262' as varchar(5))
						when upper(l1_desc) like '%EXPORT%'      		then cast('262' as varchar(5))
						when upper(l1_desc) like '%UNACCEPTABLE%'      		then cast('262' as varchar(5))
						when upper(l1_desc) like '%INVALID CHARACTERS%'      	then cast('601' as varchar(5))
						when upper(l1_desc) like '%INVALID PROVIDERZIP%'      	then cast('601' as varchar(5))
						when upper(l1_desc) like '%INVALID RECIPIENTSTATE%'   	then cast('601' as varchar(5)) 
						else '0'
					end as lvl2_disp, 
					cast(upper(l1_desc) as varchar(100))  as lvl2_description
				from	
				(       /**----------- step 1 - get stars welltok dispostion -----------**/ 
					select 
					case 
						when x.carrier is not null then 7     
						else                            15
					end as cs_campaign_id, 	 
					claim_gid, d.batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.silverlink_fax_disp_ssi_2022 d left join
					     sb_adhnc.table_wc_rr_comm x on d.extnl_lvl_id1 = x.carrier
					union 
					select 8  as cs_campaign_id, claim_gid, batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.supd_fax_disp_2022
					union
					select 12 as cs_campaign_id, claim_gid, batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.phmcy_fax_disp_wellcare_2022
					union
					select 16 as cs_campaign_id, claim_gid, batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.supd_fax_disp_centene_2022
					union
					select 17 as cs_campaign_id, claim_gid, batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.phmcy_fax_disp_centene_2022
					union					
					select 18 as cs_campaign_id, claim_gid, batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.phmcy_fax_ltf_wellcare_2022
					union
					select 19 as cs_campaign_id, claim_gid, batch_date as execution_date, eventdate  as outreach_date, eventdescription as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.phmcy_fax_ltf_centene_2022
					union
					
					/**----------- step 2 - get stars conduent dispostion = not collected so disp = 1 + 10 -----------**/ 
					select cs_campaign_id, claim_gid, execution_date, cast(execution_date AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date, 'CONDUENT' as l1_desc, strtok(oracleid,'-',2) as queue_id   
					from sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022_loc a   			
					where a.cs_campaign_id in (7) 
					and upper(a.adh_pgm_vendor) = 'CONDUENT'
					union   
					select cs_campaign_id, claim_gid, execution_date, cast(execution_date AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date, 'CONDUENT' as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.supd_fax_hix_tgt_data_2022_loc a   			
					where a.cs_campaign_id in (8) 
					and upper(a.adh_pgm_vendor) = 'CONDUENT'
					union 
					select cs_campaign_id, claim_gid, execution_date, cast(execution_date AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date, 'CONDUENT' as l1_desc, strtok(oracleid,'-',2) as queue_id
					from sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022_loc a			
					where a.cs_campaign_id in (15)
					and upper(a.adh_pgm_vendor) = 'CONDUENT'
					union 
					select cs_campaign_id, claim_gid, execution_date, cast(execution_date AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date, 'CONDUENT' as l1_desc, strtok(oracleid,'-',2) as queue_id						
					from sb_adhnc.cnc_supd_fax_hix_tgt_data_2022_loc a			
					where a.cs_campaign_id in (16)  
					and upper(a.adh_pgm_vendor) = 'CONDUENT'					
				) b   
				where b.claim_gid > 0
				qualify rank() over (partition by b.cs_campaign_id, b.claim_gid, b.execution_date 
				                     order by b.outreach_date desc, b.l1_desc desc) = 1 /** <------------------------------------------- lastest and greatest **/ 

			) with data primary index(cs_campaign_id, claim_gid, execution_date)

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;	
		
		%m_table_statistics(data_in=dss_cea2.dqi_stars_welltok_disp_fax, index_in=%str(cs_campaign_id, claim_gid, execution_date));   		
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - match welltok dispositions prescribers to sent ECOA opportunities
		|
		|
		+---------------------------------------------------------------------------------------------*/		
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table _stars_disps_welltok_fax  as 
		select * from connection to tera
		(
			 select top 200000 *
			 from (/**----------- step 6 - match ecoa opportunity to stars opportunity disposition -----------**/
				select distinct		
				o.oppty_evnt_id,
				o.msg_evnt_id,
				d.smart_id,
				d.smart_id_disp,
				d.stars_vendor,
				d.stars_channel,
				d.outreach_date,
				d.lvl1_disp,
				d.lvl1_description,
				d.lvl2_disp,	
				d.lvl2_description

				from dss_cea2.dqi_eoms_opportunity o inner join  
				
				(       /**----------- step 3 - match stars opportunity to stars disposition -----------**/
					select 
						trim(extract(year from a.execution_date) (format '9999'))||trim(extract(month from a.execution_date) (format '99'))||
						trim(extract(day from a.execution_date) (format '99'))||trim(a.cs_campaign_id) as ticket, 
						cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||'0' as varchar(100)) as smart_id,
						cast(trim(to_char(a.cs_campaign_id))||'.'||trim(ticket)||'.'||trim(a.oppt_event_id)||'.'||trim(b.queue_id) as varchar(100)) as smart_id_disp,						
						upper(a.adh_pgm_vendor) as stars_vendor,
						upper(a.channel)        as stars_channel,
						b.outreach_date,
						b.lvl1_disp,
						b.lvl1_description,
						b.lvl2_disp,
						b.lvl2_description
					from 
					
					(       /**----------- step 1a - get stars welltok opportunity -----------**/					
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel 
						from sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022 a 			
						where a.cs_campaign_id in (7,18) 
						union
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, channel 						
						from sb_adhnc.supd_fax_hix_tgt_data_2022 a   			
						where a.cs_campaign_id in (8) 												
						union
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel 
						from sb_adhnc.t_unlock_adh_pharm_mbr_tgt_hist_2022 a   			
						where a.cs_campaign_id in (12) 
						union				
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel
						from sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022 a 			
						where a.cs_campaign_id in (15,19) 
						union
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel 						
						from sb_adhnc.cnc_supd_fax_hix_tgt_data_2022 a				
						where a.cs_campaign_id in (16) 							
						union
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel 
						from sb_adhnc.t_unlock_adhcnc_pharm_mbr_tgt_hist_2022 a 			
						where a.cs_campaign_id in (17) 
						union
						
						/**----------- step 1b - get stars conduent opportunity -----------**/ 
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel   
						from sb_adhnc.t_unlock_adh_mbr_tgt_hist_2022_loc a   			
						where a.cs_campaign_id in (7) 
						union
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, channel
						from sb_adhnc.supd_fax_hix_tgt_data_2022_loc a   			
						where a.cs_campaign_id in (8) 
						union
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, pilot_group as channel 
						from sb_adhnc.t_unlock_adhcnc_mbr_tgt_hist_2022_loc a			
						where a.cs_campaign_id in (15) 	
						union 
						select cs_campaign_id, claim_gid, execution_date, cast(to_char(claim_gid) as varchar(100)) as oppt_event_id, adh_pgm_vendor, channel 						
						from sb_adhnc.cnc_supd_fax_hix_tgt_data_2022_loc a			
						where a.cs_campaign_id in (16) 						
						
					) a inner join 
					
					/**----------- step 2 - get stars welltok conduent disposition -----------**/ 
					dss_cea2.dqi_stars_welltok_disp_fax b           
					on a.cs_campaign_id=b.cs_campaign_id and a.claim_gid = b.claim_gid and a.execution_date = b.execution_date 
					
				) d on o.extnl_sys_oppty_id = d.smart_id  left join  
				
				(       /**----------- step 4 - exclude prior ecoa disposition -----------**/
					select d.contact_data_value as smart_id_disp, cast(d.prstn_dt AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') as outreach_date
					from dss_cea2.dqi_eoms_dispositions d,
					     dss_cea2.dqi_eoms_opportunity o 
					where d.oppty_evnt_id=o.oppty_evnt_id
					and d.msg_evnt_id=o.msg_evnt_id 
					and o.alt_oppty_subtyp_src_cd = 'STARS'  				
				) e on d.smart_id_disp = e.smart_id_disp and d.outreach_date = e.outreach_date left join 
				
				(       /**----------- step 5 - include only available IT opportunity + disposition -----------**/				
					select d.oppty_evnt_id, d.msg_evnt_id
					from dwv_campgn.event_message d,
					     dwv_campgn.opportunity  o 
					where d.oppty_evnt_id=o.oppty_evnt_id  
					and o.alt_oppty_subtyp_src_cd = 'STARS'					
				) it on o.oppty_evnt_id = it.oppty_evnt_id and o.msg_evnt_id = it.msg_evnt_id 
				
				where o.alt_oppty_subtyp_src_cd = 'STARS'
				and e.smart_id_disp is null             /** <--------------------------------- exclusion = disps are already available in ecoa  **/
				and it.msg_evnt_id is not null		/** <--------------------------------- inclusion = disps need availabiliy in IT to update **/
					
 			) x

		);
		disconnect from tera;
		quit; 	
		
		
		data _stars_disps;
		set _stars_disps_carenet_loc   	/** <----------------------- wc = 6,20,22,27 + cnc = 14,21,23,28 **/ 
		    _stars_disps_welltok_ivr   	/** <----------------------- wc = 5          + cnc = 13          **/
		    _stars_disps_welltok_fax    /** <----------------------- wc = 7,8,12,18  + cnc = 15,16,17,19 **/  
		    ; 
		run;
		
		proc sort data = _stars_disps nodupkey;
		by OPPTY_EVNT_ID MSG_EVNT_ID ;
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - deterimine if there are any dispositions to process
		|
		+---------------------------------------------------------------------------------------------*/
		%let cnt_stars_disps = 0;

		proc sql noprint;
		select count(*) into: cnt_stars_disps separated by ''
		from _stars_disps;
		quit;

		%put NOTE: cnt_stars_disps = &cnt_stars_disps. ; 

		%if &cnt_stars_disps ne 0 %then %do;		


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of the ecoa stars disposition file  = 9 data elements
			+---------------------------------------------------------------------------------------------*/
			data stars_disposition;
			if _n_ = 1 then set template_disp;
			set _stars_disps;


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - mapping of disposition
				|
				+---------------------------------------------------------------------------------------------*/
				OPPTY_SRC_CD='DQI';
				OPPTY_EVNT_ID=OPPTY_EVNT_ID;
				MSG_EVNT_ID=MSG_EVNT_ID;
				EXTNL_SYS_OPPTY_ID=smart_id;             /**<------------------------------------------------ smart ID **/
				MSG_DISPN_CD=lvl1_disp;                  /**<------------------------------------------------ 1  - Delivered **/
				MSG_DISPN_SUB_CD=lvl2_disp;              /**<------------------------------------------------ 10 - New Opportunity **/
				DLVRY_MTHD_CD='101';                     /**<------------------------------------------------ 101 - Display **/
				if stars_channel = 'FAX' then CONTACT_DATA_TYPE = 'F'; else CONTACT_DATA_TYPE = 'L';  /**<--- L - Landline + F = Fax **/
				CONTACT_DATA_VALUE = smart_id_disp ;     /**<------------------------------------------------ smart ID disposition **/
				date1    = "  "||put(outreach_date,datetime16.) ;
				time     = substr(date1,11) ;
				date2    = DATEPART(outreach_date); 
				PRSTN_DT = put(date2,yymmdd10.)||' '||left(trim(time)); /**<--------------------------------- outreach date **/				


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - mapping of channels 
				|
				|  SQL  - select prdct_altid, dsc_shrt_tx, dsc_tx from dwv_eoms.product where prod_typ_id = 6
				|
				|	CHOBCCCRNT      = 16 = Outbound Call Center (Carenet) = Carenet LOC
				|	CHOBIVRSLK      = 8  = Outbound IVR (SilverLink) = Welltok IVR = Silverlink = Welltok = Virgin Pulse
				|       CHOBCCCONTINUUM = 59 = Specialty Call Center (Continuum) = Continuum (aka Conduent) LOC
				|
				+---------------------------------------------------------------------------------------------*/
				if stars_vendor      = 'CARENET'  then CHNL_CD='CHOBCCCRNT'; 
				else if stars_vendor = 'WELLTOK'  then CHNL_CD='CHOBIVRSLK';
				else if stars_vendor = 'CONDUENT' then CHNL_CD='CHOBCCCONTINUUM';
				else CHNL_CD="CHUNKNOWN";    
				

			keep &variables_disp.  ;
			run;
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - qc to validate the opp + msg ID are unique  
			|
			+---------------------------------------------------------------------------------------------*/			
			%let cntddp = 0;
			
			proc sort data=stars_disposition dupout=duplicate_recs nodupkey;
			by OPPTY_EVNT_ID MSG_EVNT_ID ;
			run;
			
			%let dsid=%sysfunc(open(duplicate_recs));
			%let cntddp=%sysfunc(attrn(&dsid,nlobs));
			%let rc=%sysfunc(close(&dsid));

			%if &cntddp > 0 %then %do;

				proc export data=work.duplicate_recs
				dbms=xlsx 
				outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_dispositions_STARS.xlsx" 
				replace; 
				sheet="validate_duplicates"; 
				run;

				%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_dispositions_STARS.xlsx),
						 abend_message=%str(There is an issue with the creation of the dispostion STARS data - There are duplicate IDs));			

			%end;			

		
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - create ecoa disposition files - 880123 = stars
			|
			+---------------------------------------------------------------------------------------------*/
			proc export data = stars_disposition
			outfile = "&dir_out./sfmc_disposition.&c_s_file_date.880123"
			dbms = DLM Replace;
			putnames=NO;
			delimiter = '|';
			run;

			%let disp_cnt=0;

			proc sql noprint;
			select count(*) into: disp_cnt separated by '' 
			from stars_disposition;
			quit;

			data _null_;
			format x $100. ;
			file "&dir_out./sfmc_disposition.&c_s_file_date.880123.CNTL";
			x="OD|sfmc_disposition.&c_s_file_date.880123|"||"&disp_cnt."||"|SFMC|I||||";
			x=compress(x);
			put x;
			run;

			data _null_;
			file "&dir_out./sfmc_rx_disposition.&c_s_file_date.880123";
			run;

			data _null_;
			file "&dir_out./sfmc_rx_disposition.&c_s_file_date.880123.CNTL";
			put "OD|sfmc_rx_disposition.&c_s_file_date.880123|0|SFMC|I||||";
			run;
			
		%end;
		
		
	%end;  /**<------------------------------- end - create disposition  **/
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - SFTP opportunity and disposition files
	|
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_stars_opps. > 0 or &cnt_stars_disps. > 0 %then %do;  /**<------------------------------- start - sftp  **/

		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - put files on sftp site for ctl_data_integration_eoms
		|   
		+---------------------------------------------------------------------------------------------*/			
		%if &c_s_sftp_put_production. = Y %then %do;
		
			%let get_etl_directory=&dir_out.;
			%let put_etl_directory=%str(/ant591/IVR/incoming);


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - Create listing of files to SFTP
			|
			+---------------------------------------------------------------------------------------------*/	
			%put NOTE: Put dqi files from the Linux to production SFTP server - webtransport.caremark.com;

			data eoms_ftp_files;
			rc=filename("mydir","&get_etl_directory.");
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
			
			data eoms_ftp_files; 
			set eoms_ftp_files ;  
			if index(name,"&c_s_file_date.") > 0;
			run;			

			data _null_; 
			set eoms_ftp_files ;  
			put _all_;
			run;		

			data _null_; 
			set eoms_ftp_files ;  
			call symput('sftpname'||trim(left(_n_)),trim(left(name))); 
			call symput('total_sftp',trim(left(_n_)));
			run;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - Transfer opportunity + disposition ecoa files to SFTP server - webtransport
			|
			+---------------------------------------------------------------------------------------------*/

			%do g = 1 %to &total_sftp;

				filename getfile "&get_etl_directory./&&sftpname&g" recfm=f;
				filename putfile sftp "&put_etl_directory./&&sftpname&g" recfm=f 
				options="-oKexAlgorithms=diffie-hellman-group14-sha1"
				host="&sftp_eoms_id.@webtransport.caremark.com" wait_milliseconds=4000 debug;


				data _null_;
				length msg $ 384;
				rc=fcopy('getfile', 'putfile');
				if rc=0 then put 'NOTE: Copied file from linux to sftp server.';
				else do;
				msg=sysmsg();
				put rc= msg=;
				end;
				run;			

			%end;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - chmod for files on SFTP server - webtransport
			|
			+---------------------------------------------------------------------------------------------*/		
			data _null_;
			file "&get_etl_directory./put.txt";
			put "cd &put_etl_directory.";
			put "chmod 777 *"; 
			put "exit";
			run; 

			data _null_;
			rc = system("cd &get_etl_directory."); 
			rc = system("sftp &sftp_eoms_id.@webtransport.caremark.com < &get_etl_directory./put.txt"); 
			if   _error_=0  then do;
			call symput('file_sftp_flg','1');
			end;
			else  call symput('file_sftp','0');
			run;		

		%end;		
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - send out stars complete email  
		|
		+---------------------------------------------------------------------------------------------*/	
		%let pf_total = 0;
		
		proc sort data = eoms_ftp_files;
		by name;
		run;
		
		data _null_;
		set eoms_ftp_files end=eof;
		call symput('sftpname'||trim(left(_n_)),trim(left(name))); 
		call symput('pf_total',trim(left(_n_))); 
		run;	
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - email report
		+---------------------------------------------------------------------------------------------*/
		proc sql noprint;
		create table report_email as 
		select 	name label = 'Opportunity + Disposition Files' 	 
		from eoms_ftp_files;
		quit;		

		filename report "%sysfunc(pathname(work))/report_email.html"; 
		
		title;
		footnote;
				
		ods html file=report;
			proc print data=report_email style(header)=[font_face='Arial Narrow'] style(table)={just=l bordercolor=blue} obs label; 
			var name ;	 
			run;
		ods html close;			
		
		
		%if %symexist(c_s_log) %then %do; 
		  %m_qc_scan_log(infile=&c_s_maindir.&c_s_log., logcheck=ALL);
		%end; 
	
		%if &c_s_dqi_campaign_id. = 86 %then %do;
			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - COMPLETED STARS ECOA File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";
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
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - email introduction
			+---------------------------------------------------------------------------------------------*/
			if _n_=1 then do;
			
				put '<br>'; 
				put "The CTT Data Profiling PBM STARS file processing has completed. ";
				put '<br>';
				put '<b>';
				put '<br>';
				put '<b><font color="#13478C">'; put '<u>'; put "CTT Data Profiling information: "; put '</b></font>'; put '</u>';
				put '</b>';
				put '<br>';	
				put "<ul>";
					put "<li> 	PBM Adapter + Profiling Framework"; 		put '</li>'; 
					put "<li> 	program: ctl_data_integration_dqi.sas"; 	put '</li>'; 
					put "<li> 	campaign ID:  &c_s_dqi_campaign_id."; 		put '</li>'; 
					put "<li> 	user: &_clientusername. "; 			put '</li>'; 
					put "<li> 	opportunity counts: &opp_cnt. "; 		put '</li>'; 
					put "<li> 	disposition counts: &disp_cnt.  "; 		put '</li>'; 
					put "<li> 	sftp files:  webtransport.caremark.com - &put_etl_directory.  "; 			put '</li>'; 
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
		
		
		%m_dqi_process_control(id=5, step=%str(STARS - DATA INTGRATION CMCTN HISTORY COMPLETE));
		

	%end;  /**<------------------------------- end - sftp  **/
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - email if no files for today
	|
	+---------------------------------------------------------------------------------------------*/	
	%else %do;
	
		filename xemail email 
		to=(&c_s_email_to.)
		subject="CTT Data Profiling - COMPLETED STARS ECOA File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  
		
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
			put "The CTT Data Profiling STARS file processing has completed. ";
			put '<br>';
			put "There are no opportunity or dispostion files for today. "; 
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
		
	
		%m_dqi_process_control(id=5, step=%str(STARS - DATA INTGRATION CMCTN HISTORY COMPLETE));
		
		
	%end;
	
	
	%m_dqi_process_control(id=10, step=%str(STARS - DATA INTGRATION COMPLETE));
	
	
%mend cd_data_integration_stars;
%cd_data_integration_stars;

