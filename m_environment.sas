/* HEADER --------------------------------------------------------------------------------------
| MACRO:    m_environment  
|  
| LOCATION: /anr_dep/dqi/prod/core   
| 
| USAGE:    
|
| PURPOSE:  creates the virtual environment for dqi 
|
| LOGIC:     
|
| TABLES:     
|
| INPUT: 	 
|
| OUTPUT:    
|
+--------------------------------------------------------------------------------------------------------------------------------------------
|            Work
| Date       Request Programmer        Description 
+--------------------------------------------------------------------------------------------------------------------------------------------
| 04APR2022  1599805 EBukowski         Government Formulary: Zinc (BoB) - Cross over
| 12JAN2022  1551301 Brian Stropich    MONY PBM MDDB project - Non-VT Campaigns
| 11OCT2021  1551301 Jeff Reboulet     MONY Code - add c_s_use_mddb = N, update default for c_s_sub_product_id=0.
| 01OCT2021  1498101 EBukowski         Add default to c_s_bob_url_mapping - support additional URL in batch for Aetna
| 03AUG2021  1470102 Jeff Reboulet     VT Child Automation. Add test location for libname vt_disr.
| 20MAY2021  1504701 EBukowski         NOCC 2021 - adding c_s_nocc_ctl_sheetnm to list of global variables
|            adhoc   EBukowski         Adding c_s_cat_incl_sheetnm to list of global variables
| 13APR2021  adhoc   EBukowski         Updating c_s_drug_sheets_ignore with INFORMATION sheet for new Neg Frmly file format
| 19MAR2021  1472301 JReboulet         Opioids Flexible QL. Add c_s_opioid_aligned and c_s_opioid_daily_dose_bypass  
| 15MAR2021  1455210 EBukowski         Clinical Implementation Consolidated BOB Formulary client intake
| 09FEB2021  1345803 JReboulet         Use FDRO BoB Drug Intake instead of Adhoc. Set c_s_drug_optional to N.
| 04JAN2021  1253401 EBukowski         Triage intake automation - mbr
| 30OCT2020  1338104 JReboulet         Custom BoB Tier Compare - 74
| 25JUL2020  1202902 EBukowski         Triage intake automation - drug
| 18MAY2020  1130904 EBukowski         Client Triage Automation changes
| 08JUN2020  1339001 Srini Konduri     New c_s_allow_1fill_days which is configurable to support Anthem MedD Targeting(c_s_allow_1fill_days =121) 
| 04MAY2020  1330504 Srini Konduri -   New c_s_anthem_allow_90day_1fill variable to support option for 1 claim in 90 days
| 23MAR2020  1199702 EBukowski         Task auto-close changes
| 03MAR2020  1272902 SKonduri          LL28 Changes--Added Global variables cntdup and cntinv 
  09OCT2019  1189401 Jeff Reboulet     Balanced Formulary. Added c_s_newtomarket_bf_override default;
| 30AUG2019  1120901 Jeff Reboulet     VT Batch enhancement.  Added c_s_email_tc_clnt_lvl1, c_i_email_tc_recipients, c_i_email_tc_records,
|                                      c_i_email_tc_hiv_not_mailed, c_i_email_tc_claims_no_elig, c_s_sub_product_id defaults. 
|                                      Change vt frmly and cellpod test libraries to common test library.
| 30AUG2019  1177601 Jeff Reboulet      Suppress creation of HIV vendor files. Added c_s_separate_HIV_vendor.
| 26AUG2019  1178101  SKonduri          1557 Automation
| 16AUG2019  1120901  EBukowski        Comm Hist Automation - preventing manual loads on or after 8/5/2019 
| 08AUG2019  1161002  SSarkar        	Added global variables c_s_cstm_curr_frmly_ids and c_s_excl_otc_drugs with default as SPACE and N respectively 
| 02AUG2019  1120901  EBukowski        Comm Hist Automation 
| 08MAY2019  1060101 Srini Konduri     Added global variable c_s_mbr_eph_lnk_id as apart of Precluded Changes
| 05MAY2019  1148403 Srini Konduri -   To support Anthem commercial CAGS added a global varibale c_s_commercial with default as N
| 26APR2019  1120901 Jeff Reboulet      Default c_s_effective_date_off_qtr to N; (version 2019.04.03)
| 18MAR2019          EBukowski          Added global variable cntfdp as a part of client intake validation 
  18MAR2019  1033102 Jeff Reboulet      DSA/NCQA migration to DQI. Adding LANG_CD, c_s_valid_lang_cd, c_s_valid_state_cd. (version 2019.03.01)
| 04FEB2019  886004  Jeff Reboulet      FDRO/ACSF/ACF Client Exclusions Consolidation (version 2019.02.01)
| 05OCT2018  858401  EBukowski          Formulary exclusions drug triage changes - new macro variable cntcdp
| 06AUG2018  837001  Jeff Reboulet      Added initialization of consolidated_in. (version 2018.08.01)
| 03AUG2018  837001  Ekaterina Bukowski Scale BOB Consolidation changes, adding c_s_clfilenm_consld and c_s_clfilenm_consld_sheet (version 2018.08.01)
| 08JUN2018  837001  Jeff Reboulet      Add call to m_validation_child_stellent.sas (version 2018.06.01)
| 27OCT2017          Jeff Reboulet      Opioids Enhancement (version 2017.10.xx)
| 06OCT2017  456     Jeff Reboulet      Added c_s_ees_hiv_open_ntwk default to N.
| 16SEP2017          Jeff Reboulet      Cell Pod Review enhancement (version 2017.09.01)
| 12MAY2017 	     Brian Stropich     work request 349 Alternative Messaging Enhancement (version 2017.05.02)
| 24MAR2017 	     Brian Stropich     work request 303 315 332 (version 2017.03.01)
| 11FEB2017  248     Jeff Reboulet      Opioids - Create new macro variables c_s_ignore_medicaid_exclusions & c_s_two_fill_targeting - set default to N.
|                                       c_i_two_fill_days_supply_mail, c_i_two_fill_days_supply_retail and initialize to 0 (version 2017.02.03)                       
| 01SEP2016          Jeff Reboulet      disable c_s_dependents_w_noaddr for campaign 40 (VT)
|                                       Initialize vt_active_eligibility to N. Use prev_mail_ord for quantity limits.
|                                       Target by drug - corrected pharmacy indicators in quantity limits.
| 13JUL2016 69       Jeff Reboulet      Added formulary libnames and variables for cell pod
|                                       messaging from target by drug.
| 11NOV2015          Brian Stropich     added aprimo activity and project number logic - bss
| 19FEB2015          Brian Stropich     Original
+(dashes)--------------------------------------------------------------------------------------HEADER*/

%macro m_environment;

	%global cntddb cntdxl cntddp cntcdb cntfdp cntcxl cntcdp mbrxl cntmdb cntmdup cntph cntpdb cntdup cntinv cntnfdp cntnsdp
            cntexpt tab2_drug_cnt drugparm mbr_extra_fields phys_extra_fields clntparm clntfile mbr_cnt mbr_ind 
            mbr_process_id phys_process_id c_s_sys_cd_sql c_s_limits_sas 
	        phmcy_ind prctr_ind claims_ind drug_ind clnt_incl_ind clnt_excl_ind
	        drug_incl_ind drug_excl_ind
	        drug_incl_ind2 drug_excl_ind2 c_s_drug_where_mddb c_s_claims_where_mddb
	        clntlvl1 clntlvl2 src_cd_x clntparm sel_q
	        phmcy_cnt phmcyparm sel_q c_s_oig_ind
		c_s_mbractivity c_s_mbrticket mbrql phm_incl_ind phm_excl_ind c_s_aprimo_schema sftp_id 
		c_s_acf_table c_s_ai_table c_s_bf_table c_s_fdro_table c_s_form_list_table
		dqi_params frmly_url_file frmly_url_sheet frmly_optn_file frmly_optn_sheet c_s_drug_sheets_ignore frmly_url_cstm_file frmly_url_cstm_sheet;
/*        c_s_clnt_sheetnm c_s_clnt_sheetnm_ote c_s_clnt_sheetnm_otex c_s_frmly_sheetnm;*/
	
	        
	options sastrace=',,,d' sastraceloc=saslog nostsuffix;
	options dbidirectexec;
	options minoperator; /* allow IN operator for %if */
	
	%m_load_ids_passwords;	
	%m_date_timestamp; 
	%m_time_start;
	
	%let c_s_schema_lib     = %str(dwu_edw_rb);
	%let td_id_lib 		= %str(DQI_APP);
	%let td_pw_lib		= %str(Care_2015);	

	/*-----initialize macro indicator variables ------------------------------------------------*/
	%let phmcy_ind  = 0;
	%let prctr_ind  = 0;
	%let claims_ind = 0;
	%let drug_ind   = 0;
	%let clnt_incl_ind = 0;
	%let clnt_excl_ind = 0;
	%let drug_incl_ind = 0;
	%let drug_excl_ind = 0;
	%let drug_incl_ind2 = 0;
	%let drug_excl_ind2 = 0;
	%let c_s_drug_where_mddb = ;
	%let c_s_claims_where_mddb = ;
	%let c_s_oig_ind = 0;
	%let prior_cmpgn_cnt = 0; 
	%let mbrql = 0;
	%let phm_incl_ind = 0;
	%let phm_excl_ind = 0;
	%let c_s_aprimo_schema = %str(pbmv_dh);		/* DO NOT CHANGE - production Aprimo schema data views for data targeting */
	%let sftp_id=%str(EACTT);                       /* webtransport.caremark.com team id */
	%let frmly_url_file = %str(Formulary List);   /* name of client intake form - xls (add values if no client level 1 - c_s_clnt_lvl1)*/
    %let frmly_url_sheet = %str(Formulary List);	   	/* sheet to use from client input file */
	%let frmly_optn_file = %str(FDRO_Formulary_Option);   /* name of client intake form - xls (add values if no client level 1 - c_s_clnt_lvl1)*/
    %let frmly_optn_sheet = %str(Sheet1);	   	/* sheet to use from client input file */
	%let frmly_url_cstm_file =%str(Aetna TC URL Mapping);
	%let frmly_url_cstm_sheet =%str(Sheet1);
	%let c_s_drug_sheets_ignore= 
		%str('IMP SHEET','DEFINITIONS','COLUMN DEFINITIONS','KEY','CHANGES','SSB+MSB HISTORY SAVED','INFORMATION');

	/*-----convert ctl variables with quotes  --------------------------------------------------*/
	%if %symexist(c_s_sys_cd) %then %do;
	  %m_convert_variables_quotes(macro_string=&c_s_sys_cd., macro_variable=c_s_sys_cd_sql);
	%end;
	%if %symexist(c_s_degree) %then %do;
	  %m_convert_variables_quotes(macro_string=&c_s_degree., macro_variable=c_s_degree);
	%end;
	%if %symexist(c_s_prctr_excl) %then %do;
	  %m_convert_variables_quotes(macro_string=&c_s_prctr_excl., macro_variable=c_s_prctr_excl); 
	%end;
	%if %symexist(c_s_phmcy_dspns_type) %then %do;
	  %m_convert_variables_quotes(macro_string=&c_s_phmcy_dspns_type., macro_variable=c_s_phmcy_dspns_type); 
	%end; 
	%if %symexist(c_s_email_to) %then %do;
	  %m_convert_variables_quotes(macro_string=&c_s_email_to., macro_variable=c_s_email_to); 
	%end; 
	%if %symexist(c_s_email_support) %then %do;
	  %m_convert_variables_quotes(macro_string=&c_s_email_support., macro_variable=c_s_email_support); 
	%end; 
	
	%if &c_s_dqi_campaign_id. = 28 %then %do;
		%if %symexist(c_s_program_type) %then %do;
		  %global c_s_program_unquote;
		  %let c_s_program_unquote = %sysfunc(translate(&c_s_program_type,'_',','));
		  %m_convert_variables_quotes(macro_string=&c_s_program_type., macro_variable=c_s_program_type);
		%end;
		%if %symexist(c_s_client_status) %then %do;
		  %m_convert_variables_quotes(macro_string=&c_s_client_status., macro_variable=c_s_client_status);
		%end;
	%end;	
	
	%if &c_s_dqi_campaign_id. = 36 %then %do;
		%if %symexist(c_s_program_type) %then %do;
		  %global c_s_program_unquote;
		  %let c_s_program_unquote = %sysfunc(translate(&c_s_program_type,'_',','));
		  %m_convert_variables_quotes(macro_string=&c_s_program_type., macro_variable=c_s_program_type);
		%end;
	%end;

	

	/*-----validate LINUX or UNIX Sever      ----------------------------------------------------*/	 
	data _null_;
	sashost="&_SASHOSTNAME.";
	sashost=upcase(compress(sashost,"'"));
	call symput('sashost',left(trim(sashost)));
	run;

	%put NOTE:  sashost = &sashost. ;
	
	%if &sashost. = PRDSAS2 %then %do;
	  %put ERROR: DQI execution is beign performed on SASNODE - Unix and not SAS Grid - Linux environment ; 
	  %m_abend_handler(abend_report=%str(DQI execution is beign performed on SASNODE - Unix and not SAS Grid - Linux environment.  Please change SAS EG connections.)); 
	%end;
	%else %do; 	
	%end;	

	/*-----create aprimo - activity + project # ------------------------------------------------*/	
	%macro create_activity_number(var=);

	      %if %symexist(&var) %then %do;
	      %end;
	      %else %do;
	      
		/**%global &var. ;
		%let &var=&c_s_ticket.; **/
		
		%m_abend_handler(abend_report=%str(CTL program is missing - c_s_aprimo_activity ));
	      %end;

	%mend create_activity_number;
	
	%create_activity_number(var=c_s_aprimo_activity);
	%put NOTE: c_s_ticket = &c_s_ticket. ;
	%put NOTE: c_s_aprimo_activity = &c_s_aprimo_activity. ;
	
	
	/*-----create macro variable values --------------------------------------------------------*/	
	%macro create_macro_variable_value(var=, default=N);

	    %if %symexist(&var) %then %do;
	    %end;
	    %else %do;
			%global &var. ;
			%if &default. = N %then %do;
			  %let &var=N; 
			%end;
			%else %if &default. = Y %then %do;
			  %let &var=Y; 
			%end;
			%else %if &default. = SPACE %then %do;
			  %let &var=%str( ); 
			%end;
			%else %do;
			  %let &var=&default.; 
			%end;
        %end;
	      
	    %put NOTE: &&var. = &&&var.;

	%mend create_macro_variable_value;
	
	%create_macro_variable_value(var=c_s_ignore_medd_exclusions);
	%create_macro_variable_value(var=c_s_clnt_spcfc_cmgpn);
	%create_macro_variable_value(var=c_s_external_claims, default=Y);
	%create_macro_variable_value(var=c_s_mbractivity);
	%create_macro_variable_value(var=c_s_mbrticket);
	%create_macro_variable_value(var=c_s_dqi_development_team);
	%create_macro_variable_value(var=c_s_compound_suppression);
	%create_macro_variable_value(var=c_s_prctr_chk);            /** default to N for TBD + FDRO + ACF ACSF **/
	%create_macro_variable_value(var=c_s_extra_fields, default=SPACE);
	%create_macro_variable_value(var=c_s_formulary_cell_pod_msg);	
	%create_macro_variable_value(var=c_s_mbr_mail_fields, default=Y);
	%create_macro_variable_value(var=c_s_create_exception, default=Y);
	%create_macro_variable_value(var=c_s_pharmacy_overlay);
	%create_macro_variable_value(var=c_s_pharmacy_overlay_invalidnabp);
	%create_macro_variable_value(var=c_s_pharmacy_overlay_abend, default=Y);
	%create_macro_variable_value(var=c_s_pharmacy_overlay_nulls, default=N);
	%create_macro_variable_value(var=c_s_ignore_medicaid_exclusions, default=N);
	%create_macro_variable_value(var=c_s_two_fill_targeting, default=N);
	%create_macro_variable_value(var=c_i_two_fill_days_supply_mail, default=120);
	%create_macro_variable_value(var=c_i_two_fill_days_supply_retail, default=45);
	%create_macro_variable_value(var=c_s_cmctn_history_drug, default=N);
	%create_macro_variable_value(var=c_s_cmctn_history_day, default=N);
	%create_macro_variable_value(var=c_s_cmctn_history_ticket, default=N);
	%create_macro_variable_value(var=xrcmod, default=DQI Campaign);
	%create_macro_variable_value(var=c_s_cmctn_history_crossover, default=N); 
	%create_macro_variable_value(var=c_s_override_alt_message, default=N); 
	%create_macro_variable_value(var=c_s_disruption, default=N);
	%create_macro_variable_value(var=c_s_consolidate, default=N);
	%create_macro_variable_value(var=c_s_consolidate_list, default=N);	
	%create_macro_variable_value(var=c_s_clt_nocc, default=Y);
	%create_macro_variable_value(var=c_s_cellpod_excl_message, default=%str(DRUG NOT COVERED; SEE BELOW WEBSITE FOR ALTERNATIVES));
	%create_macro_variable_value(var=c_s_cellpod_excl_pa_message, default=%str(DRUG IS ONLY COVERED WITH PRIOR APPROVAL; SEE BELOW WEBSITE FOR ALTERNATIVES));
	%create_macro_variable_value(var=c_s_suppress_alt_msg_on_vendor, default=N);
	%create_macro_variable_value(var=c_s_client_level1, default=N);
	%create_macro_variable_value(var=minor_ind_check, default=N);
	%create_macro_variable_value(var=minor_ind_check_aetna, default=Y);
	%create_macro_variable_value(var=c_s_dqi_bob, default=N);
	%create_macro_variable_value(var=c_s_quantity_limits_percent, default=1);
	%create_macro_variable_value(var=c_s_stellent_product, default=NOMDG);
	%create_macro_variable_value(var=email_ext_transactions, default=Y);
	%create_macro_variable_value(var=ctl_cvs_retail, default=N);
	%create_macro_variable_value(var=ctl_cvs_specialty, default=N);
	%create_macro_variable_value(var=ctl_cvs_mail, default=N);
	%create_macro_variable_value(var=c_s_ees_hiv_open_ntwk, default=N);
	%create_macro_variable_value(var=c_i_npschars, default=N);
	%create_macro_variable_value(var=c_i_npschars_variables, default=N);
	%create_macro_variable_value(var=c_s_prior_auth_required, default=%str(Prior Authorization Required));/*opioids*/
	%create_macro_variable_value(var=c_s_opioid_limit_type, default=SPACE);/*opioids*/
	%create_macro_variable_value(var=c_s_opioid_pa, default=SPACE);/*opioids*/
	%create_macro_variable_value(var=c_s_use_zero_quantity_limit_text, default=N); /* opioids */
	%create_macro_variable_value(var=c_s_opioid_retail_qty_time, default=25); /* opioids */
	%create_macro_variable_value(var=c_s_opioid_mail_qty_time, default=75); /* opioids */
	%create_macro_variable_value(var=c_s_campaign_overlap_exclusion, default=N); /* opioids */
	%create_macro_variable_value(var=c_s_campaign_overlap_project_id, default=SPACE); /* opioids */
	%create_macro_variable_value(var=c_s_campaign_overlap_drug_type, default=SPACE);     /*opioids*/
	%create_macro_variable_value(var=c_s_cstm_curr_frmly_ids, default=SPACE);/*BOB - PDL and ACSF tier compare*/
	%create_macro_variable_value(var=c_s_excl_otc_drugs, default=N);/*BOB - PDL and ACSF tier compare*/
	%create_macro_variable_value(var=c_s_run_waterfall, default=N);
	%create_macro_variable_value(var=c_s_cmctn_move, default=NO);
	%create_macro_variable_value(var=c_s_external_file, default=);
	%create_macro_variable_value(var=c_s_stellent_parent, default=SPACE);     /*vt single communication*/
	%create_macro_variable_value(var=c_s_mandatory_cmpgn, default=Y);
	%create_macro_variable_value(var=consolidated_in, default=SPACE);         /*vt single communication*/
	%create_macro_variable_value(var=c_s_clfilenm_consld, default=SPACE);
	%create_macro_variable_value(var=c_s_clfilenm_consld_sheet, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket2, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket3, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket4, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket5, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket6, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket7, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket8, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket9, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket10, default=SPACE);
	%create_macro_variable_value(var=c_s_ticket_p, default=SPACE);
	%create_macro_variable_value(var=c_s_aligned_override, default=SPACE);
	%create_macro_variable_value(var=c_s_fdro_type_override, default=SPACE);
	%create_macro_variable_value(var=c_s_newtomarket_acf_override, default=SPACE);
	%create_macro_variable_value(var=c_s_newtomarket_bf_override, default=SPACE);
	%create_macro_variable_value(var=c_s_newtomarket_fdro_override, default=SPACE);
	%create_macro_variable_value(var=c_s_set_dqi_ind_where,default=SPACE); /* DSA/NCQA */
	%create_macro_variable_value(var=c_s_clnt_intake_mbr_lang_cd,default=N);    
	%create_macro_variable_value(var=c_s_set_dqi_ind_7,default=SPACE);    /* DSA/NCQA */
	%create_macro_variable_value(var=c_s_cmctn_history_name_dob,default=N); 
	%create_macro_variable_value(var=c_s_cmctn_history_pharmacy,default=N); 
	%create_macro_variable_value(var=c_s_effective_date_off_qtr,default=N);
	%create_macro_variable_value(var=c_s_commercial,default=N); 
	%create_macro_variable_value(var=c_s_mbr_eph_lnk_id,default=SPACE);
	%create_macro_variable_value(var=c_s_separate_hiv_vendor,default=N);
    %create_macro_variable_value(var=c_s_prctr_mailing,default=N);
	%create_macro_variable_value(var=c_s_cmctn_hist_load_force,default=N);
	%create_macro_variable_value(var=c_s_1557_ind,default=Y);
    %create_macro_variable_value(var=c_s_email_tc_clnt_lvl1,default=SPACE);
    %create_macro_variable_value(var=c_i_email_tc_recipients,default=0);
    %create_macro_variable_value(var=c_i_email_tc_records,default=0);
    %create_macro_variable_value(var=c_i_email_tc_hiv_not_mailed,default=0);
    %create_macro_variable_value(var=c_i_email_tc_claims_no_elig,default=0);
    %create_macro_variable_value(var=c_s_sub_product_id,default=0);
	%create_macro_variable_value(var=c_s_anthem_allow_90day_1fill, default=Y);
    %create_macro_variable_value(var=c_s_allow_1fill_days, default=91); 
	%create_macro_variable_value(var=c_s_run_clnt_rslvd_dups,default=Y);
	%create_macro_variable_value(var=c_s_clnt_sheetnm,default=SPACE);
	%create_macro_variable_value(var=c_s_clnt_sheetnm_ote,default=SPACE);
	%create_macro_variable_value(var=c_s_clnt_sheetnm_otex,default=SPACE);
	%create_macro_variable_value(var=c_s_frmly_sheetnm,default=SPACE);
	%create_macro_variable_value(var=c_s_drug_sheetnm,default=SPACE);
	%create_macro_variable_value(var=c_s_mbr_sheetnm,default=SPACE);
	%create_macro_variable_value(var=c_s_cat_incl_filenm,default=SPACE);
	%create_macro_variable_value(var=c_s_cat_incl_sheetnm,default=SPACE);
	%create_macro_variable_value(var=c_s_nocc_ctl_sheetnm,default=SPACE);
	%create_macro_variable_value(var=c_s_carrier_to_carrier,default=N);
	%create_macro_variable_value(var=c_s_drug_optional,default=N);
	%create_macro_variable_value(var=c_s_bob_url_mapping,default=STANDARD);
        %create_macro_variable_value(var=c_s_use_mddb,default=N);

	
	%if (&c_s_dqi_campaign_id. in 40 52 53) %then %do; /* VT or Opioids */
		%create_macro_variable_value(var=c_s_opioid_aligned,default=N);
		%create_macro_variable_value(var=c_s_opioid_daily_dose_bypass,default=N);
	%end;

	/*-----set up ticket for prescriber only campaign --------------------------------------------------*/


		%if not (&c_s_ticket. > '') %then %do;
	  		%put ERROR: Ctl file issue: Need to fill in c_s_ticket. If ONE project in activity, populate with that ONLY project ID. If MEMBER and PRESCRIBER project in activity, populate with MEMBER project ID ; 
			%m_abend_handler(abend_report=%str(Need to fill in c_s_ticket. If ONE project in activity, populate with that ONLY project ID. If MEMBER and PRESCRIBER project in activity, populate with MEMBER project ID)); 
		%end;

	 /* if prescriber mailing = Y and c_s_ticket_p isn't populated, abend */
		%if not (&c_s_ticket_p. > '') and not (&c_s_ticket2. > '') and &c_s_prctr_mailing. = Y %then %do;
	  		%put ERROR: Ctl file issue: Need to fill in c_s_ticket_p. It is required when targeting PRESCRIBERS ; 
			%m_abend_handler(abend_report=%str(Ctl file issue: Need to fill in c_s_ticket_p. It is required when targeting PRESCRIBERS)); 
	%end;
		

	
	/*-----retail and mail limit calculation  --------------------------------------------------*/
	%if &c_s_dqi_campaign_id. = 40 %then %do; /* Vendor Transition */
	  %let c_s_limits_sas=%str(if ((unit_qty/days_sply) > (retail_qty_limit/retail_qty_time*&c_s_quantity_limits_percent.) and PREV_MAIL_ORD_IND ^= 'Y') 
	       or ((unit_qty/days_sply) > (mail_qty_limit/mail_qty_time*&c_s_quantity_limits_percent.) and PREV_MAIL_ORD_IND = 'Y'); );
	  %end;
	%else %do;
	  %let c_s_limits_sas=%str(if ((unit_qty/days_sply) > (retail_qty_limit/retail_qty_time*&c_s_quantity_limits_percent.) and dlvry_systm_cd='R') 
	       or ((unit_qty/days_sply) > (mail_qty_limit/mail_qty_time*&c_s_quantity_limits_percent.) and dlvry_systm_cd='M'); );
	%end;

	/* SASDOC --------------------------------------------------------------------------------------
	|  40 - Vendor Transition
	|  52 - Opioids 
	|  53 - Opioids VT
	+---------------------------------------------------------------------------------------------*/
	%if (&c_s_dqi_campaign_id. in 40 52 53) %then %do; 
		%create_macro_variable_value(var=c_s_dependents_w_noaddr, default=N);
	%end;
	%create_macro_variable_value(var=vt_active_eligibility,default=N);

	/*-----libnames ----------------------------------------------------------------------------*/
	libname working ("&c_s_filedir");
	libname saslib  ("&c_s_maindir");

	libname altdrug  '/anr_dep/fs03/cfc/druglists/pdl_messaging/msg_db';
	libname bob_cons  '/anr_dep/dqi/prod/requests/bob_consld';

	%if &c_s_dqi_production. = Y %then %do;
		libname commhist '/anr_dep/dqi/prod/data/cmctn_history/posted';
		libname posted   '/anr_dep/dqi/prod/data/cmctn_history/posted';
		libname mailed   '/anr_dep/dqi/prod/data/cmctn_history/mailed';
		libname vt_disr  '/anr_dep/dqi/prod/requests/vt_single_disruption';
		libname frmly    '/anr_dep/fs03/cfc/druglists/rxclm_formulary/frmly_db';
		%if %symexist(c_s_frmly_update_dir) %then %do;
			%if &c_s_frmly_update_dir > ' ' %then %do;
				libname frmly clear;
				libname frmly %str("/anr_dep/fs03/cfc/druglists/rxclm_formulary/frmly_db/&c_s_frmly_update_dir.");
			%end;
		%end;	
		libname thercat  '/anr_dep/fs03/cfc/druglists/cell_pod_messaging/msg_db';
		%let dqi_params = %str(/anr_dep/dqi/prod/data/dqi_params);
		%let c_s_acf_table=DQI_DRUG_ACF;
        %let c_s_ai_table=DQI_DRUG_AI;
        %let c_s_bf_table=DQI_DRUG_BF;
        %let c_s_form_list_table=DQI_FORMULARY_LIST;
        %let c_s_fdro_table=DQI_DRUG_FDRO;
	%end;
	%else %do;
		libname commhist '/anr_dep/dqi/dev1/data/cmctn_history/posted';
		libname posted   '/anr_dep/dqi/dev1/data/cmctn_history/posted';
		libname mailed   '/anr_dep/dqi/dev1/data/cmctn_history/mailed';
		libname vt_disr  '/anr_dep/dqi/tst/requests/vt_single_disruption';
		libname frmly    '/anr_dep/fs03/cfc/druglists/rxclm_formulary/frmly_db/tst';
		libname thercat  '/anr_dep/fs03/cfc/druglists/cell_pod_messaging/msg_db/tst';
		%let dqi_params = %str(/anr_dep/dqi/dev1/data/dqi_params);
		%let c_s_acf_table=DQI_DRUG_ACF_TST;
        %let c_s_ai_table=DQI_DRUG_AI_TST;
        %let c_s_bf_table=DQI_DRUG_BF_TST;
        %let c_s_form_list_table=DQI_FORMULARY_LIST_TST;
        %let c_s_fdro_table=DQI_DRUG_FDRO_TST;
	%end;

	/**no need for a libname since edw is a sql pass through **/ 
	/**libname edw&dsnme oracle user=&db_id password="&db_pw" path=edw schema=dss_clin;**/

	%if %symexist(c_s_tdtempx) %then %do;
		libname &c_s_tdtempx. teradata user=&td_id. password="&td_pw"
				  server=&c_s_server.
				  database=&c_s_tdtempx.             
				  connection=unique
				  defer=yes 
				  fastload=yes;
	%end;
	
	%if %symexist(c_s_schema) %then %do;			  
		libname dwu_edw teradata user=&td_id_lib password="&td_pw_lib"
				  server=&c_s_server
				  database=&c_s_schema_lib             
				  connection=unique
				  defer=yes 
				  fastload=no
				  access=readonly;
	%end;
	
	%if %symexist(c_s_comm_file) %then %do;	
		libname commhist "&c_s_comm_file" ;
	%end;

	/*-----validate dqi campaign ID values   ----------------------------------------------------*/	
	%let cnt_campaign_id= 0;

	proc sql noprint;
	select count(*) into: cnt_campaign_id separated by ''
	from &c_s_tdtempx..dqi_campaigns
	where dqi_campaign_id = &c_s_dqi_campaign_id.;
	quit;

	%put NOTE:  cnt_campaign_id = &cnt_campaign_id. ;
	
	%if &cnt_campaign_id. = 0 %then %do;
	  %put ERROR:  DQI campaign ID of &c_s_dqi_campaign_id. is valid ;
	  %m_abend_handler(abend_report=%str(DQI campaign ID of &c_s_dqi_campaign_id. is an invalid value.));
	%end;
	%else %do;
	  %put NOTE:  DQI campaign ID of &c_s_dqi_campaign_id. is a valid value.;	
	%end;

	/*----- 'child' stellent validation   --------------------------------------------------*/
	%if %upcase(&c_s_disruption)=Y 		
	and %upcase(&c_s_consolidate)^=Y	/* not for consolidation 'parent' processing */
	and &c_s_clfilenm_consld = ''       /*not for BOB consolidation*/
	and %upcase(&c_s_run_validation) = Y %then %do ;
	   %m_validation_child_stellent;
	%end;

	/*----- 'parent' stellent edit   --------------------------------------------------*/
	%if &c_s_stellent_parent > ' ' 
	and %upcase(&c_s_disruption)^=Y %then %do;
		%m_abend_handler(abend_report=%str(Must have c_s_disruption=Y when c_s_stellent_parent is populated.  Check values.));	
	%end;
	
	/*----- c_s_valid_lang_cd population --------------------------------------------------*/
	%global c_s_valid_lang_cd;    

	proc sql noprint;
		select distinct "'"||strip(edw2_val)||"'" into: c_s_valid_lang_cd separated by ','
		  from dwu_edw.V_CD_EDW2_XREF
		 where trgt_col_nm  = 'LANG_CD'
		   and feed_id = 'RXC_MBR_ACCT'
		   and edw2_val<>'UNKW';
	quit;
	%put NOTE: c_s_valid_lang_cd: &c_s_valid_lang_cd.;

	/*----- c_s_valid_state_cd population --------------------------------------------------*/
	%global c_s_valid_state_cd;    

	proc sql noprint;
		select distinct "'"||strip(statecode)||"'" into: c_s_valid_state_cd separated by ','
		  from sashelp.zipcode;
	quit;

	/*-- clean up validation xlsx files -------------------------------------------------------*/

	filename vldclnt "&c_s_filedir./validate_client_&tdname..xlsx";
	data _null_;
  		if (fexist('vldclnt')) then 
      	rc = fdelete('vldclnt');
		run;
	filename vldclnt clear;

	filename vlddrug "&c_s_filedir./validate_drug_&tdname..xlsx";
	data _null_;
  		if (fexist('vlddrug')) then 
      	rc = fdelete('vlddrug');
		run;
	filename vlddrug clear;

	filename vldmbr "&c_s_filedir./validate_mbr_&tdname..xlsx";
	data _null_;
  		if (fexist('vldmbr')) then 
      	rc = fdelete('vldmbr');
		run;
	filename vldmbr clear;
	%m_format_custom_frmly;
	
	/*-----process control varaiable values   --------------------------------------------------*/
	data _null_;
	  mbr_process_id=round(1000000000000*ranuni(0));
	  phys_process_id=round(1000000000000*ranuni(0));
	  call symput('mbr_process_id',left(mbr_process_id)); 
	  call symput('phys_process_id',left(phys_process_id)); 
	run;
	
	options nomprint nomlogic nosymbolgen ;

	%put NOTE: ***********************************************************************;
	%put NOTE: ** Macro Variables ****************************************************;
	%put NOTE: ***********************************************************************;
	%put NOTE: log  directory  = &c_s_maindir. ;
	%put NOTE: work directory  = %sysfunc(pathname(work)) ;
	%put NOTE: mbr_process_id  = &mbr_process_id. ;
	%put NOTE: phys_process_id = &phys_process_id. ;
	%put NOTE: sysjobid        = &sysjobid. ;
	%put NOTE: sysuserid       = &sysuserid. ;	
	%put NOTE: ***********************************************************************;	
	%put _all_;
	%put NOTE: ***********************************************************************;
	
	options mprint mlogic symbolgen ;
	%if %sysfunc(exist(&c_s_tdtempx..mbr_clients_&tdname.)) %then %do;
	  %m_table_drop_passthrough(data_in=&c_s_tdtempx..mbr_clients_&tdname.); 
	%end;
	%if %sysfunc(exist(&c_s_tdtempx..mbr_client_intake_&tdname.)) %then %do;
	  %m_table_drop_passthrough(data_in=&c_s_tdtempx..mbr_client_intake_&tdname.); 
	%end;
	
	%if (&c_s_dqi_campaign_id in 99 90 91) %then %do; 
	%end;
	%else %do;
		%if &c_s_run_validation. = N 
		and &c_s_dqi_production = Y %then %do;
			filename xemail email 
			to=('ekaterina.bukowski@cvshealth.com', 'jeffrey.reboulet@cvshealth.com')
			Subject = "DQI Campaign - Validations Turned OFF for Targeting" ;
	
			data _null_;
			file xemail;
			put "DQI Campaign Concern - Ticket &c_s_ticket. has its c_s_run_validation = N" ; 
			put " ";
			put "Targeting information:"
			/  "   name:    &c_s_mailing_name."
			/  "   program: &c_s_program. "
			/  "   aprimo activity ID:  &c_s_aprimo_activity. "
			/  "   aprimo project ID:  &c_s_ticket. "
			/  "   campaign ID:  &c_s_dqi_campaign_id. "
			/  "   project: &c_s_proj. " 
			/  "   user: &_clientusername. " 
			/  "   client:  &c_s_client_nm. "
			/  "   campaign directory:  &c_s_filedir. "
			/  "   log file:  &c_s_log. " 	;
			run;	
		%end;
		
		%if &c_s_run_waterfall. = N 
		and &c_s_dqi_production = Y %then %do;
			filename xemail email 
			to=('ekaterina.bukowski@cvshealth.com', 'jeffrey.reboulet@cvshealth.com')
			Subject = "DQI Campaign - Waterfall Turned OFF for Targeting" ;
	
			data _null_;
			file xemail;
			put "DQI Campaign Concern - Ticket &c_s_ticket. has its c_s_run_waterfall = N" ; 
			put " ";
			put "Targeting information:"
			/  "   name:    &c_s_mailing_name."
			/  "   program: &c_s_program. "
			/  "   aprimo activity ID:  &c_s_aprimo_activity. "
			/  "   aprimo project ID:  &c_s_ticket. "
			/  "   campaign ID:  &c_s_dqi_campaign_id. "
			/  "   project: &c_s_proj. " 
			/  "   user: &_clientusername. " 
			/  "   client:  &c_s_client_nm. "
			/  "   campaign directory:  &c_s_filedir. "
			/  "   log file:  &c_s_log. " 	;
			run;	
		%end;	
	%end;

%mend m_environment;
