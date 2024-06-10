/*HEADER --------------------------------------------------------------------------------(dashes)
| SAS Job:   ctl_formulary_cfc 
|            (Clinical Formulary Communication - with cell pod messaging)
|
| LOCATION: /anr_dep/dqi/prod/prg/core
|
| PURPOSE:  Business purpose of the report is to inform members when a drug they are taking 
|           is changing on the formulary
| 
| LOGIC:    User can give the required input into the control file and that calls the product 
|           specific macro and all other core macros
|
| TABLES:   n/a
|
| INPUT:    Provided by User as per request
|
| OUTPUT:   n/a
|
+--------------------------------------------------------------------------------------------------------------------------------------------
|            Work
| Date       Request     Programmer           Description 
+--------------------------------------------------------------------------------------------------------------------------------------------
| 21MAR2022  1681902     Ken Avery            Update c_s_lastclm params.  Replace obsolete GCN option with PHARM for member/pharmacy uniqueness. 
| 04JAN2021  1253401     EBukowski            Triage intake automation - mbr sheet removal
| 25JUL2020  1202902     EBukowski            Triage intake automation - drug and client sheet removal
| 02AUG2019  1120901     EBukowski            Comm Hist Automation - c_s_ticket_p added
| 24MAR2017  303 315 332 Brian Stropich       work request 303 315 332 (version 2017.03.01)
| 13JUL2016  Enh. 69     Jeff Reboulet        Added cell pod messaging functionality
| 02OCT2014              Jeff Crump           Modified to include variables across all modules
| 22SEP2014                                   Prashant J
|           .
+(dashes)--------------------------------------------------------------------------------------HEADER*/

/*SASDOC---------------------------------------------------------------------------------------(dashes)
|  This module defines the macro variables and libraries and then triggers the main program
|   
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_core_dir=/anr_dep/dqi/prod/core;			/* DO NOT CHANGE - location of dqi programs for targeting */

options mautosource sasautos = ("&c_s_core_dir." "/anr_dep/dqi/prod/sasmacro" sasautos); 
options mprint mlogic symbolgen source2 sastrace=',,,d' sastraceloc=saslog;	/* DO NOT CHANGE - standard SAS options */

/*----------------------------------Directory Variables---------------------------------*/
%let c_s_mailing_name=%str(Formulary CFC);			/* campaign mailing name */
%let c_s_rootdir = %str(/anr_dep/fs02/dqi/prod/requests);		/* root directory for the aprimo request  */
%let c_s_program = %str(bsc);				/* next 1st level down from root directory  */
%let c_s_proj = %str(1764801_1697405_cpy); 				/* next 2md level down from program directory  */
%let c_s_aprimo_activity = 1764801;				/* aprimo activity ID - parent ID for aprimo request (6 digit numeric value )*/
%let c_s_ticket = 1697405;		/* If ONE project in activity, populate with that ONLY project ID; if MEMBER and PRESCRIBER project in activity, populate with MEMBER project ID*/
%let c_s_ticket_p = XXXXXX;		/* PRESCRIBER aprimo project ID - required if targeting PRESCRIBERS; if no PRESCRIBER project, use %let c_s_ticket_p = ;*/
%let c_s_dqi_campaign_id = 2;   				/* dqi campaign id - dss_cea2.dqi_campaigns */

%let c_s_dir1 = &c_s_program.;                         		/* 1st level for library or root directory  */
%let c_s_dir2 = &c_s_proj.;                            		/* 2nd level for library or root directory  */
%let c_s_maindir = &c_s_rootdir./&c_s_dir1./&c_s_dir2./;	/* combines the root +  1st level + 2nd level for library  */
%let c_s_filedir = %str(&c_s_rootdir./&c_s_program./&c_s_proj.);/* combines the root +  program level + project level for library  */
%let c_s_log =ctl_formulary_cfc_&c_s_ticket..log;            	/* log file name for the campaign  */

/*----------------------------------Stellent Letter templates---------------------------*/
%let c_s_stellent1 = %str(106-55334A); 				/* stellent ID for the participant letter format XXXX-23702XX */
%let c_s_stellent2 = %str(87-NA); 				/* stellent ID for the physician letter */


/*----------------------------------CFC Program Specific Variables ---------------------*/
%let c_s_email_alerts = Y;					/* option to receive email alerts */
%let c_s_email_to = %str(Bennett.Curiel@CVSHealth.com);		/* users email address for email alerts */
%let c_s_email_subject = %str(Formulary CFC Program Results_&c_s_aprimo_activity._&c_s_ticket.);	/* email subject for email alerts */

/*---------------------------------Teradata Database Variables--------------------------*/
%let td_uid=%str(z165497);			/* users ID for teradata */
%let use_dqi_id=Y;				/* option to use the user ID or production ID for teradata */ 
%let tdname = %trim(t&c_s_ticket.);		/* prefix for the temporary table and dataset names */
%let c_s_schema = %str(dwu_edw);		/* DO NOT CHANGE - production schema data views for data targeting */
%let c_s_server = %str(prdedw1);		/* DO NOT CHANGE - production server */
%let c_s_tdtempx = %str(dss_cea2);		/* mco schema for storing temporary tables */

/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  Client Data Variables:
|
|  RXCLAIM levels of hierarchy. 
|    1. carrier
|    2. account 
|    3. group
|
+(dashes)---------------------------------------------------------------------------------------SASDOC*/
%let c_s_sys_cd    = %str(X); 			/* adjudication platform - X R Q */	  
%let c_s_clnt_lvl1 = %str();			/* client level 1 - carrier (add values if no client intake form - c_s_clfilenm) */
%let c_s_clnt_lvl2 = %str();			/* client level 2 - account */
%let c_s_clnt_lvl3 = %str();			/* client level 3 - group   */
%let c_s_client_nm = %str(Blue Shield of CA);			/* client name to be used in vendor mailing file */
%let c_s_src_client_nm = %str(P);		/* Source of Client Name: values A = ALGN tbl, F - Client File, P - Client Parm */
%let c_s_aetna_include = %str(N);		/* Y indicator to include members under the Aetna clients in the mailing */ 
%let c_s_cvs_include   = %str(N);		/* Y indicator to include members under the CVSCaremark clients in the mailing */


/*----------------------------------File Name and Sheet Name Variables-----------------------*/
%let c_s_dirfilenm = %str(Legacy Full True Accumulation - APR 2022_ UPDATED BSC Custom);				/* name of drug intake form - xlsx */ 

%let c_s_clfilenm = %str(Client Intake Form_August 1 Renewals_052522 revised);	               		/* name of client intake form - xlsx (add values if no client level 1 - c_s_clnt_lvl1)*/

%let c_s_mbfilenm = %str(Copy of form-MemberTargeting-8.1.22 renewal);	     				/* name of member intake form - xlsx */

%let c_s_mbr_i_e=%str(E);					/* Put 'E' if Exclusion and 'I' if Inclusion*/
%let c_s_prfilenm = %str();		                    	/* name of prescriber input file */
%let c_s_prctr_sheetnm = %str();	                     	/* sheet to use from prctr input file */
%let c_s_phmcy_filenm = %str();  		    		/* name of pharmacy input file */
%let c_s_phmcy_sheetnm = %str();	       			/* sheet to use from phmcy input file */


/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  Variables to indicate running Validation on the intake form and waterfall report.
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_run_validation       =Y;				/* to run the report validation - Y or N */
%let c_s_run_waterfall        =Y;				/* to run the waterfall - Y or N */
%let c_s_load_waterfall_table =Y;				/* to load the waterfall validation records in table */
%let c_s_del_temptable        =Y;				/* option of delete or keep teradata temp tables */
%let c_s_dqi_production	      =Y;				/* to run within the production environment or development environment */

/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  Variables to indicate what additional data needs to be retrieved.
|  Note: HITP,MOP,NP,SCP = other mail service
|
|  c_s_phmcy_dspns_type code (these values are available on MetaCenter):
|
|    CP   - Clinic Pharmacy
|    CRP  - Community/Retail Pharmacy
|    DMES - Durable Medical Equipment Services
|    DP   - Dispensing Physician
|    DSP  - Department Store Pharmacy
|    GSP  - Grocery Store Pharmacy
|    HITP - Home Infusion Therapy Provider 
|    HMOP - HMO Pharmacy
|    IHS  - Indian Health Service
|    IP   - Institutional Pharmacy
|    LTCP - Long Term Care Pharmacy
|    MCOP - Managed Care Organization(HMO)Pharmacy
|    MOP  - Mail Order Pharmacy
|    MP   - Military Pharmacy
|    NP   - Nuclear Pharmacy
|    OTHR - Other
|    SCP  - Specialty Care Pharmacy
|    VP   - VA Pharmacy
|
+(dashes)---------------------------------------------------------------------------------------SASDOC*/
%let c_s_prctrdat = Y;				/* set to Y if prctr data is required, else set to N */
%let c_s_phmcydat = Y;				/* set to Y if phmcy data is required, else set to N */
%let c_s_phmcy_dspns_type=%str(HITP,MOP,NP,SCP);/* DO NOT CHANGE - sql where - this sets the oth_ms indicator which is the standard definition */


/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  Variables to be used for comparison in the queries and the business logic:
|  NOTE:  sql where - ALL dates needs single quotes around values 
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_beg_date = %str('2021-01-01');		/* claims begin date for the claims targeting */
%let c_s_end_date = %str('2022-05-26');		/* claims end   date for the claims targeting */
%let c_s_elg_end  = %str('2022-05-27');		/* eligibility  date for the the member coverage and the client level 3 */
%let c_s_maildate = %str('2022-08-01');		/* effective    date for the vendor file */
%let c_s_age_date = %str( );		        /* member age   date for calculating the members age - if missing then date uses today */


/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  The following variables will normally not change from run to run
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_elig_dt_req = Y;			/* should be Y if elg_end has a value -or- N for todays date for eligibility -or- X for no eligiblity validation */
%let c_s_future_elig = N;			/* set to Y to include mbrs whose elig starts after c_s_elg_end */
%let c_s_minor_chk = N;				/* if Y will exclude minors */
%let c_s_zipexcl = N;               	/* exclude zips due to natural disasters */
%let c_i_minor_age = 18;			/* The age to use to be considered a minor   */
%let c_i_medb_age = 65;				/* The age to use to be considered for MEDB  */
%let c_i_senior_age = 65;			/* The age to use to be considered a senior  */


/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  Last claim sorting exclusion routine for keeping all claims, one claim or last claim for drug code.
|    1. ALL - client member claim dispense
|    2. ONE - client member
|    3. NDC - client member drug
|    4. GPI - client member gpi
|    5. PHARM - client member pharmacy
|
|  Delivery System options:
|    1. ALL - all pharmacy delivery systems
|    2. RETAIL - exclude CMK Mail/Specialty & paper claims
|    3. MAIL - include only CMK mail and paper claims
|    4. NOSPC - exclude CMK Specialty
|
|  Subject Code fields:  
|    M - Member
|    P - Physician
|    D - Dependent
|
|  Member and Provider vendor file creation:  
|    Y - create member or provider vendor file
|    N - create member or provider vendor file 
|
+(dashes)-----------------------------------------------------------------------------------------SASDOC*/
%let c_s_lastclm = ALL;				/* values = ALL, ONE, NDC, GPI, PHARM   */
%let c_s_dlvry_sys = ALL;			/* value  = ALL, RETAIL, MAIL, NOSPC      */
%let c_s_prctr_excl = %str();   		/* list of prctr ids to exclude        */
						/* NOTE: subject are always member so M should be used all the time */ 
%let c_s_sbj_code_mbr = M;			/* M=member P=physician: subject fields in the member vendor filess */
%let c_s_sbj_code_phy = M;			/* M=member P=physician: subject fields in the physician vendor files */
%let c_s_mbr_mailing = Y;			/* member mailing request */
%let c_s_prctr_mailing = N;    		/* physician mailing request */
%let c_s_prior_cmctn = N;           /* remove prior communications sent from mailing campaign - member and physician */
/*%let c_s_cmctn_history_drug = NA; /* Leave commented out template 106-55334A does not identify drug so exclude by member only */
/*%let c_s_cmctn_history_ticket = Y;
%let c_s_cmctn_history_ticket_values = %str('XXXXXXX'); /* */


/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  The following section is for program specific variables.
|
|  Reason Code = 
|    1. VF     = driven by drug intake form 
|    2. CFC    = driven by drug intake form 
|    3. MDL    = driven by drug intake form 
|    4. PDL    = driven by drug intake form 
|    5. MSB    = for alternative messaging + f_mst_st (= gnrc_nm)
|    6. MEDB   = for age exclusion (based on c_i_medb_age value as well as driven by drug intake form)
|    7. DAW    = for alternative messaging + f_mst_st (= gnrc_nm) + multi_ingrd_ind and brnd_gnrc_cd exclusion
|    8. MAC    = for alternative messaging + f_mst_st (= gnrc_nm) + multi_ingrd_ind and brnd_gnrc_cd exclusion
|    9. MOR    = for CMX_MS exclusion
|   10. TC     = Tiered Copay (use value PDL - same functionality) 
|
|  Mail Type = 
|    1. MDL  = for retail and mail limits
|    2. PDL  = for alternative messaging       
|
|  Audience sorting exclusion routine:
|
|  For c_i_pptvend - member sorting de-dupe routine:  
|    0= mbr 
|    1= mbr brand_name     
|    2= mbr brand_name lbl_name
|
|  For c_i_phyvend - physician sorting de-dupe routine:  
|    0= npi 
|    1= npi mbr brand_name 
|    2= npimbr brand_name lbl_name
|
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_reason_cd =PDL;				/* available options above */
%let c_s_mailtype = PDL;				/* available options above */
%let c_i_bulkship = 0;					/* option to bulk ship vendor files */
%let c_i_pptvend  = 9;					/* member sort de-dupe routine      */
%let c_i_phyvend  = 2;					/* physician sort de-dupe routine   */
%let c_s_dependents_w_noaddr = N;			/* dependents with no addresses */
%let c_s_degree = %str(MD, NP, DO, PA);			/* sql where statement */

/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  Variables to add custom fields to the select and where clauses of the queries.  For the _sel
|  variables, include a comma in front of the field name.  For the _where variables include
|  and AND before each comparison.  The queries for Prctr and Phmcy do not have WHERE clauses
|  so we do not have a WHERE variable for those two queries.
|
|
|  Targeting Examples:
|
|	Default
|	%let c_s_claims_where = %str(and drug.brnd_gnrc_cd not in ('XXX'));
|
|	Brands
|	%let c_s_claims_where = %str(and drug.brnd_gnrc_cd = 'BRND' );
|
|	DAW
|	%let c_s_claims_where = %str(and drug.drug_multi_src_cd = 'MULTI' and drug.brnd_gnrc_cd = 'BRND' and clm.prod_slctn_cd in (1,2) );
|
|	Generics 
|	%let c_s_claims_where = %str(and drug.brnd_gnrc_cd = 'GNRC' );
|
|	Specialty
|	%let c_s_claims_where = %str(and drug.spclt_drug_ind = 'Y');
|
|	Maintainance Drugs 
|	%let c_s_claims_where = %str(and drug.bus_maint_ind='Y');  
|       %let c_s_claims_where = %str(and drug.maint_drug_ind='Y' or extn_maint_drug_ind = 'Y');
|
|	Compound + Cost Targeting	
|	%let c_s_claims_where = %str(and clm.cmpnd_ind='Y');
|	%let c_s_elig_where = %str(and (clm.amt_tax + clm.ingrd_cst) > 300 and clm.ingrd_max_cst_flg='Y'); 
|
|	Compound Targeting	
|	%let c_s_claims_where = %str(and clm.cmpnd_ind='Y');
|
|	Compound Drugs Exclusions from Targeting	
|	%let c_s_claims_where = %str(and clm.cmpnd_ind='N');
|
|	Retail Targeting	
|	%let c_s_claims_where = %str(and clm.dlvry_systm_cd='R');
|
|	Mail Targeting	
|	%let c_s_claims_where = %str(and clm.dlvry_systm_cd='M');
|
|	Retail Mail Limits	
|	%let c_s_apply_limit_filters = Y;
|
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_drug_where = %str();				/* drug query where clause */
%let c_s_claims_where = %str(AND EXISTS (SELECT 1 FROM dwu_edw.V_CLM_COPAY_OFSET_CARD a WHERE CLM.CLM_EVNT_GID = a.CLM_EVNT_GID AND a.MFG_PAID_REIMNT_AMT > 0 ));				/* claims query where clause */
%let c_s_elig_where = %str();				/* elig query where clause */
%let c_s_claims_compound = N;				/* claims compound information */
%let c_s_apply_limit_filters = N;			/* apply mail and retail limit filters from drug intake form */


/* SASDOC --------------------------------------------------------------------------------------(dashes)
|  N - (default) no cell pod msg.
|		The following variables will not be referenced: c_s_frmly_eff_dt, c_s_frmly_id, 
|       c_s_frmly_fdro_option, c_s_cell_pod_update_filenm, c_s_cell_pod_update_sheetnm.
|
|  step1 - Target claims, populate cell pod msg and create report for review by formulary.
|          Must set the following macro variables:
|				c_s_claims_where = %str(and drug.brnd_gnrc_cd = 'BRND' );
|               c_s_formulary_cell_pod_msg=step1;		 
|               c_s_frmly_eff_dt (see info below)
|          		c_s_frmly_fdro_option (see info below)
|
|  step2 - import updated report from formulary and update cell pod msg, create QC vendor files.
|          Must set the following macro variables:
|               c_s_formulary_cell_pod_msg=step2;		 
| 				c_s_cell_pod_update_filenm (see info below)
|				c_s_cell_pod_update_sheetnm (see info below)
+(dashes)----------------------------------------------------------------------------------------SASDOC*/
%let c_s_formulary_cell_pod_msg=N; 

*-------------------------------------------------------------------*;
* PLEASE SET FORMULARY EFFECTIVE DATE IN 'CCYY-MM-DD' FORMAT        *;
*-------------------------------------------------------------------*;
%let c_s_frmly_eff_dt='2016-04-01';   

*-----------------------------------------------------------------------*;
* PLEASE SET FORMULARY ID WITH LETTER H PRECEDING NUMERIC PORTION OF ID *;
*-----------------------------------------------------------------------*;
%let c_s_frmly_id=H1555;    

/*SASDOC-------------------------------------------------------------------------------------------------
| KEEP OR DROP THE FDRO DRUGS FROM TARGETING BECAUSE SOMETIMES THEY WANT TO LETTER SEPARATELY ON THEM
| OPTION WILL DROP ALL THE EXCLUDED DRUGS FROM TARGETING IF THE FORMULARY HAS FDRO
| THAT INCLUDES ANY FORMULARIES WITH EXCLUSION, I.E. FDRO, FDRO/PA, ACF, ACSF
| FOR "OPT OUT" FORMULARIES LIKE 550 THE FDRO DRUGS ARE TIER 3 AND ALWAYS KEPT IN THE MAILING
+-------------------------------------------------------------------------------------------------SASDOC*/
%let c_s_frmly_fdro_option=KEEP;       /* KEEP, DROP */          

/*SASDOC-------------------------------------------------------------------------------------------------
| FMLY [formulary id] MSG Drug Messaging Review Report returned from Formulary (Kate Dudek)
| Include the Excel file suffix (.XLS or .XLSX) on c_s_cell_pod_update_filenm 
| and update sheet name (c_s_cell_pod_update_sheetnm)
+-------------------------------------------------------------------------------------------------SASDOC*/
%let c_s_cell_pod_update_filenm = %str(TARGET FMLY H1555 MSG Drug Messaging Review Report 510604.xlsx); 
%let c_s_cell_pod_update_sheetnm = %str(FMLY H1555 MSG Drug Messaging R);


/*---------------------------------- Optional SAS logic - if needed  --------------------------------------------------*/
%let c_s_additional_step1_sas = %str();			/* SAS logic option 1 - for applying additional logic to the targeting */
%let c_s_additional_step2_sas = %str();			/* SAS logic option 2 - for applying additional logic to the targeting */
%let c_s_additional_step3_sas = %str();			/* SAS logic option 3 - for applying additional logic to the targeting */

/*---------------------------------- Optional SQL logic - if needed  --------------------------------------------------*/
%let c_s_additional_step1_sql = %str();			/* SQL logic option 1 - for applying additional logic to the targeting */
%let c_s_additional_step2_sql = %str();			/* SQL logic option 2 - for applying additional logic to the targeting */
%let c_s_additional_step3_sql = %str();			/* SQL logic option 3 - for applying additional logic to the targeting */

%m_function_create_log_name(var=c_s_formulary_cell_pod_msg);


proc printto log = "&c_s_maindir.&c_s_log." new;
run;

	/**************************************************************************************/
	/****** programs are called within cd_formulary_cfc.sas     ***************************/
	/**************************************************************************************/
	%include "&c_s_core_dir./cd_formulary_cfc.sas";

proc printto;
run;
