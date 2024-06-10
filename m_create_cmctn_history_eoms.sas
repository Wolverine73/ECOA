
/* HEADER --------------------------------------------------------------------------------------
| MACRO:    m_create_cmctn_history_eoms
|
| LOCATION:  
|
| USAGE:    Create cmctn history files for the Hercules CMCTN History + PeopleSafe CMCTN History
|
| PURPOSE:   
|
| LOGIC:        
|
| TABLES:   
|
| INPUT:    sas dataset from a predecessor step
|
| OUTPUT:   external transaction files - - ext_transactions_yyyymmdd_n.txt 
|
+------------------------------------------------------------------------------
| HISTORY:  
|
|	2020.10.01 - CVSH           - Original 
|
+------------------------------------------------------------------------------------------HEADER*/


%macro m_create_cmctn_history_eoms(mail_date,source,rcrcde,dstcde,sbj_com_rle_cde,com_sts_cde,drg_exp_desc_cde,nhu_type_cd);

options mprint errorabend;

/*----------------------------------------------------------------------------------------------------
    macro parameter definitions:
    
    1) source - the name of the input sas data set

    2) rcrcde - receiver communication role code.  The Role of the individual to whom the communication
         is addressed or directed.
           valid values:
		1= Participant
		2= Prescriber
		3= Client 
		4= Other
		5= Cardholder
    3) dstcde - A code denoting the communications Distribution Type.
           valid values:
		1=   Letter
		2=   Fax
		3=   Phone Call
		4    Email
		6=   Televox
		15=  IVR
		16=  PDA
		17=  Text Message
   4) sbj_com_rle_cde - The Role of the individual to whom the communication is "about" - the
        subject person.
            valid values:
		1= Participant
		2= Prescriber
		3= Client
		4= Other
		5= Cardholder
   5) com_sts_cde - Indicates the Status of the Communication Transaction.
            valid values:
		2= Communication Sent
		Set when communications have been sent,
		and need to be inserted into Communication History
		4= Cancel
		Set when communications need to be 'cancelled' from Communication History.
		'Cancel' transactions require the original Communication Transaction data record - with a '4' in the Communication Status Code column.
		For example, when a mailing Initiative is created and Communication History is
		updated, but the mailing will not be sent - these communications will need to be
		'cancelled' from Communication History.
   6) drg_exp_desc_cde - A value that represents the label or description of the drug
       referenced in the communication
           valid values:
            Blank value (default value - use this when the ndc is set to null/missing within the source
                            sas data set)
            If NDC is not null/missing then:
            Example:  %syslput drug_expl_desc_cd=%str(3);
		1= Brand with Generic Available
		2= Non-Formulary with Formulary Available
		3= Target Drug
		4= Conflicting Drug
		5= Controlled Substance
		6= Denied Prescription
  7) nhu_type_cd - NDC HRI UPC Type Code
          valid values:
           Blank value (default value - use this when the ndc is null/missing within the source
                            sas data set)
           If NDC is not null/missing then:
           Example:  %syslput drug_nhu_typ_cd=%str(1);
		1= NDC, National Drug Code
		2= HRI, Health Related Item
		3= UPC, Universal Product Code
		4= DMR, DMR Dummy NDC Code
		5= NCPDP, NCPDP Dummy NDC Code
		6= MFR, Manufacturer Drug Code
----------------------------------------------------------------------------------------------------*/


	/**assign local macro variables*/
	%local dsnme cdte tdy;

	/**assign global macro variable*/
	%global com_hist_file_nme;
	%global com_hist_file_nme1;

	%macro numobs(dsn=_last_);

	   %global NUM;
	   %let NUM=0;

	   data _NULL_;
	   IF 0 THEN do;
	   SET &dsn. NOBS=COUNT;
	   end;
	   CALL SYMPUT('NUM',LEFT(PUT(COUNT,8.)));
	   run;

	%mend numobs;

	/*initialize nummiss to zero*/
	%let nummiss=0;

	data _null_;
	/*  x=put(today(),yymmdd10.);*/
	  x=input("&mail_date",yymmdd10.);
	  y="'"||x||"'";
	  dsnme=int(time());
	  hr=put(hour(DATETIME()),Z2.0);
	  min=put(minute(DATETIME()),Z2.0);
	  sec=put(second(DATETIME()),Z2.0);
	  cdte=put(x,yymmdd10.)||'-'||left(hr)||'.'||left(min)||'.'||left(sec)||'.000000';
	  tdy=compress(left(year(x))||left(month(x))||left(day(x)));
	  call symput('dsnme',left(trim(dsnme)));
	  call symput('cdte',trim(cdte));
	  call symput('tdy',trim(tdy));
	run;

	/*open the directory and get number of files = today within*/
	data did_&dsnme;
	rc=filename("mydir","&myfilerf");
	did=dopen("mydir");
	memcount=dnum(did);
	do i=1 to memcount;
		name=dread(did,i);
		match=find(name,"&tdy","i t",1);
		if match then output;
	end;
	rc=dclose(did);
	run;
	
	data did_&dsnme;
	set did_&dsnme;
	n=scan(name,4,'_');
	nn=scan(n,1,'.');
	cnt=nn*1;
	if cnt >= 2000 ;
	run;	

	/* SASDOC --------------------------------------------------------------------------------------
	| 01OCT2017 - Brian Stropich CMCTN History Enhancement
	| Determine largest file suffix number
	+-----------------------------------------------------------------------------------------SASDOC */
	%let cnt_did = 0;
	%let email_ext_transactions = email_ext_transactions;
	
	proc sql noprint;
	select count(*) into: cnt_did separated by ''
	from did_&dsnme;
	quit;
	
	%put NOTE: cnt_did = &cnt_did. ;
	
	%if &cnt_did. = 0 %then %do;
		%let ifleloop_total=2000;
	%end;
	%else %do;

		proc sort data=did_&dsnme;
		by cnt;
		run;

		data _null_;  
		set did_&dsnme  ; 
		call symput('ifleloop_total',left(put(cnt,8.)));
		run;
	%end;

	/* SASDOC --------------------------------------------------------------------------------------
	| 01OCT2017 - Brian Stropich CMCTN History Enhancement
	| Add 10 incase if other processes are generating files at the same time
	+-----------------------------------------------------------------------------------------SASDOC */
	%let ifleloop_total=%eval(&ifleloop_total+1);
	%put NOTE: ifleloop_total = &ifleloop_total.;


	/*loop through num +1 and add the new file*/ 
	%do ifleloop=&ifleloop_total. %to &ifleloop_total.;  /** start - outer loop 1  **/ 

		/*test for the existence of the file***/
		/**if it exists return to the top of the loop*/
		%if %sysfunc(fileexist(&myfilerf.ext_transactions_&tdy._&ifleloop..txt)) %then %do;
		%end;

		/*test for the existence of the file***/
		/*if it exists return to the top of the loop*/
		%else %if %sysfunc(fileexist(&myfilerf.ext_templates_&tdy._&ifleloop..txt)) %then %do;
		%end;

		/*if the file does not exist create it*/
		%else %do;  /** begin loop 2 - fileexist  **/

			data comm_his_&dsnme            (drop=insert_date userid msg_eff_dt) 
			     comm_his_missing_id_&dsnme (keep=program_id stellent1 recipient_id subject_id  recipient_gid subject_gid  insert_date userid);
			length stellent1 $15. crte_user_id updt_user_id $10. updt_ts  crte_ts $26. image_id  cmctn_hist_id $8.;
			set &source end=last;
			insert_date=input("&mail_date",yymmdd10.);
			userid="&SYSUSERID";
			stellent1=upcase(stellent1);
			cmctn_rlse_ts="&cdte"; 
			cmctn_rlse_ts=left(trim(msg_eff_dt)) || left(trim(substr(cmctn_rlse_ts,11))); /**<----------- assign based on disp date **/
			rvr_cmctn_role_cd=rvr_cmctn_role_cd;
			dstrbtn_cd=dstrbtn_cd;
			sbj_cmctn_role_cd=sbj_com_rle_cde;
			cmctn_stat_cd=cmctn_stat_cd;
			drug_expl_desc_cd=drug_expl_desc_cd;
			drug_nhu_typ_cd=drug_nhu_typ_cd;
			crte_user_id='ANRUSER';
			updt_user_id='ANRUSER';
			crte_trans_cd='3';
			updt_srce_cd='3';
			updt_trans_cd='3';
			crte_srce_cd='3';
			crte_ts=cmctn_rlse_ts;
			image_id='  ';
			cmctn_hist_id='  ';
			updt_ts=cmctn_rlse_ts;		 
			
			if dstrbtn_cd = '15' then do;
			  if tlphn_nb ne '';
			end;			
			
			/** 2018.09.01 - clinical stars pharmacy fax enhancement **/
			if rvr_cmctn_role_cd = '4' 
				and not missing(subject_id)
				and recipient_id ne '0' 
				and subject_id   ne '0'
				and recipient_id ne '.' 
				and subject_id   ne '.' then output comm_his_&dsnme;				
			else if not missing(recipient_id) 
				and not missing(subject_id)
				and recipient_id ne '0' 
				and subject_id   ne '0' 
				and recipient_id ne '.' 
				and subject_id   ne '.' then output comm_his_&dsnme;				
			else output comm_his_missing_id_&dsnme;
			run;


			proc contents noprint data=comm_his_&dsnme out=contents_stellent_&dsnme;
			run;

			***get number of stellent ids***;
			data _null_;
			set contents_stellent_&dsnme  end=last;
			where substr(lowcase(name),1,8)='stellent';
			call symput('stell'||left(_n_),name);
			if last then call symput('stellnum',left(_n_));
			run;

			proc sort nodupkey data=comm_his_&dsnme out=stellent_&dsnme(keep=program_id
			%do istllnum=1 %to &stellnum;
			&&stell&istllnum
			%end;
			);
			by program_id %do istllnum=1 %to &stellnum;
			&&stell&istllnum
			%end;;

			run;

			data stellent_&dsnme;
			set stellent_&dsnme;
			holder=_n_;
			pgm_id=left(program_id)*1;
			run;

			proc sort data=stellent_&dsnme;
			by holder pgm_id;
			run;

			proc transpose data=stellent_&dsnme out=stellent_trnspsd_&dsnme;
			by holder pgm_id;
			var %do istllnum=1 %to &stellnum;
			&&stell&istllnum
			%end;;
			run;

			data stellent_trnspsd_&dsnme(drop=col1);
			length stellent_id $20.;
			set stellent_trnspsd_&dsnme;
			stellent_id=upcase(col1);
			run;

			proc sort nodupkey data=stellent_trnspsd_&dsnme(keep=pgm_id stellent_id);
			by pgm_id stellent_id;
			where stellent_id ne '  ';
			run;

			proc sql;
			connect to teradata (server= &c_s_server. user=&td_id. password="&td_pw");
			create table v_stlnt_&dsnme  as
			select * from connection to teradata(
			  select distinct  
				 stlnt.pgm_id ,
				 stlnt.appl_cmnct_id  as stellent_id
			  from &c_s_schema..v_stlnt    stlnt
			  order by stlnt.pgm_id,stlnt.appl_cmnct_id 
			);
			quit;
			
			proc sort data=v_stlnt_&dsnme;
			by pgm_id stellent_id; 
			run;			

			data stellent_trnspsd_&dsnme;
			merge stellent_trnspsd_&dsnme(in=a) v_stlnt_&dsnme(in=b);
			by pgm_id stellent_id;
			if a and not b;
			run;

			%numobs;

			%put NOTE: &num;

			%if %eval(&num)>=1 %then %do;
				data _null_;
				set stellent_trnspsd_&dsnme end=last;
				call symput('badstell'||left(_n_),trim(stellent_id));
				call symput('badpgm'||left(_n_),trim(left(pgm_id)));
				run;

				%do istllbad=1 %to &num;
					%put WARNING: Program Id: &&badpgm&istllbad/Stellent Id: &&badstell&istllbad  does not exist in the EDW stellent table.;
					%put ;
				%end;

				%put WARNING: The communications history file will not be created.;
				/*clean-up**/
				proc delete data=stellent_&dsnme  stellent_trnspsd_&dsnme contents_stellent_&dsnme v_stlnt_&dsnme;
				run;
				/**%return  ;**/

			%end;

			/*end check stellent id section*/

			/**create comm history file**/;
			/*if id's are missing append them to the missing_id ds*/
			%numobs(dsn=comm_his_missing_id_&dsnme);

			/**assign libname*/
			libname com&dsnme "&myfilerf" ;

			%if %eval(&num)>=1 %then %do;
				proc sort nodupkey data=comm_his_missing_id_&dsnme;
				by program_id stellent1 RECIPIENT_gid subject_gid;
				run;
				
				data temp001;
				set com&dsnme..communication_history_missing_id;
				run;
				
				data com&dsnme..communication_history_missing_id;
				set temp001
				    comm_his_missing_id_&dsnme;
				run;

				%let nummiss=%eval(&num);
				libname com&dsnme clear;
			%end;

			/** 2018.09.01 - clinical stars pharmacy fax enhancement **/
			%let orig_phm_npi_id_cnt = 0;

			proc contents data = comm_his_&dsnme out = contents_cmctn noprint;
			run;

			proc sql noprint;
			select count(*) into: orig_phm_npi_id_cnt separated by ''
			from contents_cmctn
			where upcase(name)='ORIG_PHM_NPI_ID';
			quit;

			%put NOTE: orig_phm_npi_id_cnt = &orig_phm_npi_id_cnt. ;

			  %if &orig_phm_npi_id_cnt. = 0 %then %do;
			  	%let m_cmctn_history_file = ext_transactions_&tdy._&ifleloop..txt;			  
			  %end;
			  %else %do;
			  	%let m_cmctn_history_file = ext_transactions_v4_&tdy._&ifleloop..txt;
			  %end;	
			  
			  %let m_cmctn_history_filecnt = 0;
			  
			  proc sql noprint;
			  select count(*) into: m_cmctn_history_filecnt separated by ''
			  from comm_his_&dsnme ;
			  quit;

			  %let m_stellentcnt = 0;
			  
			  proc sql noprint;
			  select count(*) into: m_stellentcnt separated by ''
			  from comm_his_&dsnme 
			  where stellent1 is missing;
			  quit;
			  
			%if &m_stellentcnt. ne 0 %then %do;
			  %put ERROR:  Clinical Stars campaign environment setup is invalid ;
			  %m_abend_handler(abend_report=%str(Clinical Stars CMCTN History - Invalid Stellents));
			%end;
			%else %do;
			  %put NOTE:  Clinical Stars campaign environment setup is valid ;	
			%end;			  

			  %let m_stellentcnt = 0;
			  
			  proc sql noprint;
			  select count(*) into: m_stellentcnt separated by ''
			  from comm_his_&dsnme 
			  where program_id is missing;
			  quit;
			  
			%if &m_stellentcnt. ne 0 %then %do;
			  %put ERROR:  Clinical Stars campaign environment setup is invalid ;
			  %m_abend_handler(abend_report=%str(Clinical Stars CMCTN History - Invalid Stellents));
			%end;
			%else %do;
			  %put NOTE:  Clinical Stars campaign environment setup is valid ;	
			%end;
			
			data _null_;
			  set  comm_his_&dsnme end=EFIEOD;
			  %let _EFIREC_ = 0;    /* clear export record count macro variable */
			  
			  /** 2018.09.01 - clinical stars pharmacy fax enhancement **/
			  %if &orig_phm_npi_id_cnt. = 0 %then %do;
			  	file "&myfilerf.ext_transactions_&tdy._&ifleloop..txt"    delimiter='|' DSD DROPOVER lrecl=32767;			  
			  %end;
			  %else %do;
			  	file "&myfilerf.ext_transactions_v4_&tdy._&ifleloop..txt" delimiter='|' DSD DROPOVER lrecl=32767;
			  %end;			  

			  format
			  trans_id $8.
			  program_id $8.
			  stellent1 $15.
			  cmctn_rlse_ts $26.
			  rvr_cmctn_role_cd $4.
			  recipient_id $9.
			  rvr_full_nm $50.
			  dstrbtn_cd $4.
			  sbj_cmctn_role_cd $4.
			  subject_id $9.
			  address1_tx $40.
			  address2_tx $40.
			  address3_tx $40.
			  address4_tx $40.
			  city_tx $40.
			  state $2.
			  zip_cd $5.
			  zip_suffix_cd $4.
			  intl_postal_cd $12.
			  cntry_cd $2.
			  email_address $79.
			  tlphn_nb $7.
			  tlphn_area_cd $3.
			  tlphn_ext $5.
			  cmctn_stat_cd $4.
			  image_id $8.
			  drug_expl_desc_cd $4.
			  drug_ndc_id $11.
			  drug_nhu_typ_cd $4.
			  crte_trans_cd $4.
			  crte_srce_cd $4.
			  crte_user_id $8.
			  crte_ts $26.
			  updt_trans_cd $4.
			  updt_srce_cd $4.
			  updt_user_id $8.
			  updt_ts $26.

			  /** 2018.09.01 - clinical stars pharmacy fax enhancement **/
			  %if &orig_phm_npi_id_cnt. = 0 %then %do;
			  %end;
			  %else %do;
				  rx_nb $20. 
				  orig_phm_npi_id $20. 
			  %end;
			  ;

			  /** 2018.09.01 - clinical stars pharmacy fax enhancement **/
			  if _n_ = 1 then do;			  
				  %if &orig_phm_npi_id_cnt. = 0 %then %do;
				    put 'HEADER' '|'
					    'ANR' '|'
						"&cdte";			  
				  %end;
				  %else %do;
				    put 'HEADER' '|'
					    'ANR-FAX' '|'
						"&cdte"; 
				  %end;
			  end;
			  do;
				    EFIOUT + 1;
				  put trans_id $ @;
				  put program_id $ @;
				  put stellent1 $ @;
				  put cmctn_rlse_ts $ @;
				  put rvr_cmctn_role_cd $ @;
				  put recipient_id $ @;
				  put rvr_full_nm $ @;
				  put dstrbtn_cd $ @;
				  put sbj_cmctn_role_cd $ @;
				  put subject_id $ @;
				  put address1_tx $ @;
				  put address2_tx $ @;
				  put address3_tx $ @;
				  put address4_tx $ @;
				  put city_tx $ @;
				  put state $ @;
				  put zip_cd $ @;
				  put zip_suffix_cd $ @;
				  put intl_postal_cd $ @;
				  put cntry_cd $ @;
				  put email_address $ @;
				  put tlphn_nb $ @;
				  put tlphn_area_cd $ @;
				  put tlphn_ext $ @;
				  put cmctn_stat_cd $ @;
				  put image_id $ @;
				  put drug_expl_desc_cd $ @;
				  put drug_ndc_id $ @;
				  put drug_nhu_typ_cd $ @;
				  put crte_trans_cd $ @;
				  put crte_srce_cd $ @;
				  put crte_user_id $ @;
				  put crte_ts $ @;
				  put updt_trans_cd $ @;
				  put updt_srce_cd $ @;
				  put updt_user_id $ @;
				  put updt_ts $ @;

				  /** 2018.09.01 - clinical stars pharmacy fax enhancement **/
				  %if &orig_phm_npi_id_cnt. = 0 %then %do;
					put cmctn_hist_id $ ;
				  %end;
				  %else %do;
					put cmctn_hist_id $ @; 
					put RX_NB $ @;
					put FILL_DT $ @;
					put ORIG_PHM_NPI_ID $ @;
					put NEXT_PICK_UP_DT $ @;
					put LTR_REFERENCE_ID  ;
				  %end;

			  end;
			  if efieod then do;
			  put 'TRAILER'  '|'  efiout ;
			  ;
			  end;
			
			run;


			/*%put The file:  &myfilerf.ext_transactions_&tdy._&ifleloop..txt was created.;*/
			%if %eval(&nummiss)>0 %then %do;
				%put WARNING:  &nummiss rows were NOT included in the communications history file because either the recipient_id or subject_id field contained missing values;
				%let com_hist_file_nme1=%str(ext_transactions_&tdy._&ifleloop..txt);
				%let com_hist_file_nme=%str(&myfilerf.ext_transactions_&tdy._&ifleloop..txt);
			%end;

			/***multiple transaction file section***/
			/*if more than one stellent create multiple template file*/
			%if %eval(&stellnum)>1 %then %do;

				proc sort nodupkey data=comm_his_&dsnme 
						   out=mult_template_&dsnme(
				keep=trans_id
				program_id
				%do istllnum=2 %to &stellnum;
				&&stell&istllnum
				%end;  );
				by trans_id program_id
				%do istllnum=2 %to &stellnum;
				&&stell&istllnum
				%end;;
				run;

				proc transpose data=mult_template_&dsnme out=mtmplte_trnspsd_&dsnme;
				by trans_id program_id;
				var %do istllnum=2 %to &stellnum;
				   &&stell&istllnum
				  %end;;
				run;

				proc sort data=mtmplte_trnspsd_&dsnme;
				by program_id trans_id  _name_;
				run;

				data mtmplte_trnspsd_&dsnme(drop=col1);
				set mtmplte_trnspsd_&dsnme end=last;
				by program_id trans_id _name_;
				if first.trans_id then disp_seq_number=0;
				disp_seq_number+1;
				stellent=upcase(col1);
				if stellent='  ' then delete;
				run;

				 /*create multiple template file*/
				data _null_;
				  set  mtmplte_trnspsd_&dsnme end=EFIEOD;
				  %let _EFIREC_ = 0;    /* clear export record count macro variable */
				  file "&myfilerf.ext_templates_&tdy._&ifleloop..txt" delimiter='|'
				    DSD DROPOVER lrecl=32767;
				  format
				  trans_id $8.
				  program_id $8.
				  stellent $10.
				  disp_seq_number 8. ;
				  if _n_ = 1 then do;
				    put 'HEADER' '|'
					    'ANR' '|'
						"&cdte";
				   end;do;
				    EFIOUT + 1;
				  put trans_id $ @;
				  put program_id $ @;
				  put stellent $ @;
				  put disp_seq_number ;
				    ;
				  end;
				  if efieod then do;
				  put 'TRAILER'  '|' efiout ;
				  ;
				end;
				run;

				proc delete data=mult_template_&dsnme mtmplte_trnspsd_&dsnme;
				run;
			%end;

			/*end multiple template file section*/
			/*end the loop*/
			%let ifleloop=%eval(&ifleloop+10);

		%end;  /** end loop 2 - fileexist  **/

	%end;  /** end loop 1 - ifleloop  **/


/*clean-up**/
proc delete data=comm_his_&dsnme comm_his_missing_id_&dsnme did_&dsnme stellent_trnspsd_&dsnme stellent_&dsnme contents_stellent_&dsnme v_stlnt_&dsnme;
run;

%put NOTE: &com_hist_file_nme;
%put NOTE: &com_hist_file_nme1;
options noerrorabend;

%mend m_create_cmctn_history_eoms;