
/* HEADER --------------------------------------------------------------------------------------
| MACRO:    m_create_anthem_mcs_eoms 
|
| LOCATION:  
|
| USAGE:     
|
| PURPOSE:   
|
| LOGIC:        
|
| TABLES:   
|
| INPUT:     
|
| OUTPUT:    
|
+------------------------------------------------------------------------------
| HISTORY:  
|
|	2020.10.01 - CVSH           - Original 
|
+------------------------------------------------------------------------------------------HEADER*/


%macro m_create_anthem_mcs_eoms(source);

	%local dsnme cdte ;
	
	data _null_; 
	  x=put(today() - &execution_date. ,yymmdd10.); 
	  y="'"||x||"'";
	  dsnme=int(time());
	  hr=put(hour(DATETIME()),Z2.0);
	  min=put(minute(DATETIME()),Z2.0);
	  sec=put(second(DATETIME()),Z2.0);
	  cdte=compress(x,'-')||left(hr)||left(min)||left(sec); 
	  call symput('dsnme',left(trim(dsnme)));
	  call symput('cdte',trim(cdte)); 
	run;

	%let m_cmctn_history_filecnt = 0; 
	%let m_cmctn_history_file=MCS_CTT_&cdte..txt;
	%let m_cmctn_history_file_ctl=MCS_CTT_&cdte._cntl.txt; 
	
	proc sql noprint;
	select count(*) into: m_cmctn_history_filecnt separated by ''
	from &source. ;
	quit;
	
	%put NOTE: m_cmctn_history_filecnt = &m_cmctn_history_filecnt. ;
	
	%if &m_cmctn_history_filecnt. ne 0 %then %do;

		data comm_his_&dsnme    
		     comm_his_missing_id_&dsnme  ; 
		set &source end=last;
		if not missing(MemberSourceID) or not missing(TemplateID) then output comm_his_&dsnme;				
		else output comm_his_missing_id_&dsnme;
		run;

		data _null_;
		  set  comm_his_&dsnme end=EFIEOD;
		  file "&myfilerf.&m_cmctn_history_file."    delimiter='|' dsd dropover lrecl=32767;		

			put CommID $ @;
			put MemberSourceID $ @;
			put MemberSource $ @;
			put ClientCode $ @;
			put ClientID $ @;
			put CarrierID $ @;
			put AccountID $ @;
			put GroupID $ @;
			put MemberID $ @;
			put ContractID $ @;
			put PBPID $ @;
			put LOB $ @;
			put CommSourceSystem $ @;
			put CommName $ @;
			put CommDeliveryChannel $ @;
			put CommContactInfo $ @;
			put CommSenderInfo $ @;
			put CommSubject $ @;
			put CommDeliveryDate $ @;
			put CommDeliveryStatus $ @;
			put CommOutcome $ @;
			put CommFirstAttempt $ @;
			put CommFinalAttempt $ @;
			put TemplateID $ @;
			put CommDocName $ @;
			put CommDocInd $ @;
			put CltPlatformCode $ @;
			put CltDivision $ @;
			put CltMasterGrp $ @;
			put CltPlanType $ @;
			put CltGroup $ @;
			put CltFamilyID $ @;
			put CltDependentCode $ @;
			put CltMemberDOB $ ;

		run; 
		
		data _null_; 
		  file "&myfilerf.&m_cmctn_history_file_ctl."    delimiter='|' dsd dropover lrecl=32767;
		  file_name="&m_cmctn_history_file.";
		  file_count="&m_cmctn_history_filecnt.";

			put file_name $ @;
			put file_count $ ;

		run; 		

	%end;   

%mend m_create_anthem_mcs_eoms;


