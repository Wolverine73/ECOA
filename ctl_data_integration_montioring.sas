
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    ctl_data_integration_montioring
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
| HISTORY:  20221201 - Clinical Stars Team - Original (version 2022.12.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/


/* SASDOC --------------------------------------------------------------------------------------
|  STEP - Assign campaign Information
+---------------------------------------------------------------------------------------------*/ 
%let c_s_email_to 	= %str('brian.stropich3@caremark.com' );
%let c_s_logdir		= %str(/mbr_engmt/reserve/SASData/PROD/data_integration/eoms_cmctn_history);
%let c_s_dqi_campaign_id= 88;

%include "/mbr_engmt/reserve/SASControl/DEV/bss_test/macros/m_function_check_roles.sas";


data x;
format c_s_file_date_delete c_s_file_date_linux $10.;  
x=today()-2;  /** <--------------------------------------- change date for testing **/
x=today()-0;  
xx=round(time()); 
c_s_file_date=left(trim(put(x,yymmddn8.)));
c_s_file_date_delete='%'||left(trim(c_s_file_date))||'%';
c_s_file_date_linux='*'||left(trim(c_s_file_date))||'*';    
c_s_log_date=left(trim(put(x,yymmddn8.)))||left(trim(xx));  
call symput('c_s_file_date',compress(c_s_file_date)); 
call symput('c_s_log_date',compress(c_s_log_date));  
run;
	

%macro do_monitoring;


	data trigger;
	format trigger_text $20. trigger_numeric 8. ;
	trigger_text="&systime.";
	trigger_numeric=compress(trigger_text,':')*1;
	call symput('trigger_numeric',left(trim(trigger_numeric)));
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - SFTP Server Files
	+---------------------------------------------------------------------------------------------*/ 
	%let sftp_get_file1	= cpl*.&c_s_file_date.*;
	%let sftp_get_file2	= sfmc*.&c_s_file_date.*;


	%put NOTE: c_s_file_date   = &c_s_file_date. ;
	%put NOTE: trigger_numeric = &trigger_numeric. ;
	
	
	

	%if &trigger_numeric. < 1401 %then %do; 

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get files for daily batch process - AT THE TIME ECOA
		+---------------------------------------------------------------------------------------------*/ 
		data _null_;
		x "rm /anr_dep/communication_history/eoms_monitoring1/*"; 
		run;
		
		data _null_;
		file "/anr_dep/communication_history/eoms_monitoring1/get.txt";
		put "cd /ant591/IVR/incoming"; 
		put "get cpl*.&c_s_file_date.*"; 
		put "exit";
		run;

		data _null_;
		rc = system("cd /anr_dep/communication_history/eoms_monitoring1");
		rc = system("sftp uy2huh5a@webtransport.caremark.com < /anr_dep/communication_history/eoms_monitoring1/get.txt");
		if _error_=0 then do;
		call symput('file_sftp_flg','1');
		end;
		else call symput('file_sftp','0');
		run;
		
		data ecoa_before;
		rc=filename("mydir","/anr_dep/communication_history/eoms_monitoring1");
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
		
		proc sort data = ecoa_before;
		by name;
		run; 		
		
		data ecoa_match ;
		set ecoa_before ;
		run;
		
		proc sql noprint;
		select count(*) into: ecoa_match separated by ''
		from ecoa_match;
		quit;
		
		proc sql noprint;
		select name into: ecoa_match_files separated by ', '
		from ecoa_match;
		quit;
		
		%put NOTE: ecoa_match = &ecoa_match. ;
		%put NOTE: ecoa_match_files = &ecoa_match_files. ;		
		
		%if &ecoa_match. ne 0 %then %do;
		 
			filename xemail email 
			to=(&c_s_email_to.)
			subject="ECOA Monitoring + Observability - INFORMATIVE File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html"; 	 
			

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
				put "The ECOA Monitoring + Observability is sending an INFORMATIVE message of files being sent for the batch processing execution. ";
				put '<br>';
				put '<b>';
				put '<br>';
				put '<b><font color="#13478C">'; put '<u>'; put "ECOA Monitoring + Observability Information: "; put '</b></font>'; put '</u>';
				put '</b>';
				put '<br>';	
				put "<ul>";
					put "<li> 	ECOA Monitoring + Observability Framework"; 	put '</li>'; 
					put "<li> 	program: ctl_data_integration_monitoring.sas"; 	put '</li>';  
					put "<li> 	informative file counts: &ecoa_match. "; 	put '</li>'; 
					put "<li> 	informative file names: &ecoa_match_files.  "; 	put '</li>'; 
					put "<li> 	informative file location:  webtransport.caremark.com - /ant591/IVR/incoming  "; 			put '</li>'; 
					put "<li> 	ECOA log directory:  &c_s_logdir."; 		put '</li>'; 
					put "<li> 	ECOA log file:  &c_s_log."; 			put '</li>'; 
				put "</ul>";
				put "<br>";
 

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email closing
				+---------------------------------------------------------------------------------------------*/ 
				put "</ul>";
				put "<br>";	
				put "Thank you and have a great week."; put '<br>';
				put '<br>';
				put "Sincerely,"; put '<br>';
				put "EA - Campaign Targeting Team"; put '<br>';
				put " "; put '<br>';	 		 
			
			run;			
		
		%end;			

	%end;
	
	%else %do;
	
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get files for daily batch process - AFTER ECOA
		+---------------------------------------------------------------------------------------------*/ 
		data _null_;
		x "rm /anr_dep/communication_history/eoms_monitoring2/*"; 
		run;
		
		data _null_;
		file "/anr_dep/communication_history/eoms_monitoring2/get.txt";
		put "cd /ant591/IVR/incoming"; 
		put "get cpl*.&c_s_file_date.*"; 
		put "exit";
		run;

		data _null_;
		rc = system("cd /anr_dep/communication_history/eoms_monitoring2");
		rc = system("sftp uy2huh5a@webtransport.caremark.com < /anr_dep/communication_history/eoms_monitoring2/get.txt");
		if _error_=0 then do;
		call symput('file_sftp_flg','1');
		end;
		else call symput('file_sftp','0');
		run;
		
		
		data ecoa_before;
		rc=filename("mydir","/anr_dep/communication_history/eoms_monitoring1");
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

		data ecoa_after;
		rc=filename("mydir","/anr_dep/communication_history/eoms_monitoring2");
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
		
		proc sort data = ecoa_before;
		by name;
		run;
		
		proc sort data = ecoa_after;
		by name;
		run;		
		
		data ecoa_match ecoa_nomatch;
		merge ecoa_before (in=a)
		      ecoa_after  (in=b);
		if a and b then output ecoa_match;
		else output ecoa_nomatch;
		run;
		
		proc sql noprint;
		select count(*) into: ecoa_nomatch separated by ''
		from ecoa_nomatch;
		quit;
		
		proc sql noprint;
		select name into: ecoa_nomatch_files separated by ', '
		from ecoa_nomatch;
		quit;
		
		%put NOTE: ecoa_nomatch = &ecoa_nomatch. ;
		%put NOTE: ecoa_nomatch_files = &ecoa_nomatch_files. ;
		
		%if &ecoa_nomatch. ne 0 %then %do;
		 
			filename xemail email 
			to=(&c_s_email_to.)
			subject="ECOA Monitoring + Observability - ISSUE File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html"; 	 
			

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
				put "The ECOA Monitoring + Observability has detected an issue of files being sent after the batch processing execution. ";
				put '<br>';
				put '<b>';
				put '<br>';
				put '<b><font color="#13478C">'; put '<u>'; put "ECOA Monitoring + Observability Information: "; put '</b></font>'; put '</u>';
				put '</b>';
				put '<br>';	
				put "<ul>";
					put "<li> 	ECOA Monitoring + Observability Framework"; 	put '</li>'; 
					put "<li> 	program: ctl_data_integration_monitoring.sas"; 	put '</li>';  
					put "<li> 	gap file counts: &ecoa_nomatch. "; 		put '</li>'; 
					put "<li> 	gap file names: &ecoa_nomatch_files.  "; 	put '</li>'; 
					put "<li> 	gap file location:  webtransport.caremark.com - /ant591/IVR/incoming  "; 			put '</li>'; 
					put "<li> 	ECOA log directory:  &c_s_logdir."; 		put '</li>'; 
					put "<li> 	ECOA log file:  &c_s_log."; 				put '</li>'; 
				put "</ul>";
				put "<br>";
 

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email closing
				+---------------------------------------------------------------------------------------------*/ 
				put "</ul>";
				put "<br>";	
				put "Thank you and have a great week."; put '<br>';
				put '<br>';
				put "Sincerely,"; put '<br>';
				put "EA - Campaign Targeting Team"; put '<br>';
				put " "; put '<br>';	 		 
			
			run;			
		
		%end;	
	
	%end;
	
	
%mend do_monitoring;


/* SASDOC --------------------------------------------------------------------------------------
|  STEP - call the campaign work flo
+---------------------------------------------------------------------------------------------*/ 
%let c_s_log 		=data_integration_eoms_&c_s_dqi_campaign_id._&c_s_log_date..log;

proc printto log = "&c_s_logdir./&c_s_log." new;
run;	
	
	%do_monitoring;	
	%put _all_;
	
proc printto;
run;	
