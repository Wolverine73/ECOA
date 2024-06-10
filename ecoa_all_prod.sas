  
/* HEADER---------------------------------------------------------------------------------------
| MACRO:     ecoa_all_prod 
|
| LOCATION:  T:\anr_dep\cfc\requests\BSS\vm_server\ecoa_logs 
| 
| PURPOSE:   creates a report of missing dipsosions from a request from sharepoint
|
| LOGIC:     
|
| INPUT:     
|
| OUTPUT:    
|
| NOTE:     
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20190507 - Clinical Stars Team - Original (version 2019.06.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/

options noxwait;
options nospool;
options xmin;
options mprint mlogic nosymbolgen source2;	
options sastrace=',,,d' sastraceloc=saslog nostsuffix;
options dbidirectexec;
options minoperator; /* allow IN operator for %if */


/*-----database server variables ------------------------------------------------*/
%include "T:\anr_dep\cfc\requests\BSS\vm_server\mdp_logs\m_load_ids_passwords.sas";
%m_load_ids_passwords;


/** production environment -------------------------------------------------------------------------------------------------------**/
%let tdrive_dir			= %str(T:\anr_dep\cfc\requests\BSS\vm_server\ecoa_logs);
%let tdrive_connect		= %str(\\azshfsp01n\clinicalservicesoperations);
%let pending_dir		= %str(V:\000000_000000_MDP\pending_ecoa);  /** <------------------------------------------------------------------------------------ landing location            **/
%let sharepoint_dir		= %str(https://collab.corp.cvscaremark.com/sites/Marketing/CampaignTargeting/Blue Chip 2022/99 - Blue Chip - Surveillance Reporting); /** <--------- proc http **/
%let sharepoint_dir2		= %nrstr(https://collab.corp.cvscaremark.com/sites/Marketing/CampaignTargeting/_layouts/15/start.aspx#/Blue%20Chip%202022/Forms/AllItems.aspx?RootFolder=%2Fsites%2FMarketing%2FCampaignTargeting%2FBlue%20Chip%202022%2F99%20%2D%20Blue%20Chip%20%2D%20Surveillance%20Reporting&FolderCTID=0x012000D791BB5D884BF646878A40D38C394B47&View=%7BFCA3BCB7%2D5821%2D4675%2DA62D%2DB50855539606%7D);

%let sharepoint_windows_dir	= %str('dir "\\collab.corp.cvscaremark.com@ssl\sites\Marketing\CampaignTargeting\Blue Chip 2022\99 - Blue Chip - Surveillance Reporting"' ); /** <-- directory listing           **/
%let sharepoint_connect		= %str("\\collab.corp.cvscaremark.com@ssl\sites\Marketing\CampaignTargeting\Blue Chip 2022\99 - Blue Chip - Surveillance Reporting" ); /** <-- directory listing           **/
%let linux_dir			= %str(/mbr_engmt/reserve/SASControl/DEV/bss_test);  /** <---------------------------------------------------------------------- linux macro folder          **/
%let vdrive_dir			= v:;  /** <-------------------------------------------------------------------------------------------------- v-drive for aprimo requests PROD **/
%let vdrive_connect		= %str(\\shea-s200s01\ClinicalFulfillment_Team_Properties\Groups\ClinicalFulfillmentTeam);


%let email_alerts		= %str('brian.stropich3@caremark.com' );
%let email_alerts		= %str('brian.stropich3@caremark.com',  'eugene.carbine@cvshealth.com', 'nischalareddy.chilkamarri@cvshealth.com', 'chad.bucher@cvshealth.com', 'yella.seeram@cvshealth.com', 'johnathan.dumas@cvshealth.com', 'jalene.ellis@cvshealth.com');


%let j = 1;
%let k = 1;
%let i = 1;
%let m = 1;
%let n = 1;
%let o = 1;
%let p = 1;
%let qc_product    = DNE;
%let qc_subproduct = DNE;
%let c_s_requestor = DNE;
%let qc_client 	   = DNE;
%let qc_first_nm   = DNE;
%let parameter_qc  = ;
%let parameter_qc2 = ;
%let c_s_email_message = ;
%let file_total=0;


libname v "&vdrive_dir.";

%macro get_sharepoint_requests;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - establish environment
	+---------------------------------------------------------------------------------------------*/
	libname &c_s_tdtempx. teradata user=&td_id. password="&td_pw"
					  server=&c_s_server.
					  database=&c_s_tdtempx.             
					  connection=unique
					  defer=yes 
					  fastload=yes;

	data _null_;
	x=put(datetime(),datetime20.3); 
	call symput('timestart',left(x));  
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - get a directory list of sharepoint and collect the metadata about the parameter intakes
	+---------------------------------------------------------------------------------------------*/
	filename tmp pipe &sharepoint_windows_dir.;
	                                                                                   
	data sharepoint_directory_listing1;
	  format CS_FILE_DAY CS_FILE_TIME CS_FILE_AMPM CS_FILE_NAME CS_FILE_NAME_ORIG
	         DQI_APRIMO_ACTIVITY DQI_TICKET CS_JOB_STATUS CS_USER_ID $100.;
	  length CS_CMD_LINE $2000;
	  infile tmp lrecl=32000 dlm="@";
	  input CS_CMD_LINE; 
	  if index(lowcase(CS_CMD_LINE),'ecoa_disposition_gaps.xlsx') > 0;  /*<------------------ standard = allows for upper or lower case ACFs  **/
	  CS_FILE_DAY=scan(CS_CMD_LINE,1,' '); 
	  CS_FILE_TIME=scan(CS_CMD_LINE,2,' ');
	  CS_FILE_MINUTE=scan(CS_FILE_TIME,2,':');
	  CS_FILE_AMPM=scan(CS_CMD_LINE,3,' ');
	  CS_FILE_SIZE=compress(scan(CS_CMD_LINE,4,' '),',')*1;
	  CS_FILE_NAME_ORIG=scan(CS_CMD_LINE,5,' ');
	  CS_FILE_NAME=scan(CS_CMD_LINE,5,' ');
	  DQI_APRIMO_ACTIVITY='1115555';
	  DQI_TICKET='600001';
	  CS_JOB_STATUS='COMPLETE - PHASE5';
	  CS_LOG='Successful ECOA Report execution';
	  CS_USER_ID='EA Team';
	run;

	data sharepoint_directory_listing;
	  set sharepoint_directory_listing1 ; 
	run;
	
	data sharepoint_directory_listing; 
	set sharepoint_directory_listing;  
	if upcase(scan(scan(CS_FILE_NAME,3,'_'),1,'.')) = 'GAPS';
	run;	

	data sharepoint_directory_listing;
	format CS_FILE_DAY_FILTER mmddyy10. ;
	set sharepoint_directory_listing;
	CS_FILE_DAY_FILTER =input(CS_FILE_DAY, ? anydtdte21.); 
	if CS_FILE_DAY_FILTER > today() - 365;
	drop CS_FILE_DAY_FILTER;
	run;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - validate process queue history if directory list of sharepoint was processed
	+---------------------------------------------------------------------------------------------*/
	proc sql noprint;
	connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
	create table cs_process_queue as
	select * from connection to tera
	(
		select * 
		from dss_cea2.cs_process_queue  
		where cs_ts > current_date - 365
		and cs_cmd_line like '%GAPS%'
	);
	disconnect FROM tera;
	quit;

	data cs_process_queue;
	set cs_process_queue;
	CS_FILE_MINUTE=scan(CS_FILE_TIME,2,':');  /*<----- need to check by minute and not time due to hour -2 VM AZ time  **/
	run;                                      /*<----- exclude cs_file_ampm since there is crossover at noon due to hour -2 VM AZ time **/

	proc sort data = cs_process_queue;
	by dqi_aprimo_activity dqi_ticket cs_file_name cs_file_day cs_file_minute cs_file_size;
	run;

	proc sort data = sharepoint_directory_listing;
	by dqi_aprimo_activity dqi_ticket cs_file_name cs_file_day cs_file_minute cs_file_size;
	run;

	data sharepoint_directory_listing ;
	merge sharepoint_directory_listing (in=a)
		cs_process_queue               (in=b);
	if a and not b;
	by dqi_aprimo_activity dqi_ticket cs_file_name cs_file_day cs_file_minute cs_file_size; 
	run;
	
	data sharepoint_directory_listing ;
	format cs_file_day2 cs_exclude_date mmddyy10. ;
	set sharepoint_directory_listing ;
	cs_file_day2=input(cs_file_day, ? anydtdte21.); 
	cs_exclude_date = '06JUN20'd;
	if cs_file_day2 < cs_exclude_date then delete;  /**<----------- EA SharePoint Move - ACF dates change **/
	drop cs_file_day2 cs_exclude_date;
	run;			


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - apply processing throttle
	+---------------------------------------------------------------------------------------------*/
	data sharepoint_directory_listing;
	**set sharepoint_directory_listing (obs=2);  /** <---- throttle - 2 campaigns per batch **/
	set sharepoint_directory_listing;            /** <---- no throttle - all campaigns per batch due to execute background **/
	drop cs_file_minute; 
	run;

	data cs_process_queue;
	set cs_process_queue; 
	drop cs_file_minute;
	run;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - load directory list of sharepoint into process queue
	+---------------------------------------------------------------------------------------------*/
	

	data _null_;
	set sharepoint_directory_listing end=eof;
	ii=left(put(_n_,4.)); 
	call symput('cs_file_name'||ii,trim(left(cs_file_name)));
	if eof then call symput('file_total',ii);
	run;

	%put NOTE: file_total = &file_total. ;

	%if &file_total = 0 %then %do;  /** start - total loop - 1 **/
	%end;
	%else %do;

		%do j = 1 %to &file_total. ;    /** start - file loop - 2 **/

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - load parameter intake request into process queue
			+---------------------------------------------------------------------------------------------*/
			%load_excel_intake_contents(data_teradata=dss_cea2.cs_process_queue, data_excel=&&cs_file_name&j, data_sas=sharepoint_directory_listing, data_queue=Y );

		%end;  /** end - file loop - 2 **/

	%end;  /** start - total loop - 1 **/

%mend get_sharepoint_requests;

%macro get_sharepoint_parameterintake;

	%if &file_total = 0 %then %do;  /** start - total loop - 0 **/
	%end;
	%else %do;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get the lastest list of requests from process queue
		|
		|  LOGIC - allows for new, updates, and existing that may not have been processed
		+---------------------------------------------------------------------------------------------*/
		proc sql noprint;
		connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
		create table cs_process_queue1 as
		select * from connection to tera
		(
			select * 
			from dss_cea2.cs_process_queue 
			where cs_job_ts > current_date - 1
		);
		disconnect FROM tera;
		quit;

		proc sort data = cs_process_queue1 ;
		by dqi_aprimo_activity dqi_ticket descending cs_ts descending cs_job_status ;
		run;

		proc sort data = cs_process_queue1 out = cs_process_queue_files nodupkey;
		by dqi_aprimo_activity dqi_ticket ;
		run;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - load parameter intake into process parameter
		+---------------------------------------------------------------------------------------------*/
		data cs_process_queue_files;
		set cs_process_queue_files;
		if cs_job_status = 'COMPLETE - PHASE5' ;
		if cs_job_ts = cs_ts;             /** <------------  process the latest + greatest if not processed **/
		/** where dqi_ticket = '1592953';**/ 
		run;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - create explicit paramter intakes to process
		+---------------------------------------------------------------------------------------------*/
		proc sql noprint;
		select cs_row_gid into: cs_row_gid_all separated by ','
		from cs_process_queue_files;
		quit;

		%put NOTE: cs_row_gid_all = &cs_row_gid_all. ;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - loop around complete - pending
		+---------------------------------------------------------------------------------------------*/
		%let queue_total1 = 0;
		%let vdrive_dir_original = &vdrive_dir. ;

		data _null_;
		set cs_process_queue_files end=eof;
		ii=left(put(_n_,4.));
		call symput('cs_row_gid'||ii,trim(left(cs_row_gid)));
		call symput('dqi_ticket'||ii,trim(left(dqi_ticket)));
		call symput('dqi_aprimo_activity'||ii,trim(left(dqi_aprimo_activity)));
		call symput('cs_file_name'||ii,trim(left(cs_file_name))); 
		call symput('cs_file_name_orig'||ii,trim(left(cs_file_name_orig)));
		if eof then call symput('queue_total1',ii);
		run;

		proc sql noprint;
		select cs_file_name into: cs_file_name separated by ' '
		from cs_process_queue_files;
		quit;

		proc sql noprint;
		select cs_file_name_orig into: cs_file_name_orig separated by ' '
		from cs_process_queue_files;
		quit; 

		%put NOTE: cs_file_name = &cs_file_name.  ;
		%put NOTE: cs_file_name_orig = &cs_file_name_orig.  ;
		%put NOTE: queue_total1 = &queue_total1. ;
		%put NOTE: file_total  = &file_total. ; 

		%if &queue_total1 = 0 or &file_total = 0 %then %do;   /** start - total loop - 1 **/
		%end;
		%else %do;

			%do i = 1 %to &queue_total1. ;  /** start - queue loop - 2 **/

				%put *****************************************************************************************;
				%put NOTE: i = &i. ;
				%put NOTE: cs_row_gid = &&cs_row_gid&i. ;
				%put NOTE: cs_file_name = &&cs_file_name&i. ;
				%put NOTE: cs_file_name_orig = &&cs_file_name_orig&i. ;
				%put *****************************************************************************************;

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - get parameter intake from sharepoint
				+---------------------------------------------------------------------------------------------*/
				%let filepath=&sharepoint_dir./&&cs_file_name_orig&i;
				filename out "&pending_dir.\&&cs_file_name&i";

				proc http 
				out=out
				url="&filepath."
				method="get" 
				proxyport=8080; 
				run;

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - import parameter intake
				+---------------------------------------------------------------------------------------------*/
				proc import out=parameter_in_&&dqi_ticket&i 
				datafile="&pending_dir.\&&cs_file_name&i"
				dbms=xlsx replace; 
				sheet="Sheet1";
				getnames=no;
				run;

				data parameter_in_&&dqi_ticket&i ;
				format parameter_id $10. parameter_required $100. parameter_value $10. ;
				set parameter_in_&&dqi_ticket&i (firstobs=2 keep = a b c ) ;
				parameter_id=left(trim(a));
				parameter_required=left(trim(b));
				parameter_value=left(trim(c)); 
				acf_row_id='';
				if b = 'ECOA Request' then delete;
				drop a b c ;
				run;


				options linesize=200;

				data _null_ ;
				set parameter_in_&&dqi_ticket&i ;
				put @1 parameter_id @10 parameter_required @50 parameter_value  ;
				run;

				data tmp_cs_process_queue_files;
				set cs_process_queue_files;
				if cs_file_name = "&&cs_file_name&i";
				run;

				data x ;
				set parameter_in_&&dqi_ticket&i ;
				parameter_value2="'"||left(trim(parameter_value))||"'";
				parameter_value3=compress(parameter_value2,"'-");
				call symput('sql_date',left(trim(parameter_value2)));
				call symput('report_date',left(trim(parameter_value3)));
				run;

				%put NOTE: sql_date = &sql_date. ;
				%put NOTE: report_date = &report_date. ;


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - where elgibility logic condition - state + language    
				+---------------------------------------------------------------------------------------------*/
				proc sql noprint;
				connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
				create table _gaps as
				select * from connection to tera
				(

					select execution_date, source_team, 'DISPOSITIONS', 
					case when m.oppty_msg_altid is null then 'NO MATCH'
					else 'MATCH' end as validation,
					oppty_evnt_id, msg_evnt_id
					from 
					(
					    select execution_date, source_team, oppty_evnt_id, msg_evnt_id
					    from  dss_cea2.dqi_eoms_dispositions
					    where oppty_evnt_id > 100 
					    and execution_date = &sql_date. 
					    and source_team = 'CPL'
					) e left join
					dwu_edw_rb.v_oppty_msg m on e.msg_evnt_id = m.msg_altid 
					where validation = 'NO MATCH'

				);
				disconnect FROM tera;
				quit;

				proc export 
					data=work._gaps
					dbms=xlsx 
					outfile="&pending_dir.\ecoa_disposition_gaps_&report_date..xlsx" 
					replace; 
				run;	

				proc sql noprint;
				select count(*) into: ecoa_cnt separated by ''
				from work._gaps ;
				quit;

				%put NOTE: ecoa_cnt = &ecoa_cnt. ;

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - deliver eoy report
				|
				+---------------------------------------------------------------------------------------------*/	
				filename in "&pending_dir.\ecoa_disposition_gaps_&report_date..xlsx" ;

				proc http 
				in=in
				url="&sharepoint_dir./ecoa_disposition_gaps_&report_date..xlsx" 
				method="put" 
				proxyport=8080; 
				run;

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - email failure
				+---------------------------------------------------------------------------------------------*/
				%let ecoa_report_name=%str(ecoa_disposition_gaps_&report_date..xlsx);

				%let sasapp = paz1sascapl4p 7551;
				options comamid=tcp;
				signon SASApp userid="&linux_id."   password="&linux_pw."  ;

				%syslput c_s_email_to=&email_alerts.; 
				%syslput ecoa_report_name=&ecoa_report_name.;
				%syslput ecoa_location=&sharepoint_dir2.; 
				%syslput ecoa_cnt=&ecoa_cnt.; 
				%syslput report_date=&report_date.; 

				rsubmit SASApp ;

					filename xemail email to=(&c_s_email_to) subject = "ECOA Validation - CPL Disposition Gap Report for &report_date." content_type="text/html" ; 


					data _null_;
					file xemail lrecl=2000;	  
	
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
					put " "; put '<br>'; 
					put "This is an automated email sent by SAS on behalf of Campaign Targeting Team."; put '<br>'; put '<b>';
					put "Please DO NOT RESPOND TO THIS E-MAIL"; put '</b>'; put '<br>';
					put "The ECOA Validation - CPL Disposition Gap Report has been created on the EA SharePoint Site. " ; put '<br>'; put '<br>'; 
				
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - email body
					+---------------------------------------------------------------------------------------------*/				
					put '<b><font color="#13478C">';put '<u>'; put "ECOA Report - Information:"; put '</b></font>';put '</u>'; put '<br>'; 
						put "<ul>";
							put "<li>    ECOA Report - name:      &ecoa_report_name "; put '</li>';
							put "<li>    ECOA Report - gap date:  &report_date"; put '</li>';
							put "<li>    ECOA Report - counts:    &ecoa_cnt"; put '</li>';
							put "<li>    ECOA Report - location:  &ecoa_location"; put '</li>'; 
					put "</ul>";
					put "<br>";
							 
					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - email closing
					+---------------------------------------------------------------------------------------------*/	
					put "Thank you and have a great week."; put '<br>';
					put '<br>';
					put "Sincerely,"; put '<br>';
					put "EA - Campaign Targeting Team"; put '<br>';
					put " "; put '<br>';
					run; 					
 

				endrsubmit;
				signoff;


			%end;  /** end - queue loop - 2 **/

		%end;  /** end - total loop - 1 **/
		
	%end;  /** start - total loop - 0 **/


%mend get_sharepoint_parameterintake;
 

%macro load_excel_intake_contents(data_teradata=, data_excel=, data_sas=, data_queue=NO);

	proc contents data =  &data_teradata. 
		      out  = temp_variables (keep = name varnum type format length) 
		      noprint;
	run;

	data temp_variables;
	set temp_variables;
	_name_=upcase(name);
	run;

	data _null_;
	set &data_sas. end=eof;
	where cs_file_name = "&data_excel.";
	ii=left(put(_n_,4.));
	%if &data_queue = NO %then %do;
	call symput('parameter_id'||ii,trim(left(parameter_id)));
	%end;
	%else %do;
	%end;
	if eof then call symput('parameter_total',ii);
	run;

	%put NOTE: file_total = &parameter_total. ;

	%do k = 1 %to &parameter_total. ;  /** start - parameter loop - 3 **/
		
		data tmp_parameter;
		set &data_sas.;
		%if &data_queue = NO %then %do;
		if parameter_id = "&&parameter_id&k" ;
		%end;
		%else %do;
		if cs_file_name = "&data_excel.";
		%end;
		run;

		proc transpose data=tmp_parameter out=temp_transpose1 ;
		var _char_ ;
		run;

		proc transpose data=tmp_parameter  out=temp_transpose2 ;
		var _numeric_ ;
		run;

		data temp_transpose1 (rename=(col1b=col1));
		format col1b $600.;
		set temp_transpose1;
		col1b=left(col1); 
		keep _name_ _label_ col1b;
		run;

		data temp_transpose2 (rename=(col1b=col1));
		format col1b $600.;
		set temp_transpose2;
		col1b=left(col1);
		keep _name_ _label_ col1b;
		run;

		data temp_transpose;
		format col2 $600. ;
		set temp_transpose1 temp_transpose2;
		_name_=upcase(_name_);
		col2=left(trim(_name_));
		run;	

		proc sort data = temp_transpose;
		by _name_;
		run; 

		proc sort data = temp_variables;
		by _name_;
		run; 	

		data temp_insert_variables;
		merge temp_transpose temp_variables ;
		by _name_;
		name=lowcase(name);
		%if &data_queue = NO %then %do;
			**if type = 2 and index(col1,"'") > 0 then col1=translate(col1,'',"'");
			if type = 2 and index(col1,"'") > 0 then col1=tranwrd(col1, "'", "''");
			if type = 2 then col2="'"||strip(trim(col1))||"'"; 
			if _NAME_ = 'CS_ROW_GID' then col2=col1;  
			if _NAME_ = 'CS_TS'   then col2="CAST(CURRENT_DATE AS TIMESTAMP(0)) + ((CURRENT_TIME - TIME '00:00:00') HOUR TO SECOND(0))";
		%end;
		%else %do;
			if type = 2 then col2="'"||strip(trim(col1))||"'";
			if type = 2 and index(col1,"'") > 0 then col2="''"||strip(trim(col1))||"''";
			if _NAME_ = 'CS_ROW_GID' then col2='1';
			if _NAME_ = 'CS_FILE_SIZE' then col2=col1; 
			if _NAME_ = 'CS_JOB_TS'   then col2="CAST(CURRENT_DATE AS TIMESTAMP(0)) + ((CURRENT_TIME - TIME '00:00:00') HOUR TO SECOND(0))";
			if _NAME_ = 'CS_TS'   then col2="CAST(CURRENT_DATE AS TIMESTAMP(0)) + ((CURRENT_TIME - TIME '00:00:00') HOUR TO SECOND(0))";
		%end;
		run;

		proc sort data = temp_insert_variables;
		by varnum;
		run;	

		proc sql noprint;
		select col2 into: cs_variables separated by ', '
		from temp_insert_variables;
		quit; 

		%put NOTE: macro cs_variables = &cs_variables.  ;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - insert queue data into queue table
		+---------------------------------------------------------------------------------------------*/
		proc sql;
			connect to teradata (user=&td_id  password="&td_pw"  tdpid=&c_s_server.);
			execute (insert into  &data_teradata.  
				 values (  &cs_variables. )
				 ) by teradata;
			execute (commit) by teradata;
			disconnect from teradata;
		quit;

	%end;  /** end - parameter loop - 3 **/

%mend load_excel_intake_contents;


%macro m_clean_table_bulkload_data(libname=, data_set=);

	%let c_i_npschars=N;
	%let c_i_npschars_variables=NO_VARIABLES;
	
	data test_npschars_list (drop=i npschars);
	length decimalv $5 hexadecv $6 ascichar compress specnpsc $3 npschars $161;
	do i=0 to  255;
	 if i=0 then npschars=byte(i);
	 else npschars=trim(npschars)||byte(i);
	end;
	do i=0 to 255;
	 decimalv="("||strip(put(rank(byte(i)), best.))||")";
	 hexadecv="("||strip(put(byte(i), $hex4.))||")";
	 ascichar="("||byte(i)||")";
	 compress="("||compress(byte(i), ,'KW')||")";
	 specnpsc="("||compress(byte(i),npschars)||")";
	 output;
	end;
	label
	 decimalv="Decimal value"
	 hexadecv="Hexadecimal value"
	 ascichar="Ascii character"
	 compress="Char after 'COMPRESS func with KW mod'"
	 specnpsc="Char after 'NPSC specified in COMPRESS'";
	run;	

	proc contents data = &libname..&data_set. 
	              out  = clean_variables (keep = name type) 
	              noprint;
	run;
	
	data clean_variables;
	set clean_variables;
	if indexc(trim(name),' /\?-()') > 0 then delete ;
	run;
	
	data _null_;
	  set clean_variables end=last; 
	  where type = 2;
	  call symput('clean_variables'||left(_n_),trim(name));
	  if last then call symput('total_clean_variables',left(_n_));
	run;
	
	%put NOTE: total_clean_variables = &total_clean_variables. ;
	%put NOTE: Remove non-printable special characters such as CR LF or non-breaking space ;

	data npschars (drop = i);  
	length npschars $42 bsps $1; *43 is the total number of characters for the loop;
	retain npschars;
		if _n_=1 then do; 
			do i=0 to 31, 127, 128, 129, 141, 142, 143, 144, 157, 158, 173 ;
			  if i=0 then npschars=byte(i);
			  else npschars=trim(npschars)||byte(i);
			end;
			bsps=byte(160);
		end; 
	run;
	
	data &libname..&data_set. (drop = npschars bsps);
	if _n_=1 then set npschars;
	set &libname..&data_set.;
		%do r = 1 %to &total_clean_variables. ;
		n=left(put(_n_,8.));
		
          if indexc(&&clean_variables&r, bsps) > 0 then do; 
				do until (position2=0);
				  position2=indexc(&&clean_variables&r, bsps);
				  if position2 > 0 then substr(&&clean_variables&r, position2, 1) = " ";
				end; 
				put "NOTE: Variable with NPSC - column = &&clean_variables&r + value = " &&clean_variables&r;
				call symput ('c_i_npschars_variables'||n,"&&clean_variables&r "); 		
		  		call symput ('c_i_npschars','Y');
		  end;
		
		  if indexc(&&clean_variables&r, npschars) > 0 then do;
		  
			%if %index(%upcase(&&clean_variables&r),ALT)    > 0 
			 or %index(%upcase(&&clean_variables&r),TEXT)   > 0 
			 or %index(%upcase(&&clean_variables&r),FREE)   > 0 
			 or %index(%upcase(&&clean_variables&r),MERGE)  > 0
			 or %index(%upcase(&&clean_variables&r),MESSAG) > 0
			 or %index(%upcase(&&clean_variables&r),MSG)    > 0 %then %do;  
				do until (position=0);
				  position=indexc(&&clean_variables&r, npschars);
				  if position > 0 then substr(&&clean_variables&r, position, 1) = " ";
				end; 
				put "NOTE: Variable with NPSC - column = &&clean_variables&r + value = " &&clean_variables&r;
				call symput ('c_i_npschars_variables'||n,"&&clean_variables&r "); 
			%end;
			%else %do;  
				&&clean_variables&r=compress(&&clean_variables&r, npschars);  
				put "NOTE: Variable with NPSC - column = &&clean_variables&r + value = " &&clean_variables&r;
				call symput ('c_i_npschars_variables'||n,"&&clean_variables&r ");
			%end;
			
		  	call symput ('c_i_npschars','Y');
		  end;
		%end;		
	run;

	data vmacro_search;
	set sashelp.vmacro;
	if index(upcase(name), 'C_I_NPSCHARS_VARIABLES') > 0;
	if length(name) > 22;
	run;

	proc sql noprint;
	select count(*) into: vmacro_search_cnt separated by ''
	from vmacro_search;
	quit;

	%if &vmacro_search_cnt > 0 %then %do;
		proc sql noprint;
		select distinct value into: c_i_npschars_variables separated by ' '
		from vmacro_search;
		quit;
	%end;

	%put NOTE: c_i_npschars = &c_i_npschars.;
	%put NOTE: c_i_npschars_variables = &c_i_npschars_variables.;		
	
%mend m_clean_table_bulkload_data;

%macro get_sysparm;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - collect sysparm from the operating environment
	+---------------------------------------------------------------------------------------------*/
	data sysparm;
	  length  sysparm express param value $ 300 ;
	  sysparm = symget('sysparm');
	  do i=1 to 50 until(express = '');
	    express = left(scan(sysparm, i, ' '));
	    param   = left(upcase(scan(express, 1, '=')));
	    value  = left(scan(express, 2, '='));
		if param ne '';
		output;
	    if param ne '' and length(param) <=32 then do;
		call symput(param, trim(left(value)));
	    end;
	  end;
	run;
	
	data _null_;
	 set sysparm;
	 put _all_;
	run;
	
	proc sql noprint;
	  select count(*) into: globalvarscnt 
	  from sysparm;
	quit;
	
	%if &globalvarscnt ne 0 %then %do;

		data vmacro;
		set sashelp.vmacro;
		run; 

		proc sql noprint;
		  create table sysparm as
		  select a.*, b.scope
		  from sysparm as a left join 
               vmacro as b
          on a.param = b.name ;
		quit;

		%let globalvars= ;
		
		proc sql noprint;
		  select param into: globalvars separated by " "
		  from sysparm
		  where scope = '';
		quit;

		%put NOTE: globalvars = &globalvars. ;

		%global &globalvars.  ;
	
	%end;
	%else %do;
		%put NOTE: globalvars = No sysparm variables exist.;
	%end;

%mend get_sysparm;

%macro m_convert_variables_quotes(macro_string=, macro_variable=);

	%if &macro_string > ' ' or &macro_variable. = c_s_prctr_excl %then %do;

		%let macro_temp=;

		data macro_string ; 
		  i = 1 ;
		  do while ( scan("&macro_string", i, ',') ^= ' ' ) ;
			 macro_string = scan(upcase("&macro_string"), i, ',');
			 macro_string=left(trim(macro_string));
			 output ;
			 i + 1 ;
		  end ;
		  drop i ;
		run ;

		proc sql noprint;
		select count(*) into: m_cnt separated by ''
		from macro_string;
		quit;

		proc sql noprint;
		  select distinct "'"||trim(macro_string)||"'"  into: macro_temp separated by ','
		  from macro_string
		  where macro_string > ' ' ;
		quit;

		%if &m_cnt = 0 and &macro_variable. = c_s_prctr_excl %then %do;
		  %let c_s_prctr_excl=%str('NO_prctr_exclusions');
		%end;
		%else %do;
		  %let &macro_variable=&macro_temp;
		%end;

		%put NOTE: macro &macro_variable. = &&&macro_variable.  ;
	
	%end;

%mend m_convert_variables_quotes;



/* SASDOC --------------------------------------------------------------------------------------
|  STEP - execute module compartmentalization solutions 
+---------------------------------------------------------------------------------------------*/
%get_sharepoint_requests;
%get_sharepoint_parameterintake;