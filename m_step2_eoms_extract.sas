
/* HEADER---------------------------------------------------------------------------------------
| MACRO:	m_step2_eoms_extract
|
| LOCATION:  
| 
| PURPOSE:	Process CPL - SMCE data for Opportunity + Opportunity RX + Disposition + Disposition RX  
|
| LOGIC:     
|		step 2 = loads 16 files = select * from dss_cea2.dqi_eoms_cmctn_history where execution_date = current_date and clm_evnt_gid < 5 
|
| QC:
|		select distinct clm_evnt_gid, clnt_cd, a.source_team, coalesce(source_opp_file,source_opprx_file) as name, source_new_file, source_file 
|		from dss_cea2.dqi_eoms_cmctn_history a inner join 
|		dss_cea2.dqi_eoms_files b on  coalesce(a.source_opp_file,a.source_opprx_file) = b.source_new_file where clm_evnt_gid < 5
|		where b.source_new_file like '%YYYYMMDD%'
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

%macro m_step2_eoms_extract(team_id=, layout_id=);


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - determine which files are available for processing
	|   
	+---------------------------------------------------------------------------------------------*/
	%macro m_create_eoms_list(data_out=);


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - check directory for files
		|   
		+---------------------------------------------------------------------------------------------*/
		data eoms_files;
		rc=filename("mydir","&eoms_directory.");
		did=dopen("mydir");
		memcount=dnum(did);
		do i=1 to memcount;
			name=dread(did,i);
			%if &layout_id. = 1 %then %do;
			match=find(name,"opportunity","i t",1);
			%end;
			%if &layout_id. = 2 %then %do;
			match=find(name,"rx_opportunity","i t",1);
			%end;
			%if &layout_id. = 3 %then %do;
			match=find(name,"disposition","i t",1);
			%end;
			%if &layout_id. = 4 %then %do;
			match=find(name,"rx_disposition","i t",1);
			%end;			
			if match then output;
		end;
		rc=dclose(did);
		run;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - filter files for processing
		|   
		+---------------------------------------------------------------------------------------------*/	
		data eoms_files;
		set eoms_files;
			%if &layout_id. = 1 or &layout_id. = 3 %then %do;
			if index(name,"_rx_") = 0;
			%end;
			%if &layout_id. = 2 or &layout_id. = 4 %then %do;
			if index(name,"_rx_") > 0;
			%end; 			
			%if &team_id. = cpl %then %do;
			if index(name,"&team_id.") > 0;
			if index(name,'000123') > 0;           /*<------------------------- new suffix for opp  */
			%end;
			%if &team_id. = sfmc %then %do;;
			if index(name,"&team_id.") > 0;
			if index(name,'000456') > 0;           /*<------------------------- new suffix for disp */
			%end; 
			if index(name,"&c_s_file_date.") > 0;  /*<------------------------- only process submitted date from CTL */
		run;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - exclude metadata history = sftp + extract (not cmctn + opps + disps)
		|
		|  NOTE - reason since if invalid          then wont exist in cmctn > 5
		|         reason since if valid or invalid then will be in  history < 5
		+---------------------------------------------------------------------------------------------*/		
		proc sql noprint;
		connect to teradata as tera (user=&td_id  password="&td_pw"  tdpid=&c_s_server. fastload=yes);
		create table dqi_eoms_cmctn_history as
		select * from connection to tera
		( 			
			select distinct clm_evnt_gid, clnt_cd, a.source_team, coalesce(source_opp_file,source_opprx_file) as name, source_new_file, source_file
			from &c_s_table_name.  a inner join
			     &c_s_table_files. b
			on coalesce(a.source_opp_file,a.source_opprx_file) = b.source_new_file
			where clm_evnt_gid < 5			
		);
		disconnect FROM tera;
		quit;		

		proc sort data = dqi_eoms_cmctn_history;
		by name;
		run;

		proc sort data = eoms_files;
		by name;
		run;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - removed prior files that were processed
		|   
		+---------------------------------------------------------------------------------------------*/
		data eoms_files;
		merge eoms_files (in=a) dqi_eoms_cmctn_history (in=b);
		by name;
		if a and not b;
		run;

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - process only files that dont exist in history and submitted date by CTL
		|   
		|  NOTE - can expand the filter_date for lookback but shut off for process old files
		|
		+---------------------------------------------------------------------------------------------*/
		data &data_out.;
		format source_team $30. filter_date mmddyy10. ; 
		set eoms_files;
		source_team=upcase(scan(name,1,'_'));
		filter_date=mdy( substr(scan(name,2,'.'),5,2), substr(scan(name,2,'.'),7,2), substr(scan(name,2,'.'),1,4) );
		if filter_date = today() - &execution_date. ;		/*<------------------------- only process submitted date from CTL */
		run;
		
	%mend m_create_eoms_list;
	%m_create_eoms_list(data_out=temp01_extract);
	

	%let cnt_cmctn_history = 0;

	proc sql noprint;
	select count(*) into: cnt_cmctn_history separated by ''
	from temp01_extract;
	quit;
	
	%put NOTE: cnt_cmctn_history = &cnt_cmctn_history. ;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - these are the data files sent for today
	|   
	+---------------------------------------------------------------------------------------------*/	
	data _null_;
	set temp01_extract;
	put @1 _n_ @10 source_team @20 name;
	run;
	
	%if &cnt_cmctn_history. = 9999999 %then %do;
	
		data working.qc_step2;
		set temp01_extract;
		run;				

		%let c_s_email_subject = EA ECOA CPL SMFC No Daily Files Failure;
		%m_abend_handler(abend_report=%str(EA ECOA - No Daily Files - step 2),
		abend_message=%str(EA ECOA - No Daily Files));	
	
	%end;
	

	%put NOTE: ************************************************************************;
	%put NOTE: team_id = &team_id. - &layout_id. ;
	%put NOTE: cnt_cmctn_history = &cnt_cmctn_history. ;
	%put NOTE: ************************************************************************;

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - loop through metadata for OPP + OPPRX + CNTL files
	+---------------------------------------------------------------------------------------------*/
	%if &cnt_cmctn_history. ne 0 %then %do;  /** <----------------------------------------------------- start - cnt_cmctn_history **/

		data x;
		set temp01_extract;
		eoms_type=2;
		if index(upcase(name),'CNTL') > 0 then eoms_type=1;
		eoms_name=scan(name,2,'.');
		eoms_sht_name=substr(eoms_name,1,8);
		call symput('eoms_type'||left(_n_),trim(left(eoms_type)));
		call symput('eoms_name'||left(_n_),trim(left(eoms_name))); 
		call symput('eoms_sht_name'||left(_n_),trim(left(eoms_sht_name))); 
		call symput('eoms_files'||left(_n_),trim(left(name))); 
		call symput('eoms_total',trim(left(_n_)));
		run;

		%put NOTE: eoms_total = &eoms_total. ;

		%do i = 1 %to &eoms_total. ;  /** <----------------------------------------------------- start - eoms_total **/

			%put NOTE: file name = &&eoms_files&i ;
			%put NOTE: file type = &&eoms_type&i ;
			%put NOTE: short name = &&eoms_sht_name&i ;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - import data and create metadata for CNTL files
			+---------------------------------------------------------------------------------------------*/				
			%if &&eoms_type&i = 1 %then %do;  /** <----------------------------------------------------- start - eoms_type **/

				data &team_id._&layout_id._cntl_&&eoms_name&i.;
				length x1-x9  $100. ;
				infile "&eoms_directory.&SYS_DIR.&&eoms_files&i" firstobs=1 missover pad lrecl=32000 delimiter='|' dsd;
				input x1-x9 ; 
				if missing(x2) then delete;
				run;

				data &team_id._&layout_id._cntl_&&eoms_name&i.;
				set &team_id._&layout_id._cntl_&&eoms_name&i.;
				eoms_file="&&eoms_files&i.";
				eoms_file_cntl=x2;
				eoms_file_count=x3*1;
				drop x1-x9;
				call symput('eoms_file',trim(left(eoms_file)));
				call symput('eoms_file_count',trim(left(eoms_file_count)));
				run;
				
				%let qc_cntl_001 = &eoms_file_count.;

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					insert into &c_s_table_name. values ( 
						0,
						&layout_id., 
						null,
						0,
						0,
						null,
						null,
						null,
						%if &layout_id. = 1 %then %do;
							'CNTLOPPFILES', 
						%end; 
						%else %if &layout_id. = 2 %then %do;
							'CNTLOPPRXFILES',  
						%end; 
						%else %if &layout_id. = 3 %then %do;
							'CNTLDISPFILES',  
						%end;
						%else %if &layout_id. = 4 %then %do;
							'CNTLDISPRXFILES',  
						%end;						
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null, 
						null,
						null,
						null,
						null,
						null,
						%tslit(%upcase(&team_id.)),
						%if &layout_id. = 1 or &layout_id. = 3 %then %do;
							%tslit(&eoms_file.),
							&eoms_file_count.,
						%end; 						
						%else %do;
							null,
							null,
						%end;
						%if &layout_id. = 2 or &layout_id. = 4 %then %do;
							%tslit(&eoms_file.),
							&eoms_file_count.,
						%end;
						%else %do;
							null,
							null,
						%end; 
						null,
						null,
						null,
						null,
						'0',
						'SUCCESS',
						current_date - &execution_date.
					)

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 
				

			%end;
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - import data and create metadata for OPP + OPPRX + DIPS + DIPSRX files
			+---------------------------------------------------------------------------------------------*/			
			%else %if &&eoms_type&i = 2 %then %do;
			

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - determine the verison of the eoms files
				|	  version 1 = opp 250 - opprx 279 - disp 13 - disprx 13
				|	  version 2 = opp 629 - opprx 400 - disp 22 - disprx 22
				+---------------------------------------------------------------------------------------------*/
				%let version = 1;   /** <--------------------- initialize for empty files */
				
				data version;
				length version $3000. ;
				infile "&eoms_directory.&SYS_DIR.&&eoms_files&i" firstobs=1 missover pad lrecl=32000 delimiter='=' dsd;
				input version; 
				version=countc(version,'|');
				%if &layout_id. = 1 or &layout_id. = 2 %then %do;
					if version > 300 then version='2';
					else version='1';
				%end;
				%if &layout_id. = 3 or &layout_id. = 4 %then %do;
					if version > 15 then version='2';
					else version='1';
				%end;				
				if _n_ = 2 then stop;
				call symput('version',left(trim(version)));
				run;	
				
				%put NOTE: version = &version. ;

				proc sql noprint;
				create table dqi_eoms_layout as
				select *
				from dss_cea2.dqi_eoms_layout
				where layout_id = &layout_id.
				%if &version. = 1 %then %do;
					and required ne 'X'
				%end;
				order by layout_id, sequence_id;
				quit;

				data _null_;
				set dqi_eoms_layout; 
				n=trim(left(_n_));
				call symput('name_var'||n,trim(left(data_attribute_name))); 
				call symput('name_var_total',n);
				run;

				data &team_id._&layout_id._&&eoms_name&i.;
				length %do k=1 %to &name_var_total; &&name_var&k %end; $100. ;
				infile "&eoms_directory.&SYS_DIR.&&eoms_files&i" firstobs=1 missover pad lrecl=32000 delimiter='|' dsd;
				input %do j=1 %to &name_var_total; &&name_var&j %end; ; 
				run;
				
				data &team_id._&layout_id._&&eoms_name&i.;
				set &team_id._&layout_id._&&eoms_name&i.;
				if length(oppty_evnt_id) > 5;
				run;
				
				data &team_id._&layout_id._&&eoms_name&i.;
				set &team_id._&layout_id._&&eoms_name&i.;
				if missing(OPPTY_EVNT_ID) then OPPTY_EVNT_ID = 'X';
				if missing(MSG_EVNT_ID) then MSG_EVNT_ID = 'X';
				%if &layout_id. = 2 or &layout_id. = 4 %then %do;
				if missing(RX_FILL_EVNT_ID) then RX_FILL_EVNT_ID = 'X';
				%end;
				run;
				

				/* SASDOC --------------------------------------------------------------------------------------
				|  QC - validate unique record IDs
				|
				|  RULE - opp + disp     = OPPTY_EVNT_ID MSG_EVNT_ID should be unique 
				|         opprx + disprx = OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID should be unique
				|
				|  NOTE - since EA is managing identity columns for IT then MSG_EVNT_ID and RX_FILL_EVNT_ID are uniquie for file subject
				|         the rx will still be tied to opportunity of OPPTY_EVNT_ID for multiple channels and audiences of PTNT + PRSC 
				|
				|	select opp.oppty_evnt_id, em.msg_evnt_id, rx.rx_fill_evnt_id, 
				|              ep.evnt_pty_role_cd, em.chnl_cd, rxm.rx_dispn_chnl_cd, rxm.evnt_rel_id, 
				|              count(*) as cnt
				|	from  dwv_eoms.opportunity opp
				|	join dwv_eoms.event_party ep       on opp.oppty_evnt_id = ep.evnt_id
				|	join dwv_eoms.event_message em     on opp.oppty_evnt_id = em.oppty_evnt_id 
				|	join dwv_eoms.event_rx_fill rx     on opp.oppty_evnt_id = rx.oppty_evnt_id 
				|	join dwv_eoms.event_rx_message rxm on opp.oppty_evnt_id = rxm.oppty_evnt_id 
				|	where  year(opp.rec_crte_ts) = 2021
				|	and  month(opp.rec_crte_ts) = 3
				|	group by 1,2,3,4,5,6,7
				|	having cnt > 1 	
				|
				|	File Names:
				|	-----------------------------------------------------------------------------
				|	1 = 000123 = cpl  opportunity
				|	1 = 000456 = sfmc opportunity
				|	2 = 000123 = cpl  rx opportunity
				|	2 = 000456 = sfmc rx opportunity
				|	3 = 000123 = cpl  disposition
				|	3 = 000456 = sfmc disposition
				|	4 = 000123 = cpl  rx disposition
				|	4 = 000456 = sfmc rx disposition				
				|
				+---------------------------------------------------------------------------------------------*/
				
				proc sort data=&team_id._&layout_id._&&eoms_name&i.  dupout=duplicate_recs nodupkey; 
				%if &layout_id. = 4 %then %do;
				by  OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID;
				%end;
				%if &layout_id. = 3 %then %do;
				by  OPPTY_EVNT_ID MSG_EVNT_ID ;
				%end;				
				%if &layout_id. = 2 %then %do;
				by  OPPTY_EVNT_ID MSG_EVNT_ID RX_FILL_EVNT_ID;
				%end;				
				%if &layout_id. = 1 %then %do;
				by  OPPTY_EVNT_ID MSG_EVNT_ID;
				%end;
				run;	

				%let dsid=%sysfunc(open(duplicate_recs));
				%let cntddp=%sysfunc(attrn(&dsid,nlobs));
				%let rc=%sysfunc(close(&dsid));

				%if &cntddp > 0 %then %do;

					proc export data=work.duplicate_recs
					dbms=xlsx 
					outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_flatfiles_&team_id..xlsx" 
					replace; 
					sheet="validate_duplicates"; 
					run;

					%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_flatfiles_&team_id..xlsx),
							 abend_message=%str(There is an issue with the imported intake form drug - There are duplicate drugs));			

				%end;
					

				%if &version. = 2 %then %do;
				%end;
				%else %do;
				
					proc sql noprint;
					create table dqi_eoms_layout_x as
					select *
					from dss_cea2.dqi_eoms_layout
					where layout_id = &layout_id.
					and required = 'X'
					order by layout_id, sequence_id;
					quit;

					data _null_;
					set dqi_eoms_layout_x; 
					n=trim(left(_n_));
					call symput('name_varx'||n,trim(left(data_attribute_name))); 
					call symput('name_varx_total',n);
					run;				

					data &team_id._&layout_id._&&eoms_name&i.;
					length %do k=1 %to &name_varx_total; &&name_varx&k %end; $100. ;
					set &team_id._&layout_id._&&eoms_name&i.;
					%do k=1 %to &name_varx_total; &&name_varx&k =''; %end; 
					run;

					data &team_id._&layout_id._&&eoms_name&i.;
					retain 
					%do k=1 %to &name_var_total; &&name_var&k %end; 
					%do kk=1 %to &name_varx_total; &&name_varx&kk %end;;
					set &team_id._&layout_id._&&eoms_name&i.; 
					run;
	
				%end;
				
				%let cnt_001 = 0;
				
				proc sql noprint;
				select count(*) into: cnt_001 separated by ''
				from &team_id._&layout_id._&&eoms_name&i.;
				quit;
				
				%if &cnt_001. = 0 %then %do;
				data record1;
				x=1;
				run;
				%end;
				
				%let qc_data_001 = &cnt_001.;
				

				%if %upcase(&team_id.) = CPL and &layout_id. = 1 %then %do;

					%put NOTE:  Remove invalid records from CPL opportunity data - PRSN_SSK_1;
					
					data &team_id._&layout_id._&&eoms_name&i.;
					set &team_id._&layout_id._&&eoms_name&i.;
					if PRSN_SSK_1 = '76AA' then delete;
					run;

				%end;


				data &team_id._&layout_id._&&eoms_name&i.;
				format eoms_file $100. eoms_record 8.;
				%if &cnt_001. = 0 %then %do;
					merge &team_id._&layout_id._&&eoms_name&i. record1;
					eoms_file="&&eoms_files&i";
					eoms_record=0;				
				%end;
				%else %do;
					set &team_id._&layout_id._&&eoms_name&i.;
					eoms_file="&&eoms_files&i";
					eoms_record=_n_;				
				%end;
				call symput('eoms_file',trim(left(eoms_file)));
				call symput('eoms_file_count',trim(left(eoms_record)));
				run;

				proc sql;
				connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
				execute(  

					insert into &c_s_table_name. values ( 
						0,
						&layout_id., 
						null,
						0,
						0,						
						null,
						null,
						null,
						%if &layout_id. = 1 %then %do;
							'OPPFILES', 
						%end; 
						%else %if &layout_id. = 2 %then %do;
							'OPPRXFILES',  
						%end; 
						%else %if &layout_id. = 3 %then %do;
							'DISPFILES',  
						%end;
						%else %if &layout_id. = 4 %then %do;
							'DISPRXFILES',  
						%end;
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null, 
						null,
						null,
						null,
						null,
						null,
						%tslit(%upcase(&team_id.)),
						%if &layout_id. = 1 or &layout_id. = 3 %then %do;
							%tslit(&eoms_file.),
							&eoms_file_count.,
						%end;
						%else %do;
							null,
							null,
						%end;
						%if &layout_id. = 2 or &layout_id. = 4 %then %do;
							%tslit(&eoms_file.),
							&eoms_file_count.,
						%end;
						%else %do;
							null,
							null,
						%end; 
						null,
						null,
						null,
						null,
						'0',
						'SUCCESS',
						current_date - &execution_date.
					)

				) by tera;
				execute (commit work) by tera;  
				disconnect from tera;
				quit; 
				
				%m_table_statistics(data_in=&c_s_table_name., index_in=eoms_row_gid);
				%m_table_statistics(data_in=&c_s_table_name., column_in=clm_evnt_gid);

			%end;  /** <----------------------------------------------------- end - cnt_cmctn_history **/

		%end;  /** <----------------------------------------------------- end - eoms_total **/

		
		%if &qc_data_001. = &qc_cntl_001. %then %do;
			%put NOTE:  QC - The control file - &qc_cntl_001. equals the data file - &qc_data_001.;
		%end;
		%else %do;
			%put WARNING:  QC - The control file - &qc_cntl_001. does not equals the data file - &qc_data_001. - issue with transmission to SFTP;
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - send out start email
			|   
			+---------------------------------------------------------------------------------------------*/
			%if &c_s_dqi_campaign_id. = 80 %then %do;
				filename xemail email 
				to=(&c_s_email_to.)
				subject="CTT Data Profiling - WARNING PBM File Processing has invalid CNTL counts ***SECUREMAIL*** "  lrecl=32000; 
			%end; 	

			options noquotelenmax;

			data _null_;
			file xemail;
				put " "; 
				put "The CTT Data Profiling PBM warning - There are invalid CNTL counts.";
				put " ";
				put "Thanks,";
				put " ";
				put "EA - Campaign Targeting Team";
				put " ";
			run;			
		%end;		

	%end;  /** <----------------------------------------------------- end - eoms_type **/
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - test
	|
	+---------------------------------------------------------------------------------------------*/			
	%if &ecoa_test.	= YES2B %then %do;

		data working.qc_step2b;
		x=1;
		run;	
		
		%m_ecoa_process(do_tasks=ALL);	

	%end;
		

%mend m_step2_eoms_extract;