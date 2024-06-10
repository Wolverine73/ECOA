
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    cd_data_integration_dqi
|
| LOCATION:  
| 
| PURPOSE:  
|
| LOGIC:   
|
|	logic ID 	     = team ID + campaign ID + ticket ID + opp ID + message ID
|
|	smart ID opportunity = campaign ID + ticket ID + member GID + 0 
|	smart ID disposition = campaign ID + ticket ID + member GID + stellent suffix ID
|
|	ecoa  ID opportunity = extnl_sys_oppty_id = smart ID opportunity
|	ecoa  ID disposition = contact_data_value = smart ID disposition
|
|	opportunity processing - match on smart_id      = extnl_sys_oppty_id
|	disposition processing - NA
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
|  STEP - ecoa directory for ecoa templates + output for dqi opportunity + disposition 
+---------------------------------------------------------------------------------------------*/
%let dir_in        = %str(/anr_dep/communication_history/eoms_files);
%let dir_out       = %str(/anr_dep/communication_history/dqi_files);
%let template_opp  = %str(cpl_opportunity.20220618000123);
%let template_disp = %str(cpl_disposition.20220714000123);
%let template_chnl_cd = CHLTRVER;  /**<--------------------------------- 23 - Letter (Veritas) = CHLTRVER     **/   

%let opp_cnt         = 0;
%let disp_cnt        = 0;
%let SYS_DIR 	     = %str(/);



%macro cd_data_integration_dqi;

	options nomlogic nosymbolgen;
	
	%if &c_s_dqi_campaign_id. = 85 %then %do;
	
		filename xemail email 
		to=(&c_s_email_to.)
		subject="CTT Data Profiling - STARTED DQI ECOA File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  
		
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
			put "The CTT Data Profiling DQI file processing has started. ";
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

	%m_dqi_process_control(id=1, step=%str(DQI - DATA INTGRATION BEGIN));
	
	
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
	|  STEP - get dqi opportunity + disposition data 
	|
	|	1. smart_id      = campaign ID + request ID + opp ID (mbr_acct_gid) + 0
	|	2. smart_id disp = campaign ID + request ID + opp ID (mbr_acct_gid) + stellent suffix	
	|
	+---------------------------------------------------------------------------------------------*/
	%m_table_statistics(data_in=&c_s_tdtempx..dqi_cmctn_history,       column_in=ticket); 
	%m_table_statistics(data_in=&c_s_tdtempx..dqi_cmctn_history_match, column_in=ticket); 
	%m_table_statistics(data_in=&c_s_tdtempx..dqi_eoms_opportunity,    column_in=extnl_sys_oppty_id); 
	%m_table_statistics(data_in=&c_s_tdtempx..dqi_eoms_opportunity,    column_in=oppty_cmpgn_request_id); 
	%m_table_statistics(data_in=&c_s_tdtempx..dqi_process_control,     column_in=dqi_ticket);
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create ticket xref for member
	|
	|  NOTE - do audience separately since some tickets contain both members + prescribers
	|
	|  NOTE - or campaigns do triage 1st then campaigns 2nd for same ticket
	|	  or incorrect mdp runs and selection of campaign ID
	|	  or NCQA and DSA campaigns - ID flip since start as 42 then switches to 43
	|	  or pharm terms do TBD 1st then pharm term campaign 2nd
	|	  or VT of 40s and 2 or 53
	|         solution use last process ID for ticket to get latest and greatest
	|
	|  NOTE - counts of history vs final will be slightly lower since vt parents have multiple process IDs for vt child 
	|
	|  EXAMPLE - 1807740 = NCQA and DSA
	|	     1732709 = VT
	|	     1803504 = VT 
	|            1778408 = Pharm Term
	|
	+---------------------------------------------------------------------------------------------*/
	%macro year_metrics;
	
		---------------------------- 2022.11.08 = 4484 tickets
		select distinct a.dqi_ticket, a.dqi_campaign_id, c.dqi_campaign_description, dqi_location
		from  dss_cea2.dqi_process_control a left join
		dss_cea2.dqi_campaigns c on a.dqi_campaign_id=c.dqi_campaign_id 
		where cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)||'0101' as date format 'yyyymmdd')
		and a.dqi_campaign_id < 83 
		and a.dqi_campaign_id not in (41,46,47,78,65,45,72) 
		and substr(a.dqi_ticket,1,1) = '1'
		and dqi_location not like '%PRT%'
		and substr(a.dqi_ticket,1,7) in (select prjct_id from pbmv_dh.v_prjct_dtl )
	
	%mend year_metrics;
	
	%m_table_drop(data_in=dss_cea2.dqi_aprimo_projects_ptnt);
	
	proc sql;
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw" mode=teradata);
	execute(
		create multiset table dss_cea2.dqi_aprimo_projects_ptnt as
		(       /**----------- step 5 - all aprimo + dqi tickets  -----------**/
			select distinct 
			'NONVT' as dqi_type,
			ptnt.dqi_ticket, 
			ptnt.dqi_campaign_id, 
			ptnt.dqi_campaign_description, 
			cast(regexp_replace(base.prod_id, '[^0-9a-z]', ' ', 1, 0, 'i')         as varchar(100) ) as prod_id,
			cast(regexp_replace(base.prod_subcat_id, '[^0-9a-z]', ' ', 1, 0, 'i')  as varchar(100) ) as prod_subcat_id,
			cast(regexp_replace(base.title_nm, '[^0-9a-z]', ' ', 1, 0, 'i')        as varchar(100) ) as prod_title
			
			from 
			(       /**----------- step 4 - get aprimo tickets  -----------**/	
				select distinct p.prjct_id as dqi_ticket, cast(p.snap_ts as date) as snap_ts, p.title_nm, a.prod_nm as prod_id,  				
				case upper(p.typ_cd)
				      when 'CONSOLIDATED COMMUNICATION - CHILD' then p.prod_sub_ctgry_cd   
				      else p.prod_subcat_id						   
				end as prod_subcat_id				 
				from pbmv_dh.v_prjct_dtl p,
				pbmv_dh.v_actvy_dtl a
				where a.actvy_id = p.actvy_id
				and cast(p.snap_ts as date) > cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
			) base inner join

			(       /**----------- step 3 - last dqi execution  -----------**/
				select distinct a.ticket as dqi_ticket, b.dqi_campaign_id, c.dqi_campaign_description
				from (  /**----------- step 1 - get dqi history tickets -----------**/
					select distinct ticket, process_id 
					from dss_cea2.dqi_cmctn_history 
					where rvr_cmctn_role_cd = '1'
					and cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
				      ) a left join
				    
				     (  /**----------- step 2 - get dqi campaign last execution -----------**/
				     	select distinct dqi_campaign_id, mbr_process_id as process_id, dqi_ts 
				     	from  dss_cea2.dqi_process_control
				     	where cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
				     ) b on a.process_id = b.process_id left join
				     
				     dss_cea2.dqi_campaigns       c on b.dqi_campaign_id=c.dqi_campaign_id  
				qualify rank() over (partition by a.ticket order by b.dqi_ts desc) = 1   
			) ptnt on base.dqi_ticket = substr(ptnt.dqi_ticket,1,7)

		)with data primary index (dqi_ticket)
	)by tera; 
	quit; 
		
	%m_table_statistics(data_in=dss_cea2.dqi_aprimo_projects_ptnt, index_in=dqi_ticket);
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create VT member indicator  
	|
	+---------------------------------------------------------------------------------------------*/	
	proc sql;
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw" mode=teradata);
	execute(
		update dss_cea2.dqi_aprimo_projects_ptnt
		set dqi_type = 'VT'
		where
		(
			dqi_campaign_id  in 
					(
						select dqi_campaign_id
						from dss_cea2.dqi_campaigns 
						where upper(dqi_campaign_description) like any ( '%VT%', '%VENDOR%') 
					)

			or dqi_ticket  in 
					(
						select dqi_ticket
						from dss_cea2.dqi_process_control 
						where upper(dqi_location) like any ( '%VENDOR%') 
					)
			or dqi_ticket  in 
					(
						select ticket
						from dss_cea2.dqi_cmctn_history 
						where substr(coalesce(module_107,'XX'),1,2) = 'VT' 
					)
			or dqi_ticket  in 
					(
						select distinct ticket
						from dss_cea2.dqi_cmctn_history_match  
					)
		)
		and dqi_type = 'NONVT'
	)by tera; 
	quit; 
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create ticket xref for prescriber 
	|
	|  NOTE - do audience separately since some tickets contain both members + prescribers
	|
	|  Example - 1751807 + 1689002 + 1592196
	|
	+---------------------------------------------------------------------------------------------*/	
	%m_table_drop(data_in=dss_cea2.dqi_aprimo_projects_prsc);	
	
	proc sql;
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw" mode=teradata);
	execute(
		create multiset table dss_cea2.dqi_aprimo_projects_prsc as
		(       /**----------- step 5 - all aprimo + dqi tickets  -----------**/
			select distinct 
			'NONVT' as dqi_type,
			prsc.dqi_ticket, 
			prsc.dqi_campaign_id, 
			prsc.dqi_campaign_description, 
			cast(regexp_replace(base.prod_id, '[^0-9a-z]', ' ', 1, 0, 'i')         as varchar(100) ) as prod_id,
			cast(regexp_replace(base.prod_subcat_id, '[^0-9a-z]', ' ', 1, 0, 'i')  as varchar(100) ) as prod_subcat_id,
			cast(regexp_replace(base.title_nm, '[^0-9a-z]', ' ', 1, 0, 'i')        as varchar(100) ) as prod_title			
			
			from 
			(       /**----------- step 4 - get aprimo tickets  -----------**/		
				select distinct p.prjct_id as dqi_ticket, a.prod_nm as prod_id,
				case upper(p.typ_cd)
				      when 'CONSOLIDATED COMMUNICATION - CHILD' then p.prod_sub_ctgry_cd   
				      else p.prod_subcat_id						   
				end AS prod_subcat_id, 
				p.title_nm 
				from pbmv_dh.v_prjct_dtl p,
				pbmv_dh.v_actvy_dtl a
				where a.actvy_id = p.actvy_id
				and cast(p.snap_ts as date) > cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
			) base inner join

			(       /**----------- step 3 - last dqi execution  -----------**/
				select distinct a.ticket as dqi_ticket, b.dqi_campaign_id, c.dqi_campaign_description
				from (  /**----------- step 1 - get dqi history tickets -----------**/
					select distinct ticket, process_id 
					from dss_cea2.dqi_cmctn_history 
					where rvr_cmctn_role_cd = '2'
					and cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
				      ) a left join
				    
				     (  /**----------- step 2 - get dqi campaign last execution -----------**/
				     	select distinct dqi_campaign_id, phys_process_id as process_id, dqi_ts 
				     	from  dss_cea2.dqi_process_control
				     	where cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
				     ) b on a.process_id = b.process_id left join
				     
				     dss_cea2.dqi_campaigns       c on b.dqi_campaign_id=c.dqi_campaign_id  
				qualify rank() over (partition by a.ticket order by b.dqi_ts desc) = 1   
			) prsc on base.dqi_ticket = substr(prsc.dqi_ticket,1,7)

		)with data primary index (dqi_ticket)
	)by tera; 
	quit; 	
		 
	%m_table_statistics(data_in=dss_cea2.dqi_aprimo_projects_prsc, index_in=dqi_ticket);
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create VT prescriber indicator  
	|
	+---------------------------------------------------------------------------------------------*/
	proc sql;
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw" mode=teradata);
	execute(
		update dss_cea2.dqi_aprimo_projects_prsc
		set dqi_type = 'VT'
		where
		(
			dqi_campaign_id  in 
					(
						select dqi_campaign_id
						from dss_cea2.dqi_campaigns 
						where upper(dqi_campaign_description) like any ( '%VT%', '%VENDOR%') 
					)

			or dqi_ticket  in 
					(
						select dqi_ticket
						from dss_cea2.dqi_process_control 
						where upper(dqi_location) like any ( '%VENDOR%') 
					)
			or dqi_ticket  in 
					(
						select ticket
						from dss_cea2.dqi_cmctn_history 
						where substr(coalesce(module_107,'XX'),1,2) = 'VT' 
					)
			or dqi_ticket  in 
					(
						select distinct ticket
						from dss_cea2.dqi_cmctn_history_match  
					)
		)
		and dqi_type = 'NONVT'
	)by tera; 
	quit; 
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create sub-product xref for vt 
	|
	+---------------------------------------------------------------------------------------------*/	
	%m_table_drop(data_in=dss_cea2.dqi_vt_projects);			

	proc sql;
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw");
	execute(
		create multiset table dss_cea2.dqi_vt_projects as
		(				
			select  x.child_template_id, x.aprimo_product_category as vt_product_category, max(x.aprimo_product_sub_category) as vt_product_sub_category
			from (
				select  
				c.child_template_id, 
				cast(upper(c.aprimo_product_category)  as varchar(100) ) as  aprimo_product_category, 
				cast(upper(c.aprimo_product_sub_category)  as varchar(100) ) as  aprimo_product_sub_category
				from (
				
					select parent_template_id,
					       child_template_id,
					       (CASE
					        	WHEN (position('PHARMACY' IN upper(section_description) ) > 0 ) THEN 'PHARMACY NETWORK DISRUPTION'
					        	WHEN (position('OPIOIDS' IN upper(section_description) ) > 0 ) THEN 'OPIOIDS'
					        	WHEN (position('DIABETES' IN upper(section_description) ) > 0 ) THEN 'TDC'
					        	WHEN (position('CHOICE' IN upper(section_description) ) > 0 ) THEN 'MCHOICE'
					        	WHEN (position('WRITTEN' IN upper(section_description) ) > 0 ) THEN 'DISPENSE AS WRITTEN DAW'
					        	WHEN (position('NON SPECIALTY EXCLUSIONS' IN upper(section_description) ) > 0 ) THEN 'SPECIALTY'
					        	WHEN (position('TIER' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('FORMULARY' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('FDRO' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('DISRUPTION' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('EXCLUSIONS' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('ACSF' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('BASIC' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position('GLUCOSE' IN upper(section_description) ) > 0 ) THEN 'FORMULARY'
					        	WHEN (position(' PA' IN upper(section_description) ) > 0 ) THEN 'PRIOR AUTHORIZATION'
					        	WHEN (position('STEP' IN upper(section_description) ) > 0 ) THEN 'STEP THERAPY NOTIFICATION'
					        	WHEN (position('MAIL' IN upper(section_description) ) > 0 ) THEN 'MAIL ORDER'
					        	WHEN (position('SPECIALTY' IN upper(section_description) ) > 0 ) THEN 'SPECIALTY'
					        	WHEN (position('COMPOUND' IN upper(section_description) ) > 0 ) THEN 'COMPOUND CONTROLLED SUBSTANCE'
					       		ELSE 'OTHER'
        				       END) AS aprimo_product_category,
					       parent_description as aprimo_product_category2,
					       section_description as aprimo_product_sub_category
					from dss_cea2.dqi_consolidated_crossref 
					
				) c 
				where length(c.child_template_id) > 2
				qualify rank() over (partition by c.child_template_id order by c.parent_template_id, c.aprimo_product_category, c.aprimo_product_sub_category ) = 1
			) x
			group by 1,2
			
		)with data primary index (child_template_id)
	) by tera;
	execute (commit work) by tera;
	disconnect from tera;
	quit;		
	
	%m_table_statistics(data_in=dss_cea2.dqi_vt_projects, index_in=child_template_id);
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create sub-product xref for consolidated cmctn of vt parent + child
	|
	+---------------------------------------------------------------------------------------------*/	
	%m_table_drop(data_in=dss_cea2.dqi_aprimo_projects_ptnt_child);
	
	proc sql;
	connect to teradata as tera(server=&c_s_server. user=&td_id. password="&td_pw" mode=teradata);
	execute(
		create multiset table dss_cea2.dqi_aprimo_projects_ptnt_child as
		(    
			select distinct 
			actvy_id,
			trim(ptnt.dqi_ticket)||'_'||trim(ptnt.ticket_child) as ticket, 
			upper(cast(regexp_replace(base.prod_subcat_id, '[^0-9a-z]', ' ', 1, 0, 'i')  as varchar(100) )) as aprimo_product_child_category

			from 
			(       /**----------- step 4 - get aprimo tickets  -----------**/		
				select distinct p.actvy_id, p.prjct_id as ticket_child, 				
				case upper(p.typ_cd)
				      when 'CONSOLIDATED COMMUNICATION - CHILD' then p.prod_sub_ctgry_cd   
				      else p.prod_subcat_id						   
				end AS prod_subcat_id  
				from pbmv_dh.v_prjct_dtl p,
				pbmv_dh.v_actvy_dtl a
				where a.actvy_id = p.actvy_id
				and cast(p.snap_ts as date) > cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
				and upper(p.typ_cd) = 'CONSOLIDATED COMMUNICATION - CHILD'
			) base inner join

			(       /**----------- step 3 - last dqi execution  -----------**/
				select distinct a.ticket as dqi_ticket, a.ticket_child 
				from (  /**----------- step 1 - get dqi history tickets -----------**/
					select distinct ticket, coalesce(substr(strtok(module_107,'_',5),2),ticket) as ticket_child, process_id
					from dss_cea2.dqi_cmctn_history 
					where rvr_cmctn_role_cd = '1'
					and cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
					and upper(module_107) like '%VT%'
				      ) a left join

				     (  /**----------- step 2 - get dqi campaign last execution -----------**/
					select distinct dqi_campaign_id, mbr_process_id as process_id, dqi_ts 			     	
					from  dss_cea2.dqi_process_control
					where cast(trim(substr(dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)-2||'0101' as date format 'yyyymmdd')
				     ) b on a.process_id = b.process_id left join

				     dss_cea2.dqi_campaigns       c on b.dqi_campaign_id=c.dqi_campaign_id  
				qualify rank() over (partition by a.ticket, a.ticket_child order by b.dqi_ts desc) = 1   
			) ptnt on base.ticket_child = substr(ptnt.ticket_child,1,7)

		)with data primary index (ticket)
	)by tera; 
	quit; 
	
	%m_table_statistics(data_in=dss_cea2.dqi_aprimo_projects_ptnt_child, index_in=ticket);
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc to validate the ticket ID member 
	|
	+---------------------------------------------------------------------------------------------*/	
	%let cntopp = 0;
	
	data dqi_aprimo_projects_ptnt;
	set dss_cea2.dqi_aprimo_projects_ptnt;
	run;	

	proc sort data=dqi_aprimo_projects_ptnt dupout=duplicate_recs nodupkey;
	by dqi_ticket;
	run;					
					
	%let dsid=%sysfunc(open(duplicate_recs));
	%let cntopp=%sysfunc(attrn(&dsid,nlobs));
	%let rc=%sysfunc(close(&dsid));
	
	%put NOTE: cntopp = &cntopp. ;

	%if &cntopp > 0 %then %do;

		proc export data=work.duplicate_recs
		dbms=xlsx 
		outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_ticket_DQI.xlsx" 
		replace; 
		sheet="validate_duplicates"; 
		run;

		%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_ticket_DQI.xlsx),
				 abend_message=%str(There is an issue with the creation of the dqi ticket data - There are duplicate PTNT ticket IDs));			

	%end;	
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc to validate the ticket ID prescriber 
	|
	+---------------------------------------------------------------------------------------------*/	
	%let cntopp = 0;
	
	data dqi_aprimo_projects_prsc;
	set dss_cea2.dqi_aprimo_projects_prsc;
	run;	

	proc sort data=dqi_aprimo_projects_prsc dupout=duplicate_recs nodupkey;
	by dqi_ticket;
	run;					
					
	%let dsid=%sysfunc(open(duplicate_recs));
	%let cntopp=%sysfunc(attrn(&dsid,nlobs));
	%let rc=%sysfunc(close(&dsid));
	
	%put NOTE: cntopp = &cntopp. ;

	%if &cntopp > 0 %then %do;

		proc export data=work.duplicate_recs
		dbms=xlsx 
		outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_ticket_DQI.xlsx" 
		replace; 
		sheet="validate_duplicates"; 
		run;

		%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_ticket_DQI.xlsx),
				 abend_message=%str(There is an issue with the creation of the dqi ticket data - There are duplicate PRSC ticket IDs));			

	%end;		
	
	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc to validate the sub-category ID vt 
	|
	+---------------------------------------------------------------------------------------------*/	
	data dqi_vt_projects;
	set dss_cea2.dqi_vt_projects;
	run;	
	
	proc sort data=dqi_vt_projects dupout=duplicate_recs nodupkey;
	by child_Template_Id;
	run;					
					
	%let dsid=%sysfunc(open(duplicate_recs));
	%let cntopp=%sysfunc(attrn(&dsid,nlobs));
	%let rc=%sysfunc(close(&dsid));
	
	%put NOTE: cntopp = &cntopp. ;

	%if &cntopp > 0 %then %do;

		proc export data=work.duplicate_recs
		dbms=xlsx 
		outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_ticket_DQI.xlsx" 
		replace; 
		sheet="validate_duplicates"; 
		run;

		%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_ticket_DQI.xlsx),
				 abend_message=%str(There is an issue with the creation of the dqi ticket data - There are duplicate VT ticket IDs));			

	%end;	
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - dqi - members
	|
	|	1. smart_id      = campaign ID + request ID + opp ID (member GID) + 0
	|	2. smart_id disp = campaign ID + request ID + opp ID (member GID) + message ID
	|
	+---------------------------------------------------------------------------------------------*/	
	proc sql;  
	connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
	create table _dqi_members as
	select * from connection to tera(

		select top 200000 *
		from (
		
			select x.*
			from (  /**----------- step 4 - get dqi opportunity -----------**/
				select distinct
				'DQI' as program_sub_id,
				'PTNT'  as audience_sub_id,
				coalesce(b.dqi_campaign_id,0) as dqi_campaign_id,
				coalesce(a.ticket,0) as ticket, 
				a.mbr_gid as oppt_event_id,
				trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(ticket)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||'0' as smart_id,
				trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(ticket)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||trim(a.apn_cmctn_id) as smart_id_disp,
				a.program_id,
				a.apn_cmctn_id,
				a.effective_dt,
				a.release_dt, 
				a.process_dt,
				a.mbr_id, 
				'' as ql_bnfcy_id,  /**<---------------------- this could be missing or assigned as dummy ID when campaign executed - assigned within ECOA **/
				a.mbr_gid,					 
				a.clnt_lvl1, 
				a.clnt_lvl2, 
				a.clnt_lvl3, 
				upper(mbr_first_nm) as mbr_name_first, 
				upper(mbr_last_nm) as mbr_name_last,
				upper(mbr_addr1) as mbr_address1,
				upper(mbr_addr2) as mbr_address2,
				upper(mbr_city) as mbr_city,
				upper(mbr_state) as mbr_state,
				upper(mbr_zip) as mbr_zip,
				' ' as mbr_phone,
				upper(b.dqi_campaign_description) as dqi_product_category,
				upper(b.prod_id) as aprimo_product_category,
				upper(b.prod_subcat_id) as aprimo_product_sub_category,
				upper(b.prod_id) as vt_product_category,
				upper(b.prod_subcat_id) as vt_product_sub_category,
				upper(b.prod_title) as aprimo_product_title 
				
				/**----------- step 3 - get opportunity -----------**/
				from dss_cea2.dqi_cmctn_history   a left join
				
				dss_cea2.dqi_aprimo_projects_ptnt b on a.ticket=b.dqi_ticket   

				where cast(trim(substr(a.dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)||'0101' as date format 'yyyymmdd')
				and a.rvr_cmctn_role_cd = '1' 
				and b.dqi_type = 'NONVT' 
				and a.mbr_gid is not null 
				
				/** 
				where cast(trim(substr(a.dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 365)||'0101' as date format 'yyyymmdd')
				and a.rvr_cmctn_role_cd = '1' 
				and b.dqi_type = 'NONVT' 
				and a.mbr_gid is not null 				
				and a.clnt_lvl1 in (select lvl1_acct_id from dwu_edw_rb.v_clnt_acct_denorm where lvl0_acct_id = 'S-2976' ) 
				**/
				
			) x left join 

			(       /**----------- step 2 - exclude ecoa opportunity -----------**/
				select distinct extnl_sys_oppty_id
				from dss_cea2.dqi_eoms_opportunity 
				where substr(execution_triage,1,1) = '0'
				and oppty_evnt_id is not null
				and source_team = 'DQI'
				and alt_oppty_subtyp_src_cd in ('DQI','VT')
			) o on x.smart_id = o.extnl_sys_oppty_id 

			where o.extnl_sys_oppty_id is null
			and dqi_campaign_id > 0

			union
				
			select xx.*
			from (  /**----------- step 4 - get vt opportunity -----------**/
				select distinct
					'VT' as program_sub_id, 
					'PTNT'  as audience_sub_id,
					coalesce(b.dqi_campaign_id,0) as dqi_campaign_id,
					case
					  when length(c.vt_product_category) > 3 THEN a.ticket_child 
					  else a.ticket
					end AS ticket,	  
					a.match_mbr_acct_gid as oppt_event_id, 
					case
					  when length(c.vt_product_category) > 3 THEN  trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(a.ticket_child)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||'0'
					  else trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(a.ticket)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||'0'
					end AS smart_id,						
					case
					  when length(c.vt_product_category) > 3 THEN  trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(a.ticket_child)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||trim(a.apn_cmctn_id)||'_'||trim(a.child_template_id) 
					  else trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(a.ticket)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||trim(apn_cmctn_id)
					end AS smart_id_disp,	 
					a.program_id,
					case
					  when length(c.vt_product_category) > 3 THEN trim(a.apn_cmctn_id)||'_'||trim(a.child_template_id) 
					  else a.apn_cmctn_id
					end AS apn_cmctn_id,	 
					a.effective_dt,
					a.release_dt, 
					a.process_dt,
					'' as mbr_id, 
					'' as ql_bnfcy_id,  /**<---------------------- this could be missing or assigned as dummy ID when campaign executed - assigned within ECOA **/
					a.match_mbr_acct_gid as mbr_gid,					 
					'' as clnt_lvl1, 
					'' as clnt_lvl2, 
					'' as clnt_lvl3,
					upper(mbr_first_nm) as mbr_name_first, 
					upper(mbr_last_nm) as mbr_name_last,
					upper(mbr_addr1) as mbr_address1,
					upper(mbr_addr2) as mbr_address2,
					upper(mbr_city) as mbr_city,
					upper(mbr_state) as mbr_state,
					upper(mbr_zip) as mbr_zip,
					' ' as mbr_phone,
					upper(b.dqi_campaign_description) as dqi_product_category,
					upper(b.prod_id) as aprimo_product_category,
					upper(b.prod_subcat_id) as aprimo_product_sub_category,
					upper(c.vt_product_category) as vt_product_category,
					upper(c.vt_product_sub_category) as vt_product_sub_category,
					upper(b.prod_title) as aprimo_product_title

				from 
				(       /**----------- step 3 - get opportunity -----------**/
					select distinct
					a.row_gid,
					a.subject_id, 
					a.clm_evnt_gid, 
					a.ticket,
					case
					  when module_107 like '%VT%' then trim(a.ticket)||'_'||coalesce(substr(strtok(module_107,'_',5),2),'0')
					  else trim(a.ticket)||'_'||'0'
					end as ticket_child, 
					a.program_id,
					a.apn_cmctn_id,
					a.effective_dt,
					a.release_dt,
					a.process_dt,
					a.match_mbr_acct_gid, 					
					a.mbr_first_nm, 
					a.mbr_last_nm,
					a.mbr_addr1,
					a.mbr_addr2,
					a.mbr_city,
					a.mbr_state,
					a.mbr_zip,					
					a.module_100 as child_template_id					
					
					from dss_cea2.dqi_cmctn_history_match a inner join 
					     dwu_edw_rb.v_mbr_acct_cvrg c on a.match_mbr_acct_gid = c.mbr_acct_gid  
					where a.ptnt_gid > 0
					and a.rvr_cmctn_role_cd = '1'
					and c.src_cd = 'X'
					and cast(trim(substr(a.dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)||'0101' as date format 'yyyymmdd')
					qualify rank() over (partition by a.row_gid, a.ticket, a.subject_id  order by c.stus_cd, c.cvrg_exprn_dt desc, c.cvrg_eff_dt desc, a.clm_evnt_gid ) = 1	
				) a left join
				
				dss_cea2.dqi_aprimo_projects_ptnt b on a.ticket=b.dqi_ticket left join				
				
				dss_cea2.dqi_vt_projects          c on c.child_template_id=a.child_template_id				

				where a.match_mbr_acct_gid is not null
				and b.dqi_type = 'VT'
				and b.dqi_campaign_id > 0
				
			) xx left join 

			(       /**----------- step 2 - exclude ecoa opportunity -----------**/
				select distinct extnl_sys_oppty_id
				from dss_cea2.dqi_eoms_opportunity 
				where substr(execution_triage,1,1) = '0' 
				and oppty_evnt_id is not null
				and source_team = 'DQI'
				and alt_oppty_subtyp_src_cd in ('DQI','VT')
			) o on xx.smart_id = o.extnl_sys_oppty_id 

			where o.extnl_sys_oppty_id is null 
		
		) dqi_history
			
		order by release_dt desc 
		
	) ;
	quit;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - dqi - prescribers
	|
	|	1. smart_id      = campaign ID + request ID + opp ID (claim GID) + 0
	|	2. smart_id disp = campaign ID + request ID + opp ID (claim GID) + message ID
	|
	+---------------------------------------------------------------------------------------------*/
	proc sql;  
	connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
	create table _dqi_prescribers as
	select * from connection to tera(

		select top 200000 *
		from (
		
			select x.*
			from (  /**----------- step 4 - get dqi opportunity -----------**/
				select distinct
				'DQI' as program_sub_id,
				'PRSC'  as audience_sub_id,
				b.dqi_campaign_id,
				coalesce(a.ticket,0) as ticket,
				coalesce(a.ticket,0) as ticket_child,
				a.clm_evnt_gid as oppt_event_id,
				trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(ticket)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||'0' as smart_id,
				trim(TO_CHAR(b.dqi_campaign_id))||'.'||trim(ticket)||'.'||trim(TO_CHAR(oppt_event_id))||'.'||trim(a.apn_cmctn_id) as smart_id_disp,
				a.program_id,
				a.apn_cmctn_id,
				a.effective_dt,
				a.release_dt, 
				a.release_dt as process_dt,
				a.mbr_id, 
				a.ql_bnfcy_id, 
				a.mbr_gid,					 
				a.clnt_lvl1, 
				a.clnt_lvl2, 
				a.clnt_lvl3, 
				upper(mbr_first_nm) as mbr_name_first, 
				upper(mbr_last_nm) as mbr_name_last,
				upper(mbr_addr1) as mbr_address1,
				upper(mbr_addr2) as mbr_address2,
				upper(mbr_city) as mbr_city,
				upper(mbr_state) as mbr_state,
				upper(mbr_zip) as mbr_zip,
				' ' as mbr_phone, 	
				
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
				
				upper(b.dqi_campaign_description) as dqi_product_category,
				upper(b.prod_id) as aprimo_product_category,
				upper(b.prod_subcat_id) as aprimo_product_sub_category,
				upper(b.prod_id) as vt_product_category,
				upper(b.prod_subcat_id) as vt_product_sub_category,
				upper(b.prod_title) as aprimo_product_title
				
				/**----------- step 3 - get opportunity -----------**/
				from 
				(
					select a.ticket, clm_evnt_gid, program_id, apn_cmctn_id, effective_dt, release_dt, 
					mbr_id, ql_bnfcy_id, mbr_gid, clnt_lvl1,clnt_lvl2,clnt_lvl3,
					mbr_first_nm, mbr_last_nm, mbr_addr1, mbr_addr2, mbr_city, mbr_state, mbr_zip, 
					a.prctr_npi_id as prsc_npi, pr.ql_prscbr_id as prsc_ql, a.prctr_gid as prsc_gid, pr.dea_id as prsc_dea, 
					prctr_first_nm as first_namep, prctr_last_nm as last_namep, pr.addr_line1 as address_1p, pr.addr_line2 as address_2p, 
					pr.addr_city_nm as cityp, pr.addr_st_abbr_cd as statep, pr.addr_zip5_cd as zip_codep, 
					pr.prmry_phone_nbr as phone_numberp, pr.fax_nbr as fax_numberp
					from dss_cea2.dqi_cmctn_history a left join  
					dwu_edw_rb.v_prscbr_denorm pr on a.prctr_gid = pr.prscbr_pty_gid    			
					where cast(trim(substr(a.dqi_ts,1,10)) as date format 'yyyy/mm/dd') >= cast(extract(year from current_date - 0)||'0101' as date format 'yyyymmdd')
					and a.rvr_cmctn_role_cd = '2' 
					group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
					
				) a left join
				
				dss_cea2.dqi_aprimo_projects_prsc b on a.ticket=b.dqi_ticket   

				where b.dqi_type = 'NONVT'
				and b.dqi_campaign_id > 0
				and a.clm_evnt_gid is not null 					
				
			) x left join 

			(       /**----------- step 2 - exclude ecoa opportunity -----------**/
				select distinct extnl_sys_oppty_id
				from dss_cea2.dqi_eoms_opportunity 
				where substr(execution_triage,1,1) = '0'
				and oppty_evnt_id is not null
				and source_team = 'DQI'
				and alt_oppty_subtyp_src_cd in ('DQI','VT')
			) o on x.smart_id = o.extnl_sys_oppty_id 

			where o.extnl_sys_oppty_id is null
			
		) dqi_history
			
		order by release_dt desc
		
	) ;
	quit;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - metrics members + prescribers 
	|
	+---------------------------------------------------------------------------------------------*/	
	%macro opportunity_metrics;

		VT    Members     = 46113
		DQI   Members     = 2532741
		DQI   Prescribers = 1109313
		STARS Members     = 1155673
		STARS Prescribers = 2309345
		--------------------------------
		Total             = 7,200,000 = yearly opps

	%mend opportunity_metrics;
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - consolidate members + prescribers 
	|
	+---------------------------------------------------------------------------------------------*/	
	data _dqi;
	set _dqi_prescribers
	    _dqi_members ; 
	if program_sub_id = 'DQI' then vt_product_category='';
	if program_sub_id = 'DQI' then vt_product_sub_category=''; 
	run;
	
	proc sort data = _dqi;
	by ticket;
	run;
	
	data dqi_aprimo_projects_ptnt_child;
	set dss_cea2.dqi_aprimo_projects_ptnt_child;
	run;	 	
	
	proc sort data = dqi_aprimo_projects_ptnt_child;
	by ticket;
	run;	
	
	data _dqi;
	merge _dqi (in=a)
	      dqi_aprimo_projects_ptnt_child (in=b);
	by ticket;
	if a;
	run;
	
	proc sql noprint;
	create table qc_009 as 
	select program_sub_id, dqi_campaign_id, dqi_product_category, aprimo_product_category, count(*) as cnt
	from _dqi
	group by 1,2,3,4;
	quit;
	
	data _null_;
	set qc_009;
	put @1 _n_ @5 program_sub_id @10 dqi_campaign_id @15 aprimo_product_category @80 cnt;
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - qc to validate one member x ticket - all counts should = 1
	|
	+---------------------------------------------------------------------------------------------*/	
	%let cntopp = 0;
	
	proc sql noprint;
	create table duplicate_recs as 
	select ticket, oppt_event_id, apn_cmctn_id, count(distinct dqi_campaign_id) as cnt
	from _dqi
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
	|  QC   - DQI opportunity might have multiple stellents or messages + VT might have multiple addresses
	|	  a separate opp and msg ID will be created in these scenarios which is a small percent
	|
	|  EXAMPLE - VT  = match_mbr_acct_gid = 2406872448 
	|	     DQI = mbr_gid            = 2424920051
	|
	+---------------------------------------------------------------------------------------------*/		
	%let cntopp = 0;
	
	proc sort data=_dqi nodupkey;
	by smart_id smart_id_disp mbr_address1 mbr_address2; 
	run;	

	proc sort data=_dqi dupout=duplicate_recs nodupkey;
	by smart_id smart_id_disp mbr_address1 mbr_address2; 
	run;

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
	|  STEP - deterimine if there are any members + prescribers to process
	|
	+---------------------------------------------------------------------------------------------*/
	%let dqi_cnt = 0;
	
	proc sql noprint;
	select count(*) into: dqi_cnt separated by '' 
	from _dqi;
	quit;	

	%put NOTE: dqi_cnt = &dqi_cnt. ;

	%if &dqi_cnt ne 0 %then %do;  /** <--------------------------- start - create opportunity + disposition **/
	
	
		%m_dqi_process_control(id=4, step=%str(DQI - DATA INTGRATION CMCTN HISTORY BEGIN));


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - get max ID for oppty_evnt_id + msg_evnt_id creation for DQI or STARS
		|
		+---------------------------------------------------------------------------------------------*/
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			delete &c_s_tdtempx..dqi_eoms_queue
			where execution_source = 'DQI'
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
		|  STEP - qc smart IDs
		|
		+---------------------------------------------------------------------------------------------*/		
		data qc001;
		set _dqi;
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
			outfile="&c_s_datadir.&SYS_DIR.validate_Smart_IDS_DQI.xlsx" 
			replace; 
			sheet="validate_duplicates"; 
			run;

			%m_abend_handler(abend_report=%str(&c_s_datadir./validate_Smart_IDS_DQI.xlsx),
					 abend_message=%str(There is an issue with the creation of the smart ID in the DQI data - There are invalid IDs));			

		%end;			


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
		|  STEP - mapping of the ecoa dqi opportunity file = 30 data elements
		|
		|  NOTE - unique ID for traceability tracking = smart ID
		|
		+---------------------------------------------------------------------------------------------*/
		data dqi_opporunity;
		if _n_ = 1 then set template_opp;
		set _dqi;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of opportunity
			|
			+---------------------------------------------------------------------------------------------*/
			OPPTY_EVNT_ID=left(put(&max_id+_n_,20.));       /**<--------------------------------- Opp ID for DQI 9000000000000000+     **/ 
			OPPTY_SRC_CD='DQI';                             /**<--------------------------------- DQI Team Source                      **/ 	
			EXTNL_SYS_OPPTY_ID=smart_id;                    /**<--------------------------------- smart ID opportunity                 **/
			PGM_ID=left(put(program_id,20.));               /**<--------------------------------- Stellent Prefix                      **/
			MSG_PRDCT_ALT_ID=apn_cmctn_id;                  /**<--------------------------------- Stellent Suffix                      **/
			OPPTY_ACTION_IND='A';                           /**<--------------------------------- A - add/create as a new opportunity  **/ 
			OPPTY_STUS_CD='3';                              /**<--------------------------------- 3 - Open                             **/
			ALT_OPPTY_SUBTYP_SRC_CD=program_sub_id;         /**<--------------------------------- level 1 - Team ID                    **/
			OPPTY_CMPGN_ID=left(put(dqi_campaign_id,20.));  /**<--------------------------------- level 2 - Campaign ID                **/
			PGM_TYPE_CD=left(trim(ticket));                 /**<--------------------------------- level 3 - Request ID                 **/
			ALT_OPPTY_SUBTYP_CD=left(put(oppt_event_id,20.)); /**<------------------------------- level 4 - Opportunity ID             **/
			if   program_sub_id    = 'DQI' then KEY_CODE='DQI'||left(trim(ticket)); 
			else if program_sub_id = 'VT'  then KEY_CODE='VT' ||left(trim(ticket));  
			else KEY_CODE='DQI'||left(trim(ticket));        /**<--------------------------------- Key Code for request                 **/
			MSG_EVNT_ID='';                                 /**<--------------------------------- assignment in subsequent step below  **/
			MSG_EFF_DT=put(coalesce(effective_dt,release_dt),yymmdd10.); /**<-------------------- message effective                    **/
			MSG_EXPRN_DT=MSG_EFF_DT;                        /**<--------------------------------- message expiration                   **/	
			MSG_STUS_CD='3';                                /**<--------------------------------- 3 - Open                             **/ 
			MSG_STUS_RSN_CD='1';                            /**<--------------------------------- 1 - New Opportunity                  **/	
			EVNT_PTY_ROLE_CD=audience_sub_id;               /**<--------------------------------- PTNT - member PRSC = prescriber      **/  
			OPPTY_CMPGN_REQUEST_ID=ticket;                  /**<--------------------------------- DQI request ID                       **/
			COMM_GENERATE_DT=put(coalesce(release_dt,effective_dt),yymmdd10.); /**<-------------- communication date                   **/
			PRSN_SRC_CD='D';                                /**<--------------------------------- D - RxClaim                          **/
			EXTNL_IND='N';                                  /**<--------------------------------- N - internal to CVSH                 **/
			EPH_LINK_ID='';                                 /**<--------------------------------- assigment in m_step3_eoms_transform  **/
			PRSN_SSK_1=clnt_lvl1;                           /**<--------------------------------- carrier ID                           **/
			PRSN_SSK_2=clnt_lvl2;                           /**<--------------------------------- account ID                           **/
			PRSN_SSK_3=clnt_lvl3;                           /**<--------------------------------- group ID                             **/
			PRSN_SSK_4=mbr_id;                              /**<--------------------------------- member acccount ID                   **/
			PRSN_SSK_5='';                                  /**<--------------------------------- NA                                   **/
			PRTC_ID=ql_bnfcy_id;                            /**<--------------------------------- assigment in m_step3_eoms_transform  **/
			EDW_MBR_GID=left(put(mbr_gid,20.));             /**<--------------------------------- member account GID                   **/
			CLT_TYP='PBM';                                  /**<--------------------------------- PBM - pharmacy benefit management    **/ 
			
			OPTY_PASS_THRU_FLR1_VAL=smart_id_disp;		/**<--------------------------------- smart ID disposition                 **/
			OPTY_PASS_THRU_FLR2_VAL=dqi_product_category;   /**<--------------------------------- DQI product                          **/
			OPTY_PASS_THRU_FLR3_VAL=aprimo_product_category;/**<--------------------------------- Aprimo product                       **/
			OPTY_PASS_THRU_FLR4_VAL=aprimo_product_sub_category;/**<----------------------------- Aprimo sub-product                   **/
			OPTY_PASS_THRU_FLR5_VAL=vt_product_category;    /**<--------------------------------- VT consolidated product              **/
			OPTY_PASS_THRU_FLR6_VAL=vt_product_sub_category;/**<--------------------------------- VT consolidated sub-product          **/
			OPTY_PASS_THRU_FLR7_VAL=aprimo_product_title;   /**<--------------------------------- Aprimo title                         **/
			OPTY_PASS_THRU_FLR8_VAL=aprimo_product_child_category;/**<--------------------------- Aprimo VT child                      **/
			OPTY_PASS_THRU_FLR9_VAL=COMM_GENERATE_DT;       /**<--------------------------------- Released Date                        **/ 
			OPTY_PASS_THRU_FLR10_VAL=put(coalesce(process_dt,release_dt),yymmdd10.); /**<-------- Process Date                         **/ 
				

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
			%macro dont_do_phcy;
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
			%mend dont_do_phcy;
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of channels 
			|
			|  SQL  - select prdct_altid, dsc_shrt_tx, dsc_tx from dwv_eoms.product where prod_typ_id = 6
			|
			|	CHLTRVER      = 23 = Letter (Veritas)		
			|
			+---------------------------------------------------------------------------------------------*/
			CHNL_CD="&template_chnl_cd.";                   
			

			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of opportunity subtype product ID 
			|
			|  SQL  - select * from dwv_eoms.opportunity_denorm where program like '%DQI%'
			|
			|  SQL  - select * from dwv_eoms.opportunity_denorm where program like any ( '%DQI%' ) and opportunity_sub_type like any ('%VT%','%VENDOR%')
			|
			|  SQL  - select * from dss_cea2.dqi_consolidated_crossref where parent_template_id = '7380-45366A' and child_template_id = module_100
			|
			+---------------------------------------------------------------------------------------------*/
			OPPTY_TYP_PROD_ID='';
			
			if program_sub_id = 'VT' then do;


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - step 1 of assign VT defaults 	 
				+---------------------------------------------------------------------------------------------*/			
				if      dqi_campaign_id = 40 then OPPTY_TYP_PROD_ID='2848745055';
				else if dqi_campaign_id = 41 then OPPTY_TYP_PROD_ID='2848745056';
				else if dqi_campaign_id = 50 then OPPTY_TYP_PROD_ID='2848745060';
				else if dqi_campaign_id = 53 then OPPTY_TYP_PROD_ID='2848745062';  
				else if dqi_campaign_id = 75 then OPPTY_TYP_PROD_ID='2848745080';
				else OPPTY_TYP_PROD_ID='2848745055';


				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - step 2 of assign non-consolidated VT requests 	 
				+---------------------------------------------------------------------------------------------*/
				if length(vt_product_category) < 3 then do;  
				
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt formulary 	  
					+---------------------------------------------------------------------------------------------*/				
					if      index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'ACSF') > 0 	then OPPTY_TYP_PROD_ID='2876163647'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'ADVANCE') > 0 	then OPPTY_TYP_PROD_ID='2876163646'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'AUTO') > 0 	then OPPTY_TYP_PROD_ID='2876163646'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'BALANCE') > 0 	then OPPTY_TYP_PROD_ID='2876163649'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'CUSTOM') > 0 	then OPPTY_TYP_PROD_ID='2876163645'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'DISRUPTION') > 0 	then OPPTY_TYP_PROD_ID='2876186658'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 and index(aprimo_product_sub_category,'FDRO') > 0 	then OPPTY_TYP_PROD_ID='2876186664'; 
					else if index(aprimo_product_category,'FORMULARY') > 0 								then OPPTY_TYP_PROD_ID='2876163645'; 

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt opioid = post + pa + limit (without pa)	 
					+---------------------------------------------------------------------------------------------*/
					else if index(aprimo_product_category,'OPIOID') > 0 and index(aprimo_product_sub_category,'MME')   > 0 and index(aprimo_product_sub_category,'PA')    > 0 		
																			then OPPTY_TYP_PROD_ID='2876186681'; 
					else if index(aprimo_product_category,'OPIOID') > 0 and index(aprimo_product_sub_category,'MME')   > 0 and index(aprimo_product_sub_category,'POST')  > 0 	
																			then OPPTY_TYP_PROD_ID='2876186682'; 
					else if index(aprimo_product_category,'OPIOID') > 0 and index(aprimo_product_sub_category,'MME')   > 0 and index(aprimo_product_sub_category,'LIMIT') > 0 	
																			then OPPTY_TYP_PROD_ID='2876186686'; 
					else if index(aprimo_product_category,'OPIOID') > 0 and index(aprimo_product_sub_category,'LABEL') > 0 and index(aprimo_product_sub_category,'PA')    > 0 		
																			then OPPTY_TYP_PROD_ID='2876186684'; 
					else if index(aprimo_product_category,'OPIOID') > 0 and index(aprimo_product_sub_category,'LABEL') > 0 and index(aprimo_product_sub_category,'POST')  > 0 	
																			then OPPTY_TYP_PROD_ID='2876186680'; 
					else if index(aprimo_product_category,'OPIOID') > 0 and index(aprimo_product_sub_category,'LABEL') > 0 and index(aprimo_product_sub_category,'LIMIT') > 0 	
																			then OPPTY_TYP_PROD_ID='2876186685';
					else if index(aprimo_product_category,'OPIOID') > 0 								then OPPTY_TYP_PROD_ID='2848745062'; 

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt specialty 	 
					+---------------------------------------------------------------------------------------------*/
					else if index(aprimo_product_category,'SPECIALTY') > 0 and index(aprimo_product_sub_category,'EXCLUSIVE') > 0 	then OPPTY_TYP_PROD_ID='2876186690';
					else if index(aprimo_product_category,'SPECIALTY') > 0 and index(aprimo_product_sub_category,'SPDPD') > 0 	then OPPTY_TYP_PROD_ID='2876186691';   
					else if index(aprimo_product_category,'SPECIALTY') > 0 								then OPPTY_TYP_PROD_ID='2876186689'; 
										
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt prior authorization notification 	 
					+---------------------------------------------------------------------------------------------*/					
					else if index(aprimo_product_category,'PRIOR') > 0 and index(aprimo_product_sub_category,'PRIOR') > 0 		then OPPTY_TYP_PROD_ID='2876186677';
					else if index(aprimo_product_category,'PRIOR') > 0 and index(aprimo_product_sub_category,'PRIOR') = 0 		then OPPTY_TYP_PROD_ID='2876186677';

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt other 	 
					+---------------------------------------------------------------------------------------------*/
					else if index(aprimo_product_category,'OTHER') > 0 and index(aprimo_product_sub_category,'OTHER') > 0		then OPPTY_TYP_PROD_ID='2876186669'; 
					else if index(aprimo_product_category,'OTHER') > 0 and index(aprimo_product_sub_category,'OTHER') = 0		then OPPTY_TYP_PROD_ID='2876186669'; 
					else if index(aprimo_product_category,'VALUE') > 0 and index(aprimo_product_sub_category,'FORMULARY') > 0 	then OPPTY_TYP_PROD_ID='2876186666';
					else if index(aprimo_product_category,'STEP')  > 0 and index(aprimo_product_sub_category,'THERAPY')   > 0 	then OPPTY_TYP_PROD_ID='2876186693';
					
					else if index(aprimo_product_category,'LIMIT') > 0 and index(aprimo_product_sub_category,'WITHOUT') > 0 	then OPPTY_TYP_PROD_ID='2876186688';
					else if index(aprimo_product_category,'LIMIT') > 0 and index(aprimo_product_sub_category,'WITH')    > 0 	then OPPTY_TYP_PROD_ID='2876186683';
					
					else if index(aprimo_product_category,'PHARMACY') > 0 and index(aprimo_product_sub_category,'DISRUPTION') > 0 	then OPPTY_TYP_PROD_ID='2876186676';
					else if index(aprimo_product_category,'PHARMACY') > 0 and index(aprimo_product_sub_category,'DISRUPTION') = 0 	then OPPTY_TYP_PROD_ID='2876186676';

					else if index(aprimo_product_category,'DAW') > 0 and index(aprimo_product_sub_category,'DAW') > 0 		then OPPTY_TYP_PROD_ID='2876163644';
					else if index(aprimo_product_category,'DAW') > 0 and index(aprimo_product_sub_category,'DAW') = 0 		then OPPTY_TYP_PROD_ID='2876163644';

					else if index(aprimo_product_category,'COMPOUND') > 0 and index(aprimo_product_sub_category,'OTHER') > 0 	then OPPTY_TYP_PROD_ID='2876163643';
					else if index(aprimo_product_category,'COMPOUND') > 0 and index(aprimo_product_sub_category,'OTHER') = 0 	then OPPTY_TYP_PROD_ID='2876163643';

				end;
				

				/* SASDOC --------------------------------------------------------------------------------------
				|  STEP - step 3 of assign consolidated VT requests 	 
				+---------------------------------------------------------------------------------------------*/				
				if length(vt_product_category) > 3 then do;  	
					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt formulary 	 
					+---------------------------------------------------------------------------------------------*/					
					if index(vt_product_category,'FORMULARY') > 0      and index(vt_product_sub_category,'BALANCE')    > 0 		then OPPTY_TYP_PROD_ID='2876163649';
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'IMMUNE')     > 0 	 	then OPPTY_TYP_PROD_ID='2876163646';
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'BASIC')      > 0 		then OPPTY_TYP_PROD_ID='2876186657';
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'DISRUPTION') > 0 		then OPPTY_TYP_PROD_ID='2876186658';
					
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' and index(aprimo_product_child_category,'ACSF') > 0		then OPPTY_TYP_PROD_ID='2876163647';
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' and index(aprimo_product_child_category,'DIABETIC') > 0	then OPPTY_TYP_PROD_ID='2876186662';
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' and index(aprimo_product_child_category,'FDRO') > 0		then OPPTY_TYP_PROD_ID='2876186664';
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' and index(aprimo_product_child_category,'AUTO') > 0		then OPPTY_TYP_PROD_ID='2876163646';
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' and index(aprimo_product_child_category,'UNAPPROVED') > 0	then OPPTY_TYP_PROD_ID='2876186675';
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' and index(aprimo_product_child_category,'OTHER') > 0		then OPPTY_TYP_PROD_ID='2876163645';
					else if index(vt_product_category,'FORMULARY') > 0 and vt_product_sub_category =     'EXCLUSIONS' 								then OPPTY_TYP_PROD_ID='2876186659'; 
					
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0  and index(aprimo_product_child_category,'ACSF') > 0 		then OPPTY_TYP_PROD_ID='2876163647'; 
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0  and index(aprimo_product_child_category,'DIABETIC') > 0	then OPPTY_TYP_PROD_ID='2876186664';
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0  and index(aprimo_product_child_category,'FDRO') > 0 		then OPPTY_TYP_PROD_ID='2876186664';
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0  and index(aprimo_product_child_category,'AUTO') > 0 		then OPPTY_TYP_PROD_ID='2876163646'; 
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0  and index(aprimo_product_child_category,'UNAPPROVED') > 0	then OPPTY_TYP_PROD_ID='2876186675';
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0  and index(aprimo_product_child_category,'OTHER') > 0		then OPPTY_TYP_PROD_ID='2876163645';					
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'FDRO') > 0 								then OPPTY_TYP_PROD_ID='2876186664'; 
					
					else if index(vt_product_category,'FORMULARY') > 0 and index(vt_product_sub_category,'VALUE') > 0 								then OPPTY_TYP_PROD_ID='2876186666';

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt specialty 	 
					+---------------------------------------------------------------------------------------------*/					
					else if vt_product_category = 'SPECIALTY' and index(vt_product_sub_category,'EXCLUSIVE') > 0 		then OPPTY_TYP_PROD_ID='2876186690';
					else if vt_product_category = 'SPECIALTY' and index(vt_product_sub_category,'SPDPD')    > 0 		then OPPTY_TYP_PROD_ID='2876186691';
					
					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt quantity limit 	 
					+---------------------------------------------------------------------------------------------*/						
					else if vt_product_category = 'QUANTITY LIMIT' and index(vt_product_sub_category,'WITHOUT') > 0 	then OPPTY_TYP_PROD_ID='2876186688';
					else if vt_product_category = 'QUANTITY LIMIT' and index(vt_product_sub_category,'WITH')    > 0 	then OPPTY_TYP_PROD_ID='2876186683';					

					/* SASDOC --------------------------------------------------------------------------------------
					|  STEP - vt other 	 
					+---------------------------------------------------------------------------------------------*/					
					else if vt_product_category = 'MAIL ORDER NCMD' 							then OPPTY_TYP_PROD_ID='2876186668';
					else if vt_product_category = 'OTHER' 									then OPPTY_TYP_PROD_ID='2876186669';
					else if vt_product_category = 'PHARMACY NETWORK DISRUPTION' 						then OPPTY_TYP_PROD_ID='2876186676';
					else if vt_product_category = 'PRIOR AUTHORIZATION PA NOTIFICATION' 					then OPPTY_TYP_PROD_ID='2876186677';			
					else if vt_product_category = 'QUANTITY LIMIT' and index(vt_product_sub_category,'WITHOUT') > 0 	then OPPTY_TYP_PROD_ID='2876186688';
					else if vt_product_category = 'QUANTITY LIMIT' and index(vt_product_sub_category,'WITH')    > 0 	then OPPTY_TYP_PROD_ID='2876186683';
					else if vt_product_category = 'STEP THERAPY NOTIFICATION' 						then OPPTY_TYP_PROD_ID='2876186693';
					else if index(vt_product_category,'COMPOUND') > 0 							then OPPTY_TYP_PROD_ID='2876163643';
					else if index(vt_product_category,'DAW') > 0 								then OPPTY_TYP_PROD_ID='2876163644';					

				end;
			
			end;
			
			
			if program_sub_id = 'DQI' then do;
			
				if      dqi_campaign_id = 0  then OPPTY_TYP_PROD_ID='2847766385';
				else if dqi_campaign_id = 1  then OPPTY_TYP_PROD_ID='2847766748';
				else if dqi_campaign_id = 2  then OPPTY_TYP_PROD_ID='2848745018';
				else if dqi_campaign_id = 3  then OPPTY_TYP_PROD_ID='2848745019';
				else if dqi_campaign_id = 4  then OPPTY_TYP_PROD_ID='2848745020';
				else if dqi_campaign_id = 5  then OPPTY_TYP_PROD_ID='2848745021';
				else if dqi_campaign_id = 6  then OPPTY_TYP_PROD_ID='2848745022';
				else if dqi_campaign_id = 7  then OPPTY_TYP_PROD_ID='2848745023';
				else if dqi_campaign_id = 8  then OPPTY_TYP_PROD_ID='2848745024';
				else if dqi_campaign_id = 9  then OPPTY_TYP_PROD_ID='2848745025';
				else if dqi_campaign_id = 10 then OPPTY_TYP_PROD_ID='2848745026';
				else if dqi_campaign_id = 11 then OPPTY_TYP_PROD_ID='2848745027';
				else if dqi_campaign_id = 12 then OPPTY_TYP_PROD_ID='2848745028';
				else if dqi_campaign_id = 13 then OPPTY_TYP_PROD_ID='2848745029';
				else if dqi_campaign_id = 14 then OPPTY_TYP_PROD_ID='2848745030';
				else if dqi_campaign_id = 15 then OPPTY_TYP_PROD_ID='2848745031';
				else if dqi_campaign_id = 16 then OPPTY_TYP_PROD_ID='2848745032';
				else if dqi_campaign_id = 17 then OPPTY_TYP_PROD_ID='2848745033';
				else if dqi_campaign_id = 18 then OPPTY_TYP_PROD_ID='2848745034';
				else if dqi_campaign_id = 19 then OPPTY_TYP_PROD_ID='2848745035';
				else if dqi_campaign_id = 20 then OPPTY_TYP_PROD_ID='2848745036';
				else if dqi_campaign_id = 21 then OPPTY_TYP_PROD_ID='2848745037';
				else if dqi_campaign_id = 22 then OPPTY_TYP_PROD_ID='2848745038';
				else if dqi_campaign_id = 23 then OPPTY_TYP_PROD_ID='2848745039';
				else if dqi_campaign_id = 24 then OPPTY_TYP_PROD_ID='2848745040';
				else if dqi_campaign_id = 25 then OPPTY_TYP_PROD_ID='2848745041';
				else if dqi_campaign_id = 26 then OPPTY_TYP_PROD_ID='2848745042';
				else if dqi_campaign_id = 27 then OPPTY_TYP_PROD_ID='2848745043';
				else if dqi_campaign_id = 28 then OPPTY_TYP_PROD_ID='2848745044';
				else if dqi_campaign_id = 29 then OPPTY_TYP_PROD_ID='2848745045';
				else if dqi_campaign_id = 30 then OPPTY_TYP_PROD_ID='2848745046';
				else if dqi_campaign_id = 31 then OPPTY_TYP_PROD_ID='2848745047';
				else if dqi_campaign_id = 32 then OPPTY_TYP_PROD_ID='2848745048';
				else if dqi_campaign_id = 33 then OPPTY_TYP_PROD_ID='2848745049';
				else if dqi_campaign_id = 34 then OPPTY_TYP_PROD_ID='2848745050';
				else if dqi_campaign_id = 35 then OPPTY_TYP_PROD_ID='2848745051';
				else if dqi_campaign_id = 36 then OPPTY_TYP_PROD_ID='2848745052';
				else if dqi_campaign_id = 38 then OPPTY_TYP_PROD_ID='2848745053';
				else if dqi_campaign_id = 39 then OPPTY_TYP_PROD_ID='2848745054';
				else if dqi_campaign_id = 42 then OPPTY_TYP_PROD_ID='2848745057';
				else if dqi_campaign_id = 43 then OPPTY_TYP_PROD_ID='2848745058';
				else if dqi_campaign_id = 44 then OPPTY_TYP_PROD_ID='2848745059';
				else if dqi_campaign_id = 52 then OPPTY_TYP_PROD_ID='2848745061';
				else if dqi_campaign_id = 54 then OPPTY_TYP_PROD_ID='2848745063';
				else if dqi_campaign_id = 55 then OPPTY_TYP_PROD_ID='2848745064';
				else if dqi_campaign_id = 56 then OPPTY_TYP_PROD_ID='2848745065';
				else if dqi_campaign_id = 58 then OPPTY_TYP_PROD_ID='2848745066';
				else if dqi_campaign_id = 59 then OPPTY_TYP_PROD_ID='2848745067';
				else if dqi_campaign_id = 60 then OPPTY_TYP_PROD_ID='2848745068';
				else if dqi_campaign_id = 61 then OPPTY_TYP_PROD_ID='2848745069';
				else if dqi_campaign_id = 63 then OPPTY_TYP_PROD_ID='2848745070';
				else if dqi_campaign_id = 64 then OPPTY_TYP_PROD_ID='2848745071';
				else if dqi_campaign_id = 66 then OPPTY_TYP_PROD_ID='2848745072';
				else if dqi_campaign_id = 67 then OPPTY_TYP_PROD_ID='2848745073';
				else if dqi_campaign_id = 68 then OPPTY_TYP_PROD_ID='2848745074';
				else if dqi_campaign_id = 69 then OPPTY_TYP_PROD_ID='2848745075';
				else if dqi_campaign_id = 70 then OPPTY_TYP_PROD_ID='2848745076';
				else if dqi_campaign_id = 71 then OPPTY_TYP_PROD_ID='2848745077';
				else if dqi_campaign_id = 73 then OPPTY_TYP_PROD_ID='2848745078';
				else if dqi_campaign_id = 74 then OPPTY_TYP_PROD_ID='2848745079';
				else if dqi_campaign_id = 76 then OPPTY_TYP_PROD_ID='2848745081';
				else if dqi_campaign_id = 79 then OPPTY_TYP_PROD_ID='2848745082';
				else if dqi_campaign_id = 81 then OPPTY_TYP_PROD_ID='2848745083';
				else if dqi_campaign_id = 82 then OPPTY_TYP_PROD_ID='2848745084';
				else if dqi_campaign_id = 84 then OPPTY_TYP_PROD_ID='2848745085';
				else if dqi_campaign_id = 88 then OPPTY_TYP_PROD_ID='2848745086';
				else OPPTY_TYP_PROD_ID='2847766385';
				
			end;

		keep %do j=1 %to &name_var_total; &&name_var&j %end; ;
		run;
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - qc mapping of dqi vs vt vs aprimo
		|
		+---------------------------------------------------------------------------------------------*/	
		proc sql;  
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		create table opportunity_denorm as
		select * from connection to tera(

				select * 
				from dwv_eoms.opportunity_denorm 
				where program like any ( '%DQI%' )   
		) ;
		quit;
	
		proc sql noprint;
		create table qc_007 as
		select  
		OPPTY_CMPGN_ID as dqi_id, OPTY_PASS_THRU_FLR2_VAL as dqi_product,
		OPTY_PASS_THRU_FLR5_VAL as vt_product,OPTY_PASS_THRU_FLR6_VAL as vt_subproduct,
		OPTY_PASS_THRU_FLR3_VAL as aprimo_product, OPTY_PASS_THRU_FLR4_VAL as aprimo_subproduct, OPTY_PASS_THRU_FLR8_VAL as aprimo_subvtproduct,
		OPPTY_TYP_PROD_ID as ecoa_id, OPPORTUNITY_SUB_TYPE as ecoa_product, 
		count(*) as cnt
		from dqi_opporunity a left join
		opportunity_denorm b
		on input(a.OPPTY_TYP_PROD_ID,20.) = b.OPPORTUNITY_SUB_TYPE_PROD_ID
		group by 1,2,3,4,5,6,7,8,9
		order by 1,2,3,4,5,6,7,8,9;
		quit;		
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - max opp ID to create unique msg ID 
		|
		+---------------------------------------------------------------------------------------------*/
		%let oppty_evnt_id = 0;
		
		proc sql noprint; 
		select max(oppty_evnt_id) into: max_id_new separated by ''
		from dqi_opporunity;
		quit;

		%put NOTE: max_id_new = &max_id_new;

		data dqi_opporunity;
		set dqi_opporunity;
		MSG_EVNT_ID=left(put(&max_id_new +_n_,20.));
		run;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - qc to validate the opp + msg ID are unique  
		|
		+---------------------------------------------------------------------------------------------*/		
		%let cntopp = 0;

		proc sort data=dqi_opporunity dupout=duplicate_recs nodupkey;
		by OPPTY_EVNT_ID MSG_EVNT_ID ;
		run;

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
		|  STEP - collect queue IDs
		|
		+---------------------------------------------------------------------------------------------*/ 		
		proc sql noprint;
		select "'"||trim(max(oppty_evnt_id))||"'"   into: opp_max separated by '' 
		from dqi_opporunity;
		quit;
		
		proc sql noprint;
		select "'"||trim(max(msg_evnt_id))||"'"   into: msg_max separated by '' 
		from dqi_opporunity;
		quit;		

		%put NOTE: opp_max = &opp_max. ;
		%put NOTE: msg_max = &msg_max. ;

				
		proc sql;
		connect to teradata as tera (server= &c_s_server. user=&td_id. password="&td_pw");
		execute(  

			insert &c_s_tdtempx..dqi_eoms_queue
			select 0, &opp_max. , &msg_max. , 'DQI' , current_date 

		) by tera;
		execute (commit work) by tera;  
		disconnect from tera;
		quit;			


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
		|  STEP - get disposition from history with no dispositions in edw
		|
		|  1. only assess DQI and VT
		|  2. do a 10 day look back since of fail loads or delays
		|  3. sort by VT and descending process date
		|  4. target disposition code null in IT
		|
		+---------------------------------------------------------------------------------------------*/ 		
		proc sql;
		connect to teradata as tera (user=&td_id password="&td_pw" tdpid=&c_s_server. fastload=yes);
		create table dqi_disposition_missing  as 
		select * from connection to tera
		(
			select top 100000 
			m.oppty_evnt_id, 
			m.msg_evnt_id, 
			o.extnl_sys_oppty_id, 
			substr(o.extnl_sys_oppty_id,1,length(o.extnl_sys_oppty_id)-1)||o.msg_prdct_alt_id as OPTY_PASS_THRU_FLR1_VAL, 
			substr(m.PRSTN_DT,1,10) as MSG_EFF_DT 
			from dss_cea2.dqi_eoms_dispositions   m,
			     dss_cea2.dqi_eoms_opportunity    o, 
			     dwv_campgn.event_message         i
			where o.msg_evnt_id = m.msg_evnt_id
			and o.msg_evnt_id = to_char(i.msg_evnt_id)
			and o.oppty_src_cd = 'DQI'
			and o.alt_oppty_subtyp_src_cd not in ('STARS')
			and i.msg_dispn_cd is null
			and o.execution_date < current_date - 10
			order by o.alt_oppty_subtyp_src_cd desc, m.execution_date desc
		);
		disconnect from tera;
		quit; 
		
		data dqi_opporunity;
		set dqi_opporunity ;
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - mapping of the ecoa dqi disposition file = 9 data elements
		+---------------------------------------------------------------------------------------------*/
		data dqi_disposition;
		if _n_ = 1 then set template_disp;
		set dqi_opporunity dqi_disposition_missing;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of disposition
			|
			+---------------------------------------------------------------------------------------------*/		
			OPPTY_SRC_CD='DQI';
			OPPTY_EVNT_ID=OPPTY_EVNT_ID;
			MSG_EVNT_ID=MSG_EVNT_ID;		
			EXTNL_SYS_OPPTY_ID=EXTNL_SYS_OPPTY_ID;   	/**<------------------------------------------------ smart ID opportunity **/
			MSG_DISPN_CD='1';                        	/**<------------------------------------------------ 1 - Delivered        **/
			MSG_DISPN_SUB_CD='10';                  	/**<------------------------------------------------ 10 - New Opportunity **/
			DLVRY_MTHD_CD='102';                    	/**<------------------------------------------------ 102 - Print          **/ 
			CONTACT_DATA_TYPE = 'M';		 	/**<------------------------------------------------ M - Mail             **/ 
			CONTACT_DATA_VALUE = OPTY_PASS_THRU_FLR1_VAL;	/**<------------------------------------------------ smart ID disposition **/	 
			PRSTN_DT=MSG_EFF_DT;			 	/**<------------------------------------------------ outreach date        **/
			
			
			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - mapping of channels 
			|
			|  SQL  - select prdct_altid, dsc_shrt_tx, dsc_tx from dwv_eoms.product where prod_typ_id = 6
			|
			|	CHLTRVER      = 23 = Letter (Veritas)		
			|
			+---------------------------------------------------------------------------------------------*/ 
			CHNL_CD="&template_chnl_cd.";            
			
			
		keep &variables_disp.  ;
		run;


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - qc to validate the opp + msg ID are unique  
		|
		+---------------------------------------------------------------------------------------------*/		
		%let cntopp = 0;

		proc sort data=dqi_disposition dupout=duplicate_recs nodupkey;
		by OPPTY_EVNT_ID MSG_EVNT_ID ;
		run;

		%let dsid=%sysfunc(open(duplicate_recs));
		%let cntopp=%sysfunc(attrn(&dsid,nlobs));
		%let rc=%sysfunc(close(&dsid));
		
		%put NOTE: cntopp = &cntopp. ;

		%if &cntopp > 0 %then %do;

			proc export data=work.duplicate_recs
			dbms=xlsx 
			outfile="&c_s_datadir.&SYS_DIR.validate_duplicates_dispositions_DQI.xlsx" 
			replace; 
			sheet="validate_duplicates"; 
			run;

			%m_abend_handler(abend_report=%str(&c_s_datadir./validate_duplicates_dispositions_DQI.xlsx),
					 abend_message=%str(There is an issue with the creation of the dispostion DQI data - There are duplicate IDs));			

		%end;		


		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - create ecoa opportunity files - 990123 = dqi
		|
		+---------------------------------------------------------------------------------------------*/
		proc export data = dqi_opporunity
		outfile = "&dir_out./sfmc_opportunity.&c_s_file_date.990123"
		dbms = DLM Replace;
		delimiter = '|';
		putnames=NO;
		run;
		
		%let opp_cnt=0;

		proc sql noprint;
		select count(*) into: opp_cnt separated by '' 
		from dqi_opporunity;
		quit;

		data _null_;
		format x $100. ;
		file "&dir_out./sfmc_opportunity.&c_s_file_date.990123.CNTL";
		x="OD|sfmc_opportunity.&c_s_file_date.990123|"||"&opp_cnt."||"|SFMC|I||||";
		x=compress(x);
		put x ;
		run;

		data _null_;
		file "&dir_out./sfmc_rx_opportunity.&c_s_file_date.990123";
		run;

		data _null_;
		file "&dir_out./sfmc_rx_opportunity.&c_s_file_date.990123.CNTL";
		put "OD|sfmc_rx_opportunity.&c_s_file_date.990123|0|SFMC|I||||";
		run;
		
		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - create ecoa disposition files - 990123 = dqi
		|
		+---------------------------------------------------------------------------------------------*/
		proc export data = dqi_disposition
		outfile = "&dir_out./sfmc_disposition.&c_s_file_date.990123"
		dbms = DLM Replace;
		putnames=NO;
		delimiter = '|';
		run;
		
		%let disp_cnt=0;

		proc sql noprint;
		select count(*) into: disp_cnt separated by '' 
		from dqi_disposition;
		quit;

		data _null_;
		format x $100. ;
		file "&dir_out./sfmc_disposition.&c_s_file_date.990123.CNTL";
		x="OD|sfmc_disposition.&c_s_file_date.990123|"||"&disp_cnt."||"|SFMC|I||||";
		x=compress(x);
		put x;
		run;

		data _null_;
		file "&dir_out./sfmc_rx_disposition.&c_s_file_date.990123";
		run;

		data _null_;
		file "&dir_out./sfmc_rx_disposition.&c_s_file_date.990123.CNTL";
		put "OD|sfmc_rx_disposition.&c_s_file_date.990123|0|SFMC|I||||";
		run;

		
		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - put files on sftp site for ctl_data_integration_eoms
		|   
		+---------------------------------------------------------------------------------------------*/			
		%if &c_s_sftp_put_production. = Y %then %do;  /**<------------------------------- start - sftp  **/
		
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

		%end;  /**<------------------------------- end - sftp  **/		
		

		/* SASDOC --------------------------------------------------------------------------------------
		|  STEP - send out dqi complete email  
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
	
		%if &c_s_dqi_campaign_id. = 85 %then %do;
			filename xemail email 
			to=(&c_s_email_to.)
			subject="CTT Data Profiling - COMPLETED DQI ECOA File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";
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
				put "The CTT Data Profiling PBM DQI file processing has completed. ";
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
		
		
		%m_dqi_process_control(id=5, step=%str(DQI - DATA INTGRATION CMCTN HISTORY COMPLETE));		

	%end;  /** <--------------------------- end - create opportunity + disposition **/
	
	

	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - email if no files for today
	|
	+---------------------------------------------------------------------------------------------*/	
	%if &dqi_cnt = 0 %then %do;  /** <--------------------------- start - create opportunity + disposition **/
	
		filename xemail email 
		to=(&c_s_email_to.)
		subject="CTT Data Profiling - COMPLETED DQI ECOA File Processing &c_s_file_date. ***SECUREMAIL*** "  lrecl=32000 content_type="text/html";  
				
		
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
			put "The CTT Data Profiling DQI file processing has complete. ";
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
			
	%end; 	
	
	%m_dqi_process_control(id=10, step=%str(DQI - DATA INTGRATION COMPLETE));
	
	
%mend cd_data_integration_dqi;
%cd_data_integration_dqi;

