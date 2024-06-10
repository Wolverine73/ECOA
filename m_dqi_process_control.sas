
/* HEADER --------------------------------------------------------------------------------------
| MACRO:    m_dqi_process_control
|
| LOCATION: /SASNODE/SAS_MACROS/
|
| USAGE:    
|
| PURPOSE:  collects metadata about the dqi process within a db table 
|
| LOGIC:    1.  collects metadata about the dqi process
|           2.  bulkloads the sas data into a temp table
|           3.  insert the data into dss_cea2.dqi_process_control
|
| TABLES:     
|
| INPUT: 	 
|
| OUTPUT:    
|
+----------------------------------------------------------------------------------------
| HISTORY:  19FEB2015 - Brian Stropich - Original
+------------------------------------------------------------------------------------------HEADER*/

%macro m_dqi_process_control(id=,step=);

	%if %symexist(c_s_tdtempx) %then %do;
		libname &c_s_tdtempx. teradata user=&td_id. password="&td_pw"
				  server=&c_s_server.
				  database=&c_s_tdtempx.             
				  connection=unique
				  defer=yes 
				  fastload=yes;
	%end;

	data _null_;
	dqi_table_id=round(1000000000*ranuni(0));   
	call symput('dqi_table_id',left(dqi_table_id)); 
	run;
	
	%put NOTE: dqi_table_id = &dqi_table_id. ;
	
	data dqi_pc_&dqi_table_id.;
		row_gid=1;
		dqi_campaign_id=&c_s_dqi_campaign_id.;
		dqi_id=&id.;
		%if &id ne 99 %then %do;
			dqi_program="&c_s_program.";
			dqi_client=compress("&c_s_client_nm.","'");
			dqi_project="&c_s_proj.";
			dqi_request=compress("&c_s_mailing_name.","'");
			mbr_process_id  = "&mbr_process_id." ;
			phys_process_id = "&phys_process_id." ;
		%end; 	
		dqi_aprimo_activity="&c_s_aprimo_activity.";
		dqi_ticket="&c_s_ticket.";	
		dqi_macro_step=upcase("&step.");
		dqi_location="&c_s_maindir.";
		dqi_user_id = "%lowcase(&sysuserid.)" ;
		dqi_job_id  = "&sysjobid." ;
	run;
	
	%if &id = 99 %then %do;  /** communication history **/
	
		proc sql;
		create table process_99 as
		select *
		%if &c_s_dqi_production. = Y %then %do;
		  from &c_s_tdtempx..dqi_process_control 
		%end;
		%else %do;
		  from &c_s_tdtempx..dqi_process_control_dev 
		%end;
		where dqi_ticket = "&c_s_ticket." ;
		quit;

		proc sort data = process_99;
		by descending row_gid;
		run;

		data _null_;
		set process_99 (obs=1);
		call symput('mpid',left(trim(mbr_process_id)));
		call symput('ppid',left(trim(phys_process_id)));
		call symput('projid',left(trim(dqi_project)));
		call symput('reqid',left(trim(dqi_request)));
		call symput('clntid',left(trim(dqi_client)));
		call symput('pgmid',left(trim(dqi_program)));
		run;
		
		data dqi_pc_&dqi_table_id.;
		set dqi_pc_&dqi_table_id.; 
			mbr_process_id  = "&mpid." ;
			phys_process_id = "&ppid." ;
			dqi_program = "&pgmid." ;
			dqi_client = "&clntid." ;
			dqi_project = "&projid." ; 
			dqi_request = "&reqid." ; 			 			
		run;
	
	%end;
	
	%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_pc_&dqi_table_id.);
			
	%if &c_s_dqi_production. = Y %then %do;
	  %m_create_table_insert_variables2(data_set=&c_s_tdtempx..dqi_process_control, data_set2=dqi_pc_&dqi_table_id., macro_variable=cs_pc_variables);
	%end;
	%else %do;
	  %m_create_table_insert_variables2(data_set=&c_s_tdtempx..dqi_process_control_dev, data_set2=dqi_pc_&dqi_table_id., macro_variable=cs_pc_variables);
	%end;	

	proc sql;
		connect to teradata (user=&td_id password="&td_pw" Server=&c_s_server );
		execute (insert into 
					%if &c_s_dqi_production. = Y %then %do;
					  &c_s_tdtempx..dqi_process_control 
					%end;
					%else %do;
					  &c_s_tdtempx..dqi_process_control_dev 
					%end;
			 values (  &cs_pc_variables. )
			 ) by teradata;
		execute (commit) by teradata;
		disconnect from teradata;
	quit;
	
	%m_table_drop_passthrough(data_in=&c_s_tdtempx..dqi_pc_&dqi_table_id.);
	%m_table_drop(data_in=work.dqi_pc_&dqi_table_id.);

	/* SASDOC --------------------------------------------------------------------------------------
	|   Update file permissions to group, owner RW, other - 
	+----------------------------------------------------------------------------------------SASDOC*/
	%if &id=10 %then %do;
		x chmod -R 770 "&c_s_filedir/" ;
	%end;

%mend m_dqi_process_control;