
/* HEADER --------------------------------------------------------------------------------------
| MACRO:    m_process_control_eoms
|
| LOCATION:     
|
| PURPOSE:  collects metadata about the cs process within a db table 
|
| LOGIC:    1.  collects metadata about the cs process
|           2.  bulkloads the sas data into a temp table
|           3.  insert the data into dss_cea2.cs_process_control
|
| INPUT: 	 
|
| OUTPUT:    
|
+----------------------------------------------------------------------------------------
| HISTORY:  20190201 - Clinical Stars Team - Original (version 2019.02.01)
|
+------------------------------------------------------------------------------------------HEADER*/

%macro m_process_control_eoms(id=,step=,file_name=none.txt,file_count=0);

	%if %symexist(c_s_tdtempx) %then %do;
		libname &c_s_tdtempx. teradata user=&td_id. password="&td_pw"
				  server=&c_s_server.
				  database=&c_s_tdtempx.             
				  connection=unique
				  defer=yes 
				  fastload=yes;
	%end;

	data _null_;
	cs_table_id=round(1000000000*ranuni(0));   
	call symput('cs_table_id',left(cs_table_id)); 
	run;
	
	%put NOTE: cs_table_id = &cs_table_id. ;
	
	data cs_pc_&cs_table_id.;
		row_gid=1;
		cs_campaign_id=&c_s_campaign_id.;
		cs_id=&id.;
		%if &id ne 99 %then %do; 
			cs_program="&c_s_program."; 
			cs_client=compress("&c_s_client_nm.","'");
			cs_project="&c_s_campaign.";
			cs_request=compress("&c_s_campaign.","'");
			cs_process_id  = "&mbr_process_id." ; 
		%end; 	
		cs_ticket="&c_s_ticket.";
		cs_aprimo_activity="&c_s_log_date.";
		cs_step="&step.";
		cs_location="&c_s_maindir.";
		cs_file="&file_name.";
		cs_file_cnts=&file_count.;
		cs_user_id = "%lowcase(&sysuserid.)" ;
		cs_job_id  = "&sysjobid." ;
	run;
	
	%if &id = 99 %then %do;  /** communication history **/
	
		proc sql;
		create table process_99 as
		select *
		%if &c_s_dqi_production. = Y %then %do;
		  from &c_s_tdtempx..dqi_eoms_process_control 
		%end;
		%else %do;
		  from &c_s_tdtempx..dqi_eoms_process_control_dev 
		%end;
		where cs_ticket = "&c_s_ticket." ;
		quit;

		proc sort data = process_99;
		by descending row_gid;
		run;

		data _null_;
		set process_99 (obs=1);
		call symput('mpid',left(trim(mbr_process_id)));
		call symput('ppid',left(trim(phys_process_id)));
		call symput('projid',left(trim(cs_project)));
		call symput('reqid',left(trim(cs_request)));
		call symput('clntid',left(trim(cs_client)));
		call symput('pgmid',left(trim(cs_program)));
		run;
		
		data cs_pc_&cs_table_id.;
		set cs_pc_&cs_table_id.; 
			mbr_process_id  = "&mpid." ;
			phys_process_id = "&ppid." ;
			cs_program = "&pgmid." ;
			cs_client = "&clntid." ;
			cs_project = "&projid." ; 
			cs_request = "&reqid." ; 			 			
		run;
	
	%end;
	
	%m_table_drop_passthrough(data_in=&c_s_tdtempx..cs_pc_&cs_table_id.);

	
	%if &c_s_dqi_production. = Y %then %do;
	  %m_create_table_insert_variables2(data_set=&c_s_tdtempx..dqi_eoms_process_control, data_set2=cs_pc_&cs_table_id., macro_variable=cs_pc_variables);
	%end;
	%else %do;
	  %m_create_table_insert_variables2(data_set=&c_s_tdtempx..dqi_eoms_process_control_dev, data_set2=cs_pc_&cs_table_id., macro_variable=cs_pc_variables);
	%end;

	
	proc sql;
		connect to teradata (user=&td_id password="&td_pw" Server=&c_s_server );
		execute (insert into 
					%if &c_s_dqi_production. = Y %then %do;
					  &c_s_tdtempx..dqi_eoms_process_control 
					%end;
					%else %do;
					  &c_s_tdtempx..dqi_eoms_process_control_dev 
					%end;
			 values (  &cs_pc_variables. )
			 ) by teradata;
		execute (commit) by teradata;
		disconnect from teradata;
	quit;
		
	%m_table_drop_passthrough(data_in=&c_s_tdtempx..cs_pc_&cs_table_id.);
	%m_table_drop(data_in=work.cs_pc_&cs_table_id.);



%mend m_process_control_eoms;
