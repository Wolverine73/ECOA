
/* HEADER --------------------------------------------------------------------------------------
| MACRO:    m_create_table_insert_variables
|
| LOCATION: /SASNODE/SAS_MACROS/
|
| USAGE:    
|
| PURPOSE:  creates a macro variable of an explicit list of variables needed for table insert
|
| LOGIC:    1.  references the db table that data will be inserted into
|           2.  creates a global macro variable of the explicit list of variables
|           3.  note... the variables need to exist within the sas dataset that was bulkloaded
|		to the database for this to work
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

%macro m_create_table_insert_variables2(data_set=, data_set2=, macro_variable=);

	/*** solution - explicit list of variables instead of implicit list of variables ***/

	%global &macro_variable. ;

	proc contents data = &data_set. 
	              out  = temp_variables (keep = name varnum type format length) 
	              noprint;
	run;

	data temp_variables;
	set temp_variables;
	_name_=upcase(name);
	run;

	proc transpose data=&data_set2. out=temp_transpose1 ;
	var _char_ ;
	run;

	proc transpose data=&data_set2. out=temp_transpose2 ;
	var _numeric_ ;
	run;

	data temp_transpose2 (rename=(col1b=col1));
	format col1b $100.;
	set temp_transpose2;
	col1b=left(col1);
	drop col1;
	run;

	data temp_transpose;
	format col2 $100. ;
	set temp_transpose1 temp_transpose2;
	_name_=upcase(_name_);
	col2=left(trim(col1));
	run;	

	proc sort data = temp_transpose;
	by _name_;
	run; 
	
	proc sort data = temp_variables;
	by _name_;
	run; 	

	data temp_variables;
	merge temp_transpose temp_variables ;
	by _name_;
	name=lowcase(name);
	if type = 2 then col2="'"||strip(trim(col2))||"'"; 
	if name = 'row_gid' then col2='1';
	if name = 'execution_date' then col2='current_date'; 
	if name = 'dqi_ts'   then col2="CAST(CURRENT_DATE AS TIMESTAMP(0)) + ((CURRENT_TIME - TIME '00:00:00') HOUR TO SECOND(0))";
	run;

	proc sort data = temp_variables;
	by varnum;
	run;	

	proc sql noprint;
	select col2 into: &macro_variable. separated by ', '
	from temp_variables;
	quit; 

	%put NOTE: macro &macro_variable. = &&&macro_variable.  ;
	
%mend m_create_table_insert_variables2;
