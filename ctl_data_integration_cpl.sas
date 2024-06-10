

/* HEADER---------------------------------------------------------------------------------------
| MACRO:    ctl_data_integration_cpl
|
| LOCATION:  
| 
| PURPOSE:  
|
| LOGIC:    execute on Monday-Sunday @ 9:30 am CST 
|
| INPUT:     
|
| OUTPUT:   8 cpl dummy files since cpl is not sending the files some days 
|
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20240201 - CTT - Original (version 2024.02.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/


%let dir_out       		= %str(/anr_dep/communication_history/cpl_files);
%let sftp_eoms_id		= %str(uy2huh5a); 
%let c_s_dqi_production		= Y;  
%let c_s_sftp_put_production	= Y;

data x; 
x=today();  
c_s_file_date=left(trim(put(x,yymmddn8.)));
call symput('c_s_file_date',compress(c_s_file_date)); 
run;

data dqi_opporunity;
OPPTY_EVNT_ID=0;
run;

data dqi_opporunity;
set dqi_opporunity;
if OPPTY_EVNT_ID > 0;
run;


data dqi_disposition;
OPPTY_EVNT_ID=0;
run;

data dqi_disposition;
set dqi_disposition;
if OPPTY_EVNT_ID > 0;
run;

%macro m_create_cpl_files;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create ecoa opportunity files - 101234 = dqi
	|
	+---------------------------------------------------------------------------------------------*/
	%put NOTE: copy cpl template file to cpl opportunity file;
	data _null_; 
	x "cp /anr_dep/communication_history/cpl_files/cpl_opportunity.20240101101234 &dir_out./cpl_opportunity.&c_s_file_date.101234" 
	run;	

	%let opp_cnt=1;

	data _null_;
	format x $100. ;
	file "&dir_out./cpl_opportunity.&c_s_file_date.101234.CNTL";
	x="OD|cpl_opportunity.&c_s_file_date.101234|"||"&opp_cnt."||"|CPL|I||||";
	x=compress(x);
	put x ;
	run;

	data _null_;
	file "&dir_out./cpl_rx_opportunity.&c_s_file_date.101234";
	run;

	data _null_;
	file "&dir_out./cpl_rx_opportunity.&c_s_file_date.101234.CNTL";
	put "OD|cpl_rx_opportunity.&c_s_file_date.101234|0|CPL|I||||";
	run;


	/* SASDOC --------------------------------------------------------------------------------------
	|  STEP - create ecoa disposition files - 101234 = dqi
	|
	+---------------------------------------------------------------------------------------------*/	
	%put NOTE: copy cpl template file to cpl disposition file;
	data _null_; 
	x "cp /anr_dep/communication_history/cpl_files/cpl_disposition.20240101101234 &dir_out./cpl_disposition.&c_s_file_date.101234" 
	run;
	
	%let disp_cnt=1;

	data _null_;
	format x $100. ;
	file "&dir_out./cpl_disposition.&c_s_file_date.101234.CNTL";
	x="OD|cpl_disposition.&c_s_file_date.101234|"||"&disp_cnt."||"|CPL|I||||";
	x=compress(x);
	put x;
	run;

	data _null_;
	file "&dir_out./cpl_rx_disposition.&c_s_file_date.101234";
	run;

	data _null_;
	file "&dir_out./cpl_rx_disposition.&c_s_file_date.101234.CNTL";
	put "OD|cpl_rx_disposition.&c_s_file_date.101234|0|CPL|I||||";
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
		match2=find(name,"bss","i t",1); 	
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
		
		%put NOTE: total_sftp = &total_sftp.;


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

	
%mend m_create_cpl_files;
%m_create_cpl_files;
