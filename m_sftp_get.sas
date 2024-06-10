
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    m_sftp_get
|
| LOCATION:  
| 
| PURPOSE:   
|
| REQUIREMENT:  
|
| LOGIC:    
|
| INPUT:    
|
| OUTPUT:   
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201201 - DQI Team - Original  
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_sftp_get(get_directory=, put_directory=, get_file1=, get_file2=);


	%if &c_s_sftp_get_production. = Y %then %do;
	
		%put NOTE: Get cpl and sfmc files from the production SFTP server - webtransport;

		data _null_;
		file "&put_directory./get.txt";
		put "cd &get_directory.";
		put "get &get_file1.";
		put "get &get_file2.";
		put "exit";
		run; 

		data _null_;
		rc = system("cd &put_directory."); 
		rc = system("sftp &sftp_eoms_id.@webtransport.caremark.com < &put_directory./get.txt"); 
		if   _error_=0  then do;
		call symput('file_sftp_flg','1');
		end;
		else  call symput('file_sftp','0');
		run;
	
	%end;
	%else %if &c_s_sftp_get_production. = N %then %do;
	
		%put NOTE: Get cpl and sfmc files from the test SFTP server - tstwebtransport ;

		data _null_;
		file "&put_directory./get.txt";
		put "cd &get_directory.";
		put "get &get_file1.";
		put "get &get_file2.";
		put "exit";
		run; 

		data _null_;
		rc = system("cd &put_directory."); 
		rc = system("sftp &sftp_eoms_id.@tstwebtransport.caremark.com < &put_directory./get.txt"); 
		if   _error_=0  then do;
		call symput('file_sftp_flg','1');
		end;
		else  call symput('file_sftp','0');
		run;		
	
	%end;
	%else %do;
		%put NOTE: Do not get the Get cpl and sfmc files from the test SFTP server ;
	%end;			
	
	
%mend m_sftp_get;
%**m_sftp_get(get_directory=/ant591/IVR/incoming, put_directory=/mbr_engmt/reserve/SASControl/DEV/bss_test/000000_EA_TEST, get_file1=test.txt, get_file2=AETNA*);