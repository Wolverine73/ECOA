
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    	m_sftp_put2
|
| LOCATION:  
| 
| PURPOSE:      sent IT rsa keys for qcpi2fp + EACTT and IT set up on servers and gave me ID ftpuser to SFTP
|
| REQUIREMENT:  
|
| LOGIC:  	sftp ftpuser@eaz1etlada1a  
|
| INPUT:        /anr_dep/communication_history/eoms_IT
|
| OUTPUT:       /appl/edw/incoming/campgn
|
+-----------------------------------------------------------------------------------------------
| HISTORY:  20201201 - DQI Team - Original  
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_sftp_put2(get_directory=, put_directory=, put_file1=, put_file2=, put_cleanup=NONE);

	/**----------------------------------------------------------------------------------------

		DEV:
		Servers:                            eaz1etlada1a - DEV
		Domain:                             PBM_ENT96_DEV
		Repository:                         PBM_ENT96_DEV_RS
		Integration:                        PBM_ENT96_DEV_IS
		Port:                               6001

		SIT - QA:
		Servers:                            eaz1etlaua1a - QA
		Domain:                             PBM_ENT96_SIT
		Repository:                         PBM_ENT96_SIT_RS
		Integration:                        PBM_ENT96_SIT_IS
		Port:                               6001

		STP:
		Servers:                            eaz1etlapta1a - PREPROD
		Domain:                             PBM_ENT96_STP
		Repository:                         PBM_ENT96_STP_RS
		Integration:                        PBM_ENT96_STP_IS
		Port:                               6001

		EA PREPROD:
		tstwebtransport.caremark.com 	    /ant591/IVR/incoming

		PROD:
		Servers:                            eaz1etlapa1d - PROD  <--------- IT will get data from EA PROD and transfer to IT PROD
		Domain:                             PBM_ENT96_PROD
		Repository:                         PBM_ENT96_PROD_RS
		Integration:                        PBM_ENT96_PROD_IS
		Port:                               6001
		
		EA PROD:
		webtransport.caremark.com 	    /ant591/IVR/incoming
		
		
	
	----------------------------------------------------------------------------------------**/                 
	
	%if &put_cleanup. ne NONE %then %do;
	
	%end;
	%else %do;

		%if &c_s_sftp_put_production. = Y %then %do;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - Create listing of files to SFTP
			+---------------------------------------------------------------------------------------------*/	
			%put NOTE: Put cpl and sfmc files from the production SFTP server - webtransport.caremark.com;

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
			set eoms_ftp_files;
			if index(name,'TCH') > 0 then sort_id=9;
			else if index(name,'TRIGGER') > 0 then sort_id=6;
			else if index(name,'RX') > 0 then sort_id=1;
			else sort_id=3;
			run;
			
			proc sort data = eoms_ftp_files;
			by sort_id;
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
			|  STEP - EA PROD SFTP server
			+---------------------------------------------------------------------------------------------*/

			%do g = 1 %to &total_sftp;
			
				data _null_; 
				rc = system("rm &get_etl_directory./put.txt"); 
				run;			
				
				data _null_;
				file "&get_etl_directory./put.txt";
				put "cd &put_etl_directory."; 
				put "put &&sftpname&g";
				put "exit";
				run; 
				
				%put NOTE: SFTP File - &&sftpname&g ;

				data _null_;
				rc = system("cd &get_etl_directory."); 
				rc = system("sftp &sftp_eoms_id.@webtransport.caremark.com < &get_etl_directory./put.txt"); 
				if rc = 0 then put "NOTE: SFTP file successfully from linux to sftp server = &&sftpname&g";
				else do; msg=sysmsg(); put rc= msg=; end;		
				run;				

			%end;


			/* SASDOC --------------------------------------------------------------------------------------
			|  STEP - chmod for files on IT TEST and QA SFTP server - EA PROD
			+---------------------------------------------------------------------------------------------*/		
			data _null_;
			file "&put_linux_directory./put.txt";
			put "cd &put_etl_directory.";
			put "chmod 777 *"; 
			put "exit";
			run; 

			data _null_;
			rc = system("cd &put_linux_directory."); 
			rc = system("sftp &sftp_eoms_id.@webtransport.caremark.com < &put_linux_directory./put.txt"); 
			if rc = 0 then put "NOTE: SFTP chmod command executed successfully";
			else do; msg=sysmsg(); put rc= msg=; end;		
			run;		

		%end;
		%else %if &c_s_sftp_put_production. = N %then %do;	

		%end;
		%else %do;
			%put NOTE: Do not put the Put cpl and sfmc files to the test ETL SFTP server ;
		%end;			

	%end;		
	
%mend m_sftp_put2; 