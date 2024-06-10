
/* HEADER---------------------------------------------------------------------------------------
| MACRO:    m_vendor_file_sftp_eoms
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
| HISTORY:  20190201 - Clinical Stars Team - Original (version 2019.02.01)
|
|
+----------------------------------------------------------------------------------------HEADER*/

%macro m_vendor_file_sftp_eoms(vendor_company=, vendor_count=, get_directory=, put_directory=, get_file=, put_file=);

	%if &put_file. = %then %let put_file=&get_file.;
	
	%put NOTE: vendor_company = &vendor_company. ;
	%put NOTE: get_file = &get_file. ;
	%put NOTE: put_file = &put_file. ;

	%if &c_s_dqi_production. = Y %then %do;
	
		%put NOTE: Vendor files will be transferred through the production SFTP server ;
		
		filename getfile "&get_directory./&get_file." recfm=f;

		filename putfile sftp "&put_directory./&put_file." recfm=f
		options="-oKexAlgorithms=diffie-hellman-group14-sha1"
		host="&sftp_id.@webtransport.caremark.com" wait_milliseconds=4000 debug;

		/* Use fcopy to make a copy of file for transfer */

		data _null_;
		   length msg $ 384;
		   rc=fcopy('getfile', 'putfile');
		   if rc=0 then
		      put 'NOTE: Copied clincal stars vendor file from linux to sftp server.';
		   else do;
		      msg=sysmsg();
		      put rc= msg=;
		   end;
		run;
	
	%end;
	%else %if &c_s_dqi_production. = N %then %do;
	
		%put NOTE: Vendor files will be transferred through the test SFTP server ;
		
		filename getfile "&get_directory./&get_file." recfm=f;

		filename putfile sftp "&put_directory./&put_file." recfm=f
		options="-oKexAlgorithms=diffie-hellman-group14-sha1"
		host="&sftp_id.@tstwebtransport.caremark.com" wait_milliseconds=4000 debug;

		/* Use fcopy to make a copy of file for transfer */

		data _null_;
		   length msg $ 384;
		   rc=fcopy('getfile', 'putfile');
		   if rc=0 then
		      put 'NOTE: Copied clincal stars vendor file from linux to sftp server.';
		   else do;
		      msg=sysmsg();
		      put rc= msg=;
		   end;
		run;		
	
	%end;
	%else %if &c_s_sftp_campaign. = TEST2 %then %do;
	
		%put NOTE: Vendor files will be transferred through the test manual SFTP server - incoming folder not outgoing ;
		
		data x;
		put_directory="&put_directory.";
		put_directory=tranwrd(put_directory,'outgoing','incoming');
		call symput('put_directory',trim(put_directory));
		put _all_;
		run;
		
		%put NOTE: put_directory = &put_directory. ;
		
		filename getfile "&get_directory./&get_file." recfm=f;

		filename putfile sftp "&put_directory./&put_file." recfm=f
		options="-oKexAlgorithms=diffie-hellman-group14-sha1"
		host="&sftp_id.@tstwebtransport.caremark.com" wait_milliseconds=4000 debug;

		/* Use fcopy to make a copy of file for transfer */

		data _null_;
		   length msg $ 384;
		   rc=fcopy('getfile', 'putfile');
		   if rc=0 then
		      put 'NOTE: Copied clincal stars vendor file from linux to sftp server.';
		   else do;
		      msg=sysmsg();
		      put rc= msg=;
		   end;
		run;		
	
	%end;	
	%else %do;
		%put NOTE: Vendor files will not be transferred through the SFTP server ;
	%end;			
	
	
	/* SASDOC --------------------------------------------------------------------------------------
	| Vendor Files History
	+-----------------------------------------------------------------------------------------SASDOC*/	
	%m_cs_process_control_files;
	
	
%mend m_vendor_file_sftp_eoms;

%**m_vendor_file_sftp_eoms(get_directory=&c_s_maindir., put_directory=/mnt014/IVR/outgoing, vendor_file=Alert_Sinfonia_SSI_LTF_&today_date..xlsx);
