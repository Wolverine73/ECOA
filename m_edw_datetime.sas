%macro m_edw_datetime(data_element=, data_type=, default=1);

   %if &default. = 1 %then %do;
	   if length(&data_element.) > 5 and index(&data_element.,'-') = 0 and index(&data_element.,'/') = 0 and &data_element.*1 < 0 then do; /** sfmc = Feb  8 1973 12:00AM cpl = 19751230000000 **/
		y=scan(&data_element.,3,' ');
		m=scan(&data_element.,1,' ');
		d=scan(&data_element.,2,' ');
		&data_element.=upcase(trim(left(d))||trim(left(m))||trim(left(y)));
		&data_element.=put(input(&data_element.,anydtdte10.),yymmdd10.)||' 00.00.00';
	   end; 
	   else if length(&data_element.) > 5 and index(&data_element.,'-') = 0 and index(&data_element.,'/') = 0 and &data_element.*1 > 0 then do; /** example = 19730208 12:00AM **/
		y=substr(&data_element.,1,4);
		m=substr(&data_element.,5,2);
		d=substr(&data_element.,7,2);
		&data_element.=upcase(trim(left(d))||trim(left(m))||trim(left(y)));
		&data_element.=put(input(&data_element.,anydtdte10.),yymmdd10.)||' 00.00.00';
	   end;	
	   else if length(&data_element.) = 19 and count(&data_element.,'-') = 2 and count(&data_element.,':') = 2  
			and substr(&data_element.,11,1) = ' ' and substr(&data_element.,8,1) = '-' and substr(&data_element.,17,1) = ':' then do; /** example = 2022-02-28 10:04:43 **/
 		&data_element.=&data_element.;
	   end;		   
	   else if length(&data_element.) > 12 then &data_element.=put(input(scan(&data_element.,1,' '),anydtdte10.),yymmdd10.)||' 00.00.00';
	   else if length(&data_element.) > 5 then &data_element.=put(input(&data_element.,anydtdte10.),yymmdd10.)||' 00.00.00';

	   %if &data_type. = 1 %then %do;
		else &data_element.=put(input(put(today() - &execution_date. ,yymmdd10.),anydtdte10.),yymmdd10.)||' 00.00.00';
	   %end;
	   %if &data_type. = 2 %then %do;
		else &data_element.=put(input('31129999',anydtdte10.),yymmdd10.)||' 00.00.00';
	   %end;
	   %if &data_type. = 3 %then %do;
		else &data_element.='';
	   %end;
   %end;
   %if &default. = 2 %then %do;
	   if length(&data_element.) > 5 and index(&data_element.,'-') = 0 and index(&data_element.,'/') = 0 and &data_element.*1 < 0 then do; /** example = Feb  8 1973 12:00AM **/
		y=scan(&data_element.,3,' ');
		m=scan(&data_element.,1,' ');
		d=scan(&data_element.,2,' ');
		&data_element.=upcase(trim(left(d))||trim(left(m))||trim(left(y)));
		&data_element.=put(input(&data_element.,anydtdte10.),yymmdd10.);
	   end; 
	   else if length(&data_element.) > 5 and index(&data_element.,'-') = 0 and index(&data_element.,'/') = 0 and &data_element.*1 > 0 then do; /** example = Feb  8 1973 12:00AM **/
		y=substr(&data_element.,1,4);
		m=substr(&data_element.,5,2);
		d=substr(&data_element.,7,2);
		&data_element.=upcase(trim(left(d))||trim(left(m))||trim(left(y)));
		&data_element.=put(input(&data_element.,anydtdte10.),yymmdd10.);
	   end;				   
	   else if missing(&data_element.) then &data_element.=put(today() - &execution_date. ,yymmdd10.);
	   else if length(&data_element.) > 12 then &data_element.=put(input(scan(&data_element.,1,' '),anydtdte10.),yymmdd10.);
	   else if length(&data_element.) > 5 then &data_element.=put(input(&data_element.,anydtdte10.),yymmdd10.);
	   else &data_element.=put(input(put(today() - &execution_date. ,yymmdd10.),anydtdte10.),yymmdd10.);
   %end;   
   

%mend m_edw_datetime;