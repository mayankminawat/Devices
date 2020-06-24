--Before any DML commands are executed to manipulate the table, need to clean the data in the table

-------------------------------------------------------------------------------------------
--Step 1: Determine PK in dim table
--Its a composite key which is a combination of tpid + productpartnbr
--While joining to the fact table, its important to join on this composite key only
--in order to avoid duplication of the data
-------------------------------------------------------------------------------------------
select tpid, productpartnbr, count(*)
--select *
from public.dimtbl_may5
group by 1, 2
having count(*)>1

-----------------------------------------------------------------------
--Cleansing the data
-----------------------------------------------------------------------
--1. Cleaning the sellthruqty in the fact table to remove all the brackets and commas
UPDATE public.facttbl_may5 SET sellthruqty = regexp_replace(sellthruqty, '\(|\)|\,', '', 'g');

--2. Cleaning the whitespaces
update public.facttbl_may5
set sellthruqty = replace(sellthruqty, ' ', '')
;
  
--3. Cleaning the lone dashes  
UPDATE public.facttbl_may5 
SET sellthruqty = 0 
where sellthruqty ='-' ;

--4. Changing the datatype of the col sellthruqty
ALTER TABLE  ALTER COLUMN  TYPE INT;

--5 Changing datatype on the column sellthruqty
ALTER TABLE public.facttbl_may5
ALTER COLUMN sellthruqty TYPE INT USING sellthruqty::INT;


--second column: sellinqty

--1. Cleaning the sellthruqty in the fact table to remove all the brackets and commas, making the search global meaning across the whole string
UPDATE public.facttbl_may5 SET sellinqty = regexp_replace(sellinqty, '\(|\)|\,', '', 'g');

--2. Cleaning the whitespaces
update public.facttbl_may5
set sellinqty = replace(sellinqty, ' ', '')
;
  
--3. Cleaning the lone dashes  
UPDATE public.facttbl_may5 
SET sellinqty = 0 
where sellinqty ='-' ;

--4 Changing datatype on the column sellthruqty
ALTER TABLE public.facttbl_may5
ALTER COLUMN sellinqty TYPE INT USING sellinqty::INT;





--Multiple ways of updating the existing dim table

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Method 1: 
--creating new table - dropping old table and renaming the new table to the old table. 
--Pros: Easy to execute, less code needs to be written, runtime is faster as no DML commands are required (ALTER, UPDATE), only DDL commands like 
--drop, create, rename is required.
--Cons: the DROP (DDL) commands will issue an exclusivetablelock on the tables resulting in deadlocks in case there are active queries running at the time of execution.
--Other downside is that since a transaction isolation block is not used in this script so it may result in loss of data if the transactions are killed in between DROP and CREATE.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------
--Query to produce the extra columns
--This query will be used to create a new table called public.dimtbl_may5_new
------------------------------------------------------------------------------


drop table if exists public.dimtbl_may5_new;

--using CTAS to create the new table - this will carry fwd the datatypes of the columns
CREATE TABLE public.dimtbl_may5_new as 
SELECT *,
--The below is a case statement to first check the format of the product family name, what the naming convention is using regex and then appending the processor value accordingly
case when pfamname ~ 'i[0-9]' is true then substring(pfamname from 'i[0-9]')
	   when pfamname ~ 'GBM[0-9]GB' is true then 'M'												
	  	 when pfamname ~ 'GB M[0-9]GB' is true then 'M'
		   when pfamname ~ 'GB M [0-9]GB' is true then 'M'
		    when pfamname ~ 'GBM [0-9]GB' is true then 'M'
		   	end as Processor 
--The below is a case statement to first split the product family name into multiple values using the forward slash as the separator
--then only consider the third element of the array in case it's present, if not then substring the relevant value
,case when (regexp_split_to_array(pfamname, '/'::text))[2] is not null then ((regexp_split_to_array(pfamname, '/'::text))[2])||'GB'
	  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GBM[0-9]GB' is true then substring(substring(pfamname from 'M[0-9]GB') from '[0-9]GB')
	  	  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GB M[0-9]GB' is true then substring(substring(pfamname from 'M[0-9]GB') from '[0-9]GB')
		  	when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GB M [0-9]GB' is true then substring(substring(pfamname from 'M [0-9]GB') from '[0-9]GB')
			  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GBM [0-9]GB' is true then substring(substring(pfamname from 'M [0-9]GB') from '[0-9]GB')
				when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ ' [0-9]G' is true then substring(substring(pfamname from ' [0-9]G') from '[0-9]G') ||'B'
 				  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ ' [0-9][0-9]GB' is true then substring(substring(pfamname from ' [0-9][0-9]GB') from '[0-9][0-9]GB')
	  end RAM
--The below is a case statement to first split the product family name into multiple values using the forward slash as the separator
--then only consider the fourth element of the array in case it's present, if not then substring the relevant value
,case when (regexp_split_to_array(pfamname, '/'::text))[3] is not null 
			then case when ((regexp_split_to_array(((regexp_split_to_array(pfamname, '/'::text))[3]), ' '::text))[1]) not like '%TB%' then ((regexp_split_to_array(((regexp_split_to_array(pfamname, '/'::text))[3]), ' '::text))[1])||'GB' 
							else ((regexp_split_to_array(((regexp_split_to_array(pfamname, '/'::text))[3]), ' '::text))[1]) 
								end 
	   when (regexp_split_to_array(pfamname, '/'::text))[3] is null  and pfamname ~ '[0-9]GBM[0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GBM[0-9]GB') from '[0-9][0-9][0-9]GB')												
	  	 when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB M[0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB M[0-9]GB') from '[0-9][0-9][0-9]GB')
		   when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB M [0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB M [0-9]GB') from '[0-9][0-9][0-9]GB')
		    when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GBM [0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GBM [0-9]GB') from '[0-9][0-9][0-9]GB')
			 when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB i[0-9] [0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB i[0-9] [0-9]GB') from '[0-9][0-9][0-9]GB')
			  when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB i[0-9] [0-9]G ' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB i[0-9] [0-9]G ') from '[0-9][0-9][0-9]GB')
			   when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ ' [0-9][0-9][0-9]GB' is true then substring(substring(pfamname from ' [0-9][0-9][0-9]GB') from '[0-9][0-9][0-9]GB')
			   when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ ' [0-9]TB ' is true then substring(substring(pfamname from ' [0-9]TB ') from '[0-9]TB ')
	  end HDD
	  ,case when pfamname ilike '%Comm%' then 1 else 0 end Commercial_Flag --for commerical devices
	  ,case when pfamname ilike '%Bndl%' then 1 else 0 end Bundle_Flag --for bundled devices products
FROM public.dimtbl_may5
;

--dropping the original table
drop table public.dimtbl_may5;

--renaming the newly created table to old table
ALTER TABLE public.dimtbl_may5_new RENAME TO dimtbl_may5;





--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Method 2: 
--Running all DML commands in a transaction block to produce isloation level with other queries. 
--No use of DDL commands like DROP, CREATE etc
--Pros:
--Will prevent any potential cases of deadlocks. 
--No possible loss of data in case queries get killed in between.
--Cons:
--Longer runtime, more costlier in the query plan to run UPDATE commands.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------
--Adding extra columns 
------------------------------------------------------------------------------
ALTER TABLE public.dimtbl_may5
ADD COLUMN processor char(2),
ADD COLUMN ram char(10),
ADD COLUMN hdd char(10),
ADD COLUMN commercial_flag boolean,
ADD COLUMN bundle_flag boolean;


------------------------------------------------------------------------------
--Updating these extra columns 
------------------------------------------------------------------------------
BEGIN;

--For the column comments please refer the Method 1
UPDATE public.dimtbl_may5
SET processor = case when pfamname ~ 'i[0-9]' is true then substring(pfamname from 'i[0-9]')
	   when pfamname ~ 'GBM[0-9]GB' is true then 'M'												
	  	 when pfamname ~ 'GB M[0-9]GB' is true then 'M'
		   when pfamname ~ 'GB M [0-9]GB' is true then 'M'
		    when pfamname ~ 'GBM [0-9]GB' is true then 'M'
		   	end,
    ram = case when (regexp_split_to_array(pfamname, '/'::text))[2] is not null then ((regexp_split_to_array(pfamname, '/'::text))[2])||'GB'
	  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GBM[0-9]GB' is true then substring(substring(pfamname from 'M[0-9]GB') from '[0-9]GB')
	  	  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GB M[0-9]GB' is true then substring(substring(pfamname from 'M[0-9]GB') from '[0-9]GB')
		  	when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GB M [0-9]GB' is true then substring(substring(pfamname from 'M [0-9]GB') from '[0-9]GB')
			  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GBM [0-9]GB' is true then substring(substring(pfamname from 'M [0-9]GB') from '[0-9]GB')
				when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ ' [0-9]G' is true then substring(substring(pfamname from ' [0-9]G') from '[0-9]G') ||'B'
 				  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ ' [0-9][0-9]GB' is true then substring(substring(pfamname from ' [0-9][0-9]GB') from '[0-9][0-9]GB')
	  end,
	hdd =case when (regexp_split_to_array(pfamname, '/'::text))[3] is not null 
			then case when ((regexp_split_to_array(((regexp_split_to_array(pfamname, '/'::text))[3]), ' '::text))[1]) not like '%TB%' then ((regexp_split_to_array(((regexp_split_to_array(pfamname, '/'::text))[3]), ' '::text))[1])||'GB' 
							else ((regexp_split_to_array(((regexp_split_to_array(pfamname, '/'::text))[3]), ' '::text))[1]) 
								end 
	   when (regexp_split_to_array(pfamname, '/'::text))[3] is null  and pfamname ~ '[0-9]GBM[0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GBM[0-9]GB') from '[0-9][0-9][0-9]GB')												
	  	 when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB M[0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB M[0-9]GB') from '[0-9][0-9][0-9]GB')
		   when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB M [0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB M [0-9]GB') from '[0-9][0-9][0-9]GB')
		    when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GBM [0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GBM [0-9]GB') from '[0-9][0-9][0-9]GB')
			 when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB i[0-9] [0-9]GB' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB i[0-9] [0-9]GB') from '[0-9][0-9][0-9]GB')
			  when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ '[0-9]GB i[0-9] [0-9]G ' is true then substring(substring(pfamname from '[0-9][0-9][0-9]GB i[0-9] [0-9]G ') from '[0-9][0-9][0-9]GB')
			   when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ ' [0-9][0-9][0-9]GB' is true then substring(substring(pfamname from ' [0-9][0-9][0-9]GB') from '[0-9][0-9][0-9]GB')
			   when (regexp_split_to_array(pfamname, '/'::text))[3] is null and pfamname ~ ' [0-9]TB ' is true then substring(substring(pfamname from ' [0-9]TB ') from '[0-9]TB ')
	  end,
	commercial_flag=case when pfamname ilike '%Comm%' then 1 else 0 end,
	bundle_flag=case when pfamname ilike '%Bndl%' then 1 else 0 end 
;

END;