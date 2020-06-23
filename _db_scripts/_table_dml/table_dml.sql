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
case when pfamname ~ 'i[0-9]' is true then substring(pfamname from 'i[0-9]')
	   when pfamname ~ 'GBM[0-9]GB' is true then 'M'												
	  	 when pfamname ~ 'GB M[0-9]GB' is true then 'M'
		   when pfamname ~ 'GB M [0-9]GB' is true then 'M'
		    when pfamname ~ 'GBM [0-9]GB' is true then 'M'
		   	end as Processor
,case when (regexp_split_to_array(pfamname, '/'::text))[2] is not null then ((regexp_split_to_array(pfamname, '/'::text))[2])||'GB'
	  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GBM[0-9]GB' is true then substring(substring(pfamname from 'M[0-9]GB') from '[0-9]GB')
	  	  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GB M[0-9]GB' is true then substring(substring(pfamname from 'M[0-9]GB') from '[0-9]GB')
		  	when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GB M [0-9]GB' is true then substring(substring(pfamname from 'M [0-9]GB') from '[0-9]GB')
			  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ 'GBM [0-9]GB' is true then substring(substring(pfamname from 'M [0-9]GB') from '[0-9]GB')
				when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ ' [0-9]G' is true then substring(substring(pfamname from ' [0-9]G') from '[0-9]G') ||'B'
 				  when (regexp_split_to_array(pfamname, '/'::text))[2] is null and pfamname ~ ' [0-9][0-9]GB' is true then substring(substring(pfamname from ' [0-9][0-9]GB') from '[0-9][0-9]GB')
	  end RAM
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
	  ,case when pfamname ilike '%Comm%' then 1 else 0 end Commercial_Flag
	  ,case when pfamname ilike '%Bndl%' then 1 else 0 end Bundle_Flag 
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