


-----------------------------------------------------------------------
--Sales trend --retail vs customer sales
-----------------------------------------------------------------------

select CalendarDate, sum(sellthruqty) as SellThruQTY
, sum(SellinQTY) as SellinQTY
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
group by 1
order by 1
;


-----------------------------------------------------------------------
--product trend by month
-----------------------------------------------------------------------
select yearmonth, DeviceName, sum(sellthruqty) as SellThruQTY
, sum(SellinQTY) as SellinQTY
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
group by 1, 2
order by 1, 2
;



-----------------------------------------------------------------------
--product trend by month, 
-----------------------------------------------------------------------
select yearmonth, DeviceName, pfamname, sum(sellthruqty) as SellThruQTY
, sum(SellinQTY) as SellinQTY
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
group by 1, 2, 3
order by 1, 2, 3
;

-----------------------------------------------------------------------
--product trend by month
-----------------------------------------------------------------------
select commercial_flag
, bundle_flag
, sum(SellinQTY) as SellinQTY
, sum(SellThruQTY) as SellThruQTY
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
inner join public.dimtbl_may5 d on f.tpid=d.tpid and f.productpartnbr=d.productpartnbr
group by 1,2
order by 1
;

-----------------------------------------------------------------------
--partner count by year, quarter, month
-----------------------------------------------------------------------

select calendarquarter
, count(distinct tpid) ttl_uniq_partner_cnt
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
group by 1
order by 1
;



-----------------------------------------------------------------------
--HDD sales by year, month
-----------------------------------------------------------------------

select yearmonth, hdd, sum(sellthruqty) as SellThruQTY
, sum(SellinQTY) as SellinQTY
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
inner join public.dimtbl_may5 d on f.tpid=d.tpid and f.productpartnbr=d.productpartnbr
group by 1, 2
order by 1, 2
;






-----------------------------------------------------------------------
--sales by country by year, month
-----------------------------------------------------------------------

select yearmonth, d.subsidiaryname
, sum(sellthruqty) as SellThruQTY
, sum(SellinQTY) as SellinQTY
--select distinct SellThruQTY::int, length(SellThruQTY)
from public.facttbl_may5 f 
inner join public.dimtbl_may5 d on f.tpid=d.tpid and f.productpartnbr=d.productpartnbr
group by 1, 2
order by 1, 2
;
