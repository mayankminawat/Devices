/**************************
*      DIM Table.         *
***************************/

drop table if exists dimTbl_may5;
CREATE TABLE dimTbl_may5 
(DeviceName varchar
 ,PFAMName varchar
 ,SubsidiaryCode varchar
 ,SubsidiaryName varchar
 ,TPId varchar
 ,TPNamee varchar
 ,ProductPartNbr varchar
);


COPY dimTbl_may5 FROM '/workspace/Git_workspace/Devices/_raw_files/DimTbl_may5.csv' WITH (FORMAT csv);




/**************************
*      FACT Table.         *
***************************/

drop table if exists factTbl_may5;
CREATE TABLE factTbl_may5 
(
dateid varchar 
,CalendarDate varchar
,CalendarQuarter varchar
,YearMonth varchar
,SubsidiaryCode varchar
,SubsidiaryName varchar
,TPId varchar
,TPNamee varchar
,DeviceName varchar
,PFAMName varchar
,ProductPartNbr varchar
, SellThruQTY varchar
, SellinQTY varchar
)
;

COPY factTbl_may5 FROM '/workspace/Git_workspace/Devices/_raw_files/FactTbl_may5.csv' WITH (FORMAT csv);

