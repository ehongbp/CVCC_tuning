----- SAP-MANDANTEN

-- mandant_id FirmId  FirmDesc
-- 2	        1	    bonprix Deutschland
-- 32	        11	    bonprix Österreich
-- 45	        23	    bonprix Finnland

##### Österreichische Daten gehen in der Positions bis 2015-04-21
##### der LTV wird bis FS16 berechnet, damit die Verifizierung auf HW22 gemacht werden kann und dort der LTV bis HW15 gebildet werden kann


DECLARE Start DATE DEFAULT CURRENT_DATE();

DECLARE FirmID_decl int64 DEFAULT 1;
DECLARE MandantID_decl int64 DEFAULT 2;

DECLARE S1A DATE DEFAULT DATE_SUB(Start, INTERVAL 182 day);
DECLARE S2A DATE DEFAULT DATE_SUB(Start, INTERVAL 365 day);
DECLARE S3A DATE DEFAULT DATE_SUB(Start, INTERVAL 547 day);
DECLARE S4A DATE DEFAULT DATE_SUB(Start, INTERVAL 730 day);
DECLARE S5A DATE DEFAULT DATE_SUB(Start, INTERVAL 912 day);
DECLARE S6A DATE DEFAULT DATE_SUB(Start, INTERVAL 1095 day);
DECLARE S7A DATE DEFAULT DATE_SUB(Start, INTERVAL 1277 day);
DECLARE S8A DATE DEFAULT DATE_SUB(Start, INTERVAL 1460 day);

DECLARE S1E DATE DEFAULT DATE_SUB(Start, INTERVAL 1 day);
DECLARE S2E DATE DEFAULT DATE_SUB(Start, INTERVAL 183 day);
DECLARE S3E DATE DEFAULT DATE_SUB(Start, INTERVAL 366 day);
DECLARE S4E DATE DEFAULT DATE_SUB(Start, INTERVAL 548 day);
DECLARE S5E DATE DEFAULT DATE_SUB(Start, INTERVAL 731 day);
DECLARE S6E DATE DEFAULT DATE_SUB(Start, INTERVAL 913 day);
DECLARE S7E DATE DEFAULT DATE_SUB(Start, INTERVAL 1096 day);
DECLARE S8E DATE DEFAULT DATE_SUB(Start, INTERVAL 1278 day);

DECLARE Q1A DATE DEFAULT DATE_SUB(Start, INTERVAL 92 day);
DECLARE Q2A DATE DEFAULT DATE_SUB(Start, INTERVAL 182 day);

DECLARE Q1E DATE DEFAULT DATE_SUB(Start, INTERVAL 1 day);
DECLARE Q2E DATE DEFAULT DATE_SUB(Start, INTERVAL 93 day);
DECLARE EarliestDate DATE DEFAULT DATE_SUB(Start, INTERVAL 4567 day);

/*
--- Auskommentiert, weil dieser Vorgang sowieso schon im BQ-Lauf des CVCC-Modells passiert.
delete from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` where ReportingDate between S2A and S1E;
insert into `bpde-prd-core-dwh.sde.mkr_cvcc_positions` 
select * 
from `bpde-prd-core-dwh.vde.positions`
where date(ReportingDate) between S2A and S1E
;
*/

------------------- Aktive Kunden
CREATE OR REPLACE TABLE `bpde-prd-core-dwh.sde.aktivekunden_int_de_roll` 
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(current_timestamp, INTERVAL 12 Hour)) AS (
  WITH
    tmp AS (
    SELECT
      bpid,
      accountdate,
      SUM(KPIValueVK) AS nf
    FROM
      `bpde-prd-core-dwh.sde.mkr_cvcc_positions`
    WHERE
      rectype=1
      AND accountdate <= DATE_SUB(Start, INTERVAL 1461 Day)
      AND DATE(reportingdate) >= DATE_SUB(Start, INTERVAL 1461 Day)
    GROUP BY
      1,
      2 )
  SELECT
    b.bpid,
    b.accountDate
  FROM
    `bpde-prd-core-dwh.vde.customeroverview` b
  LEFT OUTER JOIN
    tmp
  ON
    b.bpid=tmp.bpid
    AND tmp.accountDate=b.accountDate
  WHERE
    b.accountdate <= DATE_SUB(Start, INTERVAL 1461 Day)
    AND COALESCE(tmp.nf,0)=0 );

------------------------------ Eckwerte

create or replace table `bpde-prd-core-dwh.sde.eckwerte_gesamt_int_de_roll`
options(expiration_timestamp = TIMESTAMP_ADD(current_timestamp, INTERVAL 12 Hour))
AS (
SELECT bpid, accountDate
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S1A AND S1E AND RecType = 1 THEN KPIValueVK END) bbwS1
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S2A AND S2E AND RecType = 1 THEN KPIValueVK END) bbwS2
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S3A AND S3E AND RecType = 1 THEN KPIValueVK END) bbwS3
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S4A AND S4E AND RecType = 1 THEN KPIValueVK END) bbwS4
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S5A AND S5E AND RecType = 1 THEN KPIValueVK END) bbwS5
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S6A AND S6E AND RecType = 1 THEN KPIValueVK END) bbwS6
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S7A AND S7E AND RecType = 1 THEN KPIValueVK END) bbwS7
      , SUM(CASE WHEN DATE(ReportingDate) BETWEEN S8A AND S8E AND RecType = 1 THEN KPIValueVK END) bbwS8
FROM `bpde-prd-core-dwh.sde.mkr_cvcc_positions`
WHERE DATE(ReportingDate) BETWEEN S8A AND S1E
    AND rectype=1
GROUP BY 1,2);



------------------ Dexbase-Table zusammenbauen

CREATE or Replace table `bpde-prd-core-dwh.sde.dexbase_int_de_roll` as (

Select a.bpid, a.accountDate, 
cast((date_diff(Start, cast((`bonprix-pps-bigquery-prod`.functions.dec_bdate(MandantID_decl, a.birthdate)) as date), day)/365) as int64) as Age,
cast((date_diff(S1E, a.accountdate, day)/365) as int64) as YearsOfRelationship,
SalutationID,
CASE
when a.accountDate > S1A then 2
when a.accountDate <= S1A
      and coalesce(bbwS1,0) > 0 then 3
when a.accountDate <= S1A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) > 0 then 4      
when a.accountDate <= S2A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) > 0	then 5
when a.accountDate <= S2A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0
      and coalesce(bbwS4,0) >0	then 6
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) >0	then 6
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) >0	then 6
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) <=0 
      and coalesce(bbwS7,0) >0	then 6
when a.accountDate <= S2A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) <=0 
      and coalesce(bbwS7,0) <=0 
      and coalesce(bbwS8,0) >0	then 6
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) <=0 
      and coalesce(bbwS7,0) <=0 
      and coalesce(bbwS8,0) <=0 then 6
end TargetGroupIndexRoll, 
CASE
when a.accountDate > S1A then NULL
when a.accountDate <= S1A
      and coalesce(bbwS1,0) > 0 then NULL
when a.accountDate <= S1A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) > 0 then NULL   
when a.accountDate <= S2A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) > 0	then NULL
when a.accountDate <= S2A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0
      and coalesce(bbwS4,0) >0	then 63
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) >0	then 64
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) >0	then 65
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) <=0 
      and coalesce(bbwS7,0) >0	then 66
when a.accountDate <= S2A
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) <=0 
      and coalesce(bbwS7,0) <=0 
      and coalesce(bbwS8,0) >0	then 67
when a.accountDate <= S2A 
      and coalesce(bbwS1,0) <=0 
      and coalesce(bbwS2,0) <=0 
      and coalesce(bbwS3,0) <=0 
      and coalesce(bbwS4,0) <=0 
      and coalesce(bbwS5,0) <=0 
      and coalesce(bbwS6,0) <=0 
      and coalesce(bbwS7,0) <=0 
      and coalesce(bbwS8,0) <=0 then 68
end TargetGroupIndexRollOld_org,
CASE 
when a.accountDate between S2A and S2E and coalesce(bbwS2,0) > 0  then 1
else 0
end NK3
from `bpde-prd-core-dwh.vde.customeroverview` a 
left outer join `bpde-prd-core-dwh.sde.eckwerte_gesamt_int_de_roll` eckw ON a.bpid=eckw.bpid and a.accountDate=eckw.accountDate	
left outer join `bpde-prd-core-dwh.sde.aktivekunden_int_de_roll` actCust ON actCust.bpid=a.bpid and actCust.accountDate=a.accountDate
left outer join `bpde-prd-core-dwh.vde.addressoverview` ad on ad.bpid=a.bpid and ad.accountDate=a.accountDate
left outer join (select bpid
                  from `bpde-prd-core-dwh.vde.businesspartnerexclusion`
                  where ExclusionCd in (2,4) and DecisionFg=1 and date(ValidFrom)<=S1E and date(ValidTo)>S1E
                  group by 1) ex on ex.bpid=a.bpid 

where a.BPTypeID in ('Z001', 'Z019')  
      and a.accountDate is not null 
      and coalesce(actCust.bpid,0)=0
      and coalesce(ex.bpid,0)=0
);


---------- Order-Quotes


create or replace table `bpde-prd-core-dwh.sde.order_quotes_int_de_roll` as ( 
with a as (
select a.bpid,a.accountdate
      , count(distinct(case when date(reportingdate) between S1A and S1E then orderid end)) as Orders_S1
      , count(distinct(case when date(reportingdate) between S1A and S1E and orderprofit>0 then orderid end)) as OrdersWithProfit_S1
      , count(distinct(case when date(reportingdate) between S2A and S2E then orderid end)) as Orders_S2
      , count(distinct(case when date(reportingdate) between S2A and S2E and orderprofit>0 then orderid end)) as OrdersWithProfit_S2
      , count(distinct(case when date(reportingdate) between S3A and S3E then orderid end)) as Orders_S3
      , count(distinct(case when date(reportingdate) between S3A and S3E and orderprofit>0 then orderid end)) as OrdersWithProfit_S3
      , count(distinct(case when date(reportingdate) between S4A and S4E then orderid end)) as Orders_S4
      , count(distinct(case when date(reportingdate) between S4A and S4E and orderprofit>0 then orderid end)) as OrdersWithProfit_S4
      , count(distinct(case when date(reportingdate) between S5A and S5E then orderid end)) as Orders_S5
      , count(distinct(case when date(reportingdate) between S5A and S5E and orderprofit>0 then orderid end)) as OrdersWithProfit_S5
      , count(distinct(case when date(reportingdate) between S6A and S6E then orderid end)) as Orders_S6
      , count(distinct(case when date(reportingdate) between S6A and S6E and orderprofit>0 then orderid end)) as OrdersWithProfit_S6
      , count(distinct(case when date(reportingdate) between S7A and S7E then orderid end)) as Orders_S7
      , count(distinct(case when date(reportingdate) between S7A and S7E and orderprofit>0 then orderid end)) as OrdersWithProfit_S7
      , count(distinct(case when date(reportingdate) between S8A and S8E then orderid end)) as Orders_S8
      , count(distinct(case when date(reportingdate) between S8A and S8E and orderprofit>0 then orderid end)) as OrdersWithProfit_S8

      , count(distinct(case when date(reportingdate) between S3A and S1E then orderid end)) as Orders_S123
      , count(distinct(case when date(reportingdate) between S3A and S1E and orderprofit>0 then orderid end)) as OrdersWithProfit_S123

      , count(distinct(case when date(reportingdate) between S5A and S4E then orderid end)) as Orders_S45
      , count(distinct(case when date(reportingdate) between S5A and S4E and orderprofit>0 then orderid end)) as OrdersWithProfit_S45

      , count(distinct(case when date(reportingdate) between S5A and S1E then orderid end)) as Orders_S1_5
      , count(distinct(case when date(reportingdate) between S5A and S1E and orderprofit>0 then orderid end)) as OrdersWithProfit_S1_5
      
from `bpde-prd-core-dwh.sde.dexbase_int_de_roll` a
join (select bpid, reportingdate, orderid, OrderProfit_CRM as orderprofit
                from `bpde-prd-core-dwh.vimc.order_profit`
                where firmid=FirmID_decl and reportingdate>=S8A+1 and OrderProfit_CRM <> 0) b on a.bpid=b.bpid
group by 1,2)


select bpid, accountdate
      , Orders_S1, OrdersWithProfit_S1, round((case when Orders_S1>0 then (OrdersWithProfit_S1/Orders_S1) else 0 end),3) as PositiveOrderRate_S1
      , Orders_S2, OrdersWithProfit_S2, round((case when Orders_S2>0 then (OrdersWithProfit_S2/Orders_S2) else 0 end),3) as PositiveOrderRate_S2
      , Orders_S3, OrdersWithProfit_S3, round((case when Orders_S3>0 then (OrdersWithProfit_S3/Orders_S3) else 0 end),3) as PositiveOrderRate_S3
      , Orders_S4, OrdersWithProfit_S4, round((case when Orders_S4>0 then (OrdersWithProfit_S4/Orders_S4) else 0 end),3) as PositiveOrderRate_S4
      , Orders_S5, OrdersWithProfit_S5, round((case when Orders_S5>0 then (OrdersWithProfit_S5/Orders_S5) else 0 end),3) as PositiveOrderRate_S5
      , Orders_S123, OrdersWithProfit_S123, round((case when Orders_S123>0 then (OrdersWithProfit_S123/Orders_S123) else 0 end),3) as PositiveOrderRate_S123
      , Orders_S45, OrdersWithProfit_S45, round((case when Orders_S45>0 then (OrdersWithProfit_S45/Orders_S45) else 0 end),3) as PositiveOrderRate_S45
      , Orders_S1_5, OrdersWithProfit_S1_5, round((case when Orders_S1_5>0 then (OrdersWithProfit_S1_5/Orders_S1_5) else 0 end),3) as PositiveOrderRate_S1_5
from a
);


---- Newsletter

create or replace table `bpde-prd-core-dwh.sde.tmp_NL_int_de_roll` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select bpid
       , sum(case when datum between S1A and S1E then send end) as send_S1
       , sum(case when datum between S1A and S1E then clicked end) clicked_S1
       , sum(case when datum between S1A and S1E then opened end) opened_S1
       
       , sum(case when datum between S2A and S2E then send end) as send_S2
       , sum(case when datum between S2A and S2E then clicked end) clicked_S2
       , sum(case when datum between S2A and S2E then opened end) opened_S2
       
       , sum(case when datum between S3A and S3E then send end) as send_S3
       , sum(case when datum between S3A and S3E then clicked end) clicked_S3
       , sum(case when datum between S3A and S3E then opened end) opened_S3
       
       , sum(case when datum between S4A and S4E then send end) as send_S4
       , sum(case when datum between S4A and S4E then clicked end) clicked_S4
       , sum(case when datum between S4A and S4E then opened end) opened_S4
       
       , sum(case when datum between S5A and S5E then send end) as send_S5
       , sum(case when datum between S5A and S5E then clicked end) clicked_S5
       , sum(case when datum between S5A and S5E then opened end) opened_S5
       
       , sum(case when datum between S6A and S6E then send end) as send_S6
       , sum(case when datum between S6A and S6E then clicked end) clicked_S6
       , sum(case when datum between S6A and S6E then opened end) opened_S6
from `bpde-prd-core-dwh.sde.clicks_int`
where firmid=FirmID_decl and datum between S6A and S1E
group by 1);


create or replace table `bpde-prd-core-dwh.sde.tmp_permission_int_de_roll` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select bpid, accountdate, push_perm, nl_perm
from `bpde-prd-core-dwh.vde.v_nl_push_perm_hist`
where valid_from<=S1E and valid_to>S1E
);



create or replace table `bpde-prd-core-dwh.sde.tmp_profit_int_de_roll`
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select bpid
       , sum(case when date(reportingdate) between S1A and S1E then OrderProfit_CRM end) as orderprofit_S1
       , sum(case when date(reportingdate) between S2A and S2E then OrderProfit_CRM end) as orderprofit_S2  
       , sum(case when date(reportingdate) between S3A and S3E then OrderProfit_CRM end) as orderprofit_S3
       , sum(case when date(reportingdate) between S4A and S4E then OrderProfit_CRM end) as orderprofit_S4  
       , sum(case when date(reportingdate) between S5A and S5E then OrderProfit_CRM end) as orderprofit_S5
       , sum(case when date(reportingdate) between S6A and S6E then OrderProfit_CRM end) as orderprofit_S6
from `bpde-prd-core-dwh.vimc.order_profit`
where firmid=FirmID_decl and date(reportingdate)>=S6A
group by 1
);



create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S1` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S1,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S1,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S1,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S1,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S1,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S1,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S1,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S1,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S1,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S1, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S1,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S1,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S1,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S1,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S1,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S1,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S1,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S1,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S1, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S1,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S1,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S1,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S1,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S1,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S1,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S1,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S1,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S1,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') 
or PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S1				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S1
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S1
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S1A and S1E
            and date(orderdate) between S1A and S1E
group by 1,2
) ;

create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S2` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S2,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S2,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S2,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S2,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S2,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S2,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S2,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S2,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S2,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S2, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S2,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S2,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S2,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S2,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S2,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S2,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S2,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S2,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S2, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S2,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S2,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S2,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S2,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S2,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S2,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S2,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S2,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72', '74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S2,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') 
or PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S2				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S2
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S2
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S2A and S2E+35
            and date(orderdate) between S2A and S2E
group by 1,2
) ;


create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S3` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S3,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S3,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S3,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S3,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S3,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S3,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S3,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S3,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S3,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S3, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S3,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S3,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S3,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S3,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S3,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S3,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S3,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S3,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S3, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S3,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S3,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S3,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S3,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S3,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S3,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S3,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S3,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S3,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') 
or PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S3				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S3
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S3
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S3A and S3E+35
            and date(orderdate) between S3A and S3E
group by 1,2
) ;


create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S4` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S4,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S4,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S4,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S4,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S4,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S4,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S4,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S4,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S4,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S4, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S4,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S4,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S4,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S4,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S4,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S4,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S4,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S4,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S4, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S4,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S4,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S4,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S4,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S4,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S4,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S4,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S4,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S4,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') 
or PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S4				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S4
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S4
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S4A and S4E+35
            and date(orderdate) between S4A and S4E
group by 1,2
) ;


create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S5` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S5,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S5,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S5,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S5,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S5,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S5,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S5,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S5,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S5,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S5, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S5,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S5,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S5,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S5,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S5,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S5,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S5,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S5,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S5, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S5,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S5,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S5,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S5,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S5,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S5,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S5,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S5,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S5,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') 
or PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S5				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S5
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S5
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S5A and S5E+35
            and date(orderdate) between S5A and S5E
group by 1,2
) ;


create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S6` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S6,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S6,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S6,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S6,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S6,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S6,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S6,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S6,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S6,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S6, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S6,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S6,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S6,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S6,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S6,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S6,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S6,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S6,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S6, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S6,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S6,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S6,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S6,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S6,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S6,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S6,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S6,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S6,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') 
or PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S6				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S6
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S6
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S6A and S6E+35
            and date(orderdate) between S6A and S6E
group by 1,2
) ;

create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S7` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S7,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S7,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S7,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S7,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S7,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S7,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S7,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S7,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S7,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S7, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S7,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S7,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S7,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S7,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S7,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S7,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S7,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S7,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S7, -- Online-Bestellweg
	
     sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S7,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S7,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S7,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S7,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S7,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S7,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S7,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S7,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S7,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') or 
                  PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
			and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S7				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S7
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S7
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S7A and S7E+35
            and date(orderdate) between S7A and S7E
group by 1,2
) ;


create or replace table `bpde-prd-core-dwh.sde.tmp_pos_S8` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      sum (case when recType=91 then cast(KPIValueQty as int64) end) Nabs_ges_S8,			-- Nabs Gesamt
	sum (case when recType=91 then cast(KPIValueVK as int64) end) Nums_ges_S8,				-- Nums Gesamt
      sum (case when recType=3 then cast(KPIValueQty as int64) end) StornoMenge_ges_S8,			-- Storno Menge Gesamt
	sum (case when recType=3 then cast(KPIValueVK as int64) end) StornoWert_ges_S8,				-- Storno Wert Gesamt
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueQty as int64) end) Nabs_online_S8,		-- Nabs Online-Shop
	sum (case when recType=91 and CatalogTypeID in (20,60) then cast(KPIValueVK as int64)  end) Nums_online_S8,		-- Nums OnlineShop
      sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueQty as int64) end) Nabs_katbez_S8,		-- Nabs Katbez
	sum (case when recType=91 and CatalogTypeID not in (20,60) then cast(KPIValueVK as int64)  end) Nums_katbez_S8,		-- Nums Katbez
      count(distinct(case when recType=1 and CatalogTypeID not in (20,60) then pos.OrderID end)) Orders_Katbez_S8,	-- Bestellungen mit Katalogpromotion     
	max(case when recType=1 and upper(OrderChannelCd) like '%T%' then 1 else 0 end) telbweg_S8, --Bestellweg Telefon

      sum (case when recType=5 then cast(KPIValueQty as int64) end) brabs_ges_S8,		-- Brabs Gesamt
	sum (case when recType=5 then cast(KPIValueVK as int64) end) brums_ges_S8,		-- Brums Gesamt
	sum (case when recType=6 then cast(KPIValueVK as int64) end) Retwert_S8,		-- Retwert 
	sum (case when recType=6 then cast(KPIValueQty as int64) end) RetMenge_S8,		-- RetKPIValueQty 
	sum (case when recType=1 then cast(KPIValueVK as int64) end) NF_ges_S8,			-- Nachfragewert
	sum (case when recType=1 then cast(KPIValueQty as int64) end) NFMenge_ges_S8,		-- NachfrageKPIValueQty
	count(distinct(case when recType=1 then pos.OrderID end)) Orders_ges_S8,		-- Bestellungen
	count(distinct(case when recType=1 and CatalogTypeID in (20,60) then pos.OrderID end)) Orders_OLSArt_S8,	-- Bestellungen mit OL-Shop-Artikeln
      max(case when recType=1 and upper(OrderChannelCd) like '%I%' then 1 else 0 end) onlbweg_S8, -- Online-Bestellweg
	
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,1) = '4' 
                  then cast(KPIValueQty as int64) end) TexBM_ges_S8,				      -- Tex-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '25', '21') 
                  then cast(KPIValueQty as int64) end) SchuhBM_ges_S8,					-- Schuh-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('15','16','17','18') 
                  then cast(KPIValueQty as int64) end) HakaBM_ges_S8,					-- Haka-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('22','23','24','26') 
                  then cast(KPIValueQty as int64) end) KikaBM_ges_S8,					-- Kika-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('20','33','35','36','37') 
                  then cast(KPIValueQty as int64) end) WaeBM_ges_S8,					-- Waesche-BestellKPIValueQty
      sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('65', '66') 
                  then KPIValueQty end) BadeBM_ges_S8,				                  -- Bade-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ( '67','73','34') 
                  then KPIValueQty end) SportBM_ges_S8,							-- Sport-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('76') 
                  then KPIValueQty end) AccBM_ges_S8,							      -- Accessoires-BestellKPIValueQty
	sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88') 
                  then cast(KPIValueQty as int64)  end) DobBM_ges_S8,	                        -- DOB-BestellKPIValueQty	
	sum(case when recType=1 and (substr(PromotionPurchaseCd,1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	'61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'73',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	'85',	'86',	'87',	'88','34','20','35', '36','37','67') or 
                  PromotionPurchaseCd in ('331','334','335','336','651','652','653','654','656','657','658','661','662','663','664','666','667')) 
			and ItemSize in ('48', '50', '52', '54', '56', '58', '60', '100', '105', '110', '115', 'XL', 'XXL', 'XXXL') 
                  then cast(KPIValueQty as int64)  end) RiesigBM_ges_S8				-- Riesig-BestellKPIValueQty
      , count(distinct(case when recType=701 then pos.OrderID end)) Nutzung_VKB_S8
      , sum(case when recType=1 and substr(PromotionPurchaseCd,1,2) in ('53') then cast(KPIValueQty as int64)  end) Umstand_BM_S8
from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between S8A and S8E+35
            and date(orderdate) between S8A and S8E
group by 1,2
) ;


create or replace table `bpde-prd-core-dwh.sde.tmp_pos_ltv` 
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select pos.bpid, pos.accountDate, 
      count(distinct(case when recType=1 then pos.OrderID end)) LifeTimeOrders,		
      sum (case when recType=5 then cast(KPIValueVK as int64) end) LifetimeGrossSalesAmt,
      sum (case when recType=5 then cast(KPIValueQty as int64) end) LifetimeGrossSalesQty,
      sum (case when recType=6 then cast(KPIValueQty as int64) end) LifetimeReturnAmt,
      sum (case when recType=6 then cast(KPIValueVK as int64) end) LifetimeReturnQty,		 
	sum (case when recType=91 then cast(KPIValueVK as int64) end) LifetimeNetSalesAmt,		
      sum (case when recType=91 then cast(KPIValueQty as int64) end) LifetimeNetSalesQty,	
      count(distinct(case when recType=1 and upper(OrderChannelCd) ='I' then pos.OrderID end)) LifetimeOrdersOnline,
	count(distinct(case when recType=1 and upper(OrderChannelCd) in ('T', 'S') then pos.OrderID end)) LifetimeOrdersOffline

from `bpde-prd-core-dwh.sde.mkr_cvcc_positions` pos
	where date(reportingdate) between EarliestDate and S1E
            and date(orderdate) between EarliestDate and S1E
group by 1,2
) ;




create or replace table `bpde-prd-core-dwh.sde.datamart_transactions_int_de_roll` as (
select a.bpid
, a.accountDate	
, a.Age
, case when a.accountDate>=S1E then null else Nabs_ges_S1 end as Nabs_ges_S1
, case when a.accountDate>=S1E then null else Nums_ges_S1 end as Nums_ges_S1
, case when a.accountDate>=S1E then null else Orders_katbez_S1 end as Orders_katbez_S1
, case when a.accountDate>=S1E then null else telbweg_S1 end as telbweg_S1

, case when a.accountDate>=S1E then null else brabs_ges_S1 end as brabs_ges_S1
, case when a.accountDate>=S1E then null else brums_ges_S1 end as brums_ges_S1
, case when a.accountDate>=S1E then null else Retwert_S1 end as Retwert_S1
, case when a.accountDate>=S1E then null else RetMenge_S1 end as RetMenge_S1
, case when a.accountDate>=S1E then null 
       when (RetMenge_S1+Nabs_ges_S1)>0 then (coalesce(RetMenge_S1,0)/(RetMenge_S1+Nabs_ges_S1))
       else NULL
       end as RSQ_S1
, case when a.accountDate>=S1E then null 
       when (RetWert_S1+Nums_ges_S1)>0 then (coalesce(RetWert_S1,0)/(RetWert_S1+Nums_ges_S1))
       else NULL
       end as RWQ_S1
, case when a.accountDate>=S1E then null else NF_ges_S1 end as NF_ges_S1
, case when a.accountDate>=S1E then null else NFMenge_ges_S1 end as NFMenge_ges_S1
, case when a.accountDate>=S1E then null else Orders_ges_S1 end as Orders_ges_S1
, case when a.accountDate>=S1E then null else Orders_OLSArt_S1 end as Orders_OLSArt_S1
, case when a.accountDate>=S1E then null else onlbweg_S1 end as onlbweg_S1
, case when a.accountDate>=S1E then null else TexBM_ges_S1 end as TexBM_ges_S1
, case when a.accountDate>=S1E then null else SchuhBM_ges_S1 end as SchuhBM_ges_S1
, case when a.accountDate>=S1E then null else HakaBM_ges_S1 end as HakaBM_ges_S1
, case when a.accountDate>=S1E then null else KikaBM_ges_S1 end as KikaBM_ges_S1
, case when a.accountDate>=S1E then null else WaeBM_ges_S1 end as WaeBM_ges_S1
, case when a.accountDate>=S1E then null else DobBM_ges_S1 end as DobBM_ges_S1
, case when a.accountDate>=S1E then null else RiesigBM_ges_S1 end as RiesigBM_ges_S1
, case when a.accountDate>=S1E then null else Nutzung_VKB_S1 end as Nutzung_VKB_S1
, case when a.accountDate>=S1E then null else send_S1 end as send_S1
, case when a.accountDate>=S1E then null else clicked_S1 end as clicked_S1
, case when a.accountDate>=S1E then null else opened_S1 end as opened_S1
, case when a.accountDate>=S1E then null else Umstand_BM_S1 end as Umstand_BM_S1
, case when a.accountDate>=S1E then null else op.orderprofit_S1 end as orderprofit_S1

, case when a.accountDate>=S2E then null else Nabs_ges_S2 end as Nabs_ges_S2
, case when a.accountDate>=S2E then null else Nums_ges_S2 end as Nums_ges_S2
, case when a.accountDate>=S2E then null else Orders_katbez_S2 end as Orders_katbez_S2
, case when a.accountDate>=S2E then null else telbweg_S2 end as telbweg_S2
, case when a.accountDate>=S2E then null else brabs_ges_S2 end as brabs_ges_S2
, case when a.accountDate>=S2E then null else brums_ges_S2 end as brums_ges_S2
, case when a.accountDate>=S2E then null else Retwert_S2 end as Retwert_S2
, case when a.accountDate>=S2E then null else RetMenge_S2 end as RetMenge_S2
, case when a.accountDate>=S2E then null 
       when (RetMenge_S2+Nabs_ges_S2)>0 then (coalesce(RetMenge_S2,0)/(RetMenge_S2+Nabs_ges_S2))
       else NULL
       end as RSQ_S2
, case when a.accountDate>=S2E then null 
       when (RetWert_S2+Nums_ges_S2)>0 then (coalesce(RetWert_S2,0)/(RetWert_S2+Nums_ges_S2))
       else NULL
       end as RWQ_S2

, case when a.accountDate>=S2E then null else NF_ges_S2 end as NF_ges_S2
, case when a.accountDate>=S2E then null else NFMenge_ges_S2 end as NFMenge_ges_S2
, case when a.accountDate>=S2E then null else Orders_ges_S2 end as Orders_ges_S2
, case when a.accountDate>=S2E then null else Orders_OLSArt_S2 end as Orders_OLSArt_S2
, case when a.accountDate>=S2E then null else onlbweg_S2 end as onlbweg_S2
, case when a.accountDate>=S2E then null else TexBM_ges_S2 end as TexBM_ges_S2
, case when a.accountDate>=S2E then null else SchuhBM_ges_S2 end as SchuhBM_ges_S2
, case when a.accountDate>=S2E then null else HakaBM_ges_S2 end as HakaBM_ges_S2
, case when a.accountDate>=S2E then null else KikaBM_ges_S2 end as KikaBM_ges_S2
, case when a.accountDate>=S2E then null else WaeBM_ges_S2 end as WaeBM_ges_S2
, case when a.accountDate>=S2E then null else DobBM_ges_S2 end as DobBM_ges_S2
, case when a.accountDate>=S2E then null else RiesigBM_ges_S2 end as RiesigBM_ges_S2
, case when a.accountDate>=S2E then null else Nutzung_VKB_S2 end as Nutzung_VKB_S2
, case when a.accountDate>=S2E then null else send_S2 end as send_S2
, case when a.accountDate>=S2E then null else clicked_S2 end as clicked_S2
, case when a.accountDate>=S2E then null else opened_S2 end as opened_S2
, case when a.accountDate>=S2E then null else Umstand_BM_S2 end as Umstand_BM_S2
, case when a.accountDate>=S2E then null else op.orderprofit_S2 end as orderprofit_S2

, case when a.accountDate>=S3E then null else Nabs_ges_S3 end as Nabs_ges_S3
, case when a.accountDate>=S3E then null else Nums_ges_S3 end as Nums_ges_S3
, case when a.accountDate>=S3E then null else Orders_katbez_S3 end as Orders_katbez_S3
, case when a.accountDate>=S3E then null else telbweg_S3 end as telbweg_S3
, case when a.accountDate>=S3E then null else brabs_ges_S3 end as brabs_ges_S3
, case when a.accountDate>=S3E then null else brums_ges_S3 end as brums_ges_S3
, case when a.accountDate>=S3E then null else Retwert_S3 end as Retwert_S3
, case when a.accountDate>=S3E then null else RetMenge_S3 end as RetMenge_S3
, case when a.accountDate>=S3E then null 
       when (RetMenge_S3+Nabs_ges_S3)>0 then (coalesce(RetMenge_S3,0)/(RetMenge_S3+Nabs_ges_S3))
       else NULL
       end as RSQ_S3
, case when a.accountDate>=S3E then null 
       when (RetWert_S3+Nums_ges_S3)>0 then (coalesce(RetWert_S3,0)/(RetWert_S3+Nums_ges_S3))
       else NULL
       end as RWQ_S3

, case when a.accountDate>=S3E then null else NF_ges_S3 end as NF_ges_S3
, case when a.accountDate>=S3E then null else NFMenge_ges_S3 end as NFMenge_ges_S3
, case when a.accountDate>=S3E then null else Orders_ges_S3 end as Orders_ges_S3
, case when a.accountDate>=S3E then null else Orders_OLSArt_S3 end as Orders_OLSArt_S3
, case when a.accountDate>=S3E then null else onlbweg_S3 end as onlbweg_S3
, case when a.accountDate>=S3E then null else TexBM_ges_S3 end as TexBM_ges_S3
, case when a.accountDate>=S3E then null else SchuhBM_ges_S3 end as SchuhBM_ges_S3
, case when a.accountDate>=S3E then null else HakaBM_ges_S3 end as HakaBM_ges_S3
, case when a.accountDate>=S3E then null else KikaBM_ges_S3 end as KikaBM_ges_S3
, case when a.accountDate>=S3E then null else WaeBM_ges_S3 end as WaeBM_ges_S3
, case when a.accountDate>=S3E then null else DobBM_ges_S3 end as DobBM_ges_S3
, case when a.accountDate>=S3E then null else RiesigBM_ges_S3 end as RiesigBM_ges_S3
, case when a.accountDate>=S3E then null else Nutzung_VKB_S3 end as Nutzung_VKB_S3
, case when a.accountDate>=S3E then null else send_S3 end as send_S3
, case when a.accountDate>=S3E then null else clicked_S3 end as clicked_S3
, case when a.accountDate>=S3E then null else opened_S3 end as opened_S3
, case when a.accountDate>=S3E then null else Umstand_BM_S3 end as Umstand_BM_S3
, case when a.accountDate>=S3E then null else op.orderprofit_S3 end as orderprofit_S3

, case when a.accountDate>=S4E then null else Nabs_ges_S4 end as Nabs_ges_S4
, case when a.accountDate>=S4E then null else Nums_ges_S4 end as Nums_ges_S4
, case when a.accountDate>=S4E then null else Orders_katbez_S4 end as Orders_katbez_S4
, case when a.accountDate>=S4E then null else telbweg_S4 end as telbweg_S4
, case when a.accountDate>=S4E then null else brabs_ges_S4 end as brabs_ges_S4
, case when a.accountDate>=S4E then null else brums_ges_S4 end as brums_ges_S4
, case when a.accountDate>=S4E then null else Retwert_S4 end as Retwert_S4
, case when a.accountDate>=S4E then null else RetMenge_S4 end as RetMenge_S4
, case when a.accountDate>=S4E then null 
       when (RetMenge_S4+Nabs_ges_S4)>0 then (coalesce(RetMenge_S4,0)/(RetMenge_S4+Nabs_ges_S4))
       else NULL
       end as RSQ_S4
, case when a.accountDate>=S4E then null 
       when (RetWert_S4+Nums_ges_S4)>0 then (coalesce(RetWert_S4,0)/(RetWert_S4+Nums_ges_S4))
       else NULL
       end as RWQ_S4

, case when a.accountDate>=S4E then null else NF_ges_S4 end as NF_ges_S4
, case when a.accountDate>=S4E then null else NFMenge_ges_S4 end as NFMenge_ges_S4
, case when a.accountDate>=S4E then null else Orders_ges_S4 end as Orders_ges_S4
, case when a.accountDate>=S4E then null else Orders_OLSArt_S4 end as Orders_OLSArt_S4
, case when a.accountDate>=S4E then null else onlbweg_S4 end as onlbweg_S4
, case when a.accountDate>=S4E then null else TexBM_ges_S4 end as TexBM_ges_S4
, case when a.accountDate>=S4E then null else SchuhBM_ges_S4 end as SchuhBM_ges_S4
, case when a.accountDate>=S4E then null else HakaBM_ges_S4 end as HakaBM_ges_S4
, case when a.accountDate>=S4E then null else KikaBM_ges_S4 end as KikaBM_ges_S4
, case when a.accountDate>=S4E then null else WaeBM_ges_S4 end as WaeBM_ges_S4
, case when a.accountDate>=S4E then null else DobBM_ges_S4 end as DobBM_ges_S4
, case when a.accountDate>=S4E then null else RiesigBM_ges_S4 end as RiesigBM_ges_S4
, case when a.accountDate>=S4E then null else Nutzung_VKB_S4 end as Nutzung_VKB_S4
, case when a.accountDate>=S4E then null else send_S4 end as send_S4
, case when a.accountDate>=S4E then null else clicked_S4 end as clicked_S4
, case when a.accountDate>=S4E then null else opened_S4 end as opened_S4
, case when a.accountDate>=S4E then null else Umstand_BM_S4 end as Umstand_BM_S4
, case when a.accountDate>=S4E then null else op.orderprofit_S4 end as orderprofit_S4

, case when a.accountDate>=S5E then null else Nabs_ges_S5 end as Nabs_ges_S5
, case when a.accountDate>=S5E then null else Nums_ges_S5 end as Nums_ges_S5
, case when a.accountDate>=S5E then null else Orders_katbez_S5 end as Orders_katbez_S5
, case when a.accountDate>=S5E then null else telbweg_S5 end as telbweg_S5
, case when a.accountDate>=S5E then null else brabs_ges_S5 end as brabs_ges_S5
, case when a.accountDate>=S5E then null else brums_ges_S5 end as brums_ges_S5
, case when a.accountDate>=S5E then null else Retwert_S5 end as Retwert_S5
, case when a.accountDate>=S5E then null else RetMenge_S5 end as RetMenge_S5
, case when a.accountDate>=S5E then null 
       when (RetMenge_S5+Nabs_ges_S5)>0 then (coalesce(RetMenge_S5,0)/(RetMenge_S5+Nabs_ges_S5))
       else NULL
       end as RSQ_S5
, case when a.accountDate>=S5E then null 
       when (RetWert_S5+Nums_ges_S5)>0 then (coalesce(RetWert_S5,0)/(RetWert_S5+Nums_ges_S5))
       else NULL
       end as RWQ_S5
, case when a.accountDate>=S5E then null else NF_ges_S5 end as NF_ges_S5
, case when a.accountDate>=S5E then null else NFMenge_ges_S5 end as NFMenge_ges_S5
, case when a.accountDate>=S5E then null else Orders_ges_S5 end as Orders_ges_S5
, case when a.accountDate>=S5E then null else Orders_OLSArt_S5 end as Orders_OLSArt_S5
, case when a.accountDate>=S5E then null else onlbweg_S5 end as onlbweg_S5
, case when a.accountDate>=S5E then null else TexBM_ges_S5 end as TexBM_ges_S5
, case when a.accountDate>=S5E then null else SchuhBM_ges_S5 end as SchuhBM_ges_S5
, case when a.accountDate>=S5E then null else HakaBM_ges_S5 end as HakaBM_ges_S5
, case when a.accountDate>=S5E then null else KikaBM_ges_S5 end as KikaBM_ges_S5
, case when a.accountDate>=S5E then null else WaeBM_ges_S5 end as WaeBM_ges_S5
, case when a.accountDate>=S5E then null else DobBM_ges_S5 end as DobBM_ges_S5
, case when a.accountDate>=S5E then null else RiesigBM_ges_S5 end as RiesigBM_ges_S5
, case when a.accountDate>=S5E then null else Nutzung_VKB_S5 end as Nutzung_VKB_S5
, case when a.accountDate>=S5E then null else send_S5 end as send_S5
, case when a.accountDate>=S5E then null else clicked_S5 end as clicked_S5
, case when a.accountDate>=S5E then null else opened_S5 end as opened_S5
, case when a.accountDate>=S5E then null else Umstand_BM_S5 end as Umstand_BM_S5

, case when a.accountDate>=S6E then null else Nabs_ges_S6 end as Nabs_ges_S6
, case when a.accountDate>=S6E then null else Nums_ges_S6 end as Nums_ges_S6
, case when a.accountDate>=S6E then null else Orders_katbez_S6 end as Orders_katbez_S6
, case when a.accountDate>=S6E then null else telbweg_S6 end as telbweg_S6
, case when a.accountDate>=S6E then null else brabs_ges_S6 end as brabs_ges_S6
, case when a.accountDate>=S6E then null else brums_ges_S6 end as brums_ges_S6
, case when a.accountDate>=S6E then null else Retwert_S6 end as Retwert_S6
, case when a.accountDate>=S6E then null else RetMenge_S6 end as RetMenge_S6
, case when a.accountDate>=S6E then null 
       when (RetMenge_S6+Nabs_ges_S6)>0 then (coalesce(RetMenge_S6,0)/(RetMenge_S6+Nabs_ges_S6))
       else NULL
       end as RSQ_S6
, case when a.accountDate>=S6E then null 
       when (RetWert_S6+Nums_ges_S6)>0 then (coalesce(RetWert_S6,0)/(RetWert_S6+Nums_ges_S6))
       else NULL
       end as RWQ_S6
, case when a.accountDate>=S6E then null else NF_ges_S6 end as NF_ges_S6
, case when a.accountDate>=S6E then null else NFMenge_ges_S6 end as NFMenge_ges_S6
, case when a.accountDate>=S6E then null else Orders_ges_S6 end as Orders_ges_S6
, case when a.accountDate>=S6E then null else Orders_OLSArt_S6 end as Orders_OLSArt_S6
, case when a.accountDate>=S6E then null else onlbweg_S6 end as onlbweg_S6
, case when a.accountDate>=S6E then null else TexBM_ges_S6 end as TexBM_ges_S6
, case when a.accountDate>=S6E then null else SchuhBM_ges_S6 end as SchuhBM_ges_S6
, case when a.accountDate>=S6E then null else HakaBM_ges_S6 end as HakaBM_ges_S6
, case when a.accountDate>=S6E then null else KikaBM_ges_S6 end as KikaBM_ges_S6
, case when a.accountDate>=S6E then null else WaeBM_ges_S6 end as WaeBM_ges_S6
, case when a.accountDate>=S6E then null else DobBM_ges_S6 end as DobBM_ges_S6
, case when a.accountDate>=S6E then null else RiesigBM_ges_S6 end as RiesigBM_ges_S6
, case when a.accountDate>=S6E then null else Nutzung_VKB_S6 end as Nutzung_VKB_S6
, case when a.accountDate>=S6E then null else send_S6 end as send_S6
, case when a.accountDate>=S6E then null else clicked_S6 end as clicked_S6
, case when a.accountDate>=S6E then null else opened_S6 end as opened_S6
, case when a.accountDate>=S6E then null else Umstand_BM_S6 end as Umstand_BM_S6

, case when a.accountDate>=S7E then null else Nabs_ges_S7 end as Nabs_ges_S7
, case when a.accountDate>=S7E then null else Nums_ges_S7 end as Nums_ges_S7
, case when a.accountDate>=S7E then null else Orders_katbez_S7 end as Orders_katbez_S7
, case when a.accountDate>=S7E then null else telbweg_S7 end as telbweg_S7
, case when a.accountDate>=S7E then null else brabs_ges_S7 end as brabs_ges_S7
, case when a.accountDate>=S7E then null else brums_ges_S7 end as brums_ges_S7
, case when a.accountDate>=S7E then null else Retwert_S7 end as Retwert_S7
, case when a.accountDate>=S7E then null else RetMenge_S7 end as RetMenge_S7
, case when a.accountDate>=S7E then null 
       when (RetMenge_S7+Nabs_ges_S7)>0 then (coalesce(RetMenge_S7,0)/(RetMenge_S7+Nabs_ges_S7))
       else NULL
       end as RSQ_S7
, case when a.accountDate>=S7E then null 
       when (RetWert_S7+Nums_ges_S7)>0 then (coalesce(RetWert_S7,0)/(RetWert_S7+Nums_ges_S7))
       else NULL
       end as RWQ_S7
, case when a.accountDate>=S7E then null else NF_ges_S7 end as NF_ges_S7
, case when a.accountDate>=S7E then null else NFMenge_ges_S7 end as NFMenge_ges_S7
, case when a.accountDate>=S7E then null else Orders_ges_S7 end as Orders_ges_S7
, case when a.accountDate>=S7E then null else Orders_OLSArt_S7 end as Orders_OLSArt_S7
, case when a.accountDate>=S7E then null else onlbweg_S7 end as onlbweg_S7
, case when a.accountDate>=S7E then null else TexBM_ges_S7 end as TexBM_ges_S7
, case when a.accountDate>=S7E then null else SchuhBM_ges_S7 end as SchuhBM_ges_S7
, case when a.accountDate>=S7E then null else HakaBM_ges_S7 end as HakaBM_ges_S7
, case when a.accountDate>=S7E then null else KikaBM_ges_S7 end as KikaBM_ges_S7
, case when a.accountDate>=S7E then null else WaeBM_ges_S7 end as WaeBM_ges_S7
, case when a.accountDate>=S7E then null else DobBM_ges_S7 end as DobBM_ges_S7
, case when a.accountDate>=S7E then null else RiesigBM_ges_S7 end as RiesigBM_ges_S7
, case when a.accountDate>=S7E then null else Nutzung_VKB_S7 end as Nutzung_VKB_S7
, case when a.accountDate>=S7E then null else Umstand_BM_S7 end as Umstand_BM_S7

, case when a.accountDate>=S8E then null else Nabs_ges_S8 end as Nabs_ges_S8
, case when a.accountDate>=S8E then null else Nums_ges_S8 end as Nums_ges_S8
, case when a.accountDate>=S8E then null else Orders_katbez_S8 end as Orders_katbez_S8
, case when a.accountDate>=S8E then null else telbweg_S8 end as telbweg_S8
, case when a.accountDate>=S8E then null else brabs_ges_S8 end as brabs_ges_S8
, case when a.accountDate>=S8E then null else brums_ges_S8 end as brums_ges_S8
, case when a.accountDate>=S8E then null else Retwert_S8 end as Retwert_S8
, case when a.accountDate>=S8E then null else RetMenge_S8 end as RetMenge_S8
, case when a.accountDate>=S8E then null 
       when (RetMenge_S8+Nabs_ges_S8)>0 then (coalesce(RetMenge_S8,0)/(RetMenge_S8+Nabs_ges_S8))
       else NULL
       end as RSQ_S8
, case when a.accountDate>=S8E then null 
       when (RetWert_S8+Nums_ges_S8)>0 then (coalesce(RetWert_S8,0)/(RetWert_S8+Nums_ges_S8))
       else NULL
       end as RWQ_S8
, case when a.accountDate>=S8E then null else NF_ges_S8 end as NF_ges_S8
, case when a.accountDate>=S8E then null else NFMenge_ges_S8 end as NFMenge_ges_S8
, case when a.accountDate>=S8E then null else Orders_ges_S8 end as Orders_ges_S8
, case when a.accountDate>=S8E then null else Orders_OLSArt_S8 end as Orders_OLSArt_S8
, case when a.accountDate>=S8E then null else onlbweg_S8 end as onlbweg_S8
, case when a.accountDate>=S8E then null else TexBM_ges_S8 end as TexBM_ges_S8
, case when a.accountDate>=S8E then null else SchuhBM_ges_S8 end as SchuhBM_ges_S8
, case when a.accountDate>=S8E then null else HakaBM_ges_S8 end as HakaBM_ges_S8
, case when a.accountDate>=S8E then null else KikaBM_ges_S8 end as KikaBM_ges_S8
, case when a.accountDate>=S8E then null else WaeBM_ges_S8 end as WaeBM_ges_S8
, case when a.accountDate>=S8E then null else DobBM_ges_S8 end as DobBM_ges_S8
, case when a.accountDate>=S8E then null else RiesigBM_ges_S8 end as RiesigBM_ges_S8
, case when a.accountDate>=S8E then null else Nutzung_VKB_S8 end as Nutzung_VKB_S8
, case when a.accountDate>=S8E then null else Umstand_BM_S8 end as Umstand_BM_S8

, LifeTimeOrders
, LifetimeGrossSalesAmt
, LifetimeGrossSalesQty
, LifetimeReturnAmt
, LifetimeReturnQty		 
, LifetimeNetSalesAmt		
, LifetimeNetSalesQty	
, LifetimeOrdersOnline
, LifetimeOrdersOffline

, push_perm
, nl_perm

, case when a.accountDate>=S1E then null 
      else    (coalesce(sort1.Tex,0)
              + coalesce(sort1.Schuhe,0)
              + coalesce(sort1.Haka,0)
              + coalesce(sort1.Kika,0)
              + coalesce(sort1.Wae,0)
              + coalesce(sort1.Bade,0)
              + coalesce(sort1.Sport,0)
              + coalesce(sort1.Acc,0)
              + coalesce(sort1.Dob,0)) end AnzSorti_S1
, case when a.accountDate>=S2E then null 
      else    (coalesce(sort2.Tex,0)
              + coalesce(sort2.Schuhe,0)
              + coalesce(sort2.Haka,0)
              + coalesce(sort2.Kika,0)
              + coalesce(sort2.Wae,0)
              + coalesce(sort2.Bade,0)
              + coalesce(sort2.Sport,0)
              + coalesce(sort2.Acc,0)
              + coalesce(sort2.Dob,0)) end AnzSorti_S2
, case when a.accountDate>=S3E then null 
      else    (coalesce(sort3.Tex,0)
              + coalesce(sort3.Schuhe,0)
              + coalesce(sort3.Haka,0)
              + coalesce(sort3.Kika,0)
              + coalesce(sort3.Wae,0)
              + coalesce(sort3.Bade,0)
              + coalesce(sort3.Sport,0)
              + coalesce(sort3.Acc,0)
              + coalesce(sort3.Dob,0)) end AnzSorti_S3
, case when a.accountDate>=S4E then null 
      else    (coalesce(sort4.Tex,0)
              + coalesce(sort4.Schuhe,0)
              + coalesce(sort4.Haka,0)
              + coalesce(sort4.Kika,0)
              + coalesce(sort4.Wae,0)
              + coalesce(sort4.Bade,0)
              + coalesce(sort4.Sport,0)
              + coalesce(sort4.Acc,0)
              + coalesce(sort4.Dob,0)) end AnzSorti_S4
, case when a.accountDate>=S5E then null 
      else    (coalesce(sort5.Tex,0)
              + coalesce(sort5.Schuhe,0)
              + coalesce(sort5.Haka,0)
              + coalesce(sort5.Kika,0)
              + coalesce(sort5.Wae,0)
              + coalesce(sort5.Bade,0)
              + coalesce(sort5.Sport,0)
              + coalesce(sort5.Acc,0)
              + coalesce(sort5.Dob,0)) end AnzSorti_S5
, case when a.accountDate>=S6E then null 
      else    (coalesce(sort6.Tex,0)
              + coalesce(sort6.Schuhe,0)
              + coalesce(sort6.Haka,0)
              + coalesce(sort6.Kika,0)
              + coalesce(sort6.Wae,0)
              + coalesce(sort6.Bade,0)
              + coalesce(sort6.Sport,0)
              + coalesce(sort6.Acc,0)
              + coalesce(sort6.Dob,0)) end AnzSorti_S6
, (coalesce(akti_S1,0) + 
      coalesce(akti_S2,0) + 
      coalesce(akti_S3,0) + 
      coalesce(akti_S4,0) + 
      coalesce(akti_S5,0)) as AktivKennzeichen
, (coalesce(aktiOL_S1,0) + 
      coalesce(aktiOL_S2,0) + 
      coalesce(aktiOL_S3,0) + 
      coalesce(aktiOL_S4,0) + 
      coalesce(aktiOL_S5,0)) as AktivOLKennzeichen

from `bpde-prd-core-dwh.sde.dexbase_int_de_roll` a

left outer join `bpde-prd-core-dwh.sde.tmp_pos_S1` s1 on a.bpid=s1.bpid and a.accountDate=s1.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S2` s2 on a.bpid=s2.bpid and a.accountDate=s2.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S3` s3 on a.bpid=s3.bpid and a.accountDate=s3.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S4` s4 on a.bpid=s4.bpid and a.accountDate=s4.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S5` s5 on a.bpid=s5.bpid and a.accountDate=s5.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S6` s6 on a.bpid=s6.bpid and a.accountDate=s6.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S7` s7 on a.bpid=s7.bpid and a.accountDate=s7.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_S8` s8 on a.bpid=s8.bpid and a.accountDate=s8.accountDate
left outer join `bpde-prd-core-dwh.sde.tmp_pos_ltv` ltv on a.bpid=ltv.bpid and a.accountDate=ltv.accountDate

left outer join `bpde-prd-core-dwh.sde.tmp_NL_int_de_roll` nl on a.bpid=nl.bpid
left outer join `bpde-prd-core-dwh.sde.tmp_profit_int_de_roll` op on a.bpid=op.bpid
left outer join `bpde-prd-core-dwh.sde.tmp_permission_int_de_roll` perm on a.bpid=perm.bpid
left outer join (select bpid
                        , case when TexBM_ges_S1>0 then 1 end Tex
                        , case when SchuhBM_ges_S1>0 then 1 end Schuhe
                        , case when HakaBM_ges_S1>0 then 1 end Haka
                        , case when KikaBM_ges_S1>0 then 1 end Kika
                        , case when WaeBM_ges_S1>0 then 1 end Wae
                        , case when BadeBM_ges_S1>0 then 1 end Bade
                        , case when SportBM_ges_S1>0 then 1 end Sport
                        , case when AccBM_ges_S1>0 then 1 end Acc
                        , case when DobBM_ges_S1>0 then 1 end Dob         
                  from `bpde-prd-core-dwh.sde.tmp_pos_S1`) sort1 on sort1.bpid=a.bpid
left outer join (select bpid
                        , case when TexBM_ges_S2>0 then 1 end Tex
                        , case when SchuhBM_ges_S2>0 then 1 end Schuhe
                        , case when HakaBM_ges_S2>0 then 1 end Haka
                        , case when KikaBM_ges_S2>0 then 1 end Kika
                        , case when WaeBM_ges_S2>0 then 1 end Wae
                        , case when BadeBM_ges_S2>0 then 1 end Bade
                        , case when SportBM_ges_S2>0 then 1 end Sport
                        , case when AccBM_ges_S2>0 then 1 end Acc
                        , case when DobBM_ges_S2>0 then 1 end Dob         
                  from `bpde-prd-core-dwh.sde.tmp_pos_S2`) sort2 on sort2.bpid=a.bpid
left outer join (select bpid
                        , case when TexBM_ges_S3>0 then 1 end Tex
                        , case when SchuhBM_ges_S3>0 then 1 end Schuhe
                        , case when HakaBM_ges_S3>0 then 1 end Haka
                        , case when KikaBM_ges_S3>0 then 1 end Kika
                        , case when WaeBM_ges_S3>0 then 1 end Wae
                        , case when BadeBM_ges_S3>0 then 1 end Bade
                        , case when SportBM_ges_S3>0 then 1 end Sport
                        , case when AccBM_ges_S3>0 then 1 end Acc
                        , case when DobBM_ges_S3>0 then 1 end Dob         
                  from `bpde-prd-core-dwh.sde.tmp_pos_S3`) sort3 on sort3.bpid=a.bpid
left outer join (select bpid
                        , case when TexBM_ges_S4>0 then 1 end Tex
                        , case when SchuhBM_ges_S4>0 then 1 end Schuhe
                        , case when HakaBM_ges_S4>0 then 1 end Haka
                        , case when KikaBM_ges_S4>0 then 1 end Kika
                        , case when WaeBM_ges_S4>0 then 1 end Wae
                        , case when BadeBM_ges_S4>0 then 1 end Bade
                        , case when SportBM_ges_S4>0 then 1 end Sport
                        , case when AccBM_ges_S4>0 then 1 end Acc
                        , case when DobBM_ges_S4>0 then 1 end Dob         
                  from `bpde-prd-core-dwh.sde.tmp_pos_S4`) sort4 on sort4.bpid=a.bpid
left outer join (select bpid
                        , case when TexBM_ges_S5>0 then 1 end Tex
                        , case when SchuhBM_ges_S5>0 then 1 end Schuhe
                        , case when HakaBM_ges_S5>0 then 1 end Haka
                        , case when KikaBM_ges_S5>0 then 1 end Kika
                        , case when WaeBM_ges_S5>0 then 1 end Wae
                        , case when BadeBM_ges_S5>0 then 1 end Bade
                        , case when SportBM_ges_S5>0 then 1 end Sport
                        , case when AccBM_ges_S5>0 then 1 end Acc
                        , case when DobBM_ges_S5>0 then 1 end Dob         
                  from `bpde-prd-core-dwh.sde.tmp_pos_S5`) sort5 on sort5.bpid=a.bpid
left outer join (select bpid
                        , case when TexBM_ges_S6>0 then 1 end Tex
                        , case when SchuhBM_ges_S6>0 then 1 end Schuhe
                        , case when HakaBM_ges_S6>0 then 1 end Haka
                        , case when KikaBM_ges_S6>0 then 1 end Kika
                        , case when WaeBM_ges_S6>0 then 1 end Wae
                        , case when BadeBM_ges_S6>0 then 1 end Bade
                        , case when SportBM_ges_S6>0 then 1 end Sport
                        , case when AccBM_ges_S6>0 then 1 end Acc
                        , case when DobBM_ges_S6>0 then 1 end Dob         
                  from `bpde-prd-core-dwh.sde.tmp_pos_S6`) sort6 on sort6.bpid=a.bpid
left outer join (select a.bpid, a.accountdate
                  , case when coalesce(Nums_Ges_S1,0)>0 then 32 end akti_S1
                  , case when coalesce(Nums_Ges_S2,0)>0 then 16 end akti_S2
                  , case when coalesce(Nums_Ges_S3,0)>0 then 8 end akti_S3
                  , case when coalesce(Nums_Ges_S4,0)>0 then 4 end akti_S4
                  , case when coalesce(Nums_Ges_S5,0)>0 then 2 end akti_S5 
                  , case when coalesce(Nums_online_S1,0)>0 then 32 end aktiOL_S1
                  , case when coalesce(Nums_online_S2,0)>0 then 16 end aktiOL_S2
                  , case when coalesce(Nums_online_S3,0)>0 then 8 end aktiOL_S3
                  , case when coalesce(Nums_online_S4,0)>0 then 4 end aktiOL_S4
                  , case when coalesce(Nums_online_S5,0)>0 then 2 end aktiOL_S5 
                  from `bpde-prd-core-dwh.sde.dexbase_int_de_roll` a
                  left outer join `bpde-prd-core-dwh.sde.tmp_pos_S1` s1 on s1.bpid=a.bpid 
                                    and s1.accountDate=a.accountdate
                  left outer join `bpde-prd-core-dwh.sde.tmp_pos_S2` s2 on s2.bpid=a.bpid 
                                    and s2.accountDate=a.accountdate
                  left outer join `bpde-prd-core-dwh.sde.tmp_pos_S3` s3 on s3.bpid=a.bpid 
                                    and s3.accountDate=a.accountdate
                  left outer join `bpde-prd-core-dwh.sde.tmp_pos_S4` s4 on s4.bpid=a.bpid 
                                    and s4.accountDate=a.accountdate
                  left outer join `bpde-prd-core-dwh.sde.tmp_pos_S5` s5 on s5.bpid=a.bpid 
                                    and s5.accountDate=a.accountdate
                  ) as akti on akti.bpid=a.bpid and akti.accountdate=a.accountDate
);


---- Webshop-Datamart

create or replace table `bpde-prd-core-dwh.sde.dm_webshop_S1`                                                     
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select ktnr_glob as bpid
		, AVG(session_length_sek)                                                                           avg_session_length_S1
		, sum(page_visits)                                                                                  pagevisits_S1
		, sum(page_visits_ads)                                                                              pagevisits_ads_S1
		, count(distinct (device))                                                                          device_unique_S1
		, count(distinct(betr.wkorbid))                                                                     Sessions_S1
            , count(distinct(case when kauf_bool=0 and page_visits_ads>0  then betr.wkorbid end))               KVB_S1
		, count(distinct(case when kauf_bool=0 and  wkorb_fill_bool=1 then betr.wkorbid end))               WKA_S1
		, sum(case when device=0 then 1 end)                                                                desktopAnz_S1
		, sum(case when device=1 and shopapi_client_type = '1' then 1 end)                                  mobileBrowser_S1
            , sum(case when device=1 and shopapi_client_type = '3' then 1 end)                                  mobileApp_S1
		, sum(case when device=2 then 1 end)                                                                tablet_S1
  
from `bpde-prd-core-dwh.tracy.pv_session` pv 
  join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on betr.ktnr=pv.ktnr_glob and betr.wkorbid=pv.wkorbid 
where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and betr.gilt_von<=S1E and betr.gilt_bis>S1E 
      and pv.partition_date between S1A and S1E 
group by 1);

create or replace table `bpde-prd-core-dwh.sde.dm_webshop_S2`                                                    
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select ktnr_glob as bpid
		, AVG(session_length_sek)                                                                           avg_session_length_S2
		, sum(page_visits)                                                                                  pagevisits_S2
		, sum(page_visits_ads)                                                                              pagevisits_ads_S2
		, count(distinct (device))                                                                          device_unique_S2
		, count(distinct(betr.wkorbid))                                                                     Sessions_S2
            , count(distinct(case when kauf_bool=0 and page_visits_ads>0  then betr.wkorbid end))               KVB_S2
		, count(distinct(case when kauf_bool=0 and  wkorb_fill_bool=1 then betr.wkorbid end))               WKA_S2
		, sum(case when device=0 then 1 end)                                                                desktopAnz_S2
		, sum(case when device=1 and shopapi_client_type = '1' then 1 end)                                  mobileBrowser_S2
            , sum(case when device=1 and shopapi_client_type = '3' then 1 end)                                  mobileApp_S2
		, sum(case when device=2 then 1 end)                                                                tablet_S2
  
from `bpde-prd-core-dwh.tracy.pv_session` pv 
  join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on betr.ktnr=pv.ktnr_glob and betr.wkorbid=pv.wkorbid 
where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and betr.gilt_von<=S1E and betr.gilt_bis>S1E 
      and pv.partition_date between S2A and S2E 
group by 1);

create or replace table `bpde-prd-core-dwh.sde.dm_webshop_S3`                                                    
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select ktnr_glob as bpid
		, AVG(session_length_sek)                                                                           avg_session_length_S3
		, sum(page_visits)                                                                                  pagevisits_S3
		, sum(page_visits_ads)                                                                              pagevisits_ads_S3
		, count(distinct (device))                                                                          device_unique_S3
		, count(distinct(betr.wkorbid))                                                                     Sessions_S3
            , count(distinct(case when kauf_bool=0 and page_visits_ads>0  then betr.wkorbid end))               KVB_S3
		, count(distinct(case when kauf_bool=0 and  wkorb_fill_bool=1 then betr.wkorbid end))               WKA_S3
		, sum(case when device=0 then 1 end)                                                                desktopAnz_S3
		, sum(case when device=1 and shopapi_client_type = '1' then 1 end)                                  mobileBrowser_S3
            , sum(case when device=1 and shopapi_client_type = '3' then 1 end)                                  mobileApp_S3
		, sum(case when device=2 then 1 end)                                                                tablet_S3
  
from `bpde-prd-core-dwh.tracy.pv_session` pv 
  join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on betr.ktnr=pv.ktnr_glob and betr.wkorbid=pv.wkorbid 
where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and betr.gilt_von<=S1E and betr.gilt_bis>S1E 
      and pv.partition_date between S3A and S3E 
group by 1);


create or replace table `bpde-prd-core-dwh.sde.dm_webshop_S4`                                                    
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select ktnr_glob as bpid
		, AVG(session_length_sek)                                                                           avg_session_length_S4
		, sum(page_visits)                                                                                  pagevisits_S4
		, sum(page_visits_ads)                                                                              pagevisits_ads_S4
		, count(distinct (device))                                                                          device_unique_S4
		, count(distinct(betr.wkorbid))                                                                     Sessions_S4
            , count(distinct(case when kauf_bool=0 and page_visits_ads>0  then betr.wkorbid end))               KVB_S4
		, count(distinct(case when kauf_bool=0 and  wkorb_fill_bool=1 then betr.wkorbid end))               WKA_S4
		, sum(case when device=0 then 1 end)                                                                desktopAnz_S4
		, sum(case when device=1 and shopapi_client_type = '1' then 1 end)                                  mobileBrowser_S4
            , sum(case when device=1 and shopapi_client_type = '3' then 1 end)                                  mobileApp_S4
		, sum(case when device=2 then 1 end)                                                                tablet_S4
  
from `bpde-prd-core-dwh.tracy.pv_session` pv 
  join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on betr.ktnr=pv.ktnr_glob and betr.wkorbid=pv.wkorbid 
where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and betr.gilt_von<=S1E and betr.gilt_bis>S1E  
      and pv.partition_date between S4A and S4E 
group by 1);

create or replace table `bpde-prd-core-dwh.sde.dm_webshop_S5`                                                  
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select ktnr_glob as bpid
		, AVG(session_length_sek)                                                                           avg_session_length_S5
		, sum(page_visits)                                                                                  pagevisits_S5
		, sum(page_visits_ads)                                                                              pagevisits_ads_S5
		, count(distinct (device))                                                                          device_unique_S5
		, count(distinct(betr.wkorbid))                                                                     Sessions_S5
            , count(distinct(case when kauf_bool=0 and page_visits_ads>0  then betr.wkorbid end))               KVB_S5
		, count(distinct(case when kauf_bool=0 and  wkorb_fill_bool=1 then betr.wkorbid end))               WKA_S5
		, sum(case when device=0 then 1 end)                                                                desktopAnz_S5
		, sum(case when device=1 and shopapi_client_type = '1' then 1 end)                                  mobileBrowser_S5
            , sum(case when device=1 and shopapi_client_type = '3' then 1 end)                                  mobileApp_S5
		, sum(case when device=2 then 1 end)                                                                tablet_S5
  
from `bpde-prd-core-dwh.tracy.pv_session` pv 
  join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on betr.ktnr=pv.ktnr_glob and betr.wkorbid=pv.wkorbid 
where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and betr.gilt_von<=S1E and betr.gilt_bis>S1E  
      and pv.partition_date between S5A and S5E 
group by 1);

create or replace table `bpde-prd-core-dwh.sde.dm_webshop_S6`                                                  
options (expiration_timestamp=timestamp_add(current_timestamp, interval 12 hour))
as (
select ktnr_glob as bpid
		, AVG(session_length_sek)                                                                           avg_session_length_S6
		, sum(page_visits)                                                                                  pagevisits_S6
		, sum(page_visits_ads)                                                                              pagevisits_ads_S6
		, count(distinct (device))                                                                          device_unique_S6
		, count(distinct(betr.wkorbid))                                                                     Sessions_S6
            , count(distinct(case when kauf_bool=0 and page_visits_ads>0  then betr.wkorbid end))               KVB_S6
		, count(distinct(case when kauf_bool=0 and  wkorb_fill_bool=1 then betr.wkorbid end))               WKA_S6
		, sum(case when device=0 then 1 end)                                                                desktopAnz_S6
		, sum(case when device=1 and shopapi_client_type = '1' then 1 end)                                  mobileBrowser_S6
            , sum(case when device=1 and shopapi_client_type = '3' then 1 end)                                  mobileApp_S6
		, sum(case when device=2 then 1 end)                                                                tablet_S6
  
from `bpde-prd-core-dwh.tracy.pv_session` pv 
  join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on betr.ktnr=pv.ktnr_glob and betr.wkorbid=pv.wkorbid 
where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and betr.gilt_von<=S1E and betr.gilt_bis>S1E  
      and pv.partition_date between S6A and S6E 
group by 1);



create or replace table `bpde-prd-core-dwh.sde.styleviewQ1_int_de_roll`
options(expiration_timestamp=TIMESTAMP_ADD(current_timestamp, INTERVAL 12 Hour))
as (
with StyleviewBasis as (
SELECT wkorbid
      , substr(cast(style_id as string),1,2) as FT
      , count(style_id) as AnzStyles
			, sum(CASE when substr(cast(style_id as string),1,2) in ('11',	'13',	'27',	'28',	'51',	'52',	'53', '54', '55',	'57',	'58',	
              '61',	'62',	'63',	'64',	'68',	'69',	'70',	'71',	'72',	'74',	'75',	'78',	'81',	'82',	'83',	'84',	
              '85',	'86',	'87',	'88') then 1 else 0 end) as dob_ges
			, sum(CASE when substr(cast(style_id as string),1,2) in ('20','33','35','36','37') then 1 else 0 end) as wae
			, sum(CASE when substr(cast(style_id as string),1,2) in ('15','16','17','18') then 1 else 0 end) as haka
			, sum(CASE when substr(cast(style_id as string),1,2) in ('22','23','24','26') then 1 else 0 end) as kika
			, sum(CASE when substr(cast(style_id as string),1,2) in ('41','42','43','45','47','48','49') then 1 else 0 end) as wohnen
			, sum(CASE when substr(cast(style_id as string),1,2) in ('21','25') then 1 else 0 end) as schuhe
			, sum(CASE when substr(cast(style_id as string),1,2) in ('65','66') then 1 else 0 end) as bade
			, sum(CASE when substr(cast(style_id as string),1,2) in ('67','73','34') then 1 else 0 end) as sport
			, sum(CASE when substr(cast(style_id as string),1,2) in ('76') then 1 else 0 end) as acc
	from `bpde-prd-core-dwh.bp_int_rdb_view.styleview`
	where mandant_id=MandantID_decl and partition_Date between Q1A and Q1E 
	group by 1,2
)

SELECT pv.ktnr_glob as bpid
		, count(distinct (CASE when coalesce(svfts.wkorbid,0)>0 then svfts.FT end)) as AnzFT_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(svfts.AnzStyles as int64) else 0 end) as AnzStyles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.dob_ges as int64) else 0 end) as dob_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.wae as int64) else 0 end) as wae_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.haka as int64) else 0 end) as haka_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.kika as int64) else 0 end) as kika_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.wohnen as int64) else 0 end) as wohnen_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.schuhe as int64) else 0 end) as schuhe_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.bade as int64) else 0 end) as bade_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.sport as int64) else 0 end) as sport_Styles_Q1
		, sum(CASE when coalesce(svfts.wkorbid,0)>0 then cast(SVFTs.acc as int64) else 0 end) as acc_Styles_Q1
from `bpde-prd-core-dwh.tracy.pv_session` pv 
join `bpde-prd-core-dwh.bp_int_rdb_view.pv_session_hist` betr on pv.wkorbid=betr.wkorbid
left outer join StyleviewBasis SVFTs ON SVFTs.wkorbid=pv.wkorbid 
	where pv.mandant_id=MandantID_decl
      and betr.mandant_id=MandantID_decl
      and session_click=1 
      and testkunde = 0 
      and pv.partition_Date between Q1A and Q1E 
      and gilt_von<=Q1E and gilt_bis>=Q1E
group by 1)
;


create or replace table `bpde-prd-core-dwh.sde.datamart_webshop_int_de_roll`
as (
select a.bpid, a.accountdate
      , avg_session_length_S1
	, pagevisits_S1
	, pagevisits_ads_S1
	, device_unique_S1
	, Sessions_S1
      , KVB_S1
	, WKA_S1
	, desktopAnz_S1
	, mobileBrowser_S1
      , mobileApp_S1
	, tablet_S1

      , avg_session_length_S2
	, pagevisits_S2
	, pagevisits_ads_S2
	, device_unique_S2
	, Sessions_S2
      , KVB_S2
	, WKA_S2
	, desktopAnz_S2
	, mobileBrowser_S2
      , mobileApp_S2
	, tablet_S2

      , avg_session_length_S3
	, pagevisits_S3
	, pagevisits_ads_S3
	, device_unique_S3
	, Sessions_S3
      , KVB_S3
	, WKA_S3
	, desktopAnz_S3
	, mobileBrowser_S3
      , mobileApp_S3
	, tablet_S3

      , avg_session_length_S4
	, pagevisits_S4
	, pagevisits_ads_S4
	, device_unique_S4
	, Sessions_S4
      , KVB_S4
	, WKA_S4
	, desktopAnz_S4
	, mobileBrowser_S4
      , mobileApp_S4
	, tablet_S4

      , avg_session_length_S5
	, pagevisits_S5
	, pagevisits_ads_S5
	, device_unique_S5
	, Sessions_S5
      , KVB_S5
	, WKA_S5
	, desktopAnz_S5
	, mobileBrowser_S5
      , mobileApp_S5
	, tablet_S5

      , avg_session_length_S6
	, pagevisits_S6
	, pagevisits_ads_S6
	, device_unique_S6
	, Sessions_S6
      , KVB_S6
	, WKA_S6
	, desktopAnz_S6
	, mobileBrowser_S6
      , mobileApp_S6
	, tablet_S6

	, AnzFT_Q1
	, AnzStyles_Q1
	, dob_Styles_Q1

from `bpde-prd-core-dwh.sde.dexbase_int_de_roll` a
left outer join `bpde-prd-core-dwh.sde.dm_webshop_S1` s1 on a.bpid=s1.bpid
left outer join `bpde-prd-core-dwh.sde.dm_webshop_S2` s2 on a.bpid=s2.bpid
left outer join `bpde-prd-core-dwh.sde.dm_webshop_S3` s3 on a.bpid=s3.bpid
left outer join `bpde-prd-core-dwh.sde.dm_webshop_S4` s4 on a.bpid=s4.bpid
left outer join `bpde-prd-core-dwh.sde.dm_webshop_S5` s5 on a.bpid=s5.bpid
left outer join `bpde-prd-core-dwh.sde.dm_webshop_S6` s6 on a.bpid=s6.bpid
left outer join `bpde-prd-core-dwh.sde.styleviewQ1_int_de_roll` styleQ1 on a.bpid=styleQ1.bpid
);



create or replace table `bpde-prd-core-dwh.sde.datamart_int_de_roll` as (
with basis as (
select a.*
      , a.bpid as CustomerID
      , case 
            when a.TargetGroupindexRollOld_org in (63,64,65) then 345
            when a.TargetGroupindexRollOld_org in (66,67,68) then 678
        end TargetGroupindexRollOld
      , t.* except(bpid, accountdate, Age)
      , w.* except(bpid, accountdate)
from `bpde-prd-core-dwh.sde.dexbase_int_de_roll` a
left outer join `bpde-prd-core-dwh.sde.datamart_transactions_int_de_roll` t on a.bpid=t.bpid and a.accountDate=t.accountDate
left outer join `bpde-prd-core-dwh.sde.datamart_webshop_int_de_roll` w on a.bpid=w.bpid and a.accountDate=w.accountDate
)

select *,  row_number() over(partition by TargetGroupIndexRoll, TargetGroupindexRollOld) as rownumber
from basis
);
