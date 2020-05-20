SET NOCOUNT ON
IF OBJECT_ID('tempdb..#PERKS')  IS NOT NULL         BEGIN               DROP TABLE #PERKS      END
 SELECT lc.customer_id, date_from_f, date_to_f, insert_user, loyalty_code , gender,email_address
		 ,case when [birth_date] is NULL then 'Missing'
			when datediff(year,cast([birth_date] as date),getdate())<15 then '<=15'
			when datediff(year,cast([birth_date] as date),getdate())<20 then '16-18'
			when datediff(year,cast([birth_date] as date),getdate())<25 then '19-21'
			when datediff(year,cast([birth_date] as date),getdate())<30 then '22-24'
			when datediff(year,cast([birth_date] as date),getdate())<35 then '25-29'
			when datediff(year,cast([birth_date] as date),getdate())<40 then '30-34'
			when datediff(year,cast([birth_date] as date),getdate())<45 then '35-44'
			when datediff(year,cast([birth_date] as date),getdate())<50 then '45-60'
			when datediff(year,cast([birth_date] as date),getdate())>59 then '60+'
			else 'xx' end as age_band

		,case when [birth_date] is NULL then 'Missing'			
			when datediff(year,cast([birth_date] as date),getdate())>80 then 'Missing'
			when datediff(year,cast([birth_date] as date),getdate())<10 then 'Missing'
			when datediff(year,cast([birth_date] as date),getdate())<19 then 'Young Reach (<19)'
			when datediff(year,cast([birth_date] as date),getdate())<25 then 'Edit (19-24)'
			when datediff(year,cast([birth_date] as date),getdate())<40 then 'Older Reach (25-39)'
			when datediff(year,cast([birth_date] as date),getdate())>=40 then 'Older Reach (40+)'
			else 'Missing' end as age_segment

		,isnull(datediff(year,cast([birth_date] as date),getdate()), 9999) as age
		,[Store Type] AS signup_storetype
		,Store_sign_up
		, case when [Store Type]='INSTORE' then postcode else billing_post_code end as cust_postcode -- based on store sign up web-billing
		
		,    CAST(ROW_NUMBER() OVER (PARTITION BY lc.customer_id ORDER BY date_from_f ASC) AS DECIMAL) as uq_cust_rank
 INTO #PERKS
       FROM 
             (SELECT customer_id, loyaltycustomer_id, loyalty_code
                    FROM [RMS].[dbo].[loyaltycustomer] (nolock)
                    WHERE status_ind = 'A'
                    GROUP BY customer_id, loyaltycustomer_id, loyalty_code) AS lc
             INNER JOIN
             (SELECT  [loyaltycustomer_id]
                           ,cast([date_from] as date) as date_from_f
                           ,cast([date_to] as date) as date_to_f
                           ,case when insert_user IN( 'WEB','RD Script') or  insert_user IS NULL then insert_user else 'OTHER' end as insert_user
                    FROM [RMS].[dbo].[loyaltycustmember] (nolock)
                    WHERE cast([date_from] as date)<=getdate()) as lcm             
             ON lc.loyaltycustomer_id=lcm.loyaltycustomer_id	
			LEFT JOIN (select customer_id, birth_date, billing_post_code, gender , email_address from [RMS].[dbo].[customer]  (nolock)) c  ON lc.customer_id=c.customer_id 
			LEFT JOIN (select [Customer ID],Store as Store_sign_up from [RPT].[dbo].[LoyaltyCustStores]  (nolock)) s  ON lc.customer_id=s.[Customer ID]  
			LEFT JOIN (select [Store Code],[Store Type] ,[Store Brand],[Store Description] from [RPT].[dbo].[vw_Dim_store] as S with (nolock) ) s_lkp ON S.Store_sign_up = s_lkp.[Store Code]  
			LEFT JOIN (select location_code, postcode from [rms].[dbo].[location] with (nolock)) pcode_lkp  on s_lkp.[Store Code]  =pcode_lkp.location_code

SET ANSI_WARNINGS OFF
SET NOCOUNT ON
IF OBJECT_ID('tempdb..#transaction_summary')  IS NOT NULL         BEGIN               DROP TABLE #transaction_summary      END
	Select  FST.customer_id
		--KIDS RECENCY
		 ,max(case when division in ('2 - KIDS') then FORMAT((transaction_date_time), 'dd/MM/yyyy') end) as Transacted_KIDS	
		--BABY FLAG
		,max(case when Department in ('21 - BABY') then 1 else 0 end) as Transacted_BABY
		,max(case when Department in ('21 - BABY')   then 0 else 1 end) as trans_other_than_BABY
		-- MENS DEP
		,max(case when (division in ('1 - COTTONON','5 - FACTORIE') and ((CHARINDEX('BOY', department)>0 or CHARINDEX('MENS', department)>0 or CHARINDEX('GUY', department)>0))) then 1 else 0 end) as trans_mens
		,max(case when (division in ('1 - COTTONON','5 - FACTORIE') and ((CHARINDEX('BOY', department)>0 or CHARINDEX('MENS', department)>0 or CHARINDEX('GUY', department)>0))) then 0 else 1 end) as trans_other_than_mens


		--CURVE FLAG
		,max(case when category='165 - CURVE'  then 1 else 0 end) as trans_curve
		,max(case when category='165 - CURVE'  then 0 else 1 end) as trans_other_than_curve

	
		--AFL & NRL combined FLAG
		,max(case when (CHARINDEX('AFL ', [Item Description])>0 ) and  Division='11 - COMMUNITY'  then 1 else 0 end) as trans_AFL
		,max(case when (CHARINDEX('NRL ', [Item Description])>0 ) and  Division='11 - COMMUNITY'  then 1 else 0 end) as trans_NRL
		,max(case when (CHARINDEX('AFL ', [Item Description])>0 OR CHARINDEX('NRL ', [Item Description])>0 ) and  Division='11 - COMMUNITY'  then 0 else 1 end) as trans_other_than_AFL_NRL

		
		--KIDS FLAG
		,max(case when division in ('2 - KIDS', '13 - SUNNY BUDDY (DNU)')   then 1 else 0 end) as trans_Kids
		,max(case when division in ('2 - KIDS', '13 - SUNNY BUDDY (DNU)')   then 0 else 1 end) as trans_other_than_Kids

		--CO-BRAND FLAG
		,max(case when division in ('1 - COTTONON', '3 - BODY', '4 - RUBI', '12 - LOST (DNU)')   then 1 else 0 end) as trans_cobrands
		,max(case when division in ('1 - COTTONON', '3 - BODY', '4 - RUBI', '12 - LOST (DNU)')   then 0 else 1 end) as trans_other_than_cobrands

		--CHANNEL
		,max(case when OnlineSale=1 then 1 else 0 end) as channel_online
		,max(case when OnlineSale=0 then 1 else 0 end) as channel_store

		--SALES
		,Sum(case when AUDSales < 0 then 0 else AUDSales end) as AUDSales
	

	into #transaction_summary
    FROM [RPT].[dbo].[vwFactSaleTable] as fst  WITH (NOLOCK) 
	 inner join [RPT].[dbo].[vw_Dim_Item] as i  WITH (NOLOCK)  ON fst.[itemcoloursize_id]=i.itemcoloursize_id    
		 and cast(transaction_date_time as date)>=DATEADD(DAY,-365,getdate()) and fst.LoyaltyCustomerFlag=1
	GROUP by fst.customer_id -- 10 days: 7secs, 50 days: 2:53, 100 days: 3:26, 200 days: 4:37, 300 days 5:59 & 9:47, 360 days: 37 & 9:19

SET ANSI_WARNINGS OFF
SET NOCOUNT ON
IF OBJECT_ID('tempdb..#transaction_kids_summary')  IS NOT NULL         BEGIN               DROP TABLE #transaction_kids_summary      END
	Select  FST.customer_id
	,Sum(case when AUDSales < 0 then 0 else AUDSales end) as kids_AUDSales
	into #transaction_kids_summary
    FROM [RPT].[dbo].[vwFactSaleTable] as fst  WITH (NOLOCK) 
	 inner join [RPT].[dbo].[vw_Dim_Item] as i  WITH (NOLOCK)  ON fst.[itemcoloursize_id]=i.itemcoloursize_id    
		 and cast(transaction_date_time as date)>=DATEADD(DAY,-365,getdate()) and fst.LoyaltyCustomerFlag=1
		 and division in ('2 - KIDS', '13 - SUNNY BUDDY (DNU)')
	GROUP by fst.customer_id

--	select count(*) from #transaction_kids_summary
--	select top 100 * from #transaction_summary

SET NOCOUNT ON
IF OBJECT_ID('tempdb..#AFL_NRLcampaigns')  IS NOT NULL         BEGIN               DROP TABLE #AFL_NRLcampaigns      END
SELECT 
	*, case when CHARINDEX('_AFLNRL', upper([EmailName]))>0 then 1 else 0 end as afl_nrl
	, case when CHARINDEX('_AFL', upper([EmailName]))>0 then 1 else 0 end as afl
	, case when CHARINDEX('_NRL', upper([EmailName]))>0 then 1 else 0 end as nrl
	into #AFL_NRLcampaigns
	from [RPT].[sfmc].[Sendjobs]  as sj with (nolock)
	 WHERE CHARINDEX('_AFL', upper([EmailName]))>0
			 OR CHARINDEX('_NRL', upper([EmailName]))>0 
			-- OR CHARINDEX('AFL', upper([EmailName]))>0  DON'T INCLUDE AS RANDOM CAMPS 
			 --OR CHARINDEX('NRL', upper([EmailName]))>0 DON'T INCLUDE AS RANDOM CAMPS 

	 --SELECT * FROM #AFL_NRLcampaigns


SET NOCOUNT ON
IF OBJECT_ID('tempdb..#opened')  IS NOT NULL         BEGIN               DROP TABLE #opened      END
Select  subscriberKey, max(afl_nrl) as opened_afl_nrl
					 , max(afl) as opened_afl
					 , max(nrl) as opened_nrl
into #opened
from [RPT].[sfmc].[Opens] as o with (nolock)
	 inner join #AFL_NRLcampaigns as cmp on o.sendid= cmp.SendID
group by subscriberKey


SET NOCOUNT ON	 
IF OBJECT_ID('tempdb..#clicked')  IS NOT NULL         BEGIN               DROP TABLE #clicked      END
Select  subscriberKey, max(afl_nrl) as clicked_afl_nrl
					 , max(afl) as clicked_afl
					 , max(nrl) as clicked_nrl
into #clicked
from [RPT].[sfmc].[Clicks] as c with (nolock)
	 inner join #AFL_NRLcampaigns as cmp on c.sendid= cmp.SendID
group by subscriberKey

		 
SET NOCOUNT ON		
IF OBJECT_ID('tempdb..#SCV')  IS NOT NULL         BEGIN               DROP TABLE #SCV      END
select customer_id, Transacted_KIDS,Transacted_BABY, Transacted_Cat_Menswear,Transacted_KIDS_flag,Transacted_Cat_Sports,Transacted_Cat_Curve, Transacted_Cat_CoBrands,kids_AUDSales,AUDSales,age
	,case  
		when (kids_AUDSales/nullif(AUDSales,0)) >= .05 then 'PARENT'
		when (kids_AUDSales/nullif(AUDSales,0)) between .01 and 0.05 and age = 9999 then 'PARENT'
		when (kids_AUDSales/nullif(AUDSales,0)) < 0.05 and age < 25 then 'EDIT'
		when (kids_AUDSales/nullif(AUDSales,0)) = 0.00 and age = 9999 then 'GENERIC'
		when (kids_AUDSales/nullif(AUDSales,0)) < 0.05 and age >= 25 then 'GENERIC'
		when age < 25 then 'EDIT'
		when age = 9999 or age > 40 then 'GENERIC'
	else 'GENERIC' end as LIFESTAGE
into #SCV from (
Select p.customer_id, age, Transacted_KIDS
		,case when trans_mens=1 and  coalesce(trans_other_than_mens,0)=0 then 'Pure' 
				when trans_mens=1 and coalesce(trans_other_than_mens,0)=1 then 'Multi' 
				end as Transacted_Cat_Menswear
		,case when trans_cobrands=1 and  coalesce(trans_other_than_cobrands,0)=0 then 'Pure' 
				when trans_cobrands=1 and coalesce(trans_other_than_cobrands,0)=1 then 'Multi' 
				end as Transacted_Cat_CoBrands
		,case when Transacted_BABY=1 and  coalesce(trans_other_than_BABY,0)=0 then 'Pure' 
				when Transacted_BABY=1 and coalesce(trans_other_than_BABY,0)=1 then 'Multi' 
				end as Transacted_BABY 
		,case when trans_Kids=1 and  coalesce(trans_other_than_Kids,0)=0 then 'Pure' 
				when trans_Kids=1 and coalesce(trans_other_than_Kids,0)=1 then 'Multi' 
				end as Transacted_KIDS_flag
		--,case when trans_AFL=1 and  coalesce(trans_NRL,0)=0  and coalesce(trans_other_than_AFL_NRL,0)=0 then 'AFL' 
		--	  when trans_NRL=1 and  coalesce(trans_AFL,0)=0  and coalesce(trans_other_than_AFL_NRL,0)=0 then 'NRL' 
		--		when (trans_AFL=1 or trans_NRL=1) and coalesce(trans_other_than_AFL_NRL,0)=1 then 'Multi' 
		--		end as Transacted_Cat_Sports_original
		,case when trans_AFL=1 and  coalesce(trans_NRL,0)=0   then 'AFL' 
			  when trans_NRL=1 and  coalesce(trans_AFL,0)=0   then 'NRL' 
				when (trans_AFL=1 and trans_NRL=1) then 'Multi' 
				when clicked_afl_nrl=1 then 'Multi_em_c'
				when clicked_afl=1 then 'AFL_em_c'
				when clicked_nrl=1 then 'NRL_em_c'
				when opened_afl_nrl=1 then 'Multi_em_o'
				when opened_afl=1 then 'AFL_em_o'
				when opened_nrl=1 then 'NRL_em_o'
				end as Transacted_Cat_Sports
		,case when trans_curve=1 and  coalesce(trans_other_than_curve,0)=0 then 'Pure' 
				when trans_curve=1 and coalesce(trans_other_than_curve,0)=1 then 'Multi' 
				end as Transacted_Cat_Curve
		,cast(isnull(AUDSales, 0) as float) as AUDSales
		,cast(isnull(kids_AUDSales, 0) as float) as kids_AUDSales
from #PERKS as p
left join #transaction_summary as t on p.customer_id=t.customer_id
left join #transaction_kids_summary as tk on p.customer_id=tk.customer_id
 left join #opened as o on p.email_address=o.subscriberKey
 left join #clicked as c on p.email_address=c.subscriberKey
where uq_cust_rank=1
) aa



--select LIFESTAGE, count(*) from #SCV group by LIFESTAGE

--select * from #SCV where LIFESTAGE = 'NO KIDS TRANS'


--/*select count(*) as c, trans_AFL,trans_NRL
--from #PERKS as p
--left join #transaction_summary as t on p.customer_id=t.customer_id
--where uq_cust_rank=1
--group by trans_AFL,trans_NRL
--*/
--select top 1 * from #SCV
--select count(*) from #SCV

--select count(*) ,Transacted_Cat_Sports from #SCV group by Transacted_Cat_Sports order by Transacted_Cat_Sports
--select count(*) ,Transacted_Cat_Sports_original from #SCV group by Transacted_Cat_Sports_original



Select customer_id
		,isnull(Transacted_KIDS,'') as Transacted_KIDS
		,isnull(Transacted_BABY,'') as Transacted_BABY
		,isnull(Transacted_Cat_Menswear,'') as Transacted_Cat_Menswear
		,isnull(Transacted_KIDS_flag,'') as Transacted_KIDS_flag
		,isnull(Transacted_Cat_Sports,'') as Transacted_Cat_Sports
		,isnull(Transacted_Cat_Curve,'') as Transacted_Cat_Curve
		,isnull(Transacted_Cat_CoBrands,'') as Transacted_Cat_CoBrands
		,LIFESTAGE
from #SCV

 ----------
--full run
 ----------
 -- 100 days: 21 mins
 -- 300 days: 18 mins
 -- 360 days: 36 mins 

--   SELECT Your_Column_Name
 --   FROM Your_Table_Name
  --  INTO OUTFILE 'Filename.csv'
   -- FIELDS TERMINATED BY ','
    --ENCLOSED BY '"'
    --LINES TERMINATED BY '\n'

		 


	 
	
	
	



		 	
---------------------------------------ONE OFF FILE FOR PABLEO PEAK 20191114--------------------------------------


--/*
--IF OBJECT_ID('tempdb..#CS_EXTRACT')  IS NOT NULL         BEGIN               DROP TABLE #CS_EXTRACT      END
--Select c.email_address
--		,max(coalesce(channel_online,0)) as  channel_online
--		,max(case when coalesce(AUDSales,0)>275 then 1 else 0 end) as segment_high_value
--		,sum(coalesce(AUDSales,0)) as AUDsales
--into #CS_EXTRACT
--from #PERKS as p
--left join #transaction_summary as t on p.customer_id=t.customer_id
--left join [RMS].[DBO].[customer] as c on p.customer_id=c.customer_id
--group by  email_address
--
--
--SET NOCOUNT ON
--Select  email_address, case when coalesce(channel_online,0)=0 then 'No Prior Online Shop' else 'Shopped Online before' end as Channel
--from #CS_EXTRACT
--where segment_high_value=1 and LEN(email_address)>2
--order by AUDsales desc
--*/






------------------------------------------------------------------------------------------------------------------





	
--SET NOCOUNT ON
--	Select distinct customer_id, 'CURVE' as segment
--    FROM [RPT].[dbo].[vwFactSaleTable] as fst  WITH (NOLOCK) 
--	 inner join [RPT].[dbo].[vw_Dim_Item] as i  WITH (NOLOCK) 
--	  ON fst.[itemcoloursize_id]=i.itemcoloursize_id 
--	     and department='12 - LADIESWEAR' and  charindex('+',upper(size)) >0    
--		 and cast(transaction_date_time as date)>=DATEADD(DAY,-365,getdate())	
--		 
--		 
--	
--
--
--
--SET NOCOUNT ON
--	select customer_id,
--		case when  loyalty_code='LOYALTYAU' then concat('AUS', allbrand_salesrank)
--			when  loyalty_code='LOYALTYUS' then concat('USA', allbrand_salesrank)
--			when  loyalty_code='LOYALTYZA' then concat('ZA', allbrand_salesrank)
--			when  loyalty_code='LOYALTYMY' then concat('MLY', allbrand_salesrank)
--			when  loyalty_code='LOYALTYHK' then concat('HK', allbrand_salesrank)
--			when  loyalty_code='LOYALTYSG' then concat('SG', allbrand_salesrank)
--			when  loyalty_code='LOYALTY' then concat('NZ', allbrand_salesrank)
--			ELSE 'XX'	
--		END as segment
--	 from #SCV_SFMC
