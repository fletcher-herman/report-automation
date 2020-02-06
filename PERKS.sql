set nocount on
select *
from
(
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

		,datediff(year,cast([birth_date] as date),getdate()) as age
		,[Store Type] AS signup_storetype
		,Store_sign_up
		, case when [Store Type]='INSTORE' then postcode else billing_post_code end as cust_postcode -- based on store sign up web-billing
		,CAST(ROW_NUMBER() OVER (PARTITION BY lc.customer_id ORDER BY date_from_f ASC) AS DECIMAL) as uq_cust_rank
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
) int where uq_cust_rank = 1