set nocount on

select 
	int.customer_id
	,int.transacted_kids
	,case 
		when trans_mens=1 and  coalesce(trans_other_than_mens,0)=0 then 'Pure' 
			when trans_mens=1 and coalesce(trans_other_than_mens,0)=1 then 'Multi' 
		end as transacted_cat_menswear
		,case 
			when trans_Kids=1 and  coalesce(trans_other_than_Kids,0)=0 then 'Pure' 
			when trans_Kids=1 and coalesce(trans_other_than_Kids,0)=1 then 'Multi' 
		end as transacted_kids_flag
		,case 
			when trans_AFL=1 and  coalesce(trans_NRL,0)=0   then 'AFL' 
			when trans_NRL=1 and  coalesce(trans_AFL,0)=0   then 'NRL' 
			when (trans_AFL=1 and trans_NRL=1) then 'Multi' 
		end as transacted_cat_sports
		,case 
			when trans_curve=1 and  coalesce(trans_other_than_curve,0)=0 then 'Pure' 
			when trans_curve=1 and coalesce(trans_other_than_curve,0)=1 then 'Multi' 
		end as transacted_cat_curve
from
(		 
select  FST.customer_id
		,max(case when division in ('2 - KIDS') then FORMAT((transaction_date_time), 'dd/MM/yyyy') end) as transacted_kids	
		,max(case when (division in ('1 - COTTONON','5 - FACTORIE') and ((CHARINDEX('BOY', department)>0 or CHARINDEX('MENS', department)>0 or CHARINDEX('GUY', department)>0))) then 1 else 0 end) as trans_mens
		,max(case when (division in ('1 - COTTONON','5 - FACTORIE') and ((CHARINDEX('BOY', department)>0 or CHARINDEX('MENS', department)>0 or CHARINDEX('GUY', department)>0))) then 0 else 1 end) as trans_other_than_mens
		,max(case when category='165 - CURVE'  then 1 else 0 end) as trans_curve
		,max(case when category='165 - CURVE'  then 0 else 1 end) as trans_other_than_curve
		,max(case when (CHARINDEX('AFL ', [Item Description])>0 ) and  Division='11 - COMMUNITY'  then 1 else 0 end) as trans_AFL
		,max(case when (CHARINDEX('NRL ', [Item Description])>0 ) and  Division='11 - COMMUNITY'  then 1 else 0 end) as trans_NRL
		,max(case when (CHARINDEX('AFL ', [Item Description])>0 OR CHARINDEX('NRL ', [Item Description])>0 ) and  Division='11 - COMMUNITY'  then 0 else 1 end) as trans_other_than_AFL_NRL
		,max(case when division in ('2 - KIDS')   then 1 else 0 end) as trans_Kids
		,max(case when division in ('2 - KIDS')   then 0 else 1 end) as trans_other_than_Kids
		,max(case when OnlineSale=1 then 1 else 0 end) as channel_online
		,max(case when OnlineSale=0 then 1 else 0 end) as channel_store
		,Sum(AUDSales) as AUDSales

    FROM [RPT].[dbo].[vwFactSaleTable] as fst  WITH (NOLOCK) 
	 inner join [RPT].[dbo].[vw_Dim_Item] as i  WITH (NOLOCK)  
	  ON fst.[itemcoloursize_id]=i.itemcoloursize_id    
		 and cast(transaction_date_time as date)>=DATEADD(DAY,-10,getdate()) and fst.LoyaltyCustomerFlag=1
		 --and cast(transaction_date_time as date)>=DATEADD(DAY,-365,getdate()) and fst.LoyaltyCustomerFlag=1
	GROUP by fst.customer_id
) int	
