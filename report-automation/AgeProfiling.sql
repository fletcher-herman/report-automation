set nocount on
select  
	customer_id, store_currency_code, Channel as channel, trans_order_date, Division as division, Department as department, category, sum(AUDSales) as aud_sales
from
(
select
	fst.customer_id  
	,fst.store_currency_code
	,case 
		when onlinesale=0 then 'Store' 
		when OnlineSale=1 then 'Online' 
	end Channel
	,case 
		when OnlineSale=0 then cast(transaction_date_time as date) 
		when OnlineSale=1 then cast(OrderedDate as date) 
	end as trans_order_date	
	,DI.Division
	,DI.Department
	,DI.category
	,sum(AUDsales) as AUDSales
from [RPT].[dbo].[vw_Dim_Item] DI with (nolock)
inner join  rpt.dbo.vwFactSaleTable as fst with (nolock) on fst.itemcoloursize_id = DI.itemcoloursize_id and LoyaltyCustomerFlag=1 and store_currency_code in ('AUD','HKD','MLR','NZD','SGD','USD','ZAR')
--inner join rpt.dbo.DateDimension as dt  with (nolock) on (dt.date=cast(transaction_date_time as date) and OnlineSale=0) or (dt.date=cast(OrderedDate as date) and OnlineSale=1)
group by 
	fst.customer_id
	,fst.store_currency_code
	,case when onlinesale=0 then 'Store' when OnlineSale=1 then 'Online' end --channel
	,case when OnlineSale=0 then cast(transaction_date_time as date) when OnlineSale=1 then cast(OrderedDate as date) end -- trans_order_date
	,DI.Division, DI.Department, DI.category
	--, i.[Item Code], i.[Item Description], i.[Item Colour Description]
) int
where (int.trans_order_date BETWEEN cast(CURRENT_TIMESTAMP - 8 as date) AND cast(CURRENT_TIMESTAMP - 1 as date))
group by customer_id, store_currency_code, Channel, trans_order_date, Division, Department, category
