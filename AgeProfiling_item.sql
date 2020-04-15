select  
	customer_id, sale_code, store_currency_code, Channel as channel, TradeWeekCode, Division as division, Department as department, category, item, sum(AUDSales) as aud_sales
from
(
select
	fst.customer_id 
	,sale_code 
	,fst.store_currency_code
	,case 
		when Onlinesale=0 then 'Store' 
		when OnlineSale=1 then 'Online' 
	end Channel
	,case 
		when OnlineSale=0 then cast(transaction_date_time as date) 
		when OnlineSale=1 then cast(OrderedDate as date) 
	end as trans_order_date	
	,DI.Division
	,DI.Department
	,DI.category
	,replace(DI.[Item Description], ',', '-') as item
	--,DI.[Item Colour Description] item_colour
	,sum(AUDsales) as AUDSales
from [RPT].[dbo].[vw_Dim_Item] DI with (nolock)
inner join  rpt.dbo.vwFactSaleTable as fst with (nolock) on fst.itemcoloursize_id = DI.itemcoloursize_id and LoyaltyCustomerFlag=1 and store_currency_code in ('AUD','HKD','MLR','NZD','SGD','USD','ZAR')
group by 
	fst.customer_id
	,sale_code
	,fst.store_currency_code
	,case when Onlinesale=0 then 'Store' when OnlineSale=1 then 'Online' end --channel
	,case when OnlineSale=0 then cast(transaction_date_time as date) when OnlineSale=1 then cast(OrderedDate as date) end -- trans_order_date
	,DI.Division, DI.Department, DI.category, DI.[Item Description]
	--, i.[Item Code], i.[Item Description], i.[Item Colour Description]
) int
inner join rms.dbo.DateDimension as dd with (nolock) on int.trans_order_date = dd.Date
where (int.trans_order_date BETWEEN cast(CURRENT_TIMESTAMP - 14 as date) AND cast(CURRENT_TIMESTAMP - 1 as date))
group by customer_id, sale_code, store_currency_code, Channel, TradeWeekCode, Division, Department, category, item
