-- DROP FUNCTION public.sp_ssis_insert_leads();

CREATE OR REPLACE FUNCTION public.sp_ssis_insert_leads()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
	BEGIN
TRUNCATE TABLE "STG_Leads";

INSERT into "STG_Leads"
--INSERT into "STG_Leads"
select
	l.product,
	l.activity,
	l.leadid,
	l.accountid,
	l.customer_id,
	l.created_by,
	l.assign_to,
	company_name,
	l.first_name,
	l.last_name,
	phone_mobile,
	emails,
	date_entered,
	l.status,
	leadlifetime,
	date_converted ,
	description ,
	lead_source,
	campaign,
	campaignnum,
	coalesce(nullif(media, ''), 'NoMedia') media,
	date_modified,
	status_changed,
	do_not_call,
	statustype,
	l.campaign_date_entered,
	l.marketing_campaign ,
	cm.cost_per_lead_usd ,
	cm.cost_per_lead_ils
--,sum(case when coalesce(l.accountid,'') = '' then o.product_amount * o.product_price  else o2.product_amount * o2.product_price end ) as "Opportunity"
,cast(sum(
    case 
	when l.product = 'product_a' then lc1.estimated_profit 
    when l.product <> 'product_a'
    and o.opportunity_mid_status in ('SetupEnded','Support_Sale')
    and o.is_deleted is false 
    and product_type_c = 'features' then (o.product_amount * o.product_price)
    end)as decimal(18,2)) as "Opportunity_Sale"
 ,sum(
 	case
 		when l.product <> 'product_a' 
 		and o.opportunity_mid_status = 'Failed'
 		and o.is_deleted is false 
    	and product_type_c = 'features' then (o.product_amount * o.product_price)
 	end
 	) as "Opportunity_Failed"
 ,sum(
 	case
 		when l.product <> 'product_a' 
 		and o.opportunity_mid_status not in ('Failed','SetupEnded','Support_Sale')
 		and o.is_deleted is false 
    	and product_type_c = 'features' then (o.product_amount * o.product_price)
 	end
 	) as "Open_Opportunity"
,case when l.customer_id != 0 then 'Yes' else 'No' end as "Sales_YesOrNo"
,case when l.customer_id = 0 then 0
	  when l.product = 'product_a' then lc."TotalSales"
      when l.product = 'product_b' then coalesce(cs."TotalSales",0)
      when l.product = 'product_c' then ss."TotalSales" else 1 end as "TotalSales"
 ,coalesce(lc."TempSales",0)"LocalTempSales"
 ,coalesce(cs."Group", lc."Group") "Group"
 ,lc."GroupCategory"
 ,l.country
FROM public."Leads" l
left join public."Opportunities" o on l.leadid = o.leadid 
left join public.campaign_data cm on l.campaignnum = cm.campaignid and EXTRACT(year from l.date_entered ) = cm."year" and EXTRACT(month from l.date_entered ) = cm."Month" 
left join public.customers lc1 on lc1.accountid = l.accountid
left join lateral(select customer_id ,string_agg(distinct "Group",',' )"Group",count(*) as "TotalSales" from public.cloud_sales where l.customer_id = customer_id and saley_n = 'Yes' group by customer_id ) as cs on true 
left join lateral (select lc.customer_id ,string_agg(distinct "group",',' ) "Group" ,string_agg(distinct "groupcategory",',' ) "GroupCategory", count(*) filter (where "temporary" = 0) as "TotalSales" ,sum("temporary") filter (where "temporary" = 1) as "TempSales"from public.customers lc where l.customer_id = lc.customer_id and l.customer_id !=0 group by lc.customer_id )as lc on true
left join lateral (select ss.customer_id , count(*) as "TotalSales" from public.sim_sales ss where l.customer_id = ss.customer_id group by ss.customer_id) as ss on true
where l.date_entered >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months')
--where l.date_entered >= '2023-01-01'
and l.status != 'Invalid_test'
group by l.product,
l.activity,
l.leadid,
l.accountid,
l.customer_id,
l.created_by,
l.assign_to,
company_name,
l.first_name,
l.last_name,
phone_mobile,
emails,
date_entered,
l.status,
leadlifetime,
date_converted,
description,
lead_source,
campaign,
campaignnum,
media,
date_modified,
status_changed,
do_not_call,
statustype,
l.campaign_date_entered,
l.marketing_campaign,
cm.cost_per_lead_usd,
cm.cost_per_lead_ils
,case when l.customer_id != 0 then 'Yes' else 'No' end
,case when l.customer_id = 0 then 0
	  when l.product = 'product_a' then lc."TotalSales"
      when l.product = 'product_b' then coalesce(cs."TotalSales",0)
      when l.product = 'product_c' then ss."TotalSales" else 1 end
  ,lc."TempSales" 
  ,coalesce(cs."Group", lc."Group")
 ,lc."GroupCategory"
 ,l.country
order by l.date_entered desc;


	END;
$function$
;
