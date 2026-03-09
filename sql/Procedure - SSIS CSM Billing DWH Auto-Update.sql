USE [SalesDB]
GO
/****** Object:  StoredProcedure [dbo].[SSIS_Lake_CSM_Billing_Updated]    Script Date: 27/02/2026 15:44:12 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Adir Mamou
-- Create date: 2025-07-09
-- Description:	Update CSM Billing Procedure
-- =============================================
ALTER PROCEDURE [dbo].[SSIS_Lake_CSM_Billing_Updated]

AS
BEGIN

declare @temp1 table (invoice_date date, domain varchar(100), customer_id int, total_features_Curr numeric(18,2), final_bill numeric(18,2), IF_Final_Bill CHAR(1), Prev_Month_Total numeric(18,2), customer_name varchar(250));

  WITH base_data AS (

 SELECT 
    DATEFROMPARTS(YEAR(invoice_date), MONTH(invoice_date), 1) AS Invoice_Date,
	LTRIM(RTRIM(LOWER(domain))) AS domain,
    LTRIM(RTRIM(customer_id)) AS customer_id,
    SUM(feature_total_price) AS total_features,
	InvoiceNum,
    SUM(
	CASE 
	WHEN customerinvoicestatus = 'FINAL BILL' THEN feature_total_price 
	ELSE 0 
	END) AS final_bill,
	CASE 
      WHEN SUM(CASE WHEN customerinvoicestatus = 'FINAL BILL' THEN 1 ELSE 0 END) > 0 THEN 'Y'
      ELSE 'N'
    END AS IF_Final_Bill,
	customer_name
  FROM openquery ([DataWarehouse],'Select lbpf.*,
								bc."InvoiceNum",
								s.createdbydepartment						
										   From "Billing_Per_Feature" lbpf
										   inner join "Billing_Records" bc on bc."Invoice_id" = lbpf."invoice_id" and lbpf."CustomerType" in (''pbx'',''product_type_b'')
										   left join public.sales_records s on s.customer_id = bc.customer_id 
										   where invoice_date >= ''2024-12-01''
										   and lbpf.is_deleted = False
										   and bc.is_deleted = False
										   and invoice_date >= CURRENT_DATE - INTERVAL ''1 year''')
  WHERE invoiceNum >= 4 
  or createdbydepartment = 'Support'

  GROUP BY 
  LTRIM(RTRIM(LOWER(domain))),
  LTRIM(RTRIM(customer_id)),
  InvoiceNum,
    YEAR(invoice_date)
	,MONTH(invoice_date)
	,customer_name
),

fix_total_features as (
select Invoice_Date,
domain,
customer_id,
case
when total_features < 0 then 0
else total_features
end as total_features_Curr,
final_bill,
IF_Final_Bill,
customer_name
From base_data
),
IF_dif as(

Select Invoice_Date,
domain,
customer_id,
total_features_Curr,
final_Bill,
IF_Final_Bill,
LAG(total_features_Curr) OVER (PARTITION BY domain, customer_id ORDER BY Invoice_Date) AS Prev_Month_Total,
customer_name
From fix_total_features
)

insert into @temp1 
select Invoice_Date,
domain,
customer_id,
total_features_Curr,
final_Bill,
IF_Final_Bill,
Prev_Month_Total,
customer_name
FROM IF_dif 

DECLARE @Invoice_Date DATE, @domain VARCHAR(255), @customer_id int, 
        @total_features_Curr DECIMAL(18,2), @final_bill DECIMAL(18,2), 
        @IF_Final_Bill CHAR(1), @Prev_Month_Total DECIMAL(18,2), @customer_name varchar(250);

    DECLARE check_FinalBill cursor for  -- open loop

	SELECT Invoice_Date, domain, customer_id, total_features_Curr, final_Bill, IF_Final_Bill, Prev_Month_Total, customer_name
    FROM @temp1
    WHERE IF_Final_Bill = 'Y' -- only customers with final bill will enter the loop
    ORDER BY Invoice_Date, domain, customer_id -- Retrieving data from the loop

open check_FinalBill;
FETCH NEXT FROM check_FinalBill INTO @Invoice_Date, @domain, @customer_id, @total_features_Curr, @final_bill, @IF_Final_Bill, @Prev_Month_Total, @customer_name; --open the loop with the parameters

WHILE @@FETCH_STATUS = 0 -- only if the table isn't empty the loop will run, when the table will be empty the loop will stop
BEGIN 
    DECLARE @prev_month_ DATE = DATEFROMPARTS(YEAR(DATEADD(MONTH, 0, @Invoice_Date)), MONTH(DATEADD(MONTH, 0, @Invoice_Date)), 1) --the last month of the invoice date
    DECLARE @prev_month_1 DATE = DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, @Invoice_Date)), MONTH(DATEADD(MONTH, -1, @Invoice_Date)), 1) -- one month ago from the invoice date
	DECLARE @prev_month_2 DATE = DATEFROMPARTS(YEAR(DATEADD(MONTH, -2, @Invoice_Date)), MONTH(DATEADD(MONTH, -2, @Invoice_Date)), 1) -- two months ago from the invoice date

	DECLARE @prev_total DECIMAL(18,2);
	select @prev_total = Prev_Month_Total
	from @temp1
	where invoice_date = @prev_month_
	 AND domain = @domain 
     AND customer_id = @customer_id -- prev_value of the row the last month

	DECLARE @prev_1_total DECIMAL(18,2);
	select @prev_1_total = Prev_Month_Total
	from @temp1
	where invoice_date = @prev_month_1
	 AND domain = @domain 
     AND customer_id = @customer_id
     AND IF_Final_Bill = 'N';  -- prev_value of the row of one month ago

	DECLARE @prev_2_total DECIMAL(18,2);
	select @prev_2_total = Prev_Month_Total
	from @temp1
	where invoice_date = @prev_month_2
	 AND domain = @domain 
     AND customer_id = @customer_id
     AND IF_Final_Bill = 'N'; -- prev_value of the row of two months ago

	if @Invoice_Date >= @prev_month_ and @prev_total > 0 and @prev_total is not null-- if the total_features of last month (used lag function for prev_month_total) is greater then 0
	begin
	update @temp1 
	set final_bill = @prev_total
	where Invoice_Date = @prev_month_ and domain = @domain and customer_id = @customer_id
	end
	else if @Invoice_Date >= @prev_month_1 and @prev_1_total > 0 and @prev_1_total is not null and @prev_total is not null -- if the total_features of last 1 month (used lag function for prev_month_total) is greater then 0
	begin
	update @temp1 
	set final_bill = @prev_1_total
	where Invoice_Date = @prev_month_ and domain = @domain and customer_id = @customer_id
	end
	else if @Invoice_Date >= @prev_month_2 and @prev_2_total > 0 and @prev_2_total is not null and @prev_total is not null-- if the total_features of last 2 month (used lag function for prev_month_total) is greater then 0
	begin
	update @temp1 
	set final_bill = @prev_2_total
	where Invoice_Date = @prev_month_ and domain = @domain and customer_id = @customer_id
	end
FETCH NEXT FROM check_FinalBill INTO @Invoice_Date, @domain, @customer_id, @total_features_Curr, @final_bill, @IF_Final_Bill, @Prev_Month_Total, @customer_name; --open the loop with the parameters
END

close check_FinalBill;
deallocate check_FinalBill;

with DiffFeatures as (
Select invoice_date,
domain,
customer_id,
total_features_Curr,
final_bill,
IF_Final_Bill,
Prev_Month_Total,
CASE
  WHEN IF_Final_Bill = 'N' and (
								(total_features_Curr > Prev_Month_Total AND Prev_Month_Total = 0)
								OR (total_features_Curr = 0 AND Prev_Month_Total > 0)
								)
  THEN 0

  WHEN IF_Final_Bill = 'N' and not 
								(
								(total_features_Curr > Prev_Month_Total AND Prev_Month_Total = 0)
								OR (total_features_Curr = 0 AND Prev_Month_Total > 0)
								)
  THEN total_features_Curr - Prev_Month_Total

  When IF_Final_Bill = 'Y' Then NULL
END as DIF_Features,
customer_name
From @temp1
)
Select *
From DiffFeatures
where Invoice_Date >= dateadd(mm,-7,getdate()) -- update the last 6 months only 
END
