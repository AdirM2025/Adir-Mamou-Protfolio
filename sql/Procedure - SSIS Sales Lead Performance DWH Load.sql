USE [SalesDB]
GO

/****** Object: StoredProcedure [dbo].[SSIS_product_a_sales_lead_performance] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Adir Mamou
-- Create date: 2026-02-26
-- Description: Product A Sales Performance With Current Month Leads
-- =============================================
ALTER PROCEDURE [dbo].[SSIS_product_a_sales_lead_performance]
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

    -- Fetch the current month leads score view from the data warehouse
    SELECT * FROM OPENQUERY([DataWarehouse], 'SELECT * FROM public.vw_sales_leads_score')

END
