-- public.vw_csm_billing_summary source

CREATE OR REPLACE VIEW public.vw_csm_billing_summary
AS WITH first_columns AS (
         SELECT billing_csm_data.invoice_date,
            sum(
                CASE
                    WHEN billing_csm_data.if_final_bill = 'N'::bpchar THEN billing_csm_data.current_month_total_features
                    ELSE 0::numeric
                END) AS "Total_Features",
            sum(
                CASE
                    WHEN billing_csm_data.if_final_bill = 'Y'::bpchar THEN billing_csm_data.final_bill
                    ELSE 0::numeric
                END) AS final_bill,
            sum(billing_csm_data.diff_features) AS total_features_difference,
            sum(
                CASE
                    WHEN billing_csm_data.diff_features < 0::numeric THEN billing_csm_data.diff_features
                    ELSE NULL::numeric
                END) AS total_decreasing_features
           FROM billing_csm_data
          GROUP BY billing_csm_data.invoice_date
        )
 SELECT invoice_date,
    "Total_Features",
    final_bill,
    total_features_difference,
    total_features_difference - final_bill AS "FeatureDiffMinusFinalBill",
    total_decreasing_features
   FROM first_columns
  ORDER BY invoice_date;
