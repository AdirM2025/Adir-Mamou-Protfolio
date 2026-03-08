-- public.vw_sales_leads_score source
CREATE OR REPLACE VIEW public.vw_sales_leads_score
AS WITH matchedinteractions AS (
         SELECT get_matched_interactions_optimized."Customer_IDs",
            get_matched_interactions_optimized."Type Of Interaction",
            get_matched_interactions_optimized."Date",
            get_matched_interactions_optimized."Duration",
            get_matched_interactions_optimized."Direction",
            get_matched_interactions_optimized."Status",
            get_matched_interactions_optimized."Phone Number",
            get_matched_interactions_optimized."User",
            get_matched_interactions_optimized."Department",
            get_matched_interactions_optimized."From Email",
            get_matched_interactions_optimized."To Email"
           FROM get_matched_interactions_optimized() get_matched_interactions_optimized("Customer_IDs", "Type Of Interaction", "Date", "Duration", "Direction", "Status", "Phone Number", "User", "Department", "From Email", "To Email")
        ), activeleads AS (
         SELECT l.leadid,
            (l.first_name::text || ' '::text) || l.last_name::text AS full_name,
            l.date_entered,
            l.lead_source,
                CASE
                    WHEN l.country::text = '0'::text OR l.country::text = ''::text THEN 'None'::character varying
                    ELSE l.country
                END AS "Country",
            l.assign_to,
            l.status,
            l.leadlifetime AS "LeadLifeTime(Days)",
                CASE
                    WHEN l.accountid::text = lc.accountid::text THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Sold (Y/N)"
           FROM "Leads" l
             LEFT JOIN customers lc ON lc.accountid::text = l.accountid::text
          WHERE l.product::text = 'product_a'::text
            AND l.date_entered >= date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date
            AND l.leadid::text <> 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'::text  -- exclude internal test record
        ), leadinteractions AS (
         SELECT ll.leadid,
            ll.full_name,
            ll.date_entered,
            ll.lead_source,
                CASE
                    WHEN i."Type Of Interaction"::text = 'call'::text THEN 'Phone'::text
                    WHEN i."Type Of Interaction"::text = 'omni_channel'::text THEN 'WhatsApp'::text
                    WHEN i."Type Of Interaction"::text = 'email'::text THEN 'Email'::text
                    ELSE 'Unknown'::text
                END AS "Entry Channel",
            ll."Country",
            ll.assign_to,
            ll.status,
            ll."Sold (Y/N)",
            ll."LeadLifeTime(Days)",
            i."Date" AS "First Interaction DateTime"
           FROM activeleads ll
             LEFT JOIN LATERAL ( SELECT mi."Type Of Interaction",
                    mi."Date"
                   FROM matchedinteractions mi
                  WHERE mi."Customer_IDs"::text ~~ (('%'::text || ll.leadid::text) || '%'::text) AND mi."Date" >= ll.date_entered
                  ORDER BY mi."Date"
                 LIMIT 1) i ON true
          WHERE ll.full_name !~~* '%test%'::text
        ), unique_leads_per_customer AS (
         SELECT DISTINCT mi."Customer_IDs",
            unnest(string_to_array(mi."Customer_IDs"::text, ','::text)) AS leadid_from_interactions
           FROM leadinteractions lis
             LEFT JOIN matchedinteractions mi ON mi."Customer_IDs"::text ~~ (('%'::text || lis.leadid::text) || '%'::text)
        ), nextleadpercustomer AS (
         SELECT DISTINCT ulpc.leadid_from_interactions,
            lis.date_entered,
            lead(lis.date_entered) OVER (PARTITION BY ulpc."Customer_IDs" ORDER BY lis.date_entered) AS next_date_entered_lead,
            lag(lis.status) OVER (PARTITION BY ulpc."Customer_IDs" ORDER BY lis.date_entered) AS "Previous Status",
                CASE
                    WHEN
                    CASE
                        WHEN ulpc.leadid_from_interactions IS NOT NULL AND ulpc.leadid_from_interactions <> ''::text THEN array_length(string_to_array(ulpc."Customer_IDs"::text, ','::text), 1)
                        ELSE 0
                    END > 1 THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Is Returning",
            array_to_string(array_remove(string_to_array(ulpc."Customer_IDs"::text, ','::text), ulpc.leadid_from_interactions), ','::text) AS "Previous LeadIDs"
           FROM unique_leads_per_customer ulpc
             LEFT JOIN leadinteractions lis ON lis.leadid::text = ulpc.leadid_from_interactions
        ), calculated_values AS (
         SELECT lis.leadid,
            lis.full_name,
            lis.date_entered,
            lis.lead_source,
            lis."Entry Channel",
            lis."Country",
            lis.assign_to,
            lis.status,
            lis."Sold (Y/N)",
            lis."LeadLifeTime(Days)",
            lis."First Interaction DateTime",
                CASE
                    WHEN lis."First Interaction DateTime" IS NULL THEN NULL::double precision
                    ELSE business_hours_diff_for_leads(lis.date_entered::timestamp with time zone, lis."First Interaction DateTime"::timestamp with time zone)::double precision
                END AS "First Response Time (hrs)",
            COALESCE(nlpc."Is Returning", 'N'::text) AS "Is Returning",
            COALESCE(nlpc."Previous LeadIDs", 'None'::text) AS "Previous LeadIDs",
            nlpc."Previous Status",
            sum(
                CASE
                    WHEN mi."Date" >= lis.date_entered AND (mi."Date" < nlpc.next_date_entered_lead OR nlpc.next_date_entered_lead IS NULL) THEN 1
                    ELSE 0
                END) AS "Total Interactions",
            sum(
                CASE
                    WHEN mi."Status"::text = 'answered'::text AND mi."Date" >= lis.date_entered AND (mi."Date" < nlpc.next_date_entered_lead OR nlpc.next_date_entered_lead IS NULL) THEN 1
                    ELSE 0
                END) AS "Answered Calls",
            sum(
                CASE
                    WHEN mi."Status"::text = 'not_answered'::text AND mi."Date" >= lis.date_entered AND (mi."Date" < nlpc.next_date_entered_lead OR nlpc.next_date_entered_lead IS NULL) THEN 1
                    ELSE 0
                END) AS "Unanswered Calls",
            sum(
                CASE
                    WHEN mi."Type Of Interaction"::text = 'omni_channel'::text AND mi."Date" >= lis.date_entered AND (mi."Date" < nlpc.next_date_entered_lead OR nlpc.next_date_entered_lead IS NULL) THEN 1
                    ELSE 0
                END) AS "WhatsApp Messages",
            sum(
                CASE
                    WHEN mi."Type Of Interaction"::text = 'email'::text AND mi."Date" >= lis.date_entered AND (mi."Date" < nlpc.next_date_entered_lead OR nlpc.next_date_entered_lead IS NULL) THEN 1
                    ELSE 0
                END) AS "Emails"
           FROM leadinteractions lis
             LEFT JOIN nextleadpercustomer nlpc ON lis.leadid::text = nlpc.leadid_from_interactions
             LEFT JOIN matchedinteractions mi ON mi."Customer_IDs"::text ~~ (('%'::text || lis.leadid::text) || '%'::text)
          GROUP BY lis.leadid, lis.full_name, lis.date_entered, lis.lead_source, lis."Entry Channel", lis."Country", lis.assign_to, lis.status, lis."Sold (Y/N)", lis."LeadLifeTime(Days)", lis."First Interaction DateTime", nlpc.next_date_entered_lead, nlpc."Is Returning", nlpc."Previous LeadIDs", (business_hours_diff_for_leads(lis.date_entered::timestamp with time zone, lis."First Interaction DateTime"::timestamp with time zone)::double precision), nlpc."Previous Status"
        ), flags AS (
         SELECT cv.leadid,
            cv.full_name,
            cv.date_entered,
            cv.lead_source,
            cv."Entry Channel",
            cv."Country",
            cv.assign_to,
            cv.status,
            cv."Sold (Y/N)",
            cv."LeadLifeTime(Days)",
            cv."First Interaction DateTime",
            cv."First Response Time (hrs)",
            cv."Is Returning",
            cv."Previous LeadIDs",
            cv."Previous Status",
            cv."Total Interactions",
            cv."Answered Calls",
            cv."Unanswered Calls",
            cv."WhatsApp Messages",
            cv."Emails",
                CASE
                    WHEN cv."First Response Time (hrs)" > 4::double precision THEN 'Y'::text
                    ELSE 'N'::text
                END AS "SLA Breach",
                CASE
                    WHEN cv.status::text = 'New'::text AND cv."LeadLifeTime(Days)" > 1 THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Stale NEW",
                CASE
                    WHEN cv."Total Interactions" = 0 AND cv.status::text <> 'Converted'::text AND cv.lead_source::text <> 'Internet Site'::text THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Zero Contact",
                CASE
                    WHEN cv.assign_to::text = 'unassigned'::text THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Unassigned",
                CASE
                    WHEN cv."Is Returning" = 'Y'::text AND cv."Previous Status"::text <> 'Converted'::text AND cv."First Response Time (hrs)" > 4::double precision THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Returning Lost",
                CASE
                    WHEN cv."Answered Calls" = 0 AND cv."Unanswered Calls" = 0 THEN 'Y'::text
                    ELSE 'N'::text
                END AS "No Outbound Calls",
                CASE
                    WHEN cv."LeadLifeTime(Days)" >= 5 THEN 'Y'::text
                    ELSE 'N'::text
                END AS "Overdue LifeTime"
           FROM calculated_values cv
        ), leadscore AS (
         SELECT f.leadid,
            f.full_name,
            f.date_entered,
            f.lead_source,
            f."Entry Channel",
            f."Country",
            f.assign_to,
            f.status,
            f."Sold (Y/N)",
            f."LeadLifeTime(Days)",
            f."First Interaction DateTime",
            f."First Response Time (hrs)",
            f."Is Returning",
            f."Previous LeadIDs",
            f."Previous Status",
            f."Total Interactions",
            f."Answered Calls",
            f."Unanswered Calls",
            f."WhatsApp Messages",
            f."Emails",
            f."SLA Breach",
            f."Stale NEW",
            f."Zero Contact",
            f."Unassigned",
            f."Returning Lost",
            f."No Outbound Calls",
            f."Overdue LifeTime",
                CASE
                    WHEN f."SLA Breach" = 'Y'::text OR f."Unassigned" = 'Y'::text OR f."Returning Lost" = 'Y'::text OR f."Overdue LifeTime" = 'Y'::text THEN 'Critical'::text
                    WHEN f."Stale NEW" = 'Y'::text OR f."Zero Contact" = 'Y'::text OR f."No Outbound Calls" = 'Y'::text THEN 'Problematic'::text
                    WHEN f."SLA Breach" = 'N'::text AND f."Unassigned" = 'N'::text AND f."Stale NEW" = 'N'::text AND f."Zero Contact" = 'N'::text AND f."No Outbound Calls" = 'N'::text AND f."Overdue LifeTime" = 'N'::text AND f."Returning Lost" = 'N'::text AND f."Answered Calls" >= 1 AND f."First Response Time (hrs)" <= 2::double precision AND f."LeadLifeTime(Days)" < 5 THEN 'Excellent'::text
                    ELSE 'Good'::text
                END AS "Lead Quality Score"
           FROM flags f
        ), salescore AS (
         SELECT leadscore.leadid,
            leadscore.full_name,
            leadscore.date_entered,
            leadscore.lead_source,
            leadscore."Entry Channel",
            leadscore."Country",
            leadscore.assign_to,
            leadscore.status,
            leadscore."Sold (Y/N)",
            leadscore."LeadLifeTime(Days)",
            leadscore."First Interaction DateTime",
            leadscore."First Response Time (hrs)",
            leadscore."Is Returning",
            leadscore."Previous LeadIDs",
            leadscore."Previous Status",
            leadscore."Total Interactions",
            leadscore."Answered Calls",
            leadscore."Unanswered Calls",
            leadscore."WhatsApp Messages",
            leadscore."Emails",
            leadscore."SLA Breach",
            leadscore."Stale NEW",
            leadscore."Zero Contact",
            leadscore."Unassigned",
            leadscore."Returning Lost",
            leadscore."No Outbound Calls",
            leadscore."Overdue LifeTime",
            leadscore."Lead Quality Score",
                CASE
                    WHEN leadscore."Lead Quality Score" = 'Critical'::text THEN 1
                    WHEN leadscore."Lead Quality Score" = 'Problematic'::text THEN 2
                    WHEN leadscore."Lead Quality Score" = 'Good'::text THEN 3
                    WHEN leadscore."Lead Quality Score" = 'Excellent'::text THEN 5
                    ELSE NULL::integer
                END AS leadscoreforcalc,
                CASE
                    WHEN leadscore."Sold (Y/N)" = 'Y'::text AND leadscore.status::text = 'Converted'::text THEN 5
                    ELSE 0
                END AS salescore
           FROM leadscore
        )
 SELECT leadid,
    full_name,
    date_entered,
    lead_source,
    "Entry Channel",
    "Country",
    assign_to,
    status,
    "Sold (Y/N)",
    "LeadLifeTime(Days)",
    "First Interaction DateTime",
    "First Response Time (hrs)",
    "Is Returning",
    "Previous LeadIDs",
    "Previous Status",
    "Total Interactions",
    "Answered Calls",
    "Unanswered Calls",
    "WhatsApp Messages",
    "Emails",
    "SLA Breach",
    "Stale NEW",
    "Zero Contact",
    "Unassigned",
    "Returning Lost",
    "No Outbound Calls",
    "Overdue LifeTime",
    "Lead Quality Score",
    salescore + leadscoreforcalc AS "Lead Total Score"
   FROM salescore ss;
