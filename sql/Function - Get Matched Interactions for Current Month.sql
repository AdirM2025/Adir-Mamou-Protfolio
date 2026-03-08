CREATE OR REPLACE FUNCTION public.get_matched_interactions()
 RETURNS TABLE("Customer_IDs" character varying, "Type Of Interaction" character varying, "Date" timestamp without time zone, "Duration" integer, "Direction" character varying, "Status" character varying, "Phone Number" character varying, "User" character varying, "Department" character varying, "From Email" character varying, "To Email" character varying)
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        i.customer_ids,
        i.interaction,
        i."date",
        i.duration,
        COALESCE(i.direction, 'NONE') AS direction,
        CASE 
			WHEN i.interaction = 'email' THEN 'Sent'
			 ELSE i.status 
		END AS status,
        i.phone_number,
        i.admin_user_name,
        i.department,
        i.from_email,
        i.to_email
    FROM public.interactions i
    WHERE i.products = 'lead - product_a'
      AND EXISTS (
          SELECT 1
          FROM public."Leads" l
          WHERE l.product = 'product_a'
            AND date_trunc('month', l.date_entered) = date_trunc('month', CURRENT_DATE)
            AND i.customer_ids LIKE '%' || l.leadid || '%'
      );
$function$
