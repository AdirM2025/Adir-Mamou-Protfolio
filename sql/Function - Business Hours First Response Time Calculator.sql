-- DROP FUNCTION public.business_hours_diff_for_leads(timestamptz, timestamptz);
CREATE OR REPLACE FUNCTION public.business_hours_diff_for_leads(lead_entered timestamp with time zone, first_interaction timestamp with time zone)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    current_ts TIMESTAMPTZ := lead_entered;
    total_hours NUMERIC := 0;
    WorkDay_start TIME := '09:00'; -- Company start hour
    WorkDay_end TIME := '18:00';   -- Company end hour
    today_business_start TIMESTAMPTZ; 
    today_business_end TIMESTAMPTZ;
BEGIN
    IF lead_entered >= first_interaction THEN
        RETURN 0;
    END IF;
	WHILE current_ts < first_interaction LOOP -- while LOOP -- start with lead_entered and +1 workDateTime every run
        IF EXTRACT(DOW FROM current_ts) NOT IN (5, 6) THEN -- won't calculate worktime for Friday and Saturday
            today_business_start := date_trunc('day', current_ts) + WorkDay_start; -- today_business_start = 09:00 of the current day being examined in the LOOP
            today_business_end   := date_trunc('day', current_ts) + WorkDay_end;   -- today_business_end   = 18:00 of the current day being examined in the LOOP
            IF current_ts < today_business_start 
            THEN
                current_ts := today_business_start; -- don't count hours before opening time if the lead entered before opening hours on the same business day
            END IF;
		    IF current_ts < today_business_end 
		    THEN
			    total_hours := total_hours +
                    EXTRACT(EPOCH FROM (
                        LEAST(first_interaction, today_business_end) -- whichever comes first: the interaction with the client or the end of the workday
                        - current_ts -- the dateTime from the LEAST function minus current_ts
                    )) / 3600;
            END IF;
        END IF;
	    current_ts := date_trunc('day', current_ts) + INTERVAL '1 day' + WorkDay_start;
	END LOOP;
	RETURN round(total_hours);
END;
$function$
