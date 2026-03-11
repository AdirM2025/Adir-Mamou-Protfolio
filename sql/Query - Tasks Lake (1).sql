SELECT *
From openquery ([DataWarehouse],'select
									cast(t.id as bigint) as "Task_ID",
									ticket as "Ticket_Number",
									project ->> ''id'' as "Project_ID",
									list ->> ''name'' as "List_Name",
									stage ->> ''name'' as "Stage_Name",
									t.title,	
									pp.first_name ||'' '' ||pp.last_name as creator,
									CAST(string_agg(pp2.first_name || '' '' || pp2.last_name, '', '') AS TEXT) AS assigned_users,
									cast("comments" as int),
									percent_progress,									
									cast(parent_id as bigint) as "Parent_Task",
									cast(sub_tasks as int),
									cast(t.created_at as timestamp),
									start_date,
									cast(t.updated_at as timestamp),
									LEFT(regexp_replace(
										replace(replace(description, ''&lt;'', ''<''), ''&gt;'', ''>''), 
										''<[^>]*>'', '''', ''g''
									), 3800) AS description,
									completed,
									cast(completed_at as timestamp),
									pp1.first_name ||'' '' ||pp1.last_name as completed_by,
									cast(estimated_mins as int),
									cast(estimated_hours as int)
								from
									public.proofhub_tasks t
									LEFT JOIN public.proofhub_people pp ON CAST(t.creator ->> ''id'' AS NUMERIC) = pp.id 
									LEFT JOIN public.proofhub_people pp1 ON completed_by = pp1.id 
									LEFT JOIN LATERAL jsonb_array_elements_text(t.assigned) AS assigned_id ON TRUE
									LEFT JOIN public.proofhub_people pp2 ON CAST(assigned_id AS NUMERIC) = pp2.id
								group by 
									t.id,
									list ->> ''name'', 
									stage ->> ''name'', 
									t.title, 
									ticket, 
									pp.first_name, 
									pp.last_name, 
									project ->> ''id'', 
									assigned, 
									"comments",
									completed, 
									parent_id, 
									sub_tasks, 
									t.created_at, 
									start_date, 
									t.updated_at, 
									LEFT(regexp_replace(
										replace(replace(description, ''&lt;'', ''<''), ''&gt;'', ''>''), 
										''<[^>]*>'', '''', ''g''
									), 3800),
									completed_at, 
									pp1.first_name, 
									pp1.last_name, 
									estimated_mins, 
									estimated_hours, 
									percent_progress;')