SELECT *
FROM Openquery([DataWarehouse], '
    SELECT
        pj.id AS Project_ID, 
        CAST(pj.title AS VARCHAR(1000)) AS title,
        CAST(pj.created_at AS TIMESTAMP) AS created_at,
        CAST(pp.first_name || '' '' || pp.last_name AS VARCHAR(1000)) AS Creator,
        CAST(pp1.first_name || '' '' || pp1.last_name AS VARCHAR(1000)) AS manager,
        CAST(string_agg(pp2.first_name || '' '' || pp2.last_name, '', '') AS TEXT) AS assigned_users,
        CAST(pc."name" AS VARCHAR(1000)) AS Category,
        pj.start_date,
        pj.end_date,
        CAST(pj.updated_at AS TIMESTAMP) AS updated_at,
        CAST(pj.description AS TEXT) AS description
    FROM
        public.proofhub_projects pj
        LEFT JOIN public.proofhub_categories pc ON CAST(pj.category ->> ''id'' AS NUMERIC) = pc.id
        LEFT JOIN public.proofhub_people pp ON CAST(pj.creator ->> ''id'' AS NUMERIC) = pp.id
        LEFT JOIN public.proofhub_people pp1 ON CAST(pj.manager ->> ''id'' AS NUMERIC) = pp1.id
        LEFT JOIN LATERAL jsonb_array_elements_text(pj.assigned) AS assigned_id ON TRUE
        LEFT JOIN public.proofhub_people pp2 ON CAST(assigned_id AS NUMERIC) = pp2.id
    GROUP BY
        pj.id,
        pj.title,
        pp.first_name,
        pp.last_name,
        pp1.first_name,
        pp1.last_name,
        pc."name",
        pj.start_date,
        pj.end_date,
        pj.created_at,
        pj.updated_at,
        pj.description
');