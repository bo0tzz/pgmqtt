-- mqtt_retained_expires_at was originally declared IMMUTABLE in migration
-- 0004 even though its body calls now() — a function that depends on the
-- current transaction time cannot honour the IMMUTABLE contract. The planner
-- is free to fold IMMUTABLE function calls at plan time, which means an
-- index expression or a constant-folded INSERT could materialise the wrong
-- timestamp. Re-declare as STABLE: same within a single statement, free to
-- change between statements (now()'s contract).
CREATE OR REPLACE FUNCTION mqtt_retained_expires_at(p_props JSONB)
RETURNS TIMESTAMPTZ LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN p_props IS NULL THEN NULL
        WHEN COALESCE((p_props->>'me')::int, 0) <= 0 THEN NULL
        ELSE now() + make_interval(secs => (p_props->>'me')::int)
    END;
$$;
