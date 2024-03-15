-- DROP FUNCTION public.pg_proc_history_sync();

CREATE OR REPLACE FUNCTION public.pg_proc_history_sync()
  RETURNS text AS
$BODY$
DECLARE
  lres text;
BEGIN
  -- ВСТАВКА НОВЫХ ФУНКЦИЙ
    INSERT INTO pg_proc_history (
        code
        , prorettype_name
        , prolang_name
        , addnew
        , deleted
        , sql_create
        , prodescription
        , pronamespace_name
        , proname
        , prosrc
    )
    WITH pgfunc AS (
        SELECT DISTINCT ON (n.nspname, pr.proname, pr.proargnames, pr.proargtypes)
              n.nspname AS pronamespace_name
            , pr.proname
            , pr.proargnames
            , pr.proargtypes
            , pr.prorettype
            , pr.prosrc
            , pr.pronamespace
            , pr.prolang
            , l.lanname AS prolang_name
            , pr.oid AS pro_oid
            , pr.proowner
            , trimex(CAST(obj_description(pr.oid,'pg_proc') AS varchar)) AS prodescription
            --, substring(pg_get_functiondef(pr.oid),28,strpos(pg_get_functiondef(pr.oid),E'\n')-28) AS code -- вытаскиваем имя функции с параметрами из 
            --, n.nspname||'.'||pr.proname||'('||pg_get_function_arguments(pr.oid)||')' AS code -- список аргументов из определения функции (со значениями по умолчанию)
            , n.nspname||'.'||pr.proname||'('||pg_get_function_identity_arguments(pr.oid)||')' AS code --список аргументов, идентифицирующий функцию (без значений по умолчанию)
            , pg_get_functiondef(pr.oid) AS sql_create
            , CAST(format_type(pr.prorettype,NULL) AS varchar) AS prorettype_name
        FROM pg_proc pr
        LEFT JOIN pg_namespace n ON n.oid=pr.pronamespace
        LEFT JOIN pg_language l ON l.oid=pr.prolang
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'cron')
            --AND pr.proisagg = false -- Для агрегатных ф-ций не работает получение sql pg_get_functiondef
            AND pr.prokind <> 'a' -- С Postgres 11 вместо признака proisagg=true появился prokind='a' 
            
    )
    , histfunc AS (
        --SELECT DISTINCT ON (pronamespace_name, proname, proargnames, proargtypes)  * 
        --FROM btk_sys_pg_proc_history
        --ORDER BY pronamespace_name, proname, proargnames, proargtypes, create_date DESC
        SELECT DISTINCT ON (code)  * 
        FROM pg_proc_history
        ORDER BY code, create_date DESC
    )
    SELECT 
          p.code
        , p.prorettype_name
        , p.prolang_name
        , true AS addnew
        , false AS deleted
        , p.sql_create
        , p.prodescription
        , p.pronamespace_name
        , p.proname
        , p.prosrc
    FROM pgfunc p
    --WHERE (pronamespace_name, proname, proargnames, proargtypes) NOT IN (SELECT pronamespace_name, proname, proargnames, proargtypes FROM histfunc);
    WHERE (code) NOT IN (SELECT code FROM histfunc);


  -- ОБНОВЛЕНИЕ ФУНКЦИЙ
    INSERT INTO pg_proc_history (
        code
        , prorettype_name
        , prolang_name
        , addnew
        , deleted
        , sql_create
        , prodescription
        , pronamespace_name
        , proname
        , prosrc
    )
    WITH pgfunc AS (
        SELECT DISTINCT ON (n.nspname, pr.proname, pr.proargnames, pr.proargtypes)
              n.nspname AS pronamespace_name
            , pr.proname
            , pr.proargnames
            , pr.proargtypes
            , pr.prorettype
            , pr.prosrc
            , pr.pronamespace
            , pr.prolang
            , l.lanname AS prolang_name
            , pr.oid AS pro_oid
            , pr.proowner
            , trimex(CAST(obj_description(pr.oid,'pg_proc') AS varchar)) AS prodescription
            --, substring(pg_get_functiondef(pr.oid),28,strpos(pg_get_functiondef(pr.oid),E'\n')-28) AS code
            --, n.nspname||'.'||pr.proname||'('||pg_get_function_arguments(pr.oid)||')' AS code -- список аргументов из определения функции (со значениями по умолчанию)
            , n.nspname||'.'||pr.proname||'('||pg_get_function_identity_arguments(pr.oid)||')' AS code --список аргументов, идентифицирующий функцию (без значений по умолчанию)
            , pg_get_functiondef(pr.oid) AS sql_create
            , CAST(format_type(pr.prorettype,NULL) AS varchar) AS prorettype_name
        FROM pg_proc pr
        LEFT JOIN pg_namespace n ON n.oid=pr.pronamespace
        LEFT JOIN pg_language l ON l.oid=pr.prolang
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'cron')
            --AND pr.proisagg=false
            AND pr.prokind <> 'a' -- С Postgres 11 вместо признака proisagg=true появился prokind='a' 
    )
    , histfunc AS (
        SELECT DISTINCT ON (code)  * 
        FROM pg_proc_history
        ORDER BY code, create_date DESC
    )
    SELECT 
          p.code
        , p.prorettype_name
        , p.prolang_name
        , false AS addnew
        , false AS deleted
        , p.sql_create
        , p.prodescription
        , p.pronamespace_name
        , p.proname
        , p.prosrc
    FROM histfunc h
    --INNER JOIN pgfunc p ON COALESCE(p.pronamespace,0)=COALESCE(h.pronamespace,0) AND COALESCE(p.proname,'')=COALESCE(h.proname,'') AND COALESCE(p.proargnames,ARRAY[]::text[])=COALESCE(h.proargnames,ARRAY[]::text[]) AND p.proargtypes=h.proargtypes
    INNER JOIN pgfunc p ON p.code=h.code --COALESCE(p.pronamespace,0)=COALESCE(h.pronamespace,0) AND COALESCE(p.proname,'')=COALESCE(h.proname,'') AND COALESCE(p.proargnames,ARRAY[]::text[])=COALESCE(h.proargnames,ARRAY[]::text[]) AND p.proargtypes=h.proargtypes
    WHERE p.prosrc<>h.prosrc
        OR p.prodescription<>h.prodescription
        OR p.prolang_name<>h.prolang_name
        ;
  


  -- УДАЛЕНИЕ ФУНКЦИЙ
    INSERT INTO pg_proc_history (
        code
        , prorettype_name
        , prolang_name
        , addnew
        , deleted
        , sql_create
        , prodescription
        , pronamespace_name
        , proname
        , prosrc
    )
    WITH pgfunc AS (
        SELECT DISTINCT ON (n.nspname, pr.proname, pr.proargnames, pr.proargtypes)
              n.nspname AS pronamespace_name
            , pr.proname
            , pr.proargnames
            , pr.proargtypes
            , pr.prorettype
            , pr.prosrc
            , pr.pronamespace
            , pr.prolang
            , l.lanname AS prolang_name
            , pr.oid AS pro_oid
            , pr.proowner
            , trimex(CAST(obj_description(pr.oid,'pg_proc') AS varchar)) AS prodescription
            --, substring(pg_get_functiondef(pr.oid),28,strpos(pg_get_functiondef(pr.oid),E'\n')-28) AS code
            --, n.nspname||'.'||pr.proname||'('||pg_get_function_arguments(pr.oid)||')' AS code -- список аргументов из определения функции (со значениями по умолчанию)
            , n.nspname||'.'||pr.proname||'('||pg_get_function_identity_arguments(pr.oid)||')' AS code --список аргументов, идентифицирующий функцию (без значений по умолчанию)
            , pg_get_functiondef(pr.oid) AS sql_create
            , CAST(format_type(pr.prorettype,NULL) AS varchar) AS prorettype_name
              
        FROM pg_proc pr
        LEFT JOIN pg_namespace n ON n.oid=pr.pronamespace
        LEFT JOIN pg_language l ON l.oid=pr.prolang
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'cron')
            --AND pr.proisagg=false --Для агрегатных ф-ций не работает получение sql pg_get_functiondef
            AND pr.prokind <> 'a' -- С Postgres 11 вместо признака proisagg=true появился prokind='a' 
    ) 
    , histfunc AS (
        SELECT DISTINCT ON (code)  * 
        FROM pg_proc_history
        ORDER BY code, create_date DESC
    )
    SELECT 
          h.code
        , h.prorettype_name
        , h.prolang_name
        , false AS addnew
        , true AS deleted
        , '' AS sql_create
        , NULL AS prodescription
        , h.pronamespace_name
        , h.proname
        , '' AS prosrc
    FROM histfunc h
    LEFT JOIN pgfunc p ON p.code=h.code --COALESCE(p.pronamespace,0)=COALESCE(h.pronamespace,0) AND COALESCE(p.proname,'')=COALESCE(h.proname,'') AND COALESCE(p.proargnames,ARRAY[]::text[])=COALESCE(h.proargnames,ARRAY[]::text[]) AND p.proargtypes=h.proargtypes
    WHERE p.code IS NULL
        AND h.deleted=false;

  lres = 'ok';
  RETURN lres;
END;
$BODY$
  LANGUAGE plpgsql --IMMUTABLE
  COST 100;


-- Запуск синхронизации
-- SELECT pg_proc_history_sync()
-- Выборка данных
-- SELECT * FROM pg_proc_history ORDER BY create_date DESC