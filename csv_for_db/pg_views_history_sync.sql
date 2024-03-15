-- DROP FUNCTION public.pg_views_history_sync();

CREATE OR REPLACE FUNCTION public.pg_views_history_sync()
  RETURNS text AS
$BODY$
DECLARE
  lres text;
BEGIN
  -- ВСТАВКА НОВЫХ ФУНКЦИЙ
    INSERT INTO pg_views_history (
        code
        , addnew
        , deleted
        , sql_create
        , schemaname
        , viewname
        , definition
            )
    WITH pgviews AS (
        -- 1й вариант
        SELECT 1 AS creator
            , trim(table_schema||'.'||table_name) AS code
            , true AS addnew
            , false AS deleted
            , 'CREATE OR REPLACE VIEW '||table_schema||'.'||table_name||' AS'||chr(10)||pg_get_viewdef(table_schema||'.'||table_name, true) AS sql_create
            , table_schema AS schemaname
            , table_name AS viewname
            , view_definition AS definition
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    )
    , pgviews2 AS (
        -- 2й вариант
        SELECT DISTINCT ON (schemaname, viewname)
              1 AS creator
            , trim(p.schemaname||'.'||p.viewname) AS code
            , true AS addnew
            , false AS deleted
            , 'CREATE OR REPLACE VIEW '||schemaname||'.'||viewname||' AS'||chr(10)||pg_get_viewdef(schemaname||'.'||viewname, true) AS sql_create
            , p.schemaname
            , p.viewname
            , p.definition
        FROM pg_views p
        WHERE p.schemaname NOT IN ('pg_catalog', 'information_schema')
            AND schemaname NOT LIKE 'pg_temp%'
    )
    , histviews AS (
        SELECT DISTINCT ON (code) *
        FROM pg_views_history
        ORDER BY code, create_date DESC
    )
    SELECT p.code
        , true AS addnew
        , false AS deleted
        , p.sql_create
        , p.schemaname
        , p.viewname
        , p.definition
    FROM pgviews p
    WHERE (code) NOT IN (SELECT code FROM histviews);



  -- ОБНОВЛЕНИЕ ФУНКЦИЙ
    INSERT INTO btk_sys_pg_views_history (
        code
        , addnew
        , deleted
        , sql_create
        , schemaname
        , viewname
        , definition
    )
    WITH pgviews AS (
        -- 1й вариант
        SELECT 1 AS creator
            , trim(table_schema||'.'||table_name) AS code
            , true AS addnew
            , false AS deleted
            , 'CREATE OR REPLACE VIEW '||table_schema||'.'||table_name||' AS'||chr(10)||pg_get_viewdef(table_schema||'.'||table_name, true) AS sql_create
            , table_schema AS schemaname
            , table_name AS viewname
            , view_definition AS definition
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    )
    , histviews AS (
        SELECT DISTINCT ON (code)  *
        FROM pg_views_history
        ORDER BY code, create_date DESC
    )
    SELECT p.code
        , false AS addnew
        , false AS deleted
        , p.sql_create
        , p.schemaname
        , p.viewname
        , p.definition
    FROM histviews h
    INNER JOIN pgviews p ON p.code=h.code
    WHERE p.definition<>h.definition;
  


  -- УДАЛЕНИЕ ФУНКЦИЙ
    INSERT INTO pg_views_history (
        code
        , addnew
        , deleted
        , sql_create
        , schemaname
        , viewname
        , definition
    )
    WITH pgviews AS (
        -- 1й вариант
        SELECT 1 AS creator
            , trim(table_schema||'.'||table_name) AS code
            , true AS addnew
            , false AS deleted
            , 'CREATE OR REPLACE VIEW '||table_schema||'.'||table_name||' AS'||chr(10)||pg_get_viewdef(table_schema||'.'||table_name, true) AS sql_create
            , table_schema AS schemaname
            , table_name AS viewname
            , view_definition AS definition
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    )
    , histviews AS (
        SELECT DISTINCT ON (code)  *
        FROM pg_views_history
        ORDER BY code, create_date DESC
    )

    SELECT h.code
        , false AS addnew
        , true AS deleted
        , '' AS sql_create
        , h.schemaname
        , h.viewname
        , '' AS definition
    FROM histviews h
    LEFT JOIN pgviews p ON p.code=h.code
    WHERE p.viewname IS NULL
        AND h.deleted=false;

  lres = 'ok';
  RETURN lres;
END;
$BODY$
  LANGUAGE plpgsql --IMMUTABLE
  COST 100;


-- Запуск синхронизации
-- SELECT pg_views_history_sync()
-- Выборка данных
-- SELECT * FROM pg_views_history ORDER BY create_date DESC
