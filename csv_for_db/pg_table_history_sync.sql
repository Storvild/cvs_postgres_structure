-- Function: public.pg_table_history_sync()

-- DROP FUNCTION public.pg_table_history_sync();

CREATE OR REPLACE FUNCTION public.pg_table_history_sync()
  RETURNS text AS
$BODY$
DECLARE
  lres text;
BEGIN
  -- ВСТАВКА НОВЫХ ТАБЛИЦ
    INSERT INTO pg_table_history (
        code
        , addnew
        , deleted
        , relkind
        , sql_create
        , sql_column_comments
        , schemaname
        , tablename
        , description
    )

    WITH params AS ( 
        SELECT CAST('IF NOT EXISTS ' AS text) AS ifnotexists --В конце обязателен пробел ' '
    )
    , ftoptions AS (
        SELECT ftrelid, servername, string_agg(replace(ftoption,'=',' ''')||'''', ', ') AS ftoptions
        FROM (
            SELECT unnest(m.ftoptions) AS ftoption
                , m.ftrelid
                , srv.srvname AS servername 
            FROM pg_foreign_table m
            LEFT JOIN pg_foreign_server srv ON  srv.oid=m.ftserver
        ) m
        GROUP BY ftrelid, servername
    )

    , cols AS (
    -- Список таблиц, вьюшек (с типами полей, которых нет в бекапе sql-вьюшек), foreign_table    
        SELECT 
            n.nspname AS schema_name --m.table_catalog
            , c.relname AS table_name
            , a.attname AS column_name
            , pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type --integer, timestamp without time zone, character varying, text[]
            --, t.typname AS column_type --int4, timestamp, varchar, _text
            , CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE NULL END AS not_null 

            , a.attnum
            --, COALESCE('DEFAULT '||def.adsrc, '') AS default_value
            , 'DEFAULT '||pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS default_value
            , adesc.description AS att_desc
            , mdesc.description AS table_desc
            , a.attinhcount AS inherited_count
            , c.relkind
            , ft.ftoptions
            , ft.servername
            , pn.nspname||'.'||parent.relname AS inhtable
            , c.relhasoids -- После версии 10
            --, c.relhaspkey
            --, c.relchecks
            --, lag(adesc.description) OVER() AS prev_att_desc
        FROM pg_class c 
        LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
        INNER JOIN information_schema.tables m ON m.table_name=c.relname AND m.table_schema=n.nspname --26681
        
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_attrdef def ON def.adnum=a.attnum AND def.adrelid=c.oid
        LEFT JOIN pg_description adesc ON adesc.objoid=c.oid AND a.attnum = adesc.objsubid -- Описание поля
        LEFT JOIN pg_description mdesc ON mdesc.objoid=c.oid AND mdesc.objsubid = 0 -- Описание таблицы
        LEFT JOIN ftoptions ft ON ft.ftrelid=c.oid
        LEFT JOIN pg_inherits inh_link ON inh_link.inhrelid=c.oid
        LEFT JOIN pg_class parent ON parent.oid=inh_link.inhparent
        LEFT JOIN pg_namespace pn ON pn.oid=parent.relnamespace
        WHERE true
            AND a.atttypid>0
            --AND c.relname = 'wad_test'
            AND a.attnum > 0
            --AND c.relkind='f' --AND c.relkind NOT IN('r','v','f') --f-foreign table, r-table, v-view ('r','v')
            AND n.nspname NOT IN ('information_schema', 'pg_catalog') --cron
            --AND (n.nspname, c.relname) IN (SELECT table_schema, table_name FROM information_schema.tables)--28857
        ORDER BY n.nspname, c.relname, a.attnum
    )
    , pg_tables AS (
        SELECT 
            m.schema_name||'.'||m.table_name AS code
            , m.relkind
            , COALESCE('-- '||replace(m.table_desc,chr(10),chr(10)||'-- ')||chr(10), '')
              ||CASE WHEN m.relkind='r' THEN 'CREATE TABLE '||(SELECT ifnotexists FROM params)||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
                   WHEN m.relkind='f' THEN 'CREATE FOREIGN TABLE '||(SELECT ifnotexists FROM params)||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
                   WHEN m.relkind='v' THEN 'STRUCTURE VIEW '||m.schema_name||'.'||m.table_name||' ('||chr(10)
                   ELSE ''
              END
              || string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||chr(10) ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              --|| string_agg(COALESCE(' -- '||replace(m.prev_att_desc,chr(10),' '),'')||chr(10)||'    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ',' ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              --|| string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||COALESCE(' -- '||replace(m.prev_att_desc,chr(10),' '),'')||chr(10) ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              ||chr(10)||')' 
              ||COALESCE(chr(10)||'INHERITS ('||m.inhtable||')', '')
              ||CASE WHEN m.relhasoids THEN chr(10)||'WITH (OIDS=TRUE)' ELSE '' END --После версии 10
              ||chr(10)||';'
              AS sql_create
            , string_agg('COMMENT ON COLUMN '||m.schema_name||'.'||m.table_name||' IS '''||m.att_desc||'''', ';'||chr(10) ORDER BY m.attnum) AS sql_column_comments
            , m.schema_name AS schemaname
            , m.table_name AS tablename
            , m.table_desc AS description
        FROM cols m
        GROUP BY schema_name, table_name, m.inhtable, m.relkind, m.table_desc
                , m.relhasoids -- После версии 10
        ORDER BY m.schema_name, m.relkind, m.table_name
    )
    , histtables AS (
        SELECT DISTINCT ON (code) *
        FROM pg_table_history
        ORDER BY code, create_date DESC
    )
    
    SELECT p.code
        , true AS addnew
        , false AS deleted
        , p.relkind
        , p.sql_create
        , p.sql_column_comments
        , p.schemaname
        , p.tablename
        , p.description
    FROM pg_tables p
    WHERE code NOT IN (SELECT code FROM histtables);
    

  -- ОБНОВЛЕНИЕ ТАБЛИЦ
    INSERT INTO pg_table_history (
        code
        , addnew
        , deleted
        , relkind
        , sql_create
        , sql_column_comments
        , schemaname
        , tablename
        , description
    )
    WITH params AS ( 
        SELECT CAST('IF NOT EXISTS ' AS text) AS ifnotexists --В конце обязателен пробел ' '
    )
    , ftoptions AS (
        SELECT ftrelid, servername, string_agg(replace(ftoption,'=',' ''')||'''', ', ') AS ftoptions
        FROM (
            SELECT unnest(m.ftoptions) AS ftoption
                , m.ftrelid
                , srv.srvname AS servername 
            FROM pg_foreign_table m
            LEFT JOIN pg_foreign_server srv ON  srv.oid=m.ftserver
        ) m
        GROUP BY ftrelid, servername
    )

    , cols AS (
    -- Список таблиц, вьюшек (с типами полей, которых нет в бекапе sql-вьюшек), foreign_table    
        SELECT 
            n.nspname AS schema_name --m.table_catalog
            , c.relname AS table_name
            , a.attname AS column_name
            , pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type --integer, timestamp without time zone, character varying, text[]
            --, t.typname AS column_type --int4, timestamp, varchar, _text
            , CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE NULL END AS not_null 

            , a.attnum
            --, COALESCE('DEFAULT '||def.adsrc, '') AS default_value
            , 'DEFAULT '||pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS default_value
            , adesc.description AS att_desc
            , mdesc.description AS table_desc
            , a.attinhcount AS inherited_count
            , c.relkind
            , ft.ftoptions
            , ft.servername
            , pn.nspname||'.'||parent.relname AS inhtable
            , c.relhasoids -- После версии 10
            --, c.relhaspkey
            --, c.relchecks
            --, lag(adesc.description) OVER() AS prev_att_desc
        FROM pg_class c 
        LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
        INNER JOIN information_schema.tables m ON m.table_name=c.relname AND m.table_schema=n.nspname --26681
        
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_attrdef def ON def.adnum=a.attnum AND def.adrelid=c.oid
        LEFT JOIN pg_description adesc ON adesc.objoid=c.oid AND a.attnum = adesc.objsubid -- Описание поля
        LEFT JOIN pg_description mdesc ON mdesc.objoid=c.oid AND mdesc.objsubid = 0 -- Описание таблицы
        LEFT JOIN ftoptions ft ON ft.ftrelid=c.oid
        LEFT JOIN pg_inherits inh_link ON inh_link.inhrelid=c.oid
        LEFT JOIN pg_class parent ON parent.oid=inh_link.inhparent
        LEFT JOIN pg_namespace pn ON pn.oid=parent.relnamespace
        WHERE true
            AND a.atttypid>0
            --AND c.relname = 'wad_test'
            AND a.attnum > 0
            --AND c.relkind='f' --AND c.relkind NOT IN('r','v','f') --f-foreign table, r-table, v-view ('r','v')
            AND n.nspname NOT IN ('information_schema', 'pg_catalog') --cron
            --AND (n.nspname, c.relname) IN (SELECT table_schema, table_name FROM information_schema.tables)--28857
        ORDER BY n.nspname, c.relname, a.attnum
    )
    , pg_tables AS (
        SELECT 
            m.schema_name||'.'||m.table_name AS code
            , m.relkind
            , COALESCE('-- '||replace(m.table_desc,chr(10),chr(10)||'-- ')||chr(10), '')
              ||CASE WHEN m.relkind='r' THEN 'CREATE TABLE '||(SELECT ifnotexists FROM params)||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
                   WHEN m.relkind='f' THEN 'CREATE FOREIGN TABLE '||(SELECT ifnotexists FROM params)||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
                   WHEN m.relkind='v' THEN 'STRUCTURE VIEW '||m.schema_name||'.'||m.table_name||' ('||chr(10)
                   ELSE ''
              END
              || string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||chr(10) ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              --|| string_agg(COALESCE(' -- '||replace(m.prev_att_desc,chr(10),' '),'')||chr(10)||'    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ',' ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              --|| string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||COALESCE(' -- '||replace(m.prev_att_desc,chr(10),' '),'')||chr(10) ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              ||chr(10)||')' 
              ||COALESCE(chr(10)||'INHERITS ('||m.inhtable||')', '')
              ||CASE WHEN m.relhasoids THEN chr(10)||'WITH (OIDS=TRUE)' ELSE '' END --После версии 10
              ||chr(10)||';'
              AS sql_create
            , string_agg('COMMENT ON COLUMN '||m.schema_name||'.'||m.table_name||' IS '''||m.att_desc||'''', ';'||chr(10) ORDER BY m.attnum) AS sql_column_comments
            , m.schema_name AS schemaname
            , m.table_name AS tablename
            , m.table_desc AS description
        FROM cols m
        GROUP BY schema_name, table_name, m.inhtable, m.relkind, m.table_desc
                , m.relhasoids -- После версии 10
        ORDER BY m.schema_name, m.relkind, m.table_name
    )
    , histtables AS (
        SELECT DISTINCT ON (code) *
        FROM pg_table_history
        ORDER BY code, create_date DESC
    )

    SELECT p.code
        , false AS addnew
        , false AS deleted
        , p.relkind
        , p.sql_create
        , p.sql_column_comments
        , p.schemaname
        , p.tablename
        , p.description
    FROM histtables h
    INNER JOIN pg_tables p ON p.code=h.code
    WHERE COALESCE(p.sql_create,'')<>COALESCE(h.sql_create,'')
        OR COALESCE(p.sql_column_comments,'')<>COALESCE(h.sql_column_comments,'');
  


  -- УДАЛЕНИЕ ТАБЛИЦ
    INSERT INTO pg_table_history (
        code
        , addnew
        , deleted
        , relkind
        , sql_create
        , sql_column_comments
        , schemaname
        , tablename
        , description
    )
    WITH params AS ( 
        SELECT CAST('IF NOT EXISTS ' AS text) AS ifnotexists --В конце обязателен пробел ' '
    )
    , ftoptions AS (
        SELECT ftrelid, servername, string_agg(replace(ftoption,'=',' ''')||'''', ', ') AS ftoptions
        FROM (
            SELECT unnest(m.ftoptions) AS ftoption
                , m.ftrelid
                , srv.srvname AS servername 
            FROM pg_foreign_table m
            LEFT JOIN pg_foreign_server srv ON  srv.oid=m.ftserver
        ) m
        GROUP BY ftrelid, servername
    )

    , cols AS (
    -- Список таблиц, вьюшек (с типами полей, которых нет в бекапе sql-вьюшек), foreign_table    
        SELECT 
            n.nspname AS schema_name --m.table_catalog
            , c.relname AS table_name
            , a.attname AS column_name
            , pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type --integer, timestamp without time zone, character varying, text[]
            --, t.typname AS column_type --int4, timestamp, varchar, _text
            , CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE NULL END AS not_null 

            , a.attnum
            --, COALESCE('DEFAULT '||def.adsrc, '') AS default_value
            , 'DEFAULT '||pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS default_value
            , adesc.description AS att_desc
            , mdesc.description AS table_desc
            , a.attinhcount AS inherited_count
            , c.relkind
            , ft.ftoptions
            , ft.servername
            , pn.nspname||'.'||parent.relname AS inhtable
            , c.relhasoids -- После версии 10
            --, c.relhaspkey
            --, c.relchecks
            --, lag(adesc.description) OVER() AS prev_att_desc
        FROM pg_class c 
        LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
        INNER JOIN information_schema.tables m ON m.table_name=c.relname AND m.table_schema=n.nspname --26681
        
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_attrdef def ON def.adnum=a.attnum AND def.adrelid=c.oid
        LEFT JOIN pg_description adesc ON adesc.objoid=c.oid AND a.attnum = adesc.objsubid -- Описание поля
        LEFT JOIN pg_description mdesc ON mdesc.objoid=c.oid AND mdesc.objsubid = 0 -- Описание таблицы
        LEFT JOIN ftoptions ft ON ft.ftrelid=c.oid
        LEFT JOIN pg_inherits inh_link ON inh_link.inhrelid=c.oid
        LEFT JOIN pg_class parent ON parent.oid=inh_link.inhparent
        LEFT JOIN pg_namespace pn ON pn.oid=parent.relnamespace
        WHERE true
            AND a.atttypid>0
            --AND c.relname = 'wad_test'
            AND a.attnum > 0
            --AND c.relkind='f' --AND c.relkind NOT IN('r','v','f') --f-foreign table, r-table, v-view ('r','v')
            AND n.nspname NOT IN ('information_schema', 'pg_catalog') --cron
            --AND (n.nspname, c.relname) IN (SELECT table_schema, table_name FROM information_schema.tables)--28857
        ORDER BY n.nspname, c.relname, a.attnum
    )
    , pg_tables AS (
        SELECT 
            m.schema_name||'.'||m.table_name AS code
            , m.relkind
            , COALESCE('-- '||replace(m.table_desc,chr(10),chr(10)||'-- ')||chr(10), '')
              ||CASE WHEN m.relkind='r' THEN 'CREATE TABLE '||(SELECT ifnotexists FROM params)||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
                   WHEN m.relkind='f' THEN 'CREATE FOREIGN TABLE '||(SELECT ifnotexists FROM params)||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
                   WHEN m.relkind='v' THEN 'STRUCTURE VIEW '||m.schema_name||'.'||m.table_name||' ('||chr(10)
                   ELSE ''
              END
              || string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||chr(10) ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              --|| string_agg(COALESCE(' -- '||replace(m.prev_att_desc,chr(10),' '),'')||chr(10)||'    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ',' ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              --|| string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||COALESCE(' -- '||replace(m.prev_att_desc,chr(10),' '),'')||chr(10) ORDER BY m.attnum) --COALESCE(' -- '||replace(m.att_desc,chr(10),' '),'')
              ||chr(10)||')' 
              ||COALESCE(chr(10)||'INHERITS ('||m.inhtable||')', '')
              ||CASE WHEN m.relhasoids THEN chr(10)||'WITH (OIDS=TRUE)' ELSE '' END --После версии 10
              ||chr(10)||';'
              AS sql_create
            , string_agg('COMMENT ON COLUMN '||m.schema_name||'.'||m.table_name||' IS '''||m.att_desc||'''', ';'||chr(10) ORDER BY m.attnum) AS sql_column_comments
            , m.schema_name AS schemaname
            , m.table_name AS tablename
            , m.table_desc AS description
        FROM cols m
        GROUP BY schema_name, table_name, m.inhtable, m.relkind, m.table_desc
                , m.relhasoids -- После версии 10
        ORDER BY m.schema_name, m.relkind, m.table_name
    )
    , histtables AS (
        SELECT DISTINCT ON (code) *
        FROM pg_table_history
        ORDER BY code, create_date DESC
    )

    SELECT h.code
        , false AS addnew
        , true AS deleted
        , h.relkind
        , '' AS sql_create
        , '' AS sql_column_comments
        , h.schemaname
        , h.tablename
        , h.description
    FROM histtables h
    LEFT JOIN pg_tables p ON p.code=h.code
    WHERE p.code IS NULL
        AND h.deleted=false;

  lres = 'ok';
  RETURN lres;
END;
$BODY$
  LANGUAGE plpgsql --IMMUTABLE
  COST 100;

--SELECT pg_table_history_sync()
--SELECT * FROM pg_table_history ORDER BY uid DESC LIMIT 100
