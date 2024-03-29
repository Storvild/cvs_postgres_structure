"""
    Скрипт создающий структуру каталогов и файлов в соответствии с БД

    Настройки подключения к БД настраиваются в файле csv_pg_git_config.py
    Пример содержимого настроечного файла находится в csv_pg_git_config.py.sample
    После запуска скрипта создается каталог именем БД (если в настройках стоит DIRNAME_BY_DBNAME = True)
        Внутри создаются каталоги с именами схем БД (если в настройках стоит DIRNAME_BY_SCHEMA = True)
        Далее создаются каталоги для хранения файлов со структурами  таблиц, Views, функций
        (имена каталогов также настраиваются в конфигурационном файле).

    @author: Vadim_S
    @license:  MIT License
"""
import psycopg2
import sys
import os

try:
    from csv_pg_git_config import *
except:
    raise Exception('Ошибка! Не найден файл pg_git_config.py. Пример содержимого в файле pg_git_config.py.sample')


# Если указана директория, то устанавливаем ее как текущую, иначе текущей становится директория где лежит py скрипт
if DIRNAME_BASE:
    os.chdir(DIRNAME_BASE)
else:
    os.chdir(os.path.join(os.path.abspath(os.path.dirname(__file__)), DIRNAME_BASE))

print(f'Python: {sys.version}')
#print(f'Версия libpq: {psycopg2.__libpq_version__}')


def get_pg_server_encoding(dbhost, dbport, dbname, dbuser, dbpassword) -> str:
    with psycopg2.connect(f'host={dbhost} port={dbport} dbname={dbname} user={dbuser} password={dbpassword}') as conn:
        cur = conn.cursor()
        sql = 'SHOW server_encoding;'
        cur.execute(sql)
        ver_ = cur.fetchone()
        ver = ver_[0]
        return ver


def get_pg_version_full(dbhost, dbport, dbname, dbuser, dbpassword) -> str:
    """
    Получение полной версии Postgres
    :return: PostgreSQL 9.6.3 on x86_64-pc-linux-gnu, compiled by gcc (Debian 4.9.2-10) 4.9.2, 64-bit
    """
    with psycopg2.connect(f'host={dbhost} port={dbport} dbname={dbname} user={dbuser} password={dbpassword}') as conn:
        cur = conn.cursor()
        sql = 'SELECT version()'
        cur.execute(sql)
        ver_ = cur.fetchone()
        ver = ver_[0]
        return ver


def get_pg_version_short(dbhost, dbport, dbname, dbuser, dbpassword) -> str:
    """
     Получение номера версии Postgres
     :return: 9.6.3
     """
    ver = get_pg_version_full(dbhost, dbport, dbname, dbuser, dbpassword)
    return ver.split()[1]


def get_pg_version_major(dbhost, dbport, dbname, dbuser, dbpasswor) -> int:
    """
     Получение мажорной версии Postgres в int
     :return: 9
     """
    ver = get_pg_version_full(dbhost, dbport, dbname, dbuser, dbpasswor)
    ver = ver.split(' ')[1]
    ver = int(ver.split('.')[0])
    return ver


def get_proc(dbhost, dbport, dbname, dbuser, dbpassword):
    with psycopg2.connect(f"host={dbhost} port={dbport} dbname={dbname} user={dbuser} password={dbpassword}") as conn:
        if get_pg_version_major(dbhost, dbport, dbname, dbuser, dbpassword) <= 11:
            sql_not_aggregate = "    AND pr.proisagg = false "  # -- Для агрегатных ф-ций не работает получение sql pg_get_functiondef
        else:
            sql_not_aggregate = "    AND pr.prokind <> 'a' "  # -- С Postgres 11 вместо признака proisagg=true появился prokind='a'
        cur = conn.cursor()
        #sql = "SET client_encoding TO 'UTF8';"
        #cur.execute(sql)
        sql = """
SELECT md5(n.nspname||'.'||pr.proname||'('||pg_get_function_identity_arguments(pr.oid)||')') AS code_md5
        , n.nspname||'.'||pr.proname||'('||pg_get_function_identity_arguments(pr.oid)||')' AS code
        , n.nspname||'.'||pr.proname AS fullname
        , pg_get_functiondef(pr.oid) AS sql_create
        , n.nspname AS schemaname
        , pr.proname AS proname
FROM pg_proc pr
LEFT JOIN pg_namespace n ON n.oid=pr.pronamespace
LEFT JOIN pg_language l ON l.oid=pr.prolang
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'cron')
    AND pr.proname<>'text_clear3'
    {}
    
""".format(sql_not_aggregate)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows


def get_views(dbhost, dbport, dbname, dbuser, dbpassword):
    with psycopg2.connect(f"host={dbhost} port={dbport} dbname={dbname} user={dbuser} password={dbpassword}") as conn:
        cur = conn.cursor()
        sql = """
SELECT trim(table_schema||'.'||table_name) AS code
    , 'CREATE OR REPLACE VIEW '||table_schema||'.'||table_name||' AS'||chr(10)||pg_get_viewdef(table_schema||'.'||table_name, true) AS sql_create
    , table_schema AS schemaname
    , table_name AS viewname
    --, view_definition AS definition
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
"""
        cur.execute(sql)
        rows = cur.fetchall()
        return rows


def get_tables(dbhost, dbport, dbname, dbuser, dbpassword):
    with psycopg2.connect(f"host={dbhost} port={dbport} dbname={dbname} user={dbuser} password={dbpassword}") as conn:
        if get_pg_version_major(dbhost, dbport, dbname, dbuser, dbpassword) <= 11:
            sql_relhasoids = "        , c.relhasoids"
        else:
            sql_relhasoids = "        , false AS relhasoids"
        cur = conn.cursor()
        sql = """
WITH 
ftoptions AS (
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
        {}  -- С 12 версии поля c.relhasoids нет
        --, c.relhaspkey
        --, c.relchecks
    FROM pg_class c 
    LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
    INNER JOIN information_schema.tables m ON m.table_name=c.relname AND m.table_schema=n.nspname
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
        AND a.attnum > 0
        AND c.relkind IN ('r','v','f') --AND c.relkind IN('r','v','f') --f-foreign table, r-table, v-view
        AND n.nspname NOT IN ('information_schema', 'pg_catalog') --cron
        --AND (n.nspname, c.relname) IN (SELECT table_schema, table_name FROM information_schema.tables)
    ORDER BY n.nspname, c.relname, a.attnum
)
SELECT 
    m.schema_name||'.'||m.table_name AS code
    , CASE WHEN m.relkind='r' THEN 'CREATE TABLE IF NOT EXISTS '||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
           WHEN m.relkind='f' THEN 'CREATE FOREIGN TABLE IF NOT EXISTS '||m.schema_name||'.'||m.table_name||chr(10)||'('||chr(10)
           WHEN m.relkind='v' THEN 'STRUCTURE VIEW '||m.schema_name||'.'||m.table_name||' ('||chr(10)
           ELSE ''
      END
      || string_agg('    '||m.column_name||' '||m.column_type||COALESCE(' '||m.not_null,'')||COALESCE(' '||m.default_value,''), ','||chr(10) ORDER BY m.attnum)
      ||chr(10)||')' 
      ||COALESCE(chr(10)||'INHERITS ('||m.inhtable||')', '')
      ||CASE WHEN m.relhasoids THEN chr(10)||'WITH (OIDS=TRUE)' ELSE '' END --После версии 11 нет поля relhasoids
      ||chr(10)||';'
      AS sql_create
    , m.relkind
    , m.schema_name
    , m.table_name
FROM cols m
GROUP BY schema_name, table_name, m.inhtable, m.relkind
        , m.relhasoids -- После версии 11 нет поля relhasoids
ORDER BY m.schema_name, m.relkind, m.table_name        
""".format(sql_relhasoids)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows


def save_files_proc():
    for db in DATABASES:
        if db['ENABLE']:
            for item in get_proc(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']):
                schema_name = item[4]
                save_path = DIRNAME_BASE
                if DIRNAME_BY_DBNAME:
                    save_path = os.path.join(save_path, db['DBNAME_DIR'])
                if DIRNAME_BY_SCHEMA:
                    save_path = os.path.join(save_path, schema_name)
                save_path = os.path.join(save_path, DIRNAME_PROC)

                if not os.path.exists(save_path):
                    os.makedirs(save_path)

                filename = f'{item[2]}_[{item[0][:10]}].sql'
                filepath = os.path.join(save_path, filename)
                print(filepath)
                with open(filepath, 'w', encoding='UTF-8') as fw:
                    fw.write(item[3])


def save_files_views():
    for db in DATABASES:
        if db['ENABLE']:
            for item in get_views(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']):
                schema_name = item[2]

                save_path = DIRNAME_BASE
                if DIRNAME_BY_DBNAME:
                    save_path = os.path.join(save_path, db['DBNAME_DIR'])
                if DIRNAME_BY_SCHEMA:
                    save_path = os.path.join(save_path, schema_name)
                save_path = os.path.join(save_path, DIRNAME_VIEWS)

                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                filename = f'{item[0]}.sql'
                filepath = os.path.join(save_path, filename)
                print(filepath)
                with open(filepath, 'w', encoding='UTF-8') as fw:
                    fw.write(item[1])


def save_files_tables():
    for db in DATABASES:
        if db['ENABLE']:
            for item in get_tables(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']):
                dir_relkind = {'r': DIRNAME_TABLES, 'v': DIRNAME_VIEWS_STRUCT, 'f': DIRNAME_FOREIGN_TABLES}
                relkind = item[2]
                schema_name = item[3]
                subdir = dir_relkind[relkind]
                save_path = DIRNAME_BASE
                if DIRNAME_BY_DBNAME:
                    save_path = os.path.join(save_path, db['DBNAME_DIR'])
                if DIRNAME_BY_SCHEMA:
                    save_path = os.path.join(save_path, schema_name)
                save_path = os.path.join(save_path, subdir)
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                filename = f'{item[0]}.sql'
                filepath = os.path.join(save_path, filename)
                print(filepath)
                with open(filepath, 'w', encoding='UTF-8') as fw:
                    fw.write(item[1])


def test():
    for db in DATABASES:
        if db['ENABLE']:
            print(get_pg_version_full(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']))
            print(get_pg_version_short(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']))
            print(get_pg_version_major(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']))
            print(get_pg_server_encoding(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']))  #UTF8

    for db in DATABASES:
        if db['ENABLE']:
            print('Сервер БД: '+get_pg_version_full(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']))
            print('Представления (Views):')
            print(db['DBNAME_DIR'])
            for item in get_views(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']):
                print(item[0])

            print('Функции:')
            print(db['DBNAME_DIR'])
            for item in get_proc(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']):
                print(item[0])

            print('Таблицы:')
            print(db['DBNAME_DIR'])
            for item in get_tables(db['DBHOST'], db['DBPORT'], db['DBNAME'], db['DBUSER'], db['DBPASSWORD']):
                print(item[0])


def main():
    save_files_proc()
    save_files_views()
    save_files_tables()


if __name__ == '__main__':
    main()
    #test()
