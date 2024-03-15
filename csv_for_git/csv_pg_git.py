import psycopg2
import sys
import os

import csv_pg_git_config

#for key, val in os.environ.items():
#    print(key, val)
#print(os.getenv('TEMP', '<empty>'))
#os.environ['PG_GIT_DBNAME'] = 'btkbase'

#print(os.environ.get('PG_GIT_DBNAME'))


DIRNAME_BASE = os.getenv('PG_GIT_DIRNAME_BASE')  # Базовый путь к создаваемым файлам. '' - Текущая директория
DIRNAME_PROC = os.getenv('PG_GIT_DIRNAME_PROC')
DIRNAME_VIEWS = os.getenv('PG_GIT_DIRNAME_VIEWS')
DIRNAME_TABLES = os.getenv('PG_GIT_DIRNAME_TABLES')

DBHOST = os.getenv('PG_GIT_DBHOST')
DBNAME = os.getenv('PG_GIT_DBNAME')
DBUSER = os.getenv('PG_GIT_DBUSER')
DBPASS = os.getenv('PG_GIT_DBPASS')
DBPORT = os.getenv('PG_GIT_DBPORT')

# Если указана директория, то устанавливаем ее как текущую, иначе текущей становится директория где лежит py скрипт
if DIRNAME_BASE:
    os.chdir(DIRNAME_BASE)
else:
    os.chdir(os.path.join(os.path.abspath(os.path.dirname(__file__)), DIRNAME_BASE))

print(f'Python: {sys.version}')
#print(f'Версия libpq: {psycopg2.__libpq_version__}')


def get_pg_server_encoding() -> str:
    with psycopg2.connect(f'host={DBHOST} port={DBPORT} dbname={DBNAME} user={DBUSER} password={DBPASS}') as conn:
        cur = conn.cursor()
        sql = 'SHOW server_encoding;'
        cur.execute(sql)
        ver_ = cur.fetchone()
        ver = ver_[0]
        return ver

def get_pg_version_full() -> str:
    """
    Получение полной версии Postgres
    :return: PostgreSQL 9.6.3 on x86_64-pc-linux-gnu, compiled by gcc (Debian 4.9.2-10) 4.9.2, 64-bit
    """
    with psycopg2.connect(f'host={DBHOST} port={DBPORT} dbname={DBNAME} user={DBUSER} password={DBPASS}') as conn:
        cur = conn.cursor()
        sql = 'SELECT version()'
        cur.execute(sql)
        ver_ = cur.fetchone()
        ver = ver_[0]
        return ver

def get_pg_version_short() -> str:
    """
     Получение номера версии Postgres
     :return: 9.6.3
     """
    ver = get_pg_version_full()
    return ver.split()[1]

def get_pg_version_major() -> int:
    """
     Получение мажорной версии Postgres в int
     :return: 9
     """
    ver = get_pg_version_full()
    ver = ver.split(' ')[1]
    ver = int(ver.split('.')[0])
    return ver


def get_proc():
    with psycopg2.connect(f'host={DBHOST} port={DBPORT} dbname={DBNAME} user={DBUSER} password={DBPASS}') as conn:
        if get_pg_version_major() <= 11:
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
--LIMIT 10     
""".format(sql_not_aggregate)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows


def get_views():
    with psycopg2.connect(f'host={DBHOST} port={DBPORT} dbname={DBNAME} user={DBUSER} password={DBPASS}') as conn:
        if get_pg_version_major() <= 11:
            sql_not_aggregate = "    AND pr.proisagg = false "  # -- Для агрегатных ф-ций не работает получение sql pg_get_functiondef
        else:
            sql_not_aggregate = "    AND pr.prokind <> 'a' "  # -- С Postgres 11 вместо признака proisagg=true появился prokind='a'
        cur = conn.cursor()
        sql = """
SELECT trim(table_schema||'.'||table_name) AS code
    , 'CREATE OR REPLACE VIEW '||table_schema||'.'||table_name||' AS'||chr(10)||pg_get_viewdef(table_schema||'.'||table_name, true) AS sql_create
    , table_schema AS schemaname
    , table_name AS viewname
    --, view_definition AS definition
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
""".format(sql_not_aggregate)
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

def save_files_proc():
    save_path = os.path.join(DIRNAME_BASE, DIRNAME_PROC)
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    for item in get_proc():
        filepath = os.path.join(save_path, '{}_[{}].sql'.format(item[2], item[0][:10]))
        print(filepath)
        with open(filepath, 'w', encoding='UTF-8') as fw:
            fw.write(item[3])
        #print(item)


def save_files_views():
    save_path = os.path.join(DIRNAME_BASE, DIRNAME_VIEWS)
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    for item in get_views():
        filepath = os.path.join(save_path, '{}.sql'.format(item[0]))
        print(filepath)
        with open(filepath, 'w', encoding='UTF-8') as fw:
            fw.write(item[1])
        #print(item)


def main():
    if not os.path.exists(DIRNAME_PROC):
        os.makedirs(DIRNAME_PROC)
    if not os.path.exists(DIRNAME_VIEWS):
        os.makedirs(DIRNAME_VIEWS)

def test():
    print(get_pg_version_full())
    print(get_pg_version_short())
    print(get_pg_version_major())

if __name__ == '__main__':
    #main()
    #test()
    #for item in get_proc():
    #    print(item[2]+'_'+item[0])
    #for item in get_views():
    #    print(item)
    #print(get_pg_server_encoding())  #UTF8
    save_files_proc()
    save_files_views()