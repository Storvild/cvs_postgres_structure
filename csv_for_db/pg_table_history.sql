-- DROP TABLE public.pg_table_history;

--Создание таблицы
CREATE TABLE public.pg_table_history
(
  uid serial NOT NULL PRIMARY KEY,
  create_date timestamp without time zone NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone,
  code character varying NOT NULL, -- Код (уникальное имя Представления)
  addnew boolean NOT NULL DEFAULT false,
  deleted boolean NOT NULL DEFAULT false,
  relkind char,
  sql_create text, -- SQL создания представления
  sql_column_comments text, -- SQL создания комментариев к полям
  schemaname name,
  tablename name,
  description text
);


COMMENT ON COLUMN public.pg_table_history.code IS 'Код (уникальное имя таблицы)';
COMMENT ON COLUMN public.pg_table_history.addnew IS 'Представление добавлено';
COMMENT ON COLUMN public.pg_table_history.deleted IS 'Представление удалено';
COMMENT ON COLUMN public.pg_table_history.relkind IS 'Тип поля';
COMMENT ON COLUMN public.pg_table_history.sql_create IS 'SQL создания таблицы';
COMMENT ON COLUMN public.pg_table_history.sql_column_comments IS 'SQL создания комментариев к полям';
COMMENT ON COLUMN public.pg_table_history.schemaname IS 'Имя схемы';
COMMENT ON COLUMN public.pg_table_history.tablename IS 'Имя Таблицы';
COMMENT ON COLUMN public.pg_table_history.description IS 'Комментарий к таблице';

