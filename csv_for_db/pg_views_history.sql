-- DROP TABLE public.pg_views_history;

--Создание таблицы
CREATE TABLE public.pg_views_history
(
  uid serial NOT NULL PRIMARY KEY,
  create_date timestamp without time zone NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone,
  code character varying NOT NULL, -- Код (уникальное имя Представления)
  sql_create text, -- SQL создания представления
  addnew boolean NOT NULL DEFAULT false,
  deleted boolean NOT NULL DEFAULT false,
  schemaname name,
  viewname name,
  definition text,
  description text
);


COMMENT ON COLUMN public.pg_views_history.code IS 'Код (уникальное имя Представления)';
COMMENT ON COLUMN public.pg_views_history.sql_create IS 'SQL создания представления';
COMMENT ON COLUMN public.pg_views_history.schemaname IS 'Имя схемы';
COMMENT ON COLUMN public.pg_views_history.viewname IS 'Имя Представления';
COMMENT ON COLUMN public.pg_views_history.definition IS 'SQL Представления';
COMMENT ON COLUMN public.pg_views_history.deleted IS 'Представление удалено';
COMMENT ON COLUMN public.pg_views_history.addnew IS 'Представление добавлено';
COMMENT ON COLUMN public.pg_views_history.description IS 'Описание';

