-- DROP TABLE public.pg_proc_history;

CREATE TABLE public.pg_proc_history
(
  uid serial NOT NULL PRIMARY KEY,
  create_date timestamp without time zone NOT NULL DEFAULT ('now'::text)::timestamp(6) with time zone,
  code character varying NOT NULL, -- Код (уникальный для ф-ции)
  prorettype_name name, -- Возвращаемый тип
  prolang_name name, -- Язык
  sql_create text, -- SQL Создания
  addnew boolean NOT NULL DEFAULT false, -- Новая функция
  deleted boolean NOT NULL DEFAULT false, -- Функция удалена
  prodescription character varying, -- Описание

  pronamespace_name name, -- Имя схемы
  proname name, -- Имя функции
  prosrc text -- Текст функции
);

COMMENT ON COLUMN public.pg_proc_history.code IS 'Код (уникальный для ф-ции)';
COMMENT ON COLUMN public.pg_proc_history.pronamespace_name IS 'Имя схемы';
COMMENT ON COLUMN public.pg_proc_history.proname IS 'Имя функции';
COMMENT ON COLUMN public.pg_proc_history.prolang_name IS 'Язык';
COMMENT ON COLUMN public.pg_proc_history.prorettype_name IS 'Возвращаемый тип';
COMMENT ON COLUMN public.pg_proc_history.prodescription IS 'Описание';
COMMENT ON COLUMN public.pg_proc_history.sql_create IS 'SQL Создания функции';
COMMENT ON COLUMN public.pg_proc_history.deleted IS 'Функция удалена';
COMMENT ON COLUMN public.pg_proc_history.addnew IS 'Новая функция';
COMMENT ON COLUMN public.pg_proc_history.prosrc IS 'Текст функции';
