DROP FUNCTION IF EXISTS core.meta.set_loaded_dttm();

CREATE FUNCTION core.meta.set_loaded_dttm() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
BEGIN
   NEW.loaded_dttm = NOW();
   RETURN NEW;
END;
$$;