CREATE TABLE IF NOT EXISTS core.meta.params
(
    loaded_dttm timestamp(0),
    job_nm      varchar,
    model_nm    varchar,
    column_nm   varchar,
    param_nm    varchar,
    param_type  varchar,
    value       varchar
);

DROP TRIGGER IF EXISTS set_loaded_dttm_trigger ON core.meta.params;

CREATE TRIGGER set_loaded_dttm_trigger
    BEFORE INSERT OR UPDATE
    ON core.meta.params
    FOR EACH ROW
EXECUTE PROCEDURE core.meta.set_loaded_dttm();