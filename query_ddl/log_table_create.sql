CREATE TABLE IF NOT EXISTS public.log_etl
(
    start_run timestamp,
    command varchar(800),
    eror text,
    total_export_row bigint,
    status_script varchar(800),
    status_run varchar(800),
    script_name varchar(800),
    final_run boolean
);



