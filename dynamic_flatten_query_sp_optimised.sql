--CREATE OR REPLACE PROCEDURE `ga-realtime-api-295019.pugpig_validation`.proc_pj_app_optimised (project_id STRING, dataset_name STRING, table_name STRING, startDate STRING, endDate STRING, OUT sql STRING) 
CREATE OR REPLACE PROCEDURE `ga-realtime-api-xxx.sandbox_xx`.proc_app_optimise (project_id STRING, dataset_name STRING, table_name STRING, startDate STRING, endDate STRING, OUT sql STRING) 
BEGIN
    DECLARE table_name_argument STRING;
    DECLARE full_table_name string;
    -- Base SQL statement
    DECLARE flatten_statement string DEFAULT "WITH analytics AS ("
                                                || "SELECT "
                                                || "event_name  \n"
                                                || ", event_date \n"
                                                || ", event_timestamp \n"
                                                || ", TIMESTAMP_MICROS(event_timestamp) as event_datetime \n"
                                                || ", event_previous_timestamp \n"
                                                || ", event_value_in_usd \n"
                                                || ", event_bundle_sequence_id \n"
                                                || ", event_server_timestamp_offset \n"
                                                || ", user_id \n"
                                                || ", user_pseudo_id \n";


-- Pre-aggregated metadata tables (param_meta, user_prop_meta)
    DECLARE event_params_sql string;
    DECLARE user_props_sql string;
    DECLARE other_unnested_columns_sql string;

    --DECLARE params_sql ARRAY<STRING>;
    -- DECLARE user_props_sql ARRAY<STRING>;
    
-- Construct the full table name 
    SET full_table_name = CONCAT(project_id, '.', dataset_name, '.', table_name); 

-- Construct all event paramters 
    EXECUTE IMMEDIATE format(r"""
                        with events as (
                            SELECT
                            DISTINCT param.key as name,
                            CASE
                                WHEN param.value.string_value is not null THEN ' (SELECT value.string_value '
                                WHEN param.value.int_value is not null THEN ' (SELECT value.int_value '
                                WHEN param.value.double_value is not null THEN ' (SELECT value.double_value '
                                WHEN param.value.float_value is not null THEN  ' (SELECT value.float_value '
                            END as value
                            FROM `%s`,UNNEST(event_params) as param
                            WHERE _TABLE_SUFFIX between '%s' and '%s'
                            GROUP BY name,value)

                                 SELECT STRING_AGG(value|| "FROM UNNEST(event_params) WHERE key = '" || name || "') AS " || name || '\n' ORDER BY name) 
                                FROM events
                                """
                        ,full_table_name,startDate, endDate) INTO event_params_sql;


                       set flatten_statement = flatten_statement || ',' || event_params_sql;

--Construct all user properties
    EXECUTE IMMEDIATE format(r"""
                            with user_props as (
                                select
                                DISTINCT user_prop.key as user_prop_key,
                                case when user_prop.value.string_value is not null then ' (SELECT value.string_value ' 
                                    when user_prop.value.int_value is not null then ' (SELECT value.int_value '
                                    when user_prop.value.double_value is not null then ' (SELECT value.double_value ' 
                                    when user_prop.value.float_value is not null then ' (SELECT value.float_value '
                                    else null 
                                end as user_prop_value
                                from `%s`,unnest(user_properties) as user_prop
                                WHERE _TABLE_SUFFIX between '%s' and '%s'
                                group by
                                    user_prop_key,
                                    user_prop_value
                                order by
                                    user_prop_key
                                )

                                SELECT STRING_AGG(user_prop_value|| "FROM UNNEST(user_properties) WHERE key = '" || user_prop_key || "') AS " || user_prop_key || '\n' ORDER BY user_prop_key) 
                                FROM user_props
                               
                            """
                            ,full_table_name,startDate, endDate) INTO user_props_sql;

   set flatten_statement = flatten_statement || ',' || user_props_sql;

IF (table_name = 'events_intraday_*') THEN
    set table_name_argument = 'events_intraday';
ELSE 
    set table_name_argument = 'events';
END IF;

--Construct all other column
 EXECUTE IMMEDIATE format(r"""
                            WITH table_columns AS (
                                SELECT DISTINCT column_name, field_path, data_type 
                                FROM `%s.%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                                WHERE table_catalog = '%s'
                                AND table_schema = '%s'
                                AND table_name BETWEEN '%s_%s' and '%s_%s'
                                AND data_type IN ('STRING','INT64','FLOAT64','NUMERIC','TIMESTAMP','DATE','TIME','DATETIME','BOOL','BIGNUMERIC','ARRAY')
                                AND column_name NOT IN ('event_name', 'event_date', 'event_timestamp','event_previous_timestamp', 'event_value_in_usd','event_bundle_sequence_id','event_server_timestamp_offset','user_id','user_pseudo_id','pseudo_user_id','event_params','user_properties','items'))
                            SELECT STRING_AGG(field_path || " AS " || REPLACE(field_path, '.', '_') || ' \n' ORDER BY field_path) FROM table_columns
                          """
                          ,project_id,dataset_name,project_id,dataset_name,table_name_argument,startDate,table_name_argument,endDate) INTO other_unnested_columns_sql;
    set flatten_statement = flatten_statement || ',' || other_unnested_columns_sql;


 set flatten_statement = flatten_statement 
||    " FROM `"||full_table_name||"` \n"
||    " WHERE _TABLE_SUFFIX BETWEEN '"||startDate||"' AND '"||endDate||"' \n"
||    " ) \n"
||    "  SELECT * FROM analytics;";

set sql = flatten_statement;
    
end;


-- BEGIN
--   DECLARE sql STRING;
--   --CALL `ga-realtime-api-xx.sandbox_xx.proc_app_optimise`('ga-realtime-api-xx','analytics_xx','events_*','20240327','20240328',sql);
--   CALL `ga-realtime-api-xx.pugpig_validation.proc_pj_app_optimised`('ga-realtime-api-xx','analytics_xx','events_intraday_*','20240329','20240329',sql);
--   SELECT sql;    
--   EXECUTE IMMEDIATE (sql);
-- EXCEPTION WHEN ERROR THEN
--   SELECT
--     @@error.message,
--     @@error.stack_trace,
--     @@error.statement_text,
--     @@error.formatted_stack_trace;
-- END;