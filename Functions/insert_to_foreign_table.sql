CREATE OR REPLACE FUNCTION dz_pg.insert_to_foreign_table(
    IN  pSourceTableSchema VARCHAR
   ,IN  pSourceTableName   VARCHAR
   ,IN  pTargetForSchema   VARCHAR
   ,IN  pTargetForTable    VARCHAR
   ,IN  pBatchSize         NUMERIC DEFAULT 1000
   ,IN  pInitialOffset     NUMERIC DEFAULT 0
) RETURNS BOOLEAN
VOLATILE
AS
$BODY$ 
DECLARE
   str_sql            VARCHAR(32000);
   int_total_count    INTEGER;
   ary_columns        VARCHAR[];
   str_select         VARCHAR(32000);
   str_select2        VARCHAR(32000);
   str_primary_key    VARCHAR(32000);
   int_counter        INTEGER;
   str_dblink         VARCHAR(4000) := 'port=5432 dbname=docker host=127.0.0.1 user=docker password=docker';

BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------

   ----------------------------------------------------------------------------
   -- Step 20
   -- Check that source table exists
   ----------------------------------------------------------------------------

   ----------------------------------------------------------------------------
   -- Step 30
   -- Check that target foreign table exists and matches schema of source
   ----------------------------------------------------------------------------

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get the range of objectids 
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' COUNT(*) AS totalcount '
           || 'FROM '
           || pSourceTableSchema || '.' || pSourceTableName || ' a ';

   EXECUTE str_sql INTO int_total_count;

   IF int_total_count IS NULL
   OR int_total_count = 0 
   THEN
      RAISE EXCEPTION 'No record found in source table to insert.';

   END IF;

   ----------------------------------------------------------------------------
   -- Step 50
   -- Build the column list from source
   ----------------------------------------------------------------------------
   SELECT
   array_agg(a.column_name::varchar)
   INTO ary_columns
   FROM (
      SELECT
      aa.column_name 
      FROM 
      information_schema.columns aa 
      WHERE 
          aa.table_schema = pSourceTableSchema 
      AND aa.table_name   = pSourceTableName 
      ORDER BY 
      aa.ordinal_position
    ) a;

   str_select  := '';
   str_select2 := '';
   FOR i IN 1 .. array_length(ary_columns,1)
   LOOP
      str_select  := str_select  || ary_columns[i];
      str_select2 := str_select2 || 'a.' || ary_columns[i];

      IF i < array_length(ary_columns,1)
      THEN
         str_select  := str_select  || ',';
         str_select2 := str_select2 || ',';

      END IF;

   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Build the column list from source
   ----------------------------------------------------------------------------
   SELECT
   array_agg(a.column_name::varchar)
   INTO ary_columns
   FROM (
      SELECT
      aa.column_name 
      FROM 
      information_schema.key_column_usage aa
      LEFT JOIN
      information_schema.table_constraints bb
      ON
          aa.constraint_schema  = bb.table_schema
      AND aa.constraint_name    = bb.constraint_name    
      WHERE 
          bb.table_schema = pSourceTableSchema 
      AND bb.table_name   = pSourceTableName 
      AND bb.constraint_type = 'PRIMARY KEY'
      ORDER BY 
      aa.ordinal_position
   ) a;

   IF ary_columns IS NULL
   OR array_length(ary_columns,1) = 0
   THEN
      RAISE EXCEPTION 'Source table lacking primary key.';
      
   END IF;
   
   str_primary_key := '';
   FOR i IN 1 .. array_length(ary_columns,1)
   LOOP
      str_primary_key := str_primary_key || 'a.' || ary_columns[i];

      IF i < array_length(ary_columns,1)
      THEN
         str_primary_key := str_primary_key || ',';

      END IF;

   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Build the base sql statement
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO ' || pTargetForSchema || '.' || pTargetForTable || '('
           || str_select || ') '
           || 'SELECT ' || str_select2 || ' '
           || 'FROM ' || pSourceTableSchema || '.' || pSourceTableName || ' a '
           || 'ORDER BY ' || str_primary_key;

   ----------------------------------------------------------------------------
   -- Step 80
   -- Run the insertion loop
   ----------------------------------------------------------------------------
   int_counter := pInitialOffset;
   WHILE int_counter <= int_total_count
   LOOP
      PERFORM dblink(
          str_dblink::text
         ,str_sql::text
            || ' LIMIT '  || pBatchSize::text
            || ' OFFSET ' || int_counter::text
      );
   
      int_counter := int_counter + pBatchSize;
   
   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 90
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.insert_to_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,numeric
   ,numeric
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.insert_to_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,numeric
   ,numeric
) TO PUBLIC;

