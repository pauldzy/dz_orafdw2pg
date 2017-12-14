CREATE OR REPLACE FUNCTION dz_pg.insert_to_foreign_table(
    IN  pSourceTableSchema varchar
   ,IN  pSourceTableName   varchar
   ,IN  pTargetForSchema   varchar
   ,IN  pTargetForTable    varchar
   ,IN  pBatchSize         numeric DEFAULT 1000
) RETURNS BOOLEAN
AS
$BODY$ 
DECLARE
   str_sql            VARCHAR(32000);
   int_total_count    INTEGER;
   ary_columns        VARCHAR[];
   str_select         VARCHAR(32000);
   str_select2        VARCHAR(32000);

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
      str_select := str_select || ary_columns[i];

      IF i < array_length(ary_columns,1)
      THEN
         str_select  := str_select || ',';
         str_select2 := 'a.' || str_select || ',';

      END IF;

   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Build the base sql statement
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO ' || pTargetForSchema || '.' || pTargetForTable || '('
           || str_select || ')'
           || 'SELECT ' str_select2 || ' '
           || 'FROM ' || pSourceTableSchema || '.' || pSourceTableName || ' a ';

   ----------------------------------------------------------------------------
   -- Step 70
   -- Run the insertion loop
   ----------------------------------------------------------------------------
   int_counter := 0;
   WHILE int_counter <= int_total_count
   LOOP
      EXECUTE str_sql
           || 'LIMIT  ' || pBatchSize::varchar
           || 'OFFSET ' || int_counter::varchar;
   
      int_counter := int_counter + pBatchSize;
   
   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 100
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
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.insert_to_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,numeric
) TO PUBLIC;

