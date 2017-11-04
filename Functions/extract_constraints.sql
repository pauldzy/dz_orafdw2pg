CREATE OR REPLACE FUNCTION dz_pg.extract_constraints(
    IN  pOracleOwner    varchar
   ,IN  pOracleTable    varchar
   ,IN  pMetadataSchema varchar
) RETURNS VARCHAR[]
AS
$BODY$ 
DECLARE
   str_sql    VARCHAR(32000);
   ary_name   VARCHAR(32000)[];
   ary_type   VARCHAR(32000)[];
   int_count  INTEGER;
   r          REFCURSOR; 
   rec        RECORD;
   str_comma  VARCHAR(1);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check for existing Oracle resource 
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || 'COUNT(*) '
           || 'FROM '
           || pMetadataSchema || '.all_tables a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 ';
           
   EXECUTE str_sql INTO int_count USING pOracleOwner,pOracleTable;
   
   IF int_count <> 1
   THEN
      RAISE EXCEPTION 'Oracle table not found using existing metadata resources';
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Get list of constraints
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.constraint_name '
           || ',a.constraint_type '
           || 'FROM '
           || pMetadataSchema || '.all_constraints a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 ';
           
   OPEN r FOR EXECUTE str_sql USING pOracleOwner,pOracleTable;
   FETCH NEXT FROM r INTO rec; 
   
   WHILE FOUND 
   LOOP
      IF rec.constraint_type = 'P'
      THEN
      
      ELSIF rec.constraint_type = 'C'
      THEN
      
      END IF;
   
      FETCH NEXT FROM r INTO rec; 

   END LOOP;
   
   CLOSE r; 
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Generate the foreign table columns mapping
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- finalize and execute the create foreign table statement
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create the map entry
   ----------------------------------------------------------------------------

   ----------------------------------------------------------------------------
   -- Step 60
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.extract_constraints(
    varchar
   ,varchar
   ,varchar
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.extract_constraints(
    varchar
   ,varchar
   ,varchar
) TO PUBLIC;

