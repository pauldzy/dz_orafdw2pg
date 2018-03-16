CREATE OR REPLACE FUNCTION dz_pg.execute_postflight(
    IN  pPostFlightGroup   VARCHAR
   ,IN  pMetadataSchema    VARCHAR
) RETURNS BOOLEAN
VOLATILE
AS
$BODY$
DECLARE
   str_sql              VARCHAR(32000);
   rec                  RECORD;

BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------

   ----------------------------------------------------------------------------
   -- Step 20
   -- Check if source table already has objectid
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || 'a.postflight_action '
           || 'FROM '
           || pMetadataSchema || '.pg_orafdw_postflight a '
           || 'WHERE '
           || 'a.copy_group_keyword = $1 '
           || 'ORDER BY '
           || 'a.postflight_order ';
           
   FOR rec IN EXECUTE str_sql USING pPostFlightGroup
   LOOP
      EXECUTE rec.postflight_action;
      
   END LOOP;
  
   ----------------------------------------------------------------------------
   -- Step 30
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.execute_postflight(
    VARCHAR
   ,VARCHAR
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.execute_postflight(
    VARCHAR
   ,VARCHAR
) TO PUBLIC;

