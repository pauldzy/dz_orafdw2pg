CREATE OR REPLACE FUNCTION dz_pg.map_foreign_table(
    IN  pOracleOwner    VARCHAR
   ,IN  pOracleTable    VARCHAR[]
   ,IN  pForeignServer  VARCHAR
   ,IN  pTargetSchema   VARCHAR
   ,IN  pMetadataSchema VARCHAR
) RETURNS BOOLEAN
VOLATILE
AS
$BODY$ 
DECLARE
   boo_check BOOLEAN;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF pOracleTable IS NULL
   OR array_length(pOracleTable,1) = 0
   THEN
      RETURN false;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Do each one in order
   ----------------------------------------------------------------------------
   FOR i IN 1 .. array_length(pOracleTable,1)
   LOOP
      boo_check := dz_pg.map_foreign_table(
          pOracleOwner    := pOracleOwner
         ,pOracleTable    := pOracleTable[i]
         ,pForeignServer  := pForeignServer
         ,pTargetSchema   := pTargetSchema
         ,pMetadataSchema := pMetadataSchema
      );
      
      IF NOT boo_check 
      THEN
         RETURN false;
         
      END IF;
   
   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.map_foreign_table(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.map_foreign_table(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

