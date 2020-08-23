CREATE OR REPLACE FUNCTION dz_pg.map_foreign_table(
    IN  pOracleOwner    VARCHAR
   ,IN  pOracleTable    VARCHAR[]
   ,IN  pForeignServer  VARCHAR
   ,IN  pTargetSchema   VARCHAR
   ,IN  pMetadataSchema VARCHAR
   ,IN  pForceCharClean BOOLEAN DEFAULT TRUE
   ,IN  pCustomPrefetch INTEGER DEFAULT NULL
   ,IN  pTableCasing    VARCHAR DEFAULT 'SMART'
   ,IN  pColumnCasing   VARCHAR DEFAULT 'SMART'
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
      RETURN FALSE;
      
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
         ,pForceCharClean := pForceCharClean
         ,pCustomPrefetch := pCustomPrefetch
         ,pTableCasing    := pTableCasing
         ,pColumnCasing   := pColumnCasing
      );
      
      IF NOT boo_check 
      THEN
         RETURN FALSE;
         
      END IF;
   
   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN TRUE;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.map_foreign_table(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.map_foreign_table(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,INTEGER
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

