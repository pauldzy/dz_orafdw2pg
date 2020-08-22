CREATE OR REPLACE FUNCTION dz_pg.copy_foreign_table(
    IN  pForeignTableOwner VARCHAR
   ,IN  pForeignTableName  VARCHAR[]
   ,IN  pMetadataSchema    VARCHAR
   ,IN  pTargetSchema      VARCHAR
   ,IN  pTargetTableName   VARCHAR   DEFAULT NULL
   ,IN  pTargetTablespace  VARCHAR   DEFAULT NULL
   ,IN  pForceObjectID     BOOLEAN   DEFAULT FALSE
   ,IN  pNoCopy            BOOLEAN   DEFAULT FALSE
   ,IN  pForcePublic       BOOLEAN   DEFAULT FALSE
   ,IN  pPostFlightGroup   VARCHAR   DEFAULT NULL 
   ,IN  pPostFlightAction  VARCHAR   DEFAULT NULL
   ,IN  pPostFlightGUID    VARCHAR   DEFAULT NULL
   ,IN  pPostFlightTime    TIMESTAMP DEFAULT NULL
) RETURNS BOOLEAN
VOLATILE 
AS
$BODY$ 
DECLARE
   boo_check            BOOLEAN;
   str_unique_guid      VARCHAR(40);
   dat_current_time     TIMESTAMP;
   str_postflight       VARCHAR(255);
   str_postflight_act   VARCHAR(255);
   str_sql              VARCHAR(4000);
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF pForeignTableName IS NULL
   OR array_length(pForeignTableName,1) = 0
   THEN
      RETURN false;
      
   END IF;
   
   IF pPostFlightGroup IS NULL
   THEN
      str_postflight := 'Append';
   
   ELSE
      str_postflight := pPostFlightGroup;
      
   END IF;
   
   IF pPostFlightAction IS NULL
   THEN
      str_postflight_act := 'APPEND';
   
   ELSE
      str_postflight_act := UPPER(pPostFlightAction);
      
   END IF;
   
   IF pPostFlightGUID IS NULL
   THEN
      str_unique_guid  := '{' || uuid_generate_v1() || '}';

   ELSE
      str_unique_guid := pPostFlightGUID;

   END IF;

   IF pPostFlightTime IS NULL
   THEN
      dat_current_time := (abstime(('now'::text)::timestamp(6) with time zone));

   ELSE
      dat_current_time := pPostFlightTime;

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Flush the postflight if requested 
   ----------------------------------------------------------------------------
   IF str_postflight_act IN ('FLUSH','TRUNCATE')
   THEN
      str_sql := 'DELETE FROM ' || pMetadataSchema || '.pg_orafdw_postflight '
              || 'WHERE '
              || 'copy_group_keyword = $1 ';
              
      EXECUTE str_sql USING str_postflight;
      
      str_postflight_act := 'APPEND';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Roll through the inputs
   ----------------------------------------------------------------------------
   FOR i IN 1 .. array_length(pForeignTableName,1)
   LOOP
      boo_check := dz_pg.copy_foreign_table(
          pForeignTableOwner := pForeignTableOwner
         ,pForeignTableName  := pForeignTableName[i]
         ,pMetadataSchema    := pMetadataSchema
         ,pTargetSchema      := pTargetSchema
         ,pTargetTableName   := pTargetTableName
         ,pTargetTablespace  := pTargetTablespace
         ,pForceObjectID     := pForceObjectID 
         ,pNoCopy            := pNoCopy
         ,pForcePublic       := pForcePublic
         ,pPostFlightGroup   := str_postflight
         ,pPostFlightAction  := str_postflight_act
         ,pPostFlightGUID    := str_unique_guid
         ,pPostFlightTime    := dat_current_time
      );
      
      IF NOT boo_check 
      THEN
         RETURN FALSE;
         
      END IF;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN TRUE;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.copy_foreign_table(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,TIMESTAMP
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.copy_foreign_table(
    VARCHAR
   ,VARCHAR[]
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,TIMESTAMP
) TO PUBLIC;

