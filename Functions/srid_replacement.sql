CREATE OR REPLACE FUNCTION dz_pg.srid_replacement(
    IN  pOracleSRID     INTEGER
) RETURNS INTEGER
IMMUTABLE
AS
$BODY$ 
DECLARE   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Remap the typical Oracle items
   ----------------------------------------------------------------------------
   IF pOracleSRID = 8265
   THEN
      RETURN 4269;
      
   ELSIF pOracleSRID = 8307
   THEN
      RETURN 4326;
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Return input
   ----------------------------------------------------------------------------
   RETURN pOracleSRID;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.srid_replacement(
   INTEGER
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.srid_replacement(
   INTEGER
) TO PUBLIC;

