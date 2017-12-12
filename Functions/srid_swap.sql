CREATE OR REPLACE FUNCTION dz_pg.srid_swap(
    IN  pSRID integer
) RETURNS INTEGER
AS
$BODY$ 
DECLARE
   int_return INTEGER;
   
BEGIN

   IF pSRID = 8265
   THEN
      RETURN 4269;
      
   ELSIF pSRID = 8307
   THEN
      RETURN 4326;
      
   ELSE
      RETURN pSRID;
   
   END IF;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.srid_swap(
   integer
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.srid_swap(
   integer
) TO PUBLIC;

