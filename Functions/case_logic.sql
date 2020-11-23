CREATE OR REPLACE FUNCTION dz_pg.case_logic(
    IN  pItemName      VARCHAR
   ,IN  pCaseLogic     VARCHAR
) RETURNS VARCHAR
IMMUTABLE
AS
$BODY$ 
DECLARE
   str_output TEXT := pItemName;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameter
   ----------------------------------------------------------------------------
   IF pCaseLogic = 'UPPER'
   THEN
      str_output := UPPER(str_output);
       
   ELSIF pCaseLogic = 'LOWER'
   THEN
      str_output := LOWER(str_output);
       
   ELSIF pCaseLogic = 'SMART'
   THEN
      str_output := LOWER(str_output);
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Return what we got
   ----------------------------------------------------------------------------
   RETURN str_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.case_logic(
    VARCHAR
   ,VARCHAR
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.case_logic(
    VARCHAR
   ,VARCHAR
) TO PUBLIC;

