CREATE OR REPLACE FUNCTION dz_pg.char_cleaner(
    IN  pFieldName      varchar
   ,IN  pTableAlias     varchar DEFAULT NULL
) RETURNS VARCHAR
AS
$BODY$ 
DECLARE
   str_output      VARCHAR(32000);
   str_fieldname   VARCHAR(30);
   str_table_alias VARCHAR(1);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameter
   ----------------------------------------------------------------------------
   str_fieldname   := LOWER(pFieldName);
   str_table_alias := LOWER(pTableAlias);

   ----------------------------------------------------------------------------
   -- Step 20
   -- Remove entirely CTRL-0 NUL
   ----------------------------------------------------------------------------
   str_output := 'REPLACE(' || str_table_alias || '.' || str_fieldname || ',CHR(0),'''''''')';
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Remap 241 to proper UTF8
   ----------------------------------------------------------------------------
   str_output := 'REPLACE(' || str_output || ',CHR(241),UNISTR(''''\00F1'''')) AS ' || str_fieldname;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN str_output;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.char_cleaner(
    varchar
   ,varchar
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.char_cleaner(
    varchar
   ,varchar
) TO PUBLIC;

