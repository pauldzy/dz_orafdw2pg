CREATE OR REPLACE FUNCTION dz_pg.copy_foreign_table(
    IN  pForeignTableOwner varchar
   ,IN  pForeignTableName  varchar
   ,IN  pMetadataSchema    varchar
   ,IN  pTargetSchema      varchar
   ,IN  pTargetName        varchar DEFAULT NULL
   ,IN  pTargetTablespace  varchar DEFAULT NULL
) RETURNS BOOLEAN
AS
$BODY$ 
DECLARE
   str_sql         VARCHAR(32000);
   str_tablespace  VARCHAR(32000);
   str_target_name VARCHAR(32000);
   str_select      VARCHAR(32000);
   int_count       INTEGER;
   r               REFCURSOR; 
   rec             RECORD;
   str_comma       VARCHAR(1);
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Drop any existing table
   ----------------------------------------------------------------------------
   IF pTargetName IS NOT NULL
   THEN
      str_target_name := pTargetName;
      
   ELSE
      str_target_name := pForeignTableName;
      
   END IF;
   
   IF pTargetTablespace IS NOT NULL
   THEN
      str_tablespace := 'TABLESPACE ' || pTargetTablespace || ' ';
      
   ELSE
      str_tablespace := ' ';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Check for existing Oracle resource 
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || 'COUNT(*) '
           || 'FROM '
           || 'information_schema.tables a '
           || 'WHERE '
           || '    a.table_type = ''FOREIGN TABLE'' ' 
           || '    a.table_schema = $1 '
           || 'AND a.table_name = $2';
           
   EXECUTE str_sql INTO int_count USING pForeignTableOwner,pForeignTableName;
   
   IF int_count <> 1
   THEN
      RAISE EXCEPTION 'Foreign table not found.';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Drop any existing table
   ----------------------------------------------------------------------------
   str_sql := 'DROP TABLE IF EXISTS ' || pTargetSchema || '.' || pTargetName;
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Create the target table
   ----------------------------------------------------------------------------
   str_sql := 'CREATE TABLE ' || pTargetSchema || '.' || pTargetName || ' '
           || 'AS SELECT * FROM ' || pForeignTableOwner || '.' || pForeignTableName || ' '
           || 'WHERE 1 = 2 ' || str_tablespace;
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Look for primary key on source
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Load the target table
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO ' || pTargetSchema || '.' || pTargetName || ' '
           || 'SELECT * FROM ' || pForeignTableOwner || '.' || pForeignTableName || ' '
           || 'WHERE 1 = 1 ';
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.map_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.map_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
) TO PUBLIC;

