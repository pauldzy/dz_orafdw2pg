CREATE OR REPLACE FUNCTION dz_pg.copy_foreign_table(
    IN  pForeignTableOwner varchar
   ,IN  pForeignTableName  varchar
   ,IN  pMetadataSchema    varchar
   ,IN  pTargetSchema      varchar
   ,IN  pTargetTableName   varchar DEFAULT NULL
   ,IN  pTargetTablespace  varchar DEFAULT NULL
) RETURNS BOOLEAN
AS
$BODY$ 
DECLARE
   str_sql              VARCHAR(32000);
   str_statment         VARCHAR(32000);
   int_count            INTEGER;
   ary_items            VARCHAR(32000)[];
   
   str_tablespace       VARCHAR(255);
   str_oracle_owner     VARCHAR(255);
   str_oracle_tablename VARCHAR(255);
   str_target_schema    VARCHAR(255);
   str_target_tablename VARCHAR(255);
   
BEGIN
   
   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF pTargetTablespace IS NOT NULL
   THEN
      str_tablespace := 'TABLESPACE ' || pTargetTablespace || ' ';
      
   ELSE
      str_tablespace := ' ';
   
   END IF;
   
   str_target_schema := LOWER(pTargetSchema);
   
   IF pTargetTableName IS NOT NULL
   THEN
      str_target_tablename := LOWER(pTargetTableName);
      
   ELSE
      str_target_tablename := LOWER(pForeignTableName);
      
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Retrieve oracle source from map table
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.oracle_owner '
           || ',a.oracle_tablename '
           || 'FROM '
           || pMetadataSchema || '.oracle_fdw_table_map a '
           || 'WHERE '
           || '    a.foreign_table_schema = $1 '
           || 'AND a.foreign_table_name = $2 ';
           
   BEGIN
      EXECUTE str_sql INTO str_oracle_owner,str_oracle_tablename
      USING LOWER(pForeignTableOwner),LOWER(pForeignTableName);
   
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RAISE EXCEPTION 'Mapping entry not found in metadata map table.';
         
   END;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Check for existing Oracle resource 
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || 'COUNT(*) '
           || 'FROM '
           || pMetadataSchema || '.all_tables a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 ';
           
   EXECUTE str_sql INTO int_count USING str_oracle_owner,str_oracle_tablename;
   
   IF int_count <> 1
   THEN
      RAISE EXCEPTION 'Oracle table not found using existing metadata mapping.';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Drop any existing table
   ----------------------------------------------------------------------------
   str_sql := 'DROP TABLE IF EXISTS ' || str_target_schema || '.' || str_target_tablename;
   
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create the target table
   ----------------------------------------------------------------------------
   str_sql := 'CREATE TABLE ' || str_target_schema || '.' || str_target_tablename || ' ' || str_tablespace
           || 'AS SELECT * FROM ' || pForeignTableOwner || '.' || pForeignTableName || ' '
           || 'WHERE 1 = 2 ';
   
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Collect the indexing statements for table
   ----------------------------------------------------------------------------
   ary_items := dz_pg.extract_indexes(
       pForeignTableOwner := pForeignTableOwner
      ,pForeignTableName  := pForeignTableName
      ,pMetadataSchema    := pMetadataSchema
      ,pTargetSchema      := pTargetSchema
      ,pTargetTableName   := pTargetTableName
      ,pTargetTablespace  := pTargetTablespace
   );
   
   FOREACH str_statment IN ARRAY ary_items
   LOOP
      EXECUTE str_statment;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Collect the constraint statements for table
   ----------------------------------------------------------------------------
   ary_items := dz_pg.extract_constraints(
       pForeignTableOwner := pForeignTableOwner
      ,pForeignTableName  := pForeignTableName
      ,pMetadataSchema    := pMetadataSchema
      ,pTargetSchema      := pTargetSchema
      ,pTargetTableName   := pTargetTableName
   );
   
   FOREACH str_statment IN ARRAY ary_items
   LOOP
      EXECUTE str_statment;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Load the target table
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO ' || str_target_schema || '.' || str_target_tablename || ' '
           || 'SELECT * FROM ' || pForeignTableOwner || '.' || pForeignTableName || ' '
           || 'WHERE 1 = 1 ';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 90
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

