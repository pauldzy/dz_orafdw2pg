CREATE OR REPLACE FUNCTION dz_pg.copy_foreign_table(
    IN  pForeignTableOwner varchar
   ,IN  pForeignTableName  varchar
   ,IN  pMetadataSchema    varchar
   ,IN  pTargetSchema      varchar
   ,IN  pTargetTableName   varchar DEFAULT NULL
   ,IN  pTargetTablespace  varchar DEFAULT NULL
   ,IN  pForceObjectID     boolean DEFAULT FALSE
) RETURNS BOOLEAN
AS
$BODY$ 
DECLARE
   str_sql              VARCHAR(32000);
   str_statement        VARCHAR(32000);
   int_count            INTEGER;
   ary_items            VARCHAR(32000)[];
   r                    REFCURSOR; 
   rec                  RECORD;
   
   str_tablespace       VARCHAR(255);
   str_oracle_owner     VARCHAR(255);
   str_oracle_tablename VARCHAR(255);
   str_target_schema    VARCHAR(255);
   str_target_tablename VARCHAR(255);
   boo_insert_objectid  BOOLEAN;
   
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
   
   boo_insert_objectid := pForceObjectID;
   
   IF boo_insert_objectid IS NULL
   THEN
      boo_insert_objectid := FALSE;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Check if source table already has objectid 
   ----------------------------------------------------------------------------
   IF boo_insert_objectid
   THEN
      str_sql := 'SELECT '
              || ' a.column_name  '
              || 'FROM '
              || pMetadataSchema || '.all_tab_columns a '
              || 'WHERE '
              || '    a.owner = $1 '
              || 'AND a.table_name = $2 ';
              
      OPEN r FOR EXECUTE str_sql USING str_oracle_owner,str_oracle_tablename;
      FETCH NEXT FROM r INTO rec; 
      
      WHILE FOUND 
      LOOP
         IF LOWER(rec.column_name) = 'objectid'
         THEN
            boo_insert_objectid := FALSE;
            EXIT;
            
         END IF;
      
         FETCH NEXT FROM r INTO rec; 

      END LOOP;
      
      CLOSE r;
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
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
   -- Step 40
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
   -- Step 50
   -- Drop any existing table
   ----------------------------------------------------------------------------
   str_sql := 'DROP TABLE IF EXISTS ' || str_target_schema || '.' || str_target_tablename;
   
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Create the target table
   ----------------------------------------------------------------------------
   str_sql := 'CREATE TABLE ' || str_target_schema || '.' || str_target_tablename || ' ' || str_tablespace
           || 'AS SELECT ';
           
   IF boo_insert_objectid
   THEN
      str_sql := str_sql || 'CAST(NULL AS INTEGER) AS objectid, a.* ';
   ELSE
      str_sql := str_sql || 'a.* ';
      
   END IF;
           
   str_sql := str_sql || 'FROM ' || pForeignTableOwner || '.' || pForeignTableName || ' a '
           || 'WHERE 1 = 2 ';
   
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 70
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
   
   FOREACH str_statement IN ARRAY ary_items
   LOOP
      EXECUTE str_statement;
   
   END LOOP;
   
   IF boo_insert_objectid
   THEN
      str_sql := 'CREATE UNIQUE INDEX ' || str_target_tablename || '_oid '
              || 'ON ' || str_target_schema || '.' || str_target_tablename 
              || '(objectid) ' || str_tablespace;
      
      EXECUTE str_sql;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Collect the constraint statements for table
   ----------------------------------------------------------------------------
   ary_items := dz_pg.extract_constraints(
       pForeignTableOwner := pForeignTableOwner
      ,pForeignTableName  := pForeignTableName
      ,pMetadataSchema    := pMetadataSchema
      ,pTargetSchema      := pTargetSchema
      ,pTargetTableName   := pTargetTableName
   );
   
   FOREACH str_statement IN ARRAY ary_items
   LOOP
      EXECUTE str_statement;
   
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Load the target table
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO ' || str_target_schema || '.' || str_target_tablename || ' '
           || 'SELECT ';
           
   IF boo_insert_objectid
   THEN
      str_sql := str_sql || 'ROW_NUMBER() OVER(), a.* ';
   ELSE
      str_sql := str_sql || 'a.* ';
      
   END IF;
           
   str_sql := str_sql
           || 'FROM ' || pForeignTableOwner || '.' || pForeignTableName || ' a '
           || 'WHERE 1 = 1 ';
           
   EXECUTE str_sql;

   ----------------------------------------------------------------------------
   -- Step 100
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.copy_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
   ,boolean
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.copy_foreign_table(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
   ,boolean
) TO PUBLIC;

