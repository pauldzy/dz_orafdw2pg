CREATE OR REPLACE FUNCTION dz_pg.extract_constraints(
    IN  pForeignTableOwner varchar
   ,IN  pForeignTableName  varchar
   ,IN  pMetadataSchema    varchar
   ,IN  pTargetSchema      varchar
   ,IN  pTargetTableName   varchar DEFAULT NULL
) RETURNS VARCHAR[]
AS
$BODY$ 
DECLARE
   str_sql              VARCHAR(32000);
   str_sql2             VARCHAR(32000);
   ary_columns          VARCHAR(30)[];
   int_count            INTEGER;
   r                    REFCURSOR; 
   rec                  RECORD;
   str_temp             VARCHAR(32000);
   ary_results          VARCHAR(32000)[];
   
   str_constraint       VARCHAR(255);
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
           
   EXECUTE str_sql INTO int_count USING pOracleOwner,pOracleTable;
   
   IF int_count <> 1
   THEN
      RAISE EXCEPTION 'Oracle table not found using existing metadata resources';
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get list of constraints
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.constraint_name '
           || ',a.constraint_type '
           || ',a.search_condition '
           || ',a.index_owner     '
           || ',a.index_name      '
           || 'FROM '
           || pMetadataSchema || '.all_constraints a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 ';
           
   OPEN r FOR EXECUTE str_sql USING pOracleOwner,pOracleTable;
   FETCH NEXT FROM r INTO rec; 
   
   int_count := 1;
   WHILE FOUND 
   LOOP
      IF rec.constraint_type IN ('P','U')
      THEN
         IF rec.constraint_type = 'P'
         THEN
            str_constraint := 'PRIMARY KEY';
            
         ELSIF rec.constraint_type = 'U'
         THEN
            str_constraint := 'UNIQUE';
            
         END IF;
      
         str_sql2 := 'SELECT '
                  || 'ARRAY( '
                  || '   SELECT '
                  || '   a.column_name '
                  || '   FROM '
                  || '   ' || pMetadataSchema || '.all_cons_columns a '
                  || '   WHERE '
                  || '       a.owner = $1 '
                  || '   AND a.table_name = $2 '
                  || '   AND a.constraint_name = $3 '
                  || '   ORDER BY '
                  || '   a.position '
                  || ') ';
                  
         EXECUTE str_sql2 INTO ary_columns
         USING pOracleOwner,pOracleTable,rec.constraint_name;
         
         str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                  || 'ADD CONSTRAINT ' || LOWER(rec.constraint_name) || ' ' || str_constraint || '('
                  || LOWER(array_to_string(ary_columns,','))
                  || ') USING INDEX ' || LOWER(rec.index_name);
         
         ary_results := array_append(ary_results,str_temp);
               
      ELSIF rec.constraint_type = 'C'
      THEN
         str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                  || 'ADD CONSTRAINT ' || LOWER(rec.constraint_name) || ' CHECK('
                  || LOWER(rec.search_condition) || ') ';
         
         ary_results := array_append(ary_results,str_temp);
         
      END IF;
   
      FETCH NEXT FROM r INTO rec; 

   END LOOP;
   
   CLOSE r;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.extract_constraints(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.extract_constraints(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
) TO PUBLIC;

