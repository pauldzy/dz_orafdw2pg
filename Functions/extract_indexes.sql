CREATE OR REPLACE FUNCTION dz_pg.extract_indexes(
    IN  pForeignTableOwner varchar
   ,IN  pForeignTableName  varchar
   ,IN  pMetadataSchema    varchar
   ,IN  pTargetSchema      varchar
   ,IN  pTargetTableName   varchar DEFAULT NULL
   ,IN  pTargetTablespace  varchar DEFAULT NULL
) RETURNS VARCHAR[]
AS
$BODY$ 
DECLARE
   str_sql              VARCHAR(32000);
   str_sql2             VARCHAR(32000);
   
   ary_columns          VARCHAR(255)[];
   int_count            INTEGER;
   r                    REFCURSOR; 
   rec                  RECORD;
   str_temp             VARCHAR(32000);
   ary_results          VARCHAR(32000)[];
   
   str_unique           VARCHAR(255);
   str_tablespace       VARCHAR(255);
   str_oracle_owner     VARCHAR(255);
   str_oracle_tablename VARCHAR(255);
   str_target_schema    VARCHAR(255);
   str_target_tablename VARCHAR(255);
   str_nvl_field        VARCHAR(255);
   str_nvl_isnull       VARCHAR(255);
   str_column_name      VARCHAR(30);
   int_srid             INTEGER;
   
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
   -- Get list of constraints
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.index_name '
           || ',a.index_type '
           || ',a.uniqueness '
           || ',a.ityp_owner '
           || ',a.ityp_name  '
           || 'FROM '
           || pMetadataSchema || '.all_indexes a '
           || 'WHERE '
           || '    a.table_owner = $1 '
           || 'AND a.table_name = $2 ';
           
   OPEN r FOR EXECUTE str_sql USING str_oracle_owner,str_oracle_tablename;
   FETCH NEXT FROM r INTO rec; 
   
   int_count := 1;
   WHILE FOUND 
   LOOP
      IF rec.index_type IN ('BITMAP','NORMAL')
      THEN
         IF rec.uniqueness = 'UNIQUE'
         THEN
            str_unique := 'UNIQUE';
            
         ELSE
            str_unique := '';
            
         END IF;
         
         str_sql2 := 'SELECT '
                  || 'ARRAY( '
                  || '   SELECT '
                  || '   a.column_name '
                  || '   FROM '
                  || '   ' || pMetadataSchema || '.all_ind_columns a '
                  || '   WHERE '
                  || '       a.table_owner = $1 '
                  || '   AND a.table_name = $2 '
                  || '   AND a.index_name = $3 '
                  || '   ORDER BY '
                  || '   a.column_position '
                  || ') ';
                  
         EXECUTE str_sql2 INTO ary_columns
         USING str_oracle_owner,str_oracle_tablename,rec.index_name;
         
         str_temp := 'CREATE ' || str_unique || ' INDEX ' || LOWER(rec.index_name) || ' '
                  || 'ON ' || str_target_schema || '.' || str_target_tablename || '('
                  || LOWER(array_to_string(ary_columns,','))
                  || ') ' || str_tablespace;
               
         ary_results := array_append(ary_results,str_temp);

      ELSIF rec.index_type IN ('FUNCTION-BASED NORMAL','FUNCTION-BASED BITMAP')
      THEN
         str_sql2 := 'SELECT '
                  || 'ARRAY( '
                  || '   SELECT '
                  || '   a.column_expression '
                  || '   FROM '
                  || '   ' || pMetadataSchema || '.all_ind_expressions a '
                  || '   WHERE '
                  || '       a.table_owner = $1 '
                  || '   AND a.table_name = $2 '
                  || '   AND a.index_name = $3 '
                  || '   ORDER BY '
                  || '   a.column_position '
                  || ') ';
                  
         EXECUTE str_sql2 INTO ary_columns
         USING str_oracle_owner,str_oracle_tablename,rec.index_name;
         
         str_temp = REPLACE(ary_columns[1],'"','');
         
         IF SUBSTR(LOWER(str_temp),1,7) = 'substr('
         OR SUBSTR(LOWER(str_temp),1,6) = 'upper('
         OR SUBSTR(LOWER(str_temp),1,4) = 'nvl('
         THEN
            str_temp := REGEXP_REPLACE(str_temp,'(N|n)(V|v)(L|l)\(','coalesce(');
         
         END IF;
         
         str_temp := 'CREATE INDEX ' || LOWER(rec.index_name) || ' '
                  || 'ON ' || str_target_schema || '.' || str_target_tablename || '('
                  || str_temp || ') ' || str_tablespace;
                     
         ary_results := array_append(ary_results,str_temp);
   
      ELSIF rec.index_type = 'DOMAIN'
      AND   rec.ityp_owner = 'MDSYS'
      AND   rec.ityp_name  = 'SPATIAL_INDEX' 
      THEN
         str_sql2 := 'SELECT '
                  || 'ARRAY( '
                  || '   SELECT '
                  || '   a.column_name '
                  || '   FROM '
                  || '   ' || pMetadataSchema || '.all_ind_columns a '
                  || '   WHERE '
                  || '       a.table_owner = $1 '
                  || '   AND a.table_name = $2 '
                  || '   AND a.index_name = $3 '
                  || '   ORDER BY '
                  || '   a.column_position '
                  || ') ';
                  
         EXECUTE str_sql2 INTO ary_columns
         USING str_oracle_owner,str_oracle_tablename,rec.index_name;
         
         str_column_name = ary_columns[1];
         
         str_sql2 := 'SELECT '
                  || 'a.srid '
                  || 'FROM '
                  || pMetadataSchema || '.all_sdo_geom_metadata a '
                  || 'WHERE '
                  || '    a.owner = $1 '
                  || 'AND a.table_name  = $2 '
                  || 'AND a.column_name = $3 ';
                  
         EXECUTE str_sql2 INTO int_srid
         USING str_oracle_owner,str_oracle_tablename,str_column_name;
         
         IF int_srid IS NOT NULL
         THEN
            str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                     || 'ADD CONSTRAINT enforce_srid_shape CHECK('
                     || 'st_srid(' || LOWER(str_column_name) || ') = ' || dz_pg.srid_swap(int_srid)::varchar
                     || ') ';
                  
            ary_results := array_append(ary_results,str_temp);
         
         END IF;
         
         str_temp := 'CREATE INDEX ' || LOWER(rec.index_name) || ' '
                     || 'ON ' || str_target_schema || '.' || str_target_tablename || ' USING GIST('
                     || LOWER(str_column_name) || ') ' || str_tablespace;
                     
         ary_results := array_append(ary_results,str_temp);

      END IF;
   
      FETCH NEXT FROM r INTO rec; 

   END LOOP;
   
   CLOSE r; 
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN ary_results;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.extract_indexes(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.extract_indexes(
    varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
   ,varchar
) TO PUBLIC;

