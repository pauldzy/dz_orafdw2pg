CREATE OR REPLACE FUNCTION dz_pg.extract_constraints(
    IN  pForeignTableOwner VARCHAR
   ,IN  pForeignTableName  VARCHAR
   ,IN  pMetadataSchema    VARCHAR
   ,IN  pTargetSchema      VARCHAR
   ,IN  pTargetTableName   VARCHAR DEFAULT NULL
) RETURNS TABLE(
    pOutConstraintType     VARCHAR(1)
   ,pOutConstraintDDL      TEXT
)
STABLE
AS
$BODY$ 
DECLARE
   rec                  RECORD;
   str_sql              VARCHAR(32000);
   str_sql2             VARCHAR(32000);
   ary_columns          VARCHAR(30)[];
   ary_r_columns        VARCHAR(30)[];
   int_count            INTEGER;
   r                    REFCURSOR; 
   rec_out              RECORD;
   str_temp             VARCHAR(32000);
   ary_results          VARCHAR(32000)[];
   
   str_constraint       VARCHAR(255);
   str_oracle_owner     VARCHAR(255);
   str_oracle_tablename VARCHAR(255);
   str_target_schema    VARCHAR(255);
   str_target_tablename VARCHAR(255);
   str_r_table_owner    VARCHAR(255);
   str_r_table_name     VARCHAR(255);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
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
           || pMetadataSchema || '.pg_orafdw_table_map a '
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
      RAISE EXCEPTION 'Oracle table not found using existing metadata resources';
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 40
   -- Get list of constraints
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.constraint_name  '
           || ',a.constraint_type  '
           || ',a.search_condition '
           || ',a.r_owner '
           || ',a.r_constraint_name '
           || ',a.index_owner      '
           || ',a.index_name       '
           || 'FROM '
           || pMetadataSchema || '.all_constraints a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 ';
           
   OPEN r FOR EXECUTE str_sql USING str_oracle_owner,str_oracle_tablename;
   FETCH NEXT FROM r INTO rec; 
   
   int_count := 1;
   WHILE FOUND 
   LOOP
      IF rec.constraint_type IN ('P','U','R')
      THEN
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
         USING str_oracle_owner,str_oracle_tablename,rec.constraint_name;
         
         IF rec.constraint_type = 'R'
         THEN
            str_sql2 := 'SELECT '
                     || ' a.owner '
                     || ',a.table_name '
                     || 'FROM '
                     || pMetadataSchema || '.all_constraints a '
                     || 'WHERE '
                     || '    a.owner = $1 '
                     || 'AND a.constraint_name = $2 ';
                     
            EXECUTE str_sql2 INTO str_r_table_owner,str_r_table_name
            USING rec.r_owner,rec.r_constraint_name;   
                     
            str_sql2 := 'SELECT '
                     || 'ARRAY( '
                     || '   SELECT '
                     || '   a.column_name '
                     || '   FROM '
                     || '   ' || pMetadataSchema || '.all_cons_columns a '
                     || '   WHERE '
                     || '       a.owner = $1 '
                     || '   AND a.constraint_name = $2 '
                     || '   ORDER BY '
                     || '   a.position '
                     || ') ';
            
            EXECUTE str_sql2 INTO ary_r_columns
            USING rec.r_owner,rec.r_constraint_name;
            
            str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                     || 'ADD CONSTRAINT ' || LOWER(rec.constraint_name) || ' ' 
                     || 'FOREIGN KEY (' || LOWER(array_to_string(ary_columns,',')) || ') '
                     || 'REFERENCES ' || str_target_schema || '.' || LOWER(str_r_table_name)
                     || '(' || LOWER(array_to_string(ary_r_columns,',')) || ')';
            
         ELSE
            IF rec.constraint_type = 'P'
            THEN
               str_constraint := 'PRIMARY KEY';
               
            ELSIF rec.constraint_type = 'U'
            THEN
               str_constraint := 'UNIQUE';
               
            END IF;
         
            IF rec.index_name IS NOT NULL
            THEN         
               str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                        || 'ADD CONSTRAINT ' || LOWER(rec.constraint_name) || ' ' || str_constraint || ' '
                        || 'USING INDEX ' || LOWER(rec.index_name);
                        
            ELSE
               str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                        || 'ADD CONSTRAINT ' || LOWER(rec.constraint_name) || ' ' || str_constraint || '('
                        || LOWER(array_to_string(ary_columns,','))
                        || ') ';
            
            END IF;
         
         END IF;
         
         pOutConstraintType := rec.constraint_type;
         pOutConstraintDDL  := str_temp;
         RETURN NEXT;     
               
      ELSIF rec.constraint_type = 'C'
      THEN
         str_temp := 'ALTER TABLE ' || str_target_schema || '.' || str_target_tablename || ' '
                  || 'ADD CONSTRAINT ' || LOWER(rec.constraint_name) || ' CHECK('
                  || LOWER(REPLACE(rec.search_condition,'"','')) || ') ';
         
         pOutConstraintType := rec.constraint_type;
         pOutConstraintDDL  := str_temp;
         RETURN NEXT;   
         
      END IF;
   
      FETCH NEXT FROM r INTO rec; 

   END LOOP;
   
   CLOSE r;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.extract_constraints(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.extract_constraints(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

