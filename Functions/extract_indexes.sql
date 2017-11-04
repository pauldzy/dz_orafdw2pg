CREATE OR REPLACE FUNCTION dz_pg.extract_indexes(
    IN  pOracleOwner      varchar
   ,IN  pOracleTable      varchar
   ,IN  pMetadataSchema   varchar
   ,IN  pTargetSchema     varchar
   ,IN  pTargetName       varchar DEFAULT NULL
   ,IN  pTargetTablespace varchar DEFAULT NULL
) RETURNS VARCHAR[]
AS
$BODY$ 
DECLARE
   str_sql         VARCHAR(32000);
   str_sql2        VARCHAR(32000);
   str_tablespace  VARCHAR(32000);
   str_unique      VARCHAR(32000);
   ary_name        VARCHAR(32000)[];
   ary_type        VARCHAR(32000)[];
   ary_columns     VARCHAR(30)[];
   int_count       INTEGER;
   r               REFCURSOR; 
   rec             RECORD;
   str_comma       VARCHAR(1);
   str_temp        VARCHAR(32000);
   ary_results     VARCHAR(32000)[];
   str_tablename   VARCHAR(30);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF pTargetName IS NOT NULL
   THEN
      str_tablename := pTargetName;
      
   ELSE
      str_tablename := LOWER(pOracleTable);
      
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
   -- Step 30
   -- Get list of constraints
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.index_name '
           || ',a.index_type '
           || ',a.uniqueness '
           || 'FROM '
           || pMetadataSchema || '.all_indexes a '
           || 'WHERE '
           || '    a.table_owner = $1 '
           || 'AND a.table_name = $2 ';
           
   OPEN r FOR EXECUTE str_sql USING pOracleOwner,pOracleTable;
   FETCH NEXT FROM r INTO rec; 
   
   int_count := 1;
   WHILE FOUND 
   LOOP
      IF rec.uniqueness = 'UNIQUE'
      THEN
         str_unique := 'UNIQUE';
         
      ELSE
         str_unique := '';
         
      END IF;
   
      IF rec.index_type = 'NORMAL'
      THEN
         str_sql2 := 'SELECT '
                  || 'array_agg(a.column_name::varchar) AS column_name '
                  || 'FROM '
                  || pMetadataSchema || '.all_ind_columns a '
                  || 'WHERE '
                  || '    a.table_owner = $1 '
                  || 'AND a.table_name = $2 '
                  || 'AND a.index_name = $3 '
                  || 'ORDER BY '
                  || 'a.column_position ';
                  
         EXECUTE str_sql2 INTO ary_columns
         USING pOracleOwner,pOracleTable,rec.constraint_name;
         
         str_temp := 'CREATE ' || str_unique || ' INDEX ' || LOWER(rec.index_name) || ' '
                  || 'ON ' || pTargetSchema || '.' || str_tablename || '('
                  || LOWER(array_append(ary_results,str_temp)) ||
                  || ')' || str_tablespace;
               
         ary_append(ary_results,str_temp);
         
      ELSIF rec.constraint_type = 'C'
      THEN
         str_temp := 'l';
         
      END IF;
   
      FETCH NEXT FROM r INTO rec; 

   END LOOP;
   
   CLOSE r; 
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Generate the foreign table columns mapping
   ----------------------------------------------------------------------------
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create the map entry
   ----------------------------------------------------------------------------

   ----------------------------------------------------------------------------
   -- Step 60
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN ary_results;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.extract_constraints(
    varchar
   ,varchar
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
   ,varchar
) TO PUBLIC;

