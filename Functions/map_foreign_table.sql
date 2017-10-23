CREATE OR REPLACE FUNCTION dz_pg.map_foreign_table(
    IN  pOracleOwner    varchar
   ,IN  pOracleTable    varchar
   ,IN  pForeignServer  varchar
   ,IN  pTargetSchema   varchar
   ,IN  pMetadataSchema varchar
) RETURNS BOOLEAN
AS
$BODY$ 
DECLARE
   str_sql    VARCHAR(32000);
   str_map    VARCHAR(32000);
   str_select VARCHAR(32000);
   int_count  INTEGER;
   r          REFCURSOR; 
   rec        RECORD;
   str_comma  VARCHAR(1);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check for existing Oracle resource 
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || 'COUNT(*) '
           || 'FROM '
           || pMetadataSchema || '.all_tables a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2';
           
   EXECUTE str_sql INTO int_count USING pOracleOwner,pOracleTable;
   
   IF int_count <> 1
   THEN
      RAISE EXCEPTION 'Oracle table not found using existing metadata resources';
   
   END IF;

   ----------------------------------------------------------------------------
   -- Step 20
   -- Drop any existing foreign table
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.' || pOracleTable;
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Generate the foreign table columns mapping
   ----------------------------------------------------------------------------
   str_map := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.' || pOracleTable || '( ';
   str_select := 'SELECT ';
   
   str_sql := 'SELECT '
           || ' a.column_name '
           || ',a.data_type '
           || ',a.data_type_mod '
           || ',a.data_type_owner '
           || ',a.data_length '
           || ',a.data_precision '
           || ',a.data_scale '
           || ',a.nullable '
           || ',a.column_id '
           || 'FROM '
           || pMetadataSchema || '.all_tab_columns a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 '
           || 'ORDER BY '
           || 'a.column_id ';
   
   OPEN r FOR EXECUTE str_sql; 
   FETCH NEXT FROM r INTO rec;
   
   str_comma := ' ';
   WHILE FOUND 
   LOOP
      FETCH NEXT FROM r INTO rec; 
      
      IF rec.data_type IN ('BLOB','RAW','LONG RAW')
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map || 'bytea';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSIF rec.data_type = 'BINARY_DOUBLE'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'double precision';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSIF rec.data_type = 'BINARY_FLOAT'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'real';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSIF rec.data_type = 'CHAR'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'character(' || rec.data_length::varchar || ')';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSIF rec.data_type IN ('CLOB','NCLOB','LONG')
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'text';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSIF rec.data_type = 'DATE'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'timestamp(0)';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
               
      ELSIF rec.data_type = 'NUMBER'
      THEN
         IF rec.data_length = 22
         AND rec.data_precision IS NULL
         AND rec.data_scale = 0
         THEN
            str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
            str_map    := str_map    || 'integer';
            str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
            
            IF rec.nullable IS NOT NULL
            AND rec.nullable = 'Y'
            THEN
               str_map := str_map || ' null';
               
            END IF;
      
         ELSE
            str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
            str_map    := str_map || 'numeric';
            str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
            
            IF rec.data_precision IS NOT NULL
            THEN
               str_map := str_map || '(' || rec.data_precision::varchar;
               
               IF rec.data_scale IS NOT NULL
               AND rec.data_scale <> 0
               THEN
                  str_map := str_map || ',' || rec.data_scale::varchar;
                  
               END IF;
               
               str_map := str_map || ')';
               
            END IF;
            
            IF rec.nullable IS NOT NULL
            AND rec.nullable = 'Y'
            THEN
               str_map := str_map || ' null';
               
            END IF;
            
         END IF; 
         
         str_comma := ',';
       
      ELSIF rec.data_type = 'ROWID'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'character(10)';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
      
      ELSIF rec.data_type = 'VARCHAR2'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'character varying';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.data_length IS NOT NULL
         THEN
            str_map := str_map || '(' || rec.data_length::varchar || ')';
            
         END IF;
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
      
      ELSIF rec.data_type = 'TIMESTAMP(6)'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'timestamp(6)';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSIF rec.data_type = 'TIMESTAMP(6) WITH TIME ZONE'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'timestamp(6)';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
      
      ELSIF rec.data_type = 'SDO_GEOMETRY'
      AND rec.data_type_owner = 'MDSYS'
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map    || 'geometry';
         str_select := str_select || '   a.' || str_comma || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      END IF; 

   END LOOP;
   
   CLOSE r; 
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- finalize and execute the create foreign table statement
   ----------------------------------------------------------------------------
   str_map := str_map || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''('
           || str_select
           || '   FROM '
           || '   ' || pOracleSchema || '.' || pOracleTable || ' a '
           || ')'')';
           
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

