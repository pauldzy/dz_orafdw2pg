CREATE OR REPLACE FUNCTION dz_pg.map_foreign_table(
    IN  pOracleOwner    VARCHAR
   ,IN  pOracleTable    VARCHAR
   ,IN  pForeignServer  VARCHAR
   ,IN  pTargetSchema   VARCHAR
   ,IN  pMetadataSchema VARCHAR
) RETURNS BOOLEAN
VOLATILE
AS
$BODY$ 
DECLARE
   str_sql      VARCHAR(32000);
   str_map      VARCHAR(32000);
   str_select   VARCHAR(32000);
   int_count    INTEGER;
   r            REFCURSOR; 
   rec          RECORD;
   str_comma    VARCHAR(1);
   oid_ftrelid  OID;
   oid_ftserver OID;
      
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
   str_map := '';
   
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
           || ',a.char_used '
           || ',a.char_length '
           || 'FROM '
           || pMetadataSchema || '.all_tab_columns a '
           || 'WHERE '
           || '    a.owner = $1 '
           || 'AND a.table_name = $2 '
           || 'ORDER BY '
           || 'a.column_id ';
   
   OPEN r FOR EXECUTE str_sql USING pOracleOwner,pOracleTable;
   FETCH NEXT FROM r INTO rec; 
   
   str_comma := ' ';
   WHILE FOUND 
   LOOP
      IF rec.data_type IN ('BLOB','RAW','LONG RAW')
      THEN
         str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
         str_map    := str_map || 'bytea';
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || dz_pg.char_cleaner(LOWER(rec.column_name),'a');
         
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
               
      ELSIF rec.data_type = 'NUMBER'
      THEN
         IF ( rec.data_length = 22 AND rec.data_precision = 1     AND rec.data_scale = 0 )
         OR ( rec.data_length = 22 AND rec.data_precision IS NULL AND rec.data_scale = 0 )
         THEN
            str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
            str_map    := str_map    || 'integer';
            str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
            
            IF rec.nullable IS NOT NULL
            AND rec.nullable = 'Y'
            THEN
               str_map := str_map || ' null';
               
            END IF;
      
         ELSE
            str_map    := str_map    || '   ' || str_comma || LOWER(rec.column_name) || '   ';
            str_map    := str_map || 'numeric';
            str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
            
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || dz_pg.char_cleaner(LOWER(rec.column_name),'a');
         
         IF rec.char_used = 'C'
         AND rec.char_length IS NOT NULL
         THEN
            str_map := str_map || '(' || rec.char_length::varchar || ')';
         
         ELSE
            IF rec.data_length IS NOT NULL
            OR rec.char_length IS NOT NULL
            THEN
               str_map := str_map || '(' || rec.data_length::varchar || ')';
            
            END IF;
            
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
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
         str_select := str_select || '   ' || str_comma || 'a.' || LOWER(rec.column_name);
         
         IF rec.nullable IS NOT NULL
         AND rec.nullable = 'Y'
         THEN
            str_map := str_map || ' null';
            
         END IF;
         
         str_comma := ',';
         
      ELSE
         RAISE WARNING 'Unable to map % %', rec.data_type, rec.data_type_owner;
      
      END IF; 
      --RAISE WARNING 'map % % % %', rec.data_type, rec.data_length,rec.data_precision,rec.data_scale;
      FETCH NEXT FROM r INTO rec; 

   END LOOP;
   
   CLOSE r; 
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- finalize and execute the create foreign table statements
   ----------------------------------------------------------------------------
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.' || pOracleTable || '( '
           || str_map || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''('
           || str_select
           || '   FROM '
           || '   ' || pOracleOwner || '.' || pOracleTable || ' a '
           || ')'')';
           
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.' || pOracleTable || '_dml( '
           || str_map || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (schema ''' || pOracleOwner || ''', table ''' || pOracleTable || ''')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Get the oids of the foreign table
   ----------------------------------------------------------------------------
   str_sql := 'SELECT '
           || ' a.ftrelid '
           || ',a.ftserver '
           || 'FROM '
           || 'pg_foreign_table a '
           || 'JOIN '
           || 'pg_class b '
           || 'ON '
           || 'a.ftrelid = b.oid '
           || 'JOIN '
           || 'pg_namespace c '
           || 'ON '
           || 'c.oid = b.relnamespace '
           || 'WHERE '
           || '    c.nspname = $1 '
           || 'AND b.relname = $2 ';
           
   EXECUTE str_sql INTO oid_ftrelid,oid_ftserver
   USING LOWER(pTargetSchema),LOWER(pOracleTable);
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Create the map entry
   ----------------------------------------------------------------------------
   str_sql := 'INSERT INTO ' || pMetadataSchema || '.oracle_fdw_table_map( '
           || '    ftrelid              '
           || '   ,ftserver             '
           || '   ,oracle_owner         '
           || '   ,oracle_tablename     '
           || '   ,foreign_table_schema '
           || '   ,foreign_table_name   '
           || ') VALUES ($1,$2,$3,$4,$5,$6)   ';
           
   EXECUTE str_sql USING
    oid_ftrelid
   ,oid_ftserver
   ,pOracleOwner
   ,pOracleTable
   ,LOWER(pTargetSchema)
   ,LOWER(pOracleTable);

   ----------------------------------------------------------------------------
   -- Step 60
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

