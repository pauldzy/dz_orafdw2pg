CREATE OR REPLACE FUNCTION dz_pg.copy_foreign_table(
    IN  pForeignTableOwner VARCHAR
   ,IN  pForeignTableName  VARCHAR
   ,IN  pMetadataSchema    VARCHAR
   ,IN  pTargetSchema      VARCHAR
   ,IN  pTargetTableName   VARCHAR   DEFAULT NULL
   ,IN  pTargetTablespace  VARCHAR   DEFAULT NULL
   ,IN  pForceObjectID     BOOLEAN   DEFAULT FALSE
   ,IN  pNoCopy            BOOLEAN   DEFAULT FALSE
   ,IN  pPostFlightGroup   VARCHAR   DEFAULT NULL
   ,IN  pPostFlightAction  VARCHAR   DEFAULT NULL
   ,IN  pPostFlightGUID    VARCHAR   DEFAULT NULL
   ,IN  pPostFlightTime    TIMESTAMP DEFAULT NULL
) RETURNS BOOLEAN
VOLATILE
AS
$BODY$
DECLARE
   str_sql              VARCHAR(32000);
   str_statement        VARCHAR(32000);
   int_count            INTEGER;
   ary_items            VARCHAR(32000)[];
   r                    REFCURSOR;
   rec                  RECORD;
   str_unique_guid      VARCHAR(40);
   dat_current_time     TIMESTAMP;

   str_tablespace       VARCHAR(255);
   str_oracle_owner     VARCHAR(255);
   str_oracle_tablename VARCHAR(255);
   str_target_schema    VARCHAR(255);
   str_target_tablename VARCHAR(255);
   boo_insert_objectid  BOOLEAN;
   str_postflight       VARCHAR(255);
   str_postflight_act   VARCHAR(255);

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

   IF pPostFlightGroup IS NULL
   THEN
      str_postflight := 'Append';

   ELSE
      str_postflight := pPostFlightGroup;

   END IF;

   IF pPostFlightAction IS NULL
   THEN
      str_postflight_act := 'APPEND';

   ELSE
      str_postflight_act := UPPER(pPostFlightAction);

   END IF;

   IF pPostFlightGUID IS NULL
   THEN
      str_unique_guid  := '{' || uuid_generate_v1() || '}';

   ELSE
      str_unique_guid := pPostFlightGUID;

   END IF;

   IF pPostFlightTime IS NULL
   THEN
      dat_current_time := (abstime(('now'::text)::timestamp(6) with time zone));

   ELSE
      dat_current_time := pPostFlightTime;

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
   -- Flush the postflight if requested
   ----------------------------------------------------------------------------
   IF str_postflight_act IN ('FLUSH','TRUNCATE')
   THEN
      str_sql := 'DELETE FROM ' || pMetadataSchema || '.pg_orafdw_postflight '
              || 'WHERE '
              || 'copy_group_keyword = $1 ';

      EXECUTE str_sql USING str_postflight;

   END IF;

   ----------------------------------------------------------------------------
   -- Step 60
   -- Drop any existing table
   ----------------------------------------------------------------------------
   str_sql := 'DROP TABLE IF EXISTS ' || str_target_schema || '.' || str_target_tablename || ' '
           || 'CASCADE ';

   EXECUTE str_sql;

   ----------------------------------------------------------------------------
   -- Step 70
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
   -- Step 80
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
   -- Step 90
   -- Collect the constraint statements for table
   ----------------------------------------------------------------------------
   FOR rec IN SELECT * FROM dz_pg.extract_constraints(
       pForeignTableOwner := pForeignTableOwner
      ,pForeignTableName  := pForeignTableName
      ,pMetadataSchema    := pMetadataSchema
      ,pTargetSchema      := pTargetSchema
      ,pTargetTableName   := pTargetTableName
   )
   LOOP
      IF rec.pOutConstraintType IN ('C','P')
      THEN
         EXECUTE rec.pOutConstraintDDL;

      ELSIF rec.pOutConstraintType = 'R'
      THEN
         str_sql := 'SELECT '
                 || 'MAX(a.postflight_order) '
                 || 'FROM '
                 || pMetadataSchema || '.pg_orafdw_postflight a '
                 || 'WHERE '
                 || 'a.copy_group_keyword = $1 ';

         EXECUTE str_sql INTO int_count USING str_postflight;

         IF int_count IS NULL
         THEN
            int_count := 1;

         ELSE
            int_count := int_count + 1;

         END IF;

         str_sql := 'INSERT INTO ' || pMetadataSchema || '.pg_orafdw_postflight('
                 || '    copy_action_id '
                 || '   ,copy_action_time '
                 || '   ,copy_group_keyword '
                 || '   ,postflight_order '
                 || '   ,postflight_action '
                 || ') VALUES ('
                 || '    $1 '
                 || '   ,$2 '
                 || '   ,$3 '
                 || '   ,$4 '
                 || '   ,$5 '
                 || ') ';

         EXECUTE str_sql USING
          str_unique_guid
         ,dat_current_time
         ,str_postflight
         ,int_count
         ,rec.pOutConstraintDDL;

      END IF;

   END LOOP;

   ----------------------------------------------------------------------------
   -- Step 100
   -- Load the target table
   ----------------------------------------------------------------------------
   IF NOT pNoCopy
   THEN
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

   END IF;

   ----------------------------------------------------------------------------
   -- Step 110
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;

END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.copy_foreign_table(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,TIMESTAMP
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.copy_foreign_table(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
   ,BOOLEAN
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,TIMESTAMP
) TO PUBLIC;

