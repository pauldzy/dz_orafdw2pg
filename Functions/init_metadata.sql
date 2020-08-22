CREATE OR REPLACE FUNCTION dz_pg.init_metadata(
    IN  pForeignServer      VARCHAR
   ,IN  pMetadataSchema     VARCHAR
   ,IN  pMetadataTablespace VARCHAR DEFAULT NULL
) RETURNS BOOLEAN
AS
$BODY$ 
DECLARE
   str_sql        VARCHAR(32000);
   str_tablespace VARCHAR(32000);
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Check over incoming parameters
   ----------------------------------------------------------------------------
   IF pMetadataTablespace IS NOT NULL
   THEN
      str_tablespace := 'TABLESPACE ' || pMetadataTablespace || ' ';
      
   ELSE
      str_tablespace := ' ';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Build all_tables
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_tables';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_tables( '
           || '    owner                     VARCHAR(30)  '
           || '   ,table_name                VARCHAR(30)  '
           || '   ,tablespace_name           VARCHAR(30)  '
           || '   ,cluster_name              VARCHAR(30)  '
           || '   ,iot_name                  VARCHAR(30)  '
           || '   ,status                    VARCHAR(8)   '
           || '   ,pct_free                  NUMERIC      '
           || '   ,pct_used                  NUMERIC      '
           || '   ,ini_trans                 NUMERIC      '
           || '   ,max_trans                 NUMERIC      '
           || '   ,initial_extent            NUMERIC      '
           || '   ,next_extent               NUMERIC      '
           || '   ,min_extents               NUMERIC      '
           || '   ,max_extents               NUMERIC      '
           || '   ,pct_increase              NUMERIC      '
           || '   ,freelists                 NUMERIC      '
           || '   ,freelist_groups           NUMERIC      '
           || '   ,logging                   VARCHAR(3)   '
           || '   ,backed_up                 VARCHAR(1)   '
           || '   ,num_rows                  NUMERIC      '
           || '   ,blocks                    NUMERIC      '
           || '   ,empty_blocks              NUMERIC      '
           || '   ,avg_space                 NUMERIC      '
           || '   ,chain_cnt                 NUMERIC      '
           || '   ,avg_row_len               NUMERIC      '
           || '   ,avg_space_freelist_blocks NUMERIC      '
           || '   ,num_freelist_blocks       NUMERIC      '
           || '   ,degree                    VARCHAR(40)  '
           || '   ,instances                 VARCHAR(40)  '
           || '   ,cache                     VARCHAR(20)  '
           || '   ,table_lock                VARCHAR(8)   '
           || '   ,sample_size               NUMERIC      '
           || '   ,last_analyzed             TIMESTAMP(0) '
           || '   ,partitioned               VARCHAR(3)   '
           || '   ,iot_type                  VARCHAR(12)  '
           || '   ,temporary                 VARCHAR(1)   '
           || '   ,secondary                 VARCHAR(1)   '
           || '   ,nested                    VARCHAR(3)   '
           || '   ,buffer_pool               VARCHAR(8)   '
           || '   ,flash_cache               VARCHAR(7)   '
           || '   ,cell_flash_cache          VARCHAR(7)   '
           || '   ,row_movement              VARCHAR(8)   '
           || '   ,global_stats              VARCHAR(3)   '
           || '   ,user_stats                VARCHAR(3)   '
           || '   ,duration                  VARCHAR(15)  '
           || '   ,skip_corrupt              VARCHAR(8)   '
           || '   ,monitoring                VARCHAR(3)   '
           || '   ,cluster_owner             VARCHAR(30)  '
           || '   ,dependencies              VARCHAR(8)   '
           || '   ,compression               VARCHAR(8)   '
           || '   ,compress_for              VARCHAR(12)  '
           || '   ,dropped                   VARCHAR(3)   '
           || '   ,read_only                 VARCHAR(3)   '
           || '   ,segment_created           VARCHAR(3)   '
           || '   ,result_cache              VARCHAR(7)   '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_TABLES)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Build all_tab_columns
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_tab_columns';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_tab_columns( '
           || '    owner                     VARCHAR(30)  '
           || '   ,table_name                VARCHAR(30)  '
           || '   ,column_name               VARCHAR(30)  '
           || '   ,data_type                 VARCHAR(106) '
           || '   ,data_type_mod             VARCHAR(30)  '
           || '   ,data_type_owner           VARCHAR(120) '
           || '   ,data_length               NUMERIC      '  
           || '   ,data_precision            NUMERIC      '
           || '   ,data_scale                NUMERIC      '
           || '   ,nullable                  VARCHAR(1)   '
           || '   ,column_id                 NUMERIC      '   
           || '   ,default_length            NUMERIC      ' 
           || '   ,data_default              BYTEA        '
           || '   ,num_distinct              NUMERIC      '
           || '   ,low_value                 BYTEA        '
           || '   ,high_value                BYTEA        '
           || '   ,density                   NUMERIC      '
           || '   ,num_nulls                 NUMERIC      '
           || '   ,num_buckets               NUMERIC      '
           || '   ,last_analyzed             TIMESTAMP(0) '
           || '   ,sample_size               NUMERIC      '
           || '   ,character_set_name        VARCHAR(44)  '
           || '   ,char_col_decl_length      NUMERIC      '  
           || '   ,global_stats              VARCHAR(3)   '
           || '   ,user_stats                VARCHAR(3)   '
           || '   ,avg_col_len               NUMERIC      '
           || '   ,char_length               NUMERIC      '
           || '   ,char_used                 VARCHAR(1)   '
           || '   ,v80_fmt_image             VARCHAR(3)   '
           || '   ,data_upgraded             VARCHAR(3)   '
           || '   ,histogram                 VARCHAR(15)  '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''('
           || '   SELECT '
           || '    OWNER '
           || '   ,TABLE_NAME '
           || '   ,COLUMN_NAME '
           || '   ,DATA_TYPE '
           || '   ,DATA_TYPE_MOD '
           || '   ,DATA_TYPE_OWNER '
           || '   ,DATA_LENGTH '
           || '   ,DATA_PRECISION '
           || '   ,DATA_SCALE '
           || '   ,NULLABLE '
           || '   ,COLUMN_ID '
           || '   ,DEFAULT_LENGTH '
           || '   ,DATA_DEFAULT '
           || '   ,NUM_DISTINCT '
           || '   ,LOW_VALUE '
           || '   ,HIGH_VALUE '
           || '   ,DENSITY '
           || '   ,NUM_NULLS '
           || '   ,NUM_BUCKETS '
           || '   ,CASE WHEN LAST_ANALYZED < TO_DATE(''''01/01/1000'''',''''MM/DD/YYYY'''') THEN NULL ELSE LAST_ANALYZED END AS LAST_ANALYZED '
           || '   ,SAMPLE_SIZE '
           || '   ,CHARACTER_SET_NAME '
           || '   ,CHAR_COL_DECL_LENGTH '
           || '   ,GLOBAL_STATS '
           || '   ,USER_STATS '
           || '   ,AVG_COL_LEN '
           || '   ,CHAR_LENGTH '
           || '   ,CHAR_USED '
           || '   ,V80_FMT_IMAGE '
           || '   ,DATA_UPGRADED '
           || '   ,HISTOGRAM '
           || '   FROM '
           || '   SYS.ALL_TAB_COLUMNS '
           || ')'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Build all_constraints
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_constraints';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_constraints('
           || '    owner                     VARCHAR(120)  '
           || '   ,constraint_name           VARCHAR(30)   '
           || '   ,constraint_type           VARCHAR(1)    '
           || '   ,table_name                VARCHAR(30)   '
           || '   ,search_condition          VARCHAR(32000)'
           || '   ,r_owner                   VARCHAR(120)  '
           || '   ,r_constraint_name         VARCHAR(30)   '
           || '   ,delete_rule               VARCHAR(9)    '
           || '   ,status                    VARCHAR(8)    '
           || '   ,"deferrable"              VARCHAR(14)   '
           || '   ,deferred                  VARCHAR(9)    '
           || '   ,validated                 VARCHAR(13)   '
           || '   ,generated                 VARCHAR(14)   '
           || '   ,bad                       VARCHAR(3)    '
           || '   ,rely                      VARCHAR(4)    '
           || '   ,last_change               TIMESTAMP(0)  '
           || '   ,index_owner               VARCHAR(30)   '
           || '   ,index_name                VARCHAR(30)   '
           || '   ,invalid                   VARCHAR(7)    '
           || '   ,view_related              VARCHAR(14)   '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''('
           || '   SELECT '
           || '    OWNER '
           || '   ,CONSTRAINT_NAME '
           || '   ,CONSTRAINT_TYPE '
           || '   ,TABLE_NAME '
           || '   ,SEARCH_CONDITION '
           || '   ,R_OWNER '
           || '   ,R_CONSTRAINT_NAME '
           || '   ,DELETE_RULE '
           || '   ,STATUS '
           || '   ,DEFERRABLE '
           || '   ,DEFERRED '
           || '   ,VALIDATED '
           || '   ,GENERATED '
           || '   ,BAD '
           || '   ,RELY '
           || '   ,CASE WHEN LAST_CHANGE < TO_DATE(''''01/01/1000'''',''''MM/DD/YYYY'''') THEN NULL ELSE LAST_CHANGE END AS LAST_CHANGE '
           || '   ,INDEX_OWNER '
           || '   ,INDEX_NAME '
           || '   ,INVALID '
           || '   ,VIEW_RELATED '
           || '   FROM '
           || '   SYS.ALL_CONSTRAINTS '
           || ')'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Build all_cons_columns
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_cons_columns';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_cons_columns('
           || '    owner                     VARCHAR(30)  '
           || '   ,constraint_name           VARCHAR(30)  '
           || '   ,table_name                VARCHAR(30)  '
           || '   ,column_name               VARCHAR(4000)'
           || '   ,position                  NUMERIC      '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_CONS_COLUMNS)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Build all_indexes
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_indexes';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_indexes('
           || '    owner                     VARCHAR(30)  '
           || '   ,index_name                VARCHAR(30)  '
           || '   ,index_type                VARCHAR(27)  '
           || '   ,table_owner               VARCHAR(30)  '
           || '   ,table_name                VARCHAR(30)  '
           || '   ,table_type                VARCHAR(5)   '
           || '   ,uniqueness                VARCHAR(9)   '
           || '   ,compression               VARCHAR(38)  '
           || '   ,prefix_length             NUMERIC      '
           || '   ,tablespace_name           VARCHAR(30)  '
           || '   ,ini_trans                 NUMERIC      '
           || '   ,max_trans                 NUMERIC      '
           || '   ,initial_extent            NUMERIC      '
           || '   ,next_extent               NUMERIC      '
           || '   ,min_extents               NUMERIC      '
           || '   ,max_extents               NUMERIC      '
           || '   ,pct_increase              NUMERIC      '
           || '   ,pct_threshold             NUMERIC      '
           || '   ,include_column            NUMERIC      '
           || '   ,freelists                 NUMERIC      '
           || '   ,freelist_groups           NUMERIC      '
           || '   ,pct_free                  NUMERIC      '
           || '   ,logging                   VARCHAR(3)   '
           || '   ,blevel                    NUMERIC      '
           || '   ,leaf_blocks               NUMERIC      '
           || '   ,distinct_keys             NUMERIC      '
           || '   ,avg_leaf_blocks_per_key   NUMERIC      '
           || '   ,avg_data_blocks_per_key   NUMERIC      '
           || '   ,clustering_factor         NUMERIC      '
           || '   ,status                    VARCHAR(8)   '
           || '   ,num_rows                  NUMERIC      '
           || '   ,sample_size               NUMERIC      '
           || '   ,last_analyzed             TIMESTAMP(0) '
           || '   ,degree                    VARCHAR(40)  '
           || '   ,instances                 VARCHAR(40)  '
           || '   ,partitioned               VARCHAR(3)   '
           || '   ,temporary                 VARCHAR(1)   '
           || '   ,generated                 VARCHAR(1)   '
           || '   ,secondary                 VARCHAR(1)   '
           || '   ,buffer_pool               VARCHAR(7)   '
           || '   ,flash_cache               VARCHAR(7)   '
           || '   ,cell_flash_cache          VARCHAR(7)   '
           || '   ,user_stats                VARCHAR(3)   '
           || '   ,duration                  VARCHAR(15)  '
           || '   ,pct_direct_access         NUMERIC      '
           || '   ,ityp_owner                VARCHAR(30)  '
           || '   ,ityp_name                 VARCHAR(30)  '
           || '   ,parameters                VARCHAR(1000)'
           || '   ,global_stats              VARCHAR(3)   '
           || '   ,domidx_status             VARCHAR(12)  '
           || '   ,domidx_opstatus           VARCHAR(6)   '
           || '   ,funcidx_status            VARCHAR(8)   '
           || '   ,join_index                VARCHAR(3)   '
           || '   ,iot_redundant_pkey_elim   VARCHAR(3)   '
           || '   ,dropped                   VARCHAR(3)   '
           || '   ,visibility                VARCHAR(9)   '
           || '   ,domidx_management         VARCHAR(14)  '
           || '   ,segment_created           VARCHAR(3)   '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''('
           || '   SELECT '
           || '    OWNER '
           || '   ,INDEX_NAME '
           || '   ,INDEX_TYPE '
           || '   ,TABLE_OWNER '
           || '   ,TABLE_NAME '
           || '   ,TABLE_TYPE '
           || '   ,UNIQUENESS '
           || '   ,COMPRESSION '
           || '   ,PREFIX_LENGTH '
           || '   ,TABLESPACE_NAME '
           || '   ,INI_TRANS '
           || '   ,MAX_TRANS '
           || '   ,INITIAL_EXTENT '
           || '   ,NEXT_EXTENT '
           || '   ,MIN_EXTENTS '
           || '   ,MAX_EXTENTS '
           || '   ,PCT_INCREASE '
           || '   ,PCT_THRESHOLD '
           || '   ,INCLUDE_COLUMN '
           || '   ,FREELISTS '
           || '   ,FREELIST_GROUPS '
           || '   ,PCT_FREE '
           || '   ,LOGGING '
           || '   ,BLEVEL '
           || '   ,LEAF_BLOCKS '
           || '   ,DISTINCT_KEYS '
           || '   ,AVG_LEAF_BLOCKS_PER_KEY '
           || '   ,AVG_DATA_BLOCKS_PER_KEY '
           || '   ,CLUSTERING_FACTOR '
           || '   ,STATUS '
           || '   ,NUM_ROWS '
           || '   ,SAMPLE_SIZE '
           || '   ,CASE WHEN LAST_ANALYZED < TO_DATE(''''01/01/1000'''',''''MM/DD/YYYY'''') THEN NULL ELSE LAST_ANALYZED END AS LAST_ANALYZED '
           || '   ,DEGREE '
           || '   ,INSTANCES '
           || '   ,PARTITIONED '
           || '   ,TEMPORARY '
           || '   ,GENERATED '
           || '   ,SECONDARY '
           || '   ,BUFFER_POOL '
           || '   ,FLASH_CACHE '
           || '   ,CELL_FLASH_CACHE '
           || '   ,USER_STATS '
           || '   ,DURATION '
           || '   ,PCT_DIRECT_ACCESS '
           || '   ,ITYP_OWNER '
           || '   ,ITYP_NAME '
           || '   ,PARAMETERS '
           || '   ,GLOBAL_STATS '
           || '   ,DOMIDX_STATUS '
           || '   ,DOMIDX_OPSTATUS '
           || '   ,FUNCIDX_STATUS '
           || '   ,JOIN_INDEX '
           || '   ,IOT_REDUNDANT_PKEY_ELIM '
           || '   ,DROPPED '
           || '   ,VISIBILITY '
           || '   ,DOMIDX_MANAGEMENT '
           || '   ,SEGMENT_CREATED '
           || '   FROM '
           || '   SYS.ALL_INDEXES '
           || ')'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Build all_ind_columns
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_ind_columns';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_ind_columns('
           || '    index_owner               VARCHAR(30)  '
           || '   ,index_name                VARCHAR(30)  '
           || '   ,table_owner               VARCHAR(30)  '
           || '   ,table_name                VARCHAR(30)  '
           || '   ,column_name               VARCHAR(4000)'
           || '   ,column_position           NUMERIC      '
           || '   ,column_length             NUMERIC      '
           || '   ,char_length               NUMERIC      '
           || '   ,descend                   VARCHAR(4)   '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_IND_COLUMNS)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Build all_ind_expressions
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_ind_expressions';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_ind_expressions('
           || '    index_owner               VARCHAR(30)   '
           || '   ,index_name                VARCHAR(30)   '
           || '   ,table_owner               VARCHAR(30)   '
           || '   ,table_name                VARCHAR(30)   '
           || '   ,column_expression         VARCHAR(32000)'
           || '   ,column_position           NUMERIC       '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_IND_EXPRESSIONS)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Build all_sdo_geom_metadata
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pMetadataSchema || '.all_sdo_geom_metadata';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pMetadataSchema || '.all_sdo_geom_metadata('
           || '    owner                     VARCHAR(32)  '
           || '   ,table_name                VARCHAR(32)  '
           || '   ,column_name               VARCHAR(32)  '
           || '   ,sdo_dimname_1             VARCHAR(64)  '
           || '   ,sdo_lb_1                  NUMERIC      '
           || '   ,sdo_ub_1                  NUMERIC      '
           || '   ,sdo_tolerance_1           NUMERIC      '
           || '   ,sdo_dimname_2             VARCHAR(64)  '
           || '   ,sdo_lb_2                  NUMERIC      '
           || '   ,sdo_ub_2                  NUMERIC      '
           || '   ,sdo_tolerance_2           NUMERIC      '
           || '   ,sdo_dimname_3             VARCHAR(64)  '
           || '   ,sdo_lb_3                  NUMERIC      '
           || '   ,sdo_ub_3                  NUMERIC      '
           || '   ,sdo_tolerance_3           NUMERIC      '
           || '   ,sdo_dimname_4             VARCHAR(64)  '
           || '   ,sdo_lb_4                  NUMERIC      '
           || '   ,sdo_ub_4                  NUMERIC      '
           || '   ,sdo_tolerance_4           NUMERIC      '
           || '   ,srid                      INTEGER      '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''('
           || '   SELECT '
           || '    a.owner '
           || '   ,a.table_name '
           || '   ,a.column_name '
           || '   ,MAX(a.sdo_dimname_1)   AS sdo_dimname_1 '
           || '   ,MAX(a.sdo_lb_1)        AS sdo_lb_1 '
           || '   ,MAX(a.sdo_ub_1)        AS sdo_ub_1 '
           || '   ,MAX(a.sdo_tolerance_1) AS sdo_tolerance_1 '
           || '   ,MAX(a.sdo_dimname_2)   AS sdo_dimname_2 '
           || '   ,MAX(a.sdo_lb_2)        AS sdo_lb_2 '
           || '   ,MAX(a.sdo_ub_2)        AS sdo_ub_2 '
           || '   ,MAX(a.sdo_tolerance_2) AS sdo_tolerance_2 '
           || '   ,MAX(a.sdo_dimname_3)   AS sdo_dimname_3 '
           || '   ,MAX(a.sdo_lb_3)        AS sdo_lb_3 '
           || '   ,MAX(a.sdo_ub_3)        AS sdo_ub_3 '
           || '   ,MAX(a.sdo_tolerance_3) AS sdo_tolerance_3 '
           || '   ,MAX(a.sdo_dimname_4)   AS sdo_dimname_4 '
           || '   ,MAX(a.sdo_lb_4)        AS sdo_lb_4 '
           || '   ,MAX(a.sdo_ub_4)        AS sdo_ub_4 '
           || '   ,MAX(a.sdo_tolerance_4) AS sdo_tolerance_4 '
           || '   ,a.srid '
           || '   FROM ( '
           || '      SELECT '
           || '       aa.owner '
           || '      ,aa.table_name '
           || '      ,aa.column_name '
           || '      ,CASE WHEN aa.dim_number = 1 THEN aa.sdo_dimname   ELSE NULL END AS sdo_dimname_1 '
           || '      ,CASE WHEN aa.dim_number = 1 THEN aa.sdo_lb        ELSE NULL END AS sdo_lb_1 '
           || '      ,CASE WHEN aa.dim_number = 1 THEN aa.sdo_ub        ELSE NULL END AS sdo_ub_1 '
           || '      ,CASE WHEN aa.dim_number = 1 THEN aa.sdo_tolerance ELSE NULL END AS sdo_tolerance_1 '
           || '      ,CASE WHEN aa.dim_number = 2 THEN aa.sdo_dimname   ELSE NULL END AS sdo_dimname_2 '
           || '      ,CASE WHEN aa.dim_number = 2 THEN aa.sdo_lb        ELSE NULL END AS sdo_lb_2 '
           || '      ,CASE WHEN aa.dim_number = 2 THEN aa.sdo_ub        ELSE NULL END AS sdo_ub_2 '
           || '      ,CASE WHEN aa.dim_number = 2 THEN aa.sdo_tolerance ELSE NULL END AS sdo_tolerance_2 '
           || '      ,CASE WHEN aa.dim_number = 3 THEN aa.sdo_dimname   ELSE NULL END AS sdo_dimname_3 '
           || '      ,CASE WHEN aa.dim_number = 3 THEN aa.sdo_lb        ELSE NULL END AS sdo_lb_3 '
           || '      ,CASE WHEN aa.dim_number = 3 THEN aa.sdo_ub        ELSE NULL END AS sdo_ub_3 '
           || '      ,CASE WHEN aa.dim_number = 3 THEN aa.sdo_tolerance ELSE NULL END AS sdo_tolerance_3 '
           || '      ,CASE WHEN aa.dim_number = 4 THEN aa.sdo_dimname   ELSE NULL END AS sdo_dimname_4 '
           || '      ,CASE WHEN aa.dim_number = 4 THEN aa.sdo_lb        ELSE NULL END AS sdo_lb_4 '
           || '      ,CASE WHEN aa.dim_number = 4 THEN aa.sdo_ub        ELSE NULL END AS sdo_ub_4 '
           || '      ,CASE WHEN aa.dim_number = 4 THEN aa.sdo_tolerance ELSE NULL END AS sdo_tolerance_4 '
           || '      ,aa.srid '
           || '     FROM ( '
           || '        SELECT '
           || '          aaa.rec_id '
           || '         ,aaa.owner '
           || '         ,aaa.table_name '
           || '         ,aaa.column_name '
           || '         ,bbb.sdo_dimname '
           || '         ,bbb.sdo_lb '
           || '         ,bbb.sdo_ub '
           || '         ,bbb.sdo_tolerance '
           || '         ,aaa.srid '
           || '         ,ROW_NUMBER() OVER (PARTITION BY aaa.rec_id ORDER BY aaa.rec_id) AS dim_number '
           || '         FROM ( '
           || '            SELECT '
           || '             ROWNUM AS rec_id '
           || '            ,aaaa.owner '
           || '            ,aaaa.table_name '
           || '            ,aaaa.column_name '
           || '            ,aaaa.diminfo '
           || '            ,aaaa.srid '
           || '            FROM '
           || '            all_sdo_geom_metadata aaaa '
           || '        ) aaa '
           || '         CROSS JOIN '
           || '         TABLE(aaa.diminfo) bbb '
           || '      ) aa '
           || '   ) a '
           || '   GROUP BY '
           || '    a.owner '
           || '   ,a.table_name '
           || '   ,a.column_name '
           || '   ,a.srid '
           || ')'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Create the map table
   ----------------------------------------------------------------------------
   str_sql := 'DROP TABLE IF EXISTS ' || pMetadataSchema || '.pg_orafdw_table_map';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE TABLE ' || pMetadataSchema || '.pg_orafdw_table_map('
           || '    ftrelid                   OID          PRIMARY KEY '
           || '   ,ftserver                  OID          NOT NULL '
           || '   ,oracle_owner              VARCHAR(30)  NOT NULL '
           || '   ,oracle_tablename          VARCHAR(30)  NOT NULL '
           || '   ,foreign_table_schema      VARCHAR(255) NOT NULL '
           || '   ,foreign_table_name        VARCHAR(255) NOT NULL '
           || ') ' || str_tablespace;
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Create the postflight table
   ----------------------------------------------------------------------------
   str_sql := 'DROP TABLE IF EXISTS ' || pMetadataSchema || '.pg_orafdw_postflight';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE TABLE ' || pMetadataSchema || '.pg_orafdw_postflight('
           || '    copy_action_id            VARCHAR(40)  NOT NULL '
           || '   ,copy_action_time          TIMESTAMP    NOT NULL '
           || '   ,copy_group_keyword        VARCHAR(40)  NOT NULL '
           || '   ,postflight_order          INTEGER      NOT NULL '
           || '   ,postflight_action         TEXT '
           || ') ' || str_tablespace;
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 110
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN TRUE;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.init_metadata(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.init_metadata(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
) TO PUBLIC;

