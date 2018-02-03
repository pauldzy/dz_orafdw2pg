CREATE OR REPLACE FUNCTION dz_pg.init_metadata(
    IN  pForeignServer    VARCHAR
   ,IN  pTargetSchema     VARCHAR
   ,IN  pTargetTablespace VARCHAR DEFAULT NULL
) RETURNS BOOLEAN
VOLATILE
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
   IF pTargetTablespace IS NOT NULL
   THEN
      str_tablespace := 'TABLESPACE ' || pTargetTablespace || ' ';
      
   ELSE
      str_tablespace := ' ';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Build all_tables
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_tables';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_tables( '
           || '    owner                     character varying(30)  '
           || '   ,table_name                character varying(30)  '
           || '   ,tablespace_name           character varying(30)  '
           || '   ,cluster_name              character varying(30)  '
           || '   ,iot_name                  character varying(30)  '
           || '   ,status                    character varying(8)   '
           || '   ,pct_free                  numeric                '
           || '   ,pct_used                  numeric                '
           || '   ,ini_trans                 numeric                '
           || '   ,max_trans                 numeric                '
           || '   ,initial_extent            numeric                '
           || '   ,next_extent               numeric                '
           || '   ,min_extents               numeric                '
           || '   ,max_extents               numeric                '
           || '   ,pct_increase              numeric                '
           || '   ,freelists                 numeric                '
           || '   ,freelist_groups           numeric                '
           || '   ,logging                   character varying(3)   '
           || '   ,backed_up                 character varying(1)   '
           || '   ,num_rows                  numeric                '
           || '   ,blocks                    numeric                '
           || '   ,empty_blocks              numeric                '
           || '   ,avg_space                 numeric                '
           || '   ,chain_cnt                 numeric                '
           || '   ,avg_row_len               numeric                '
           || '   ,avg_space_freelist_blocks numeric                '
           || '   ,num_freelist_blocks       numeric                '
           || '   ,degree                    character varying(40)  '
           || '   ,instances                 character varying(40)  '
           || '   ,cache                     character varying(20)  '
           || '   ,table_lock                character varying(8)   '
           || '   ,sample_size               numeric                '
           || '   ,last_analyzed             timestamp(0)           '
           || '   ,partitioned               character varying(3)   '
           || '   ,iot_type                  character varying(12)  '
           || '   ,temporary                 character varying(1)   '
           || '   ,secondary                 character varying(1)   '
           || '   ,nested                    character varying(3)   '
           || '   ,buffer_pool               character varying(8)   '
           || '   ,flash_cache               character varying(7)   '
           || '   ,cell_flash_cache          character varying(7)   '
           || '   ,row_movement              character varying(8)   '
           || '   ,global_stats              character varying(3)   '
           || '   ,user_stats                character varying(3)   '
           || '   ,duration                  character varying(15)  '
           || '   ,skip_corrupt              character varying(8)   '
           || '   ,monitoring                character varying(3)   '
           || '   ,cluster_owner             character varying(30)  '
           || '   ,dependencies              character varying(8)   '
           || '   ,compression               character varying(8)   '
           || '   ,compress_for              character varying(12)  '
           || '   ,dropped                   character varying(3)   '
           || '   ,read_only                 character varying(3)   '
           || '   ,segment_created           character varying(3)   '
           || '   ,result_cache              character varying(7)   '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_TABLES)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Build all_tab_columns
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_tab_columns';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_tab_columns( '
           || '    owner                     character varying(30)  '
           || '   ,table_name                character varying(30)  '
           || '   ,column_name               character varying(30)  '
           || '   ,data_type                 character varying(106) '
           || '   ,data_type_mod             character varying(30)  '
           || '   ,data_type_owner           character varying(120) '
           || '   ,data_length               numeric                '  
           || '   ,data_precision            numeric                '
           || '   ,data_scale                numeric                '
           || '   ,nullable                  character varying(1)   '
           || '   ,column_id                 numeric                '   
           || '   ,default_length            numeric                ' 
           || '   ,data_default              bytea                  '
           || '   ,num_distinct              numeric                '
           || '   ,low_value                 bytea                  '
           || '   ,high_value                bytea                  '
           || '   ,density                   numeric                '
           || '   ,num_nulls                 numeric                '
           || '   ,num_buckets               numeric                '
           || '   ,last_analyzed             timestamp(0)           '
           || '   ,sample_size               numeric                '
           || '   ,character_set_name        character varying(44)  '
           || '   ,char_col_decl_length      numeric                '  
           || '   ,global_stats              character varying(3)   '
           || '   ,user_stats                character varying(3)   '
           || '   ,avg_col_len               numeric                '
           || '   ,char_length               numeric                '
           || '   ,char_used                 character varying(1)   '
           || '   ,v80_fmt_image             character varying(3)   '
           || '   ,data_upgraded             character varying(3)   '
           || '   ,histogram                 character varying(15)  '
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
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_constraints';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_constraints('
           || '    owner                     character varying(120) '
           || '   ,constraint_name           character varying(30)  '
           || '   ,constraint_type           character varying(1)   '
           || '   ,table_name                character varying(30)  '
           || '   ,search_condition          character varying(32000)'
           || '   ,r_owner                   character varying(120) '
           || '   ,r_constraint_name         character varying(30)  '
           || '   ,delete_rule               character varying(9)   '
           || '   ,status                    character varying(8)   '
           || '   ,"deferrable"              character varying(14)  '
           || '   ,deferred                  character varying(9)   '
           || '   ,validated                 character varying(13)  '
           || '   ,generated                 character varying(14)  '
           || '   ,bad                       character varying(3)   '
           || '   ,rely                      character varying(4)   '
           || '   ,last_change               timestamp(0)           '
           || '   ,index_owner               character varying(30)  '
           || '   ,index_name                character varying(30)  '
           || '   ,invalid                   character varying(7)   '
           || '   ,view_related              character varying(14)  '
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
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_cons_columns';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_cons_columns('
           || '    owner                     character varying(30)  '
           || '   ,constraint_name           character varying(30)  '
           || '   ,table_name                character varying(30)  '
           || '   ,column_name               character varying(4000)'
           || '   ,position                  numeric                '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_CONS_COLUMNS)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Build all_indexes
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_indexes';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_indexes('
           || '    owner                     character varying(30)  '
           || '   ,index_name                character varying(30)  '
           || '   ,index_type                character varying(27)  '
           || '   ,table_owner               character varying(30)  '
           || '   ,table_name                character varying(30)  '
           || '   ,table_type                character varying(5)   '
           || '   ,uniqueness                character varying(9)   '
           || '   ,compression               character varying(38)  '
           || '   ,prefix_length             numeric                '
           || '   ,tablespace_name           character varying(30)  '
           || '   ,ini_trans                 numeric                '
           || '   ,max_trans                 numeric                '
           || '   ,initial_extent            numeric                '
           || '   ,next_extent               numeric                '
           || '   ,min_extents               numeric                '
           || '   ,max_extents               numeric                '
           || '   ,pct_increase              numeric                '
           || '   ,pct_threshold             numeric                '
           || '   ,include_column            numeric                '
           || '   ,freelists                 numeric                '
           || '   ,freelist_groups           numeric                '
           || '   ,pct_free                  numeric                '
           || '   ,logging                   character varying(3)   '
           || '   ,blevel                    numeric                '
           || '   ,leaf_blocks               numeric                '
           || '   ,distinct_keys             numeric                '
           || '   ,avg_leaf_blocks_per_key   numeric                '
           || '   ,avg_data_blocks_per_key   numeric                '
           || '   ,clustering_factor         numeric                '
           || '   ,status                    character varying(8)   '
           || '   ,num_rows                  numeric                '
           || '   ,sample_size               numeric                '
           || '   ,last_analyzed             timestamp(0)           '
           || '   ,degree                    character varying(40)  '
           || '   ,instances                 character varying(40)  '
           || '   ,partitioned               character varying(3)   '
           || '   ,temporary                 character varying(1)   '
           || '   ,generated                 character varying(1)   '
           || '   ,secondary                 character varying(1)   '
           || '   ,buffer_pool               character varying(7)   '
           || '   ,flash_cache               character varying(7)   '
           || '   ,cell_flash_cache          character varying(7)   '
           || '   ,user_stats                character varying(3)   '
           || '   ,duration                  character varying(15)  '
           || '   ,pct_direct_access         numeric                '
           || '   ,ityp_owner                character varying(30)  '
           || '   ,ityp_name                 character varying(30)  '
           || '   ,parameters                character varying(1000)'
           || '   ,global_stats              character varying(3)   '
           || '   ,domidx_status             character varying(12)  '
           || '   ,domidx_opstatus           character varying(6)   '
           || '   ,funcidx_status            character varying(8)   '
           || '   ,join_index                character varying(3)   '
           || '   ,iot_redundant_pkey_elim   character varying(3)   '
           || '   ,dropped                   character varying(3)   '
           || '   ,visibility                character varying(9)   '
           || '   ,domidx_management         character varying(14)  '
           || '   ,segment_created           character varying(3)   '
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
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_ind_columns';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_ind_columns('
           || '    index_owner               character varying(30)  '
           || '   ,index_name                character varying(30)  '
           || '   ,table_owner               character varying(30)  '
           || '   ,table_name                character varying(30)  '
           || '   ,column_name               character varying(4000)'
           || '   ,column_position           numeric                '
           || '   ,column_length             numeric                '
           || '   ,char_length               numeric                '
           || '   ,descend                   character varying(4)   '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_IND_COLUMNS)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Build all_ind_expressions
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_ind_expressions';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_ind_expressions('
           || '    index_owner               character varying(30)  '
           || '   ,index_name                character varying(30)  '
           || '   ,table_owner               character varying(30)  '
           || '   ,table_name                character varying(30)  '
           || '   ,column_expression         character varying(32000)'
           || '   ,column_position           numeric                '
           || ') '
           || 'SERVER ' || pForeignServer || ' '
           || 'OPTIONS (table ''(SELECT * FROM SYS.ALL_IND_EXPRESSIONS)'')';
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 90
   -- Build all_sdo_geom_metadata
   ----------------------------------------------------------------------------
   str_sql := 'DROP FOREIGN TABLE IF EXISTS ' || pTargetSchema || '.all_sdo_geom_metadata';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE FOREIGN TABLE ' || pTargetSchema || '.all_sdo_geom_metadata('
           || '    owner                     character varying(32)  '
           || '   ,table_name                character varying(32)  '
           || '   ,column_name               character varying(32)  '
           || '   ,sdo_dimname_1             character varying(64)  '
           || '   ,sdo_lb_1                  numeric                '
           || '   ,sdo_ub_1                  numeric                '
           || '   ,sdo_tolerance_1           numeric                '
           || '   ,sdo_dimname_2             character varying(64)  '
           || '   ,sdo_lb_2                  numeric                '
           || '   ,sdo_ub_2                  numeric                '
           || '   ,sdo_tolerance_2           numeric                '
           || '   ,sdo_dimname_3             character varying(64)  '
           || '   ,sdo_lb_3                  numeric                '
           || '   ,sdo_ub_3                  numeric                '
           || '   ,sdo_tolerance_3           numeric                '
           || '   ,sdo_dimname_4             character varying(64)  '
           || '   ,sdo_lb_4                  numeric                '
           || '   ,sdo_ub_4                  numeric                '
           || '   ,sdo_tolerance_4           numeric                '
           || '   ,srid                      integer                '
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
   str_sql := 'DROP TABLE IF EXISTS ' || pTargetSchema || '.oracle_fdw_table_map';
   
   EXECUTE str_sql;
   
   str_sql := 'CREATE TABLE ' || pTargetSchema || '.oracle_fdw_table_map('
           || '    ftrelid                   oid                    PRIMARY KEY '
           || '   ,ftserver                  oid                    NOT NULL '
           || '   ,oracle_owner              character varying(30)  NOT NULL '
           || '   ,oracle_tablename          character varying(30)  NOT NULL '
           || '   ,foreign_table_schema      character varying(255) NOT NULL '
           || '   ,foreign_table_name        character varying(255) NOT NULL '
           || ') ' || str_tablespace;
           
   EXECUTE str_sql;
   
   ----------------------------------------------------------------------------
   -- Step 110
   -- Assume success
   ----------------------------------------------------------------------------
   RETURN true;
   
END;
$BODY$
LANGUAGE plpgsql;

ALTER FUNCTION dz_pg.init_metadata(
    varchar
   ,varchar
   ,varchar
) OWNER TO docker;

GRANT EXECUTE ON FUNCTION dz_pg.init_metadata(
    varchar
   ,varchar
   ,varchar
) TO PUBLIC;

