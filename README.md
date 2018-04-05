# pg_orafdw2pg
PL/pgSQL code to migrate and sample data from Oracle to PostgreSQL using Oracle_FDW

#### Example to move a set of tables from Oracle to PostgreSQL preserving structure, indexes and constraints:

```
SELECT dz_pg.init_metadata(
    pForeignServer    := 'vmwhippet'
   ,pMetadataSchema   := 'ncc_whipp'
);
```

First initialize the metadata system with Oracle.   Mainly this creates foreign tables on the pg side which tie to the ALL metadata tables on the Oracle side.  A separate schema for each foreign server is recommended but you can also just keep overwriting a scratch schema over and over.  

```
SELECT dz_pg.map_foreign_table(
    pOracleOwner    := 'ECHO_DFR'
   ,pOracleTable    := ARRAY[
       'ICP_CONTROL_DATES'
      ,'NPDES_BIOS_AMOUNTS'
      ,'NPDES_BIOS_FORMAL_ACTIONS'
      ,'NPDES_BIOS_INFML_ENF_ACTIONS'
      ,'NPDES_BIOS_INSPECTIONS'
      ,'NPDES_BIOS_MGMT_DEFICIENCIES'
      ,'NPDES_BIOS_MGMT_PATH_REDS'
      ,'NPDES_BIOS_MGMT_PRCTCE_TYPES'
      ,'NPDES_BIOS_MGMT_VECTOR_REDS'
      ,'NPDES_BIOS_PERMITS'
      ,'NPDES_BIOS_PROGRAMS_ID'
      ,'NPDES_BIOS_REP_CAT'
      ,'NPDES_BIOS_SEV_VIOLATIONS'
      ,'NPDES_BIOS_TRTMNT_PROCSS'
      ,'NPDES_BIOS_VIOLATION_STATUS'
   ]
   ,pForeignServer  := 'vmwhippet'
   ,pTargetSchema   := 'ncc_whipp'
   ,pMetadataSchema := 'ncc_whipp'
);
```

Next map the tables you wish to migrate as foreign tables on the pg side.  This generates the foreign tables mappings in the target schema.

```
SELECT dz_pg.copy_foreign_table(
    pForeignTableOwner := 'ncc_whipp'
   ,pForeignTableName  := ARRAY[
       'icp_control_dates'
      ,'npdes_bios_amounts'
      ,'npdes_bios_formal_actions'
      ,'npdes_bios_infml_enf_actions'
      ,'npdes_bios_inspections'
      ,'npdes_bios_mgmt_deficiencies'
      ,'npdes_bios_mgmt_path_reds'
      ,'npdes_bios_mgmt_prctce_types'
      ,'npdes_bios_mgmt_vector_reds'
      ,'npdes_bios_permits'
      ,'npdes_bios_programs_id'
      ,'npdes_bios_rep_cat'
      ,'npdes_bios_sev_violations'
      ,'npdes_bios_trtmnt_procss'
      ,'npdes_bios_violation_status'
   ]
   ,pMetadataSchema    := 'ncc_whipp'
   ,pForceObjectID     := TRUE
   ,pTargetSchema      := 'echo_dfr'
   ,pForcePublic       := TRUE
   ,pPostFlightGroup   := 'Biosolids_20180316' 
   ,pPostFlightAction  := 'Flush' 
);
```

The copy step does the actual work of creating a receiving table on the pg side and copying the data via the foreign table.  For large tables this can take some time.

```
SELECT dz_pg.execute_postflight(
    pPostFlightGroup   := 'Biosolids_20180316'
   ,pMetadataSchema    := 'ncc_whipp'
);
```

Finally some DDL statements need to take place after all tables have been created.  Execute this last step to add those resources.

#### Secondary Example:

```
SELECT dz_pg.init_metadata(
    pForeignServer    := 'vmclydesdale'
   ,pMetadataSchema   := 'ncc_owstg'
);
``` 

```
SELECT dz_pg.map_foreign_table(
    pOracleOwner    := 'RAD_PUBLIC'
   ,pOracleTable    := ARRAY[
       'RAD_BEACH_A'
      ,'RAD_BEACH_EVT2META'
      ,'RAD_BEACH_L'
      ,'RAD_BEACH_METADATA'
      ,'RAD_BEACH_P'
      ,'RAD_BEACH_SFID'
      ,'RAD_BEACH_SRCCIT'
   ]
   ,pForeignServer  := 'vmclydesdale'
   ,pTargetSchema   := 'ncc_owstg'
   ,pMetadataSchema := 'ncc_owstg'
);
```

```
SELECT dz_pg.copy_foreign_table(
    pForeignTableOwner := 'ncc_owstg'
   ,pForeignTableName  := ARRAY[
       'rad_beach_a'
      ,'rad_beach_evt2meta'
      ,'rad_beach_l'
      ,'rad_beach_metadata'
      ,'rad_beach_p'
      ,'rad_beach_sfid'
      ,'rad_beach_srccit'
   ]
   ,pMetadataSchema    := 'ncc_owstg'
   ,pTargetSchema      := 'loading_dock'
   ,pForcePublic       := TRUE
   ,pPostFlightGroup   := 'Beach_20180326' 
   ,pPostFlightAction  := 'Flush' 
);
```

```
SELECT dz_pg.execute_postflight(
    pPostFlightGroup   := 'Beach_20180326'
   ,pMetadataSchema    := 'ncc_owstg'
);
```


