DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      trim(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      COUNTRY,
      TYPE,
      SEPARATOR,
      LENGTH,
      DELAYED,
      HISTORY,
      MARCA,
      HUSO      
    FROM MTDT_INTERFACE_SUMMARY
    WHERE SOURCE <> 'SA';
    --where DELAYED = 'S';
    --and CONCEPT_NAME in ('TRAFD_CU_MVNO', 'TRAFE_CU_MVNO', 'TRAFV_CU_MVNO');
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      trim(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      trim(CONCEPT_NAME) = concep_name_in and
      SOURCE = source_in
    ORDER BY POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col PLS_INTEGER;
      num_column PLS_INTEGER;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;
      
      v_nombre_particion VARCHAR2(30);
      
      
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      lista_pos                                    list_posiciones := list_posiciones (); 
      
      fich_salida                                 UTL_FILE.file_type;
      fich_salida_pkg                        UTL_FILE.file_type;
      nombre_fich                              VARCHAR(40);
      nombre_fich_sh                        VARCHAR(40);
      nombre_fich_pkg                      VARCHAR(40);
      tipo_col                                      VARCHAR(70);
      nombre_interface_a_cargar   VARCHAR(70);
      pos_ini_pais                             PLS_integer;
      pos_fin_pais                             PLS_integer;
      pos_ini_fecha                           PLS_integer;
      pos_fin_fecha                           PLS_integer;
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      OWNER_TC                            VARCHAR2(60);
      PREFIJO_DM                            VARCHAR2(60);
      NAME_DM                            VARCHAR2(60);
      nombre_proceso                      VARCHAR(30);
      TABLESPACE_SA                  VARCHAR2(60);
      v_num_meses                          VARCHAR2(2);


  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  
  
  
  /* (20141219) FIN*/
  OPEN dtd_interfaz_summary;
  LOOP
    
      FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;
      nombre_fich_pkg := 'pkg_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '.sql';
      fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
      /* Angel Ruiz (20141223) Hecho porque hay paquetes que no compilan */
       if (length(reg_summary.CONCEPT_NAME) < 24) then
        nombre_proceso := 'SA_' || reg_summary.CONCEPT_NAME;
      else
        nombre_proceso := reg_summary.CONCEPT_NAME;
      end if;
      /* (20150717) ANGEL RUIZ. Nueva Funcionalidad*/
      /* Se hace un paso a historico de las Tablas de STAGING */
      /* Controlamos que le nombre de la particion no sea demasido grande que no compile */
        if (length(reg_summary.CONCEPT_NAME) <= 18) then
          v_nombre_particion := 'SA_' || reg_summary.CONCEPT_NAME;
        else
          v_nombre_particion := reg_summary.CONCEPT_NAME;
        end if;
      /* (20150717) ANGEL RUIZ. Fin */
      
      /******/
      /* COMIENZO LA GENERACION DEL PACKAGE DEFINITION */
      /******/
      UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' ||  OWNER_SA || '.pkg_' || nombre_proceso || ' AS');
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      dbms_output.put_line ('Estoy en PACKAGE DEFINITION DE LA TABLA: ' || 'SA' || '_' || reg_summary.CONCEPT_NAME);
      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pre_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'');');
      if (reg_summary.HISTORY IS NOT NULL) then
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pos_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'');');
      end if;
      /* (20160316) Angel Ruiz. NF: Se añade marcas de extraccion */
      if (reg_summary.MARCA IS NOT NULL) then
        /* El interfaz posee valor en el campo MARCA, por lo que hay que realizar su gestion */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE mar_' || nombre_proceso || ' (nom_proceso_in IN VARCHAR2, fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_inicio_in IN TIMESTAMP, num_inserts_in in NUMBER := 0, num_reads_in in NUMBER := 0, num_discards_in in NUMBER := 0);');
      end if;
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
      UTL_FILE.put_line(fich_salida_pkg, '/' );
      /******/
      /* COMIENZO LA GENERACION DEL PACKAGE BODY */
      /******/
      dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION DE LA TABLA: ' || 'SA' || '_' || reg_summary.CONCEPT_NAME);
      UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY ' || OWNER_SA || '.pkg_' || nombre_proceso || ' AS');
      UTL_FILE.put_line(fich_salida_pkg,'');
      UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_tabla (table_name_in IN VARCHAR2) return number');
      UTL_FILE.put_line(fich_salida_pkg,'  IS');
      UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
      UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_tabla varchar(30);BEGIN select table_name into nombre_tabla from all_tables where table_name = '''''' || table_name_in || '''''' and owner = '''''' || ''' || OWNER_SA || ''' || ''''''; END;'';');
      UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
      UTL_FILE.put_line(fich_salida_pkg,'  exception');
      UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
      UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
      UTL_FILE.put_line(fich_salida_pkg,'  END existe_tabla;');
      UTL_FILE.put_line(fich_salida_pkg,'');
      UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_particion (partition_name_in IN VARCHAR2, table_name_in IN VARCHAR2) return number');
      UTL_FILE.put_line(fich_salida_pkg,'  IS');
      UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
      UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_particion varchar(30);BEGIN select partition_name into nombre_particion from all_tab_partitions where partition_name = '''''' || partition_name_in || '''''' and table_name = '''''' || table_name_in || '''''' and table_owner = '''''' || ''' || OWNER_SA || ''' || ''''''; END;'';');
      UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
      UTL_FILE.put_line(fich_salida_pkg,'  exception');
      UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
      UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
      UTL_FILE.put_line(fich_salida_pkg,'  END existe_particion;');
      UTL_FILE.put_line(fich_salida_pkg,'');

      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pre_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
      UTL_FILE.put_line(fich_salida_pkg, '  IS' ); 
      UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
      UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
      UTL_FILE.put_line(fich_salida_pkg,'   fch_particion varchar2(8);');
      
      UTL_FILE.put_line(fich_salida_pkg, '  BEGIN' );
        UTL_FILE.put_line(fich_salida_pkg,'' );
      --UTL_FILE.put_line(fich_salida_pkg, '  exis_tabla :=  existe_tabla (' || '''SA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in);');      
      --UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_NUMBER(TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD'')+1,''YYYYMMDD''));');
      if (reg_summary.DELAYED = 'S') then
        UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD'')+1, ''YYYYMMDD'');'); 
        /* (20151215) Angel Ruiz. BUG: El nombre de las particiones no coincide con el nombre generado en los creates */
        --UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''PA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_''' || ' || fch_datos_in, ''SA_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (''' || v_nombre_particion || ''' || ''_''' || ' || fch_datos_in, ''SA_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        --UTL_FILE.put_line(fich_salida_pkg,'  if (exis_tabla = 1) then' );      
        UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
        /* (20151215) Angel Ruiz. BUG: El nombre de las particiones no coincide con el nombre generado en los creates */
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in;');
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg,'  else' );
        /* (20151215) Angel Ruiz. BUG: El nombre de las particiones no coincide con el nombre generado en los creates */
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' ADD PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN (TO_DATE('''''' || fch_particion || '''''', ''''YYYYMMDD'''')) TABLESPACE DWTBSP_D_MVNO_SA'';');
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN (TO_DATE('''''' || fch_particion || '''''', ''''YYYYMMDD'''')) TABLESPACE DWTBSP_D_MVNO_SA'';');
        UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
      else
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
      end if;
      
      UTL_FILE.put_line(fich_salida_pkg,'  exception');
      UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
      UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Se ha producido un error en el pre-proceso de staging. Tabla: '' || ''' || 'SA_' || reg_summary.CONCEPT_NAME || ''');');
      UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
      UTL_FILE.put_line(fich_salida_pkg,'    raise;');
      UTL_FILE.put_line(fich_salida_pkg, '  END pre_' || nombre_proceso || ';'); 
      UTL_FILE.put_line(fich_salida_pkg, '');
/************/
/************/
      if (reg_summary.HISTORY IS NOT NULL) then

        UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pos_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
        UTL_FILE.put_line(fich_salida_pkg, '  IS' ); 
        UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
        UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
        UTL_FILE.put_line(fich_salida_pkg,'   fch_particion varchar2(8);');
        
        UTL_FILE.put_line(fich_salida_pkg, '  BEGIN' );
        UTL_FILE.put_line(fich_salida_pkg,'' );
        /* Hay que truncar la partición historica en caso de que exista. Esto se cambia aqui para hacerlo en el pre-procesado */
        /* (20150717) Angel Ruiz. Nueva Funcionalidad */          
        /* Se hace un paso a historico de las Tablas de STAGING */
        if (regexp_count(reg_summary.HISTORY, '^[0-9][Mm]',1,'i') > 0) then
          v_num_meses:= substr(reg_summary.HISTORY,1,1);
        else
          /* No sigue la especificacion requerida el campo donde se guarda el tiempo de historico */
          /* Por defecto ponemos 2 meses */
          v_num_meses := 2;
        end if;
        UTL_FILE.put_line(fich_salida_pkg,'  /* Primero borramos la particion que se ha quedado obsoleta */');
        UTL_FILE.put_line(fich_salida_pkg,'  /* siempre que no estemos en una ejecucion forzosa */');
        UTL_FILE.put_line(fich_salida_pkg,'  /* en caso contrario no tiene sentido */');
        /* (20160315) Angel Ruiz. NF: Se lleva a cabo salvaguarda si la particion existe */
        UTL_FILE.put_line(fich_salida_pkg,'  if (forzado_in = ''N'') then');
        UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_CHAR(ADD_MONTHS(TO_DATE(fch_carga_in,''YYYYMMDD''), -' || v_num_meses || ') , ''YYYYMMDD'');');
        UTL_FILE.put_line(fich_salida_pkg,'    FOR nombre_particion_rec IN (');
        UTL_FILE.put_line(fich_salida_pkg,'      select partition_name' );
        UTL_FILE.put_line(fich_salida_pkg,'      from user_tab_partitions' );
        UTL_FILE.put_line(fich_salida_pkg,'      where table_name = ''SAH_' || reg_summary.CONCEPT_NAME || '''');
        UTL_FILE.put_line(fich_salida_pkg,'      and partition_name < ''' || v_nombre_particion || ''' || ''_'' || fch_particion )');
        UTL_FILE.put_line(fich_salida_pkg,'    LOOP' );
        UTL_FILE.put_line(fich_salida_pkg,'      exis_partition :=  existe_particion (nombre_particion_rec.partition_name, ' || '''SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        UTL_FILE.put_line(fich_salida_pkg,'      if (exis_partition = 1) then' );
        UTL_FILE.put_line(fich_salida_pkg,'        EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' DROP PARTITION '' || nombre_particion_rec.partition_name'  || ';');
        UTL_FILE.put_line(fich_salida_pkg,'      end if;' );
        UTL_FILE.put_line(fich_salida_pkg,'    END LOOP;' );
        UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
        UTL_FILE.put_line(fich_salida_pkg,'' );
        --UTL_FILE.put_line(fich_salida_pkg,'  exis_partition :=  existe_particion (' || '''' || v_nombre_particion || ''' || ''_''' || ' || fch_particion, ''SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        --UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' DROP PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_particion' || ';');
        --UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
        --UTL_FILE.put_line(fich_salida_pkg,'' );
        UTL_FILE.put_line(fich_salida_pkg,'  /* Segundo comrpobamos si hay que crear o truncar la particion sobre la que vamos a salvaguardar la informacion */');
        UTL_FILE.put_line(fich_salida_pkg,'  fch_particion := TO_CHAR(TO_DATE(fch_carga_in,''YYYYMMDD'')+1, ''YYYYMMDD'');'); 
        UTL_FILE.put_line(fich_salida_pkg,'  exis_partition :=  existe_particion (' || '''' || v_nombre_particion || ''' || ''_''' || ' || fch_carga_in, ''SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        /* (20160315) Angel Ruiz. NF: Se lleva a cabo salvaguarda si la particion existe */
        UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_carga_in;');
        UTL_FILE.put_line(fich_salida_pkg,'  else' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''CREATE TABLE ' || 'app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in  || '' AS SELECT * FROM SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
        /* (20160315) Angel Ruiz. NF: Se lleva a cabo salvaguarda si la particion existe */        
        UTL_FILE.put_line(fich_salida_pkg,'    if (forzado_in = ''N'') then' );
        UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_carga_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE ' || TABLESPACE_SA || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'    end if;' );
        UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
        /* (20160315) Angel Ruiz. NF: Se lleva a cabo salvaguarda si la particion existe */
        UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1 or (exis_partition = 0 and forzado_in = ''N'')) then' );
        UTL_FILE.put_line(fich_salida_pkg,'    /* TERCERO LLEVO A CABO LA SALVAGUARDA DE LA INFORMACION */' );
        UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND */ INTO ' || OWNER_SA || '.SAH_' || reg_summary.CONCEPT_NAME);
        UTL_FILE.put_line(fich_salida_pkg,'    (');
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          IF primera_col = 1 THEN /* Si es primera columna */
            UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_datail.COLUMNA);
            primera_col := 0;
          ELSE
            UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_datail.COLUMNA);
          END IF;
        END LOOP;
        CLOSE dtd_interfaz_detail;
        UTL_FILE.put_line(fich_salida_pkg,'    ,CVE_DIA');
        UTL_FILE.put_line(fich_salida_pkg,'    )');
        UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          IF primera_col = 1 THEN /* Si es primera columna */
            UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_datail.COLUMNA);
            primera_col := 0;
          ELSE
            UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_datail.COLUMNA);
          END IF;
        END LOOP;
        CLOSE dtd_interfaz_detail;
        UTL_FILE.put_line(fich_salida_pkg, '    ,TO_NUMBER(fch_carga_in)');
        UTL_FILE.put_line(fich_salida_pkg, '    FROM ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
        UTL_FILE.put_line(fich_salida_pkg, '    ;');
        UTL_FILE.put_line(fich_salida_pkg, '    commit;');
        UTL_FILE.put_line(fich_salida_pkg, '  end if;');
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'  exception');
        UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
        UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Se ha producido un error en el pre-proceso de staging. Tabla: '' || ''' || 'SA_' || reg_summary.CONCEPT_NAME || ''');');
        UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
        UTL_FILE.put_line(fich_salida_pkg,'    raise;');
        UTL_FILE.put_line(fich_salida_pkg, '  END pos_' || nombre_proceso || ';'); 
        UTL_FILE.put_line(fich_salida_pkg, '');
      end if;      
/************/
/************/
      /* (20160316). Angel Ruiz. NF: Se añade control de marcas en los ficehros cargados */
      if (reg_summary.MARCA IS NOT NULL) then
        UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE mar_' || nombre_proceso || ' (nom_proceso_in IN VARCHAR2, fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_inicio_in IN TIMESTAMP, num_inserts_in in NUMBER := 0, num_reads_in in NUMBER := 0, num_discards_in in NUMBER := 0)');
        UTL_FILE.put_line(fich_salida_pkg, '  IS' ); 
        UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
        UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
        UTL_FILE.put_line(fich_salida_pkg,'   fch_particion varchar2(8);');
        
        UTL_FILE.put_line(fich_salida_pkg,'  BEGIN' );
        UTL_FILE.put_line(fich_salida_pkg,'' );
        UTL_FILE.put_line(fich_salida_pkg,'    /* Insertamos el registro de marca para el fichero que viene por parametro */' );
        UTL_FILE.put_line(fich_salida_pkg,'    FOR MARCAS IN (');
        UTL_FILE.put_line(fich_salida_pkg,'      SELECT ' );
        UTL_FILE.put_line(fich_salida_pkg,'        MAX(' || reg_summary.MARCA || ') "MARCA_FINAL",' );
        UTL_FILE.put_line(fich_salida_pkg,'        MIN(' || reg_summary.MARCA || ') "MARCA_INICIAL",' );
        UTL_FILE.put_line(fich_salida_pkg,'        MAX(' || reg_summary.HUSO || ') "MARCA_FINAL_HUSO",' );
        UTL_FILE.put_line(fich_salida_pkg,'        MIN(' || reg_summary.HUSO || ') "MARCA_INICIAL_HUSO",' );
        UTL_FILE.put_line(fich_salida_pkg,'        FILE_NAME');
        UTL_FILE.put_line(fich_salida_pkg,'      FROM ' );
        UTL_FILE.put_line(fich_salida_pkg,'        ' || OWNER_SA || '.' || 'SA_' || reg_summary.CONCEPT_NAME);
        UTL_FILE.put_line(fich_salida_pkg,'      GROUP BY ' );
        UTL_FILE.put_line(fich_salida_pkg,'        FILE_NAME)');
        UTL_FILE.put_line(fich_salida_pkg,'    LOOP' );
        UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' ||  NAME_DM || '.inserta_monitoreo_marca (nom_proceso_in, fch_inicio_in, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), MARCAS.FILE_NAME, MARCAS.MARCA_INICIAL, MARCAS.MARCA_FINAL, MARCAS.MARCA_INICIAL_HUSO, MARCAS.MARCA_FINAL_HUSO);');
        UTL_FILE.put_line(fich_salida_pkg,'    END LOOP;' );
        UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' ||  NAME_DM || '.inserta_monitoreo (nom_proceso_in, 1, 0, fch_inicio_in, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), num_inserts_in, 0, 0, num_reads_in, num_discards_in);');
        UTL_FILE.put_line(fich_salida_pkg,'    commit;' );      
        UTL_FILE.put_line(fich_salida_pkg,'    ' );      
        UTL_FILE.put_line(fich_salida_pkg,'' );
        UTL_FILE.put_line(fich_salida_pkg,'  exception');
        UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
        UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Se ha producido un error en el pre-proceso de staging. Tabla: '' || ''' || 'SA_' || reg_summary.CONCEPT_NAME || ''');');
        UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
        UTL_FILE.put_line(fich_salida_pkg,'    raise;');
        UTL_FILE.put_line(fich_salida_pkg, '  END mar_' || nombre_proceso || ';'); 
        UTL_FILE.put_line(fich_salida_pkg, '');
      end if;
      /* (20160316). Angel Ruiz. FIN NF: Se añade control de marcas en los ficehros cargados */
      UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
      UTL_FILE.put_line(fich_salida_pkg, '/' );
      UTL_FILE.put_line(fich_salida_pkg, 'GRANT EXECUTE ON ' || OWNER_SA || '.pkg_' || nombre_proceso || ' TO ' || OWNER_TC || ';');
      --UTL_FILE.put_line(fich_salida_pkg, '/' );
      UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');
      
      UTL_FILE.FCLOSE (fich_salida_pkg);
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;


