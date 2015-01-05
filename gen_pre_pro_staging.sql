DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      CONCEPT_NAME,
      SOURCE,
      INTERFACE_NAME,
      COUNTRY,
      TYPE,
      SEPARATOR,
      LENGTH,
      DELAYED
    FROM METADATO.MTDT_INTERFACE_SUMMARY
    WHERE SOURCE <> 'SA';
    --where DELAYED = 'S';
    --and CONCEPT_NAME in ('TRAFD_CU_MVNO', 'TRAFE_CU_MVNO', 'TRAFV_CU_MVNO');
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      CONCEPT_NAME,
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      POSITION
    FROM
      METADATO.MTDT_INTERFACE_DETAIL
    WHERE
      CONCEPT_NAME = concep_name_in and
      SOURCE = source_in
    ORDER BY POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col PLS_INTEGER;
      num_column PLS_INTEGER;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;
      
      
      
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
      nombre_proceso                      VARCHAR(30);


  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
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
      
      /******/
      /* COMIENZO LA GENERACION DEL PACKAGE DEFINITION */
      /******/
      UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' ||  OWNER_SA || '.pkg_' || nombre_proceso || ' AS');
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      dbms_output.put_line ('Estoy en PACKAGE DEFINITION DE LA TABLA: ' || 'SA' || '_' || reg_summary.CONCEPT_NAME);
      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pre_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'');');
      --UTL_FILE.put_line(fich_salida_pkg, '' ); 
      --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pos_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'');');
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
      UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_tabla varchar(30);BEGIN select table_name into nombre_tabla from all_tables where table_name = '''''' || table_name_in || ''''''; END;'';');
      UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
      UTL_FILE.put_line(fich_salida_pkg,'  exception');
      UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
      UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
      UTL_FILE.put_line(fich_salida_pkg,'  END existe_tabla;');
      UTL_FILE.put_line(fich_salida_pkg,'');
      UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_particion (partition_name_in IN VARCHAR2, table_name_in IN VARCHAR2) return number');
      UTL_FILE.put_line(fich_salida_pkg,'  IS');
      UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
      UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_particion varchar(30);BEGIN select partition_name into nombre_particion from all_tab_partitions where partition_name = '''''' || partition_name_in || '''''' and table_name = '''''' || table_name_in || ''''''; END;'';');
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
      --UTL_FILE.put_line(fich_salida_pkg, '  exis_tabla :=  existe_tabla (' || '''SA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in);');      
      --UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_NUMBER(TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD'')+1,''YYYYMMDD''));');
      if (reg_summary.DELAYED = 'S') then
        UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD'')+1, ''YYYYMMDD'');'); 
        UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''PA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_''' || ' || fch_datos_in, ''SA_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        --UTL_FILE.put_line(fich_salida_pkg,'  if (exis_tabla = 1) then' );      
        UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || ''app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg,'  else' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''CREATE TABLE ' || 'app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in  || '' AS SELECT * FROM SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' ADD PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN (TO_DATE('''''' || fch_particion || '''''', ''''YYYYMMDD'''')) TABLESPACE DWTBSP_D_MVNO_SA'';');
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
      --UTL_FILE.put_line(fich_salida_pkg,'  PROCEDURE pos_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'')');
      --UTL_FILE.put_line(fich_salida_pkg,'  IS'); 
      --UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
      --UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
      --UTL_FILE.put_line(fich_salida_pkg,'  BEGIN'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    /* Proceso que se va ha encargar de hacer el pos-procesado para insertar los registros cargados en la tabla SA_' || reg_summary.CONCEPT_NAME || ' */'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    /* a la tabla SA_' || reg_summary.CONCEPT_NAME || '_YYYYMMDD */'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla :=  existe_tabla (' || '''SA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in);');      
      --UTL_FILE.put_line(fich_salida_pkg,'    if (exis_tabla = 1) then' );      
      --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || ''app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in;');
      --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''INSERT INTO app_mvnosa.SA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in || '' select * from app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
      --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  '' || ''SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in || ''UPDATE INDEXES'';');
      --UTL_FILE.put_line(fich_salida_pkg,'    else' );
      --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''CREATE TABLE ' || 'app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in  || '' AS SELECT * FROM SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
      --UTL_FILE.put_line(fich_salida_pkg,'    end if;' );
      --UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''El numero de filas insertadas es: '' || SQL%ROWCOUNT);'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    commit;'); 
      --UTL_FILE.put_line(fich_salida_pkg,'  exception'); 
      --UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
      --UTL_FILE.put_line(fich_salida_pkg,'  end pos_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || ';'); 

/************/
      UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
      UTL_FILE.put_line(fich_salida_pkg, '/' );
      UTL_FILE.put_line(fich_salida_pkg, 'GRANT EXECUTE ON ' || OWNER_SA || '.pkg_' || nombre_proceso || ' TO app_mvnotc');
      UTL_FILE.put_line(fich_salida_pkg, '/' );
      UTL_FILE.put_line(fich_salida_pkg, '');
      
      UTL_FILE.FCLOSE (fich_salida_pkg);
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;


