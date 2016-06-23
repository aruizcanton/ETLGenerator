  DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      TRIM(INTERFACE_NAME) "INTERFACE_NAME",
      trim(COUNTRY) "COUNTRY",
      TYPE,
      SEPARATOR,
      DELAYED,
      upper(trim(TYPE_VALIDATION)) TYPE_VALIDATION
  FROM MTDT_INTERFACE_SUMMARY;
  --where CONCEPT_NAME = 'CICLO';

  CURSOR dtd_interfaz_summary_history
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED,
      HISTORY
    FROM MTDT_INTERFACE_SUMMARY
    where HISTORY is not null;
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      PARTITIONED,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      trim(CONCEPT_NAME) = trim(concep_name_in) and
      SOURCE = source_in
      order by POSITION;
      
      

      reg_summary dtd_interfaz_summary%rowtype;

      reg_summary_history dtd_interfaz_summary_history%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col INTEGER;
      v_nombre_particion VARCHAR2(30);
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_columns_partitioned  IS TABLE OF VARCHAR(30);
      TYPE list_tablas_RE IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;

      
      lista_pk                                      list_columns_primary := list_columns_primary ();
      lista_pos                                    list_posiciones := list_posiciones (); 
      
      tipo_col                                      VARCHAR(70);
      lista_par                                     list_columns_partitioned := list_columns_partitioned();
      v_lista_tablas_RE                        list_tablas_RE := list_tablas_RE();
      lista_campos_particion            VARCHAR(250);
      no_encontrado                          VARCHAR(1);
      subset                                         VARCHAR(1);
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      TABLESPACE_SA                  VARCHAR2(60);
      nombre_tabla_reducido VARCHAR2(30);
      v_existe_tablas_RE integer:=0;
      v_encontrado VARCHAR2(1):='N';
      nombre_interface_a_cargar   VARCHAR2(150);
      pos_ini_pais                            PLS_integer;
      pos_fin_pais                            PLS_integer;
      pos_ini_fecha                           PLS_integer;
      pos_fin_fecha                           PLS_integer;
      pos_ini_hora                              PLS_integer;
      pos_fin_hora                              PLS_integer;
      num_column PLS_INTEGER;
      v_ulti_pos                        PLS_integer;

      
      
    
      


BEGIN
  /* (20150119) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  /* (20150119) FIN*/
  
  /* (20151117) Angel Ruiz. NF. Generacion de los creates de tablas SAD y SADH*/
  FOR nombre_tabla_HF in (
      SELECT distinct substr(table_name, 4) nombre_tabla
      FROM MTDT_TC_SCENARIO
      WHERE TABLE_TYPE = 'I'
      AND REINYECTION = 'Y')
  LOOP
    v_existe_tablas_RE:=1;
    v_lista_tablas_RE.EXTEND;
    v_lista_tablas_RE (v_lista_tablas_RE.last) := nombre_tabla_HF.nombre_tabla;
  END LOOP;
  /* (20151117) Angel Ruiz. FIN NF. Generacion de los creates de tablas SAD y SADH*/
  
  DBMS_OUTPUT.put_line('set echo on;');
  DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
    INTO reg_summary;
    EXIT WHEN dtd_interfaz_summary%NOTFOUND;
    /* (20160523) Angel Ruiz. NF: Funcionalidad para la creacion de Tablas Externas */
    if (reg_summary.TYPE_VALIDATION = 'T' or reg_summary.TYPE_VALIDATION = 'I' or reg_summary.TYPE_VALIDATION is null) then
      /* (20160523) Se trata de la creacion de una Tabla de Staging NORMAL */
      DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
      DBMS_OUTPUT.put_line('(');
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      primera_col := 1;
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        IF primera_col = 1 THEN /* Si es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'DATE';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'VARCHAR2 (8)';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
          END IF;
          primera_col := 0;
        ELSE  /* si no es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'DATE';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'VARCHAR2 (8)';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
          END IF;
        END IF;
        IF upper(reg_datail.KEY) = 'S'  then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
        END IF;
        IF reg_datail.PARTITIONED = 'S' then
          lista_par.EXTEND;
          lista_par(lista_par.LAST) := reg_datail.COLUMNA;
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      IF (lista_pk.COUNT > 0 and lista_par.COUNT = 0) THEN
        /* tenemos una tabla normal no particionada */
        DBMS_OUTPUT.put_line(',' || 'CONSTRAINT "' || reg_summary.CONCEPT_NAME || '_P"' || ' PRIMARY KEY (');
        FOR indx IN lista_pk.FIRST .. lista_pk.LAST
        LOOP
          IF indx = lista_pk.LAST THEN
            DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
          ELSE
            DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
          END IF;
        END LOOP;
      END IF;
      DBMS_OUTPUT.put_line(')'); /* Parentesis final del create*/ 
      DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_SA);
      /* tomamos el campo por el que va a estar particionada la tabla */
      if lista_par.COUNT > 0 then
        FOR indx IN lista_par.FIRST .. lista_par.LAST
        LOOP
          IF indx = lista_par.FIRST THEN
            lista_campos_particion:= lista_par (indx);
          ELSE
            lista_campos_particion:=lista_campos_particion || ',' || lista_par (indx);
          END IF;
        END LOOP;
        DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || lista_campos_particion || ')');   
        DBMS_OUTPUT.put_line('(');
        if (length(reg_summary.CONCEPT_NAME) <= 18) then
          v_nombre_particion := 'PA_' || reg_summary.CONCEPT_NAME;
        else
          v_nombre_particion := reg_summary.CONCEPT_NAME;
        end if;
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-90,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-89,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-89,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-88,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-88,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-87,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-87,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-86,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-86,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-85,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-85,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-84,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-84,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-83,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-83,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-82,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-82,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-81,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-81,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-80,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-80,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-79,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-79,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-78,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-78,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-77,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-77,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-76,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-76,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-75,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-75,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-74,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-74,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-73,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-73,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-72,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-72,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-71,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-71,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-70,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-70,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-69,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-69,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-68,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-68,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-67,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-67,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-66,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-66,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-65,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-65,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-64,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-64,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-63,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-63,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-62,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-62,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-61,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-61,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-60,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-60,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-59,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-59,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-58,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-58,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-57,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-57,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-56,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-56,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-55,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-55,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-54,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-54,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-53,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-53,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-52,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-52,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-51,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-51,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-50,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-50,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-49,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-49,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-48,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-48,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-47,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-47,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-46,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-46,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-45,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-45,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-44,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-44,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-43,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-43,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-42,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-42,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-41,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-41,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-40,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-40,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-39,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-39,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-38,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-38,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-37,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-37,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-36,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-36,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-35,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-35,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-34,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-34,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-33,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-33,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-32,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-32,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-31,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-31,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-30,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-30,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-29,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-29,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-28,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-28,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-27,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-27,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-26,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-26,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-25,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-25,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-24,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-24,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-23,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-23,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-22,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-22,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-21,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-21,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-20,'YYYYMMDD') || ''',''YYYYMMDD'')),');
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-20,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-19,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-19,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-18,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-18,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-17,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-17,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-16,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-16,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-15,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-15,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-14,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-14,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-13,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-13,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-12,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-12,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-11,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-11,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-10,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-10,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-9,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-9,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-8,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-8,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-7,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-7,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-6,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-6,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-5,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-5,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-4,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-4,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-3,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-3,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-2,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+2,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+3,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+3,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+4,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+4,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+5,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+5,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+6,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+6,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+7,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+7,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+8,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+8,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+9,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+9,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+10,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+10,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+11,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+11,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+12,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+12,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+13,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+13,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+14,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+14,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+15,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+15,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+16,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+16,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+17,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+17,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+18,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+18,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+19,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+19,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+20,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+20,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+21,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+21,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+22,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+22,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+23,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+23,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+24,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+24,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+25,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+25,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+26,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+26,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+27,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+27,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+28,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+28,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+29,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+29,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+30,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+30,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+31,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+31,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+32,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+32,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+33,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+33,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+34,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+34,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+35,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+35,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+36,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+36,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+37,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+37,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+38,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+38,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+39,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+39,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+40,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+40,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+41,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+41,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+42,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+42,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+43,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+43,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+44,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+44,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+45,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+45,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+46,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+46,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+47,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+47,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+48,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+48,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+49,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+49,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+50,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+50,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+51,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+51,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+52,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+52,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+53,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+53,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+54,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+54,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+55,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+55,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+56,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+56,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+57,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+57,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+58,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+58,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+59,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+59,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+60,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+60,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+61,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+61,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+62,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+62,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+63,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+63,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+64,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+64,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+65,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+65,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+66,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+66,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+67,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+67,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+68,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+68,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+69,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+69,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+70,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+70,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+71,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+71,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+72,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+72,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+73,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+73,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+74,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+74,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+75,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+75,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+76,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+76,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+77,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+77,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+78,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+78,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+79,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+79,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+80,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+80,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+81,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+81,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+82,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+82,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+83,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+83,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+84,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+84,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+85,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+85,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+86,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+86,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+87,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+87,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+88,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+88,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+89,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+89,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+90,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+90,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+91,'YYYYMMDD') || ''',''YYYYMMDD''))');   
        DBMS_OUTPUT.put_line(')');
      end if;
      DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
      /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
      IF (lista_pk.COUNT > 0 and lista_par.COUNT > 0) THEN 
        /* Tenemos una tabla particionada y con PK */
        /* Buscamos si elcampo de particionado forma parte de la PK, ya que si asi es podemos crear un indice PK local*/
        no_encontrado := 'N'; /* por defecto supongo que todos los campos de particionado forman parte del indice, de ahi no_encontrado = N */
        FOR indy IN lista_par.FIRST .. lista_par.LAST
        LOOP
          /* Para cada uno de los campos de particionado. Normalmente es uno*/
          /* busco si estan en los campos del indice */
          subset := 'N';
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF (lista_par(indy) = lista_pk(indx)) THEN
              subset := 'Y';
            END IF;
          END LOOP;
          if (subset = 'N') then
            /* No he encontrado el campo de particionado en los campos que forman el indice */
            no_encontrado := 'Y';
          end if;
        END LOOP;
        IF (no_encontrado = 'Y') THEN
          /* Ocurre que hay campos de particionado que no formal parte del indice por lo que no se puede crear un indice local*/
          DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX ' || reg_summary.CONCEPT_NAME || '_P ON ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
          DBMS_OUTPUT.put_line('(');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('GLOBAL;');
          DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SA_'  || reg_summary.CONCEPT_NAME || ' ADD CONSTRAINT ' || reg_summary.CONCEPT_NAME || '_P PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('USING INDEX ' || reg_summary.CONCEPT_NAME || '_P;');
        ELSE
          /* Podemos crear un Indice PK local */
          DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX ' || reg_summary.CONCEPT_NAME || '_P ON ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
          DBMS_OUTPUT.put_line('(');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
          DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SA_'  || reg_summary.CONCEPT_NAME || ' ADD CONSTRAINT ' || reg_summary.CONCEPT_NAME || '_P PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('USING INDEX ' || reg_summary.CONCEPT_NAME || '_P;');
        END IF;
      ELSE
        if (lista_pk.COUNT = 0 and lista_par.COUNT>0) then
          /* Tenemos el caso de que la tabla no tiene PK pero si esta particionada */
          /* Creamos un indice local por el campo de particionado */
          DBMS_OUTPUT.put_line('CREATE INDEX '  || reg_summary.CONCEPT_NAME || '_L ON ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' (');
          FOR indy IN lista_par.FIRST .. lista_par.LAST
          LOOP
              IF indy = lista_par.LAST THEN
                DBMS_OUTPUT.put_line(lista_par (indy) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(lista_par (indy) || ',');
              END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
        end if;
      END IF;
      DBMS_OUTPUT.put_line('');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_par.DELETE;
      /* (20151118) Angel Ruiz. NF: Creacion de tablas para inyeccion SAD */
      if v_existe_tablas_RE = 1 then
        /* Existen tablas de inyeccion */
        v_encontrado:='N';
        for indx in v_lista_tablas_RE.FIRST .. v_lista_tablas_RE.LAST
        loop
          if (v_lista_tablas_RE(indx) = reg_summary.CONCEPT_NAME) then
            v_encontrado := 'Y';
          end if;
        end loop;
        if v_encontrado = 'Y' then
          DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.SAD_' || reg_summary.CONCEPT_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
          primera_col := 1;
          LOOP
            FETCH dtd_interfaz_detail
            INTO reg_datail;
            EXIT WHEN dtd_interfaz_detail%NOTFOUND;
            IF primera_col = 1 THEN /* Si es primera columna */
              CASE 
              WHEN reg_datail.TYPE = 'AN' THEN
                tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'NU' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'DE' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'FE' THEN
                tipo_col := 'DATE';
              WHEN reg_datail.TYPE = 'IM' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
                --tipo_col := 'NUMBER (15, 3)';
              WHEN reg_datail.TYPE = 'TI' THEN
                tipo_col := 'VARCHAR2 (8)';
              END CASE;
              IF reg_datail.NULABLE = 'N'
              THEN
                DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
              ELSE
                DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
              END IF;
              primera_col := 0;
            ELSE  /* si no es primera columna */
              CASE 
              WHEN reg_datail.TYPE = 'AN' THEN
                tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'NU' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'DE' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'FE' THEN
                tipo_col := 'DATE';
              WHEN reg_datail.TYPE = 'IM' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
                --tipo_col := 'NUMBER (15, 3)';
              WHEN reg_datail.TYPE = 'TI' THEN
                tipo_col := 'VARCHAR2 (8)';
              END CASE;
              IF reg_datail.NULABLE = 'N'
              THEN
                DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
              ELSE
                DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
              END IF;
            END IF;
            IF upper(reg_datail.KEY) = 'S'  then
              lista_pk.EXTEND;
              lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
            END IF;
            IF reg_datail.PARTITIONED = 'S' then
              lista_par.EXTEND;
              lista_par(lista_par.LAST) := reg_datail.COLUMNA;
            END IF;
          END LOOP;
          CLOSE dtd_interfaz_detail;
          /* (20151123) Anyadimos la columna BAN_DESCARTE  en las tablas SAD_*/
          DBMS_OUTPUT.put_line(', BAN_DESCARTE' ||  '          '  || 'VARCHAR2(10)');
          IF (lista_pk.COUNT > 0 and lista_par .COUNT = 0) THEN
            /* tenemos una tabla normal no particionada */
            DBMS_OUTPUT.put_line(',' || 'CONSTRAINT "' || reg_summary.CONCEPT_NAME || 'HF"' || ' PRIMARY KEY (');
            FOR indx IN lista_pk.FIRST .. lista_pk.LAST
            LOOP
              IF indx = lista_pk.LAST THEN
                DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
              END IF;
            END LOOP;
          END IF;
          DBMS_OUTPUT.put_line(')'); /* Parentesis final del create*/ 
          DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_SA);
          DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
          DBMS_OUTPUT.put_line(''); /* FIN CREATE */
          lista_pk.DELETE;      /* Borramos los elementos de la lista */
          lista_par.DELETE;
        end if;
      end if;
      /* (20151118) Angel Ruiz. FIN NF. Tablas para inyeccion SAD_ */
    else  /* SE TRATA DE CREAR UNA TABLE EXTERNA PARA VALIDAR FICHEROS DE EXTRACCION */
      /**********************************************/
      /* (20160523) Angel Ruiz. NF: SE TRATA DE TABLAS EXTERNAS */
      /**********************************************/
      nombre_interface_a_cargar := reg_summary.INTERFACE_NAME;
      pos_ini_pais := instr(reg_summary.INTERFACE_NAME, '_XXX_');
      if (pos_ini_pais > 0) then
        pos_fin_pais := pos_ini_pais + length ('_XXX_');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_pais -1) || '_' || reg_summary.COUNTRY || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
      end if;
      pos_ini_fecha := instr(reg_summary.INTERFACE_NAME, '_YYYYMMDD');
      if (pos_ini_fecha > 0) then
        pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
        --nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_${FCH_DATOS}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_????????' || substr(nombre_interface_a_cargar, pos_fin_fecha);        
      end if;
      pos_ini_hora := instr(nombre_interface_a_cargar, 'HH24MISS');
      if (pos_ini_hora > 0) then
        pos_fin_hora := pos_ini_hora + length ('HH24MISS');
        nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
      end if;
      
      
      DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
      DBMS_OUTPUT.put_line('(');
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      primera_col := 1;
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        IF primera_col = 1 THEN /* Si es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'DATE';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'VARCHAR2 (8)';
          END CASE;
          DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
          primera_col := 0;
        ELSE  /* si no es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'DATE';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'VARCHAR2 (8)';
          END CASE;
          DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      DBMS_OUTPUT.put_line(')');
      DBMS_OUTPUT.put_line('ORGANIZATION EXTERNAL');
      DBMS_OUTPUT.put_line('(');
      DBMS_OUTPUT.put_line('  TYPE ORACLE_LOADER');
      DBMS_OUTPUT.put_line('  DEFAULT DIRECTORY FUENTE_DIR');
      DBMS_OUTPUT.put_line('  ACCESS PARAMETERS');
      DBMS_OUTPUT.put_line('  (');
      DBMS_OUTPUT.put_line('    RECORDS DELIMITED BY NEWLINE');
      DBMS_OUTPUT.put_line('    BADFILE DESCARTADOS_DIR:''' || 'SA_' || reg_summary.CONCEPT_NAME || '%a_%p.bad''');
      DBMS_OUTPUT.put_line('    LOGFILE TRAZAS_DIR:''' || 'SA_' || reg_summary.CONCEPT_NAME || '%a_%p.log''');
      IF reg_summary.TYPE = 'S'             /*  El fichero posee un separador de campos */
      THEN
        DBMS_OUTPUT.put_line('    FIELDS TERMINATED BY ' || reg_summary.SEPARATOR);
        DBMS_OUTPUT.put_line('    MISSING FIELD VALUES ARE NULL');
        DBMS_OUTPUT.put_line('    REJECT ROWS WITH ALL NULL FIELDS');
        DBMS_OUTPUT.put_line('    NULLIF=BLANKS');
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        num_column := 0;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          num_column := num_column+1;
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            /* (20150326) Angel Ruiz. Incidencia */
            --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE is null and reg_datail.LENGTH>2) then
            --  tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ';
            --elsif (reg_datail.NULABLE is null and (reg_datail.LENGTH>2 and reg_datail.LENGTH<=11)) then
            --  tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ';
            --elsif (reg_datail.NULABLE is null and reg_datail.LENGTH>11) then 
            --  tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ';
            --else
              tipo_col := 'CHAR (' || reg_datail.LENGTH || ')';
            --end if;
            /*(20150715) Angel Ruiz. Nueva Funcionalidad. Columna para almacenar el fichero del que se carga la informacion.*/
            --if (reg_datail.COLUMNA = 'FILE_NAME') then
            --  tipo_col := 'CONSTANT "MY_FILE"';
            --  nombre_fich_cargado := 'Y';
            --end if;
            /*(20150715) Angel Ruiz. Fin. */
          WHEN reg_datail.TYPE = 'NU' THEN
            --tipo_col := 'TO_NUMBER (' || reg_datail.LENGTH || ')';
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE is null) then
            --  tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            --else            
              tipo_col := 'INTEGER EXTERNAL(' || reg_datail.LENGTH || ')';
            --end if;
          WHEN reg_datail.TYPE = 'DE' THEN
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.NULABLE is null) then
            --  tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            --else            
              tipo_col := 'DECIMAL EXTERNAL(' || reg_datail.LENGTH || ')';
            --end if;
          WHEN reg_datail.TYPE = 'FE' THEN
            if (reg_datail.LENGTH = 14) then
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              --if (reg_datail.NULABLE is null ) then
                --tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101000000'', ''YYYYMMDDHH24MISS''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDDHH24MISS''))"';
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ') DATE_FORMAT DATE MASK "YYYYMMDDHH24MISS"';
              --else
              --  tipo_col := 'DATE "YYYYMMDDHH24MISS"';
              --end if;              
            else
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              --if (reg_datail.NULABLE is null ) then
              --  tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101'', ''YYYYMMDD''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDD''))"';
              --else
              --  tipo_col := 'DATE "YYYYMMDD"';
              --end if;
              tipo_col := 'CHAR (' || reg_datail.LENGTH || ') DATE_FORMAT DATE MASK "YYYYMMDD"';
            end if;
          WHEN reg_datail.TYPE = 'IM' THEN
            /* Tratamos el tema de los importes para que vengan con separador de miles el . y separador de decimales la , */
            tipo_col:='DECIMAL EXTERNAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'TI' THEN
            --if (reg_datail.NULABLE is null) then
            --  tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''000000'')"';
            --else            
            --  tipo_col := 'CHAR (8)';
            --end if;
            tipo_col := 'CHAR (8)';
          END CASE;
          IF primera_col = 1
          THEN
            DBMS_OUTPUT.put_line('      ( ' || reg_datail.COLUMNA || ' ' || tipo_col);
            primera_col := 0;
          ELSE
            DBMS_OUTPUT.put_line('      , ' || reg_datail.COLUMNA || ' ' || tipo_col ); 
          END IF;
        END LOOP;
        close dtd_interfaz_detail;
        DBMS_OUTPUT.put_line('      )');
        DBMS_OUTPUT.put_line('  )');
        DBMS_OUTPUT.put_line('  LOCATION (''' || nombre_interface_a_cargar || ''')');
        DBMS_OUTPUT.put_line(')');
      ELSE  /*  El fichero NO POSEE un separador de campos. Los campos son de longitud fija */
        DBMS_OUTPUT.put_line('    FIELDS');
        DBMS_OUTPUT.put_line('    MISSING FIELD VALUES ARE NULL');
        DBMS_OUTPUT.put_line('    REJECT ROWS WITH ALL NULL FIELDS');
        DBMS_OUTPUT.put_line('    NULLIF=BLANKS');
        primera_col := 1;
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          lista_pos.EXTEND;
          lista_pos(lista_pos.LAST) :=  reg_datail.POSITION;
        END LOOP;
        close dtd_interfaz_detail;
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        num_column := 0;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          num_column := num_column+1;
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            /* (20150116) Angel Ruiz. introduzco formateo en la columnas */
            /* (20150326) Angel Ruiz. Incidencia */
            tipo_col := 'CHAR(' || reg_datail.LENGTH || ') ';
            /*(20150715) Angel Ruiz. Nueva Funcionalidad. Columna para almacenar el fichero del que se carga la informacion.*/
            --if (reg_datail.COLUMNA = 'FILE_NAME') then
            --  tipo_col := 'CONSTANT "MY_FILE"';
            --  nombre_fich_cargado := 'Y';
            --end if;
            /*(20150715) Angel Ruiz. Fin. */
          WHEN reg_datail.TYPE = 'NU' THEN
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE is null) then
            --  tipo_col := 'INTEGER EXTERNAL "NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            --else            
              tipo_col := 'INTEGER EXTERNAL(' || reg_datail.LENGTH || ')';
            --end if;
            --tipo_col := '';
          WHEN reg_datail.TYPE = 'DE' THEN
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE is null) then
            --  tipo_col := 'DECIMAL EXTERNAL "NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            --else            
              tipo_col := 'DECIMAL EXTERNAL(' || reg_datail.LENGTH || ')';
            --end if;
            --tipo_col := '';
          WHEN reg_datail.TYPE = 'FE' THEN
            if (reg_datail.LENGTH = 14) then
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              --if (reg_datail.KEY is null and reg_datail.NULABLE is null ) then
              --  tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101'', ''YYYYMMDDHH24MISS''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDDHH24MISS''))"';
              --else
                --tipo_col := 'DATE "YYYYMMDDHH24MISS"';
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ') DATE_FORMAT DATE MASK "YYYYMMDDHH24MISS"';
              --end if;
            else
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              --if (reg_datail.KEY is null and reg_datail.NULABLE is null ) then
              --  tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101'', ''YYYYMMDD''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDD''))"';
              --else
              --  tipo_col := 'DATE "YYYYMMDD"';
              --end if;
              tipo_col := 'CHAR (' || reg_datail.LENGTH || ') DATE_FORMAT DATE MASK "YYYYMMDD"';
            end if;
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'DECIMAL EXTERNAL (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'TI' THEN
            --if (reg_datail.NULABLE is null) then
            --  tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''000000'')"';
            --else            
            --  tipo_col := 'CHAR';
            --end if;
            tipo_col := 'CHAR (8)';
          END CASE;
          IF primera_col = 1
          THEN
            dbms_output.put_line ('      ( ' || reg_datail.COLUMNA || '            POSITION(1:' || (lista_pos (num_column+1)-1) || ')     ' || tipo_col);
            primera_col := 0;
          ELSE
            if lista_pos.last = num_column then
              /* Se trata de la ultima columna */
              if (instr(reg_datail.LENGTH, ',') > 0) then
                /* Si aparece una coma es que es del tipo 15,3 */
                v_ulti_pos := (reg_datail.POSITION + to_number(trim(substr(reg_datail.LENGTH, 1, instr(reg_datail.LENGTH, ',') -1)))) -1;
                dbms_output.put_line ('      , ' || reg_datail.COLUMNA || '            POSITION(' || reg_datail.POSITION || ':' || to_char(v_ulti_pos) || ')     ' || tipo_col); 
              else
                v_ulti_pos := (reg_datail.POSITION + to_number(trim(reg_datail.LENGTH))) -1;
                dbms_output.put_line ('      , ' || reg_datail.COLUMNA || '            POSITION(' || reg_datail.POSITION || ':' || to_char(v_ulti_pos) || ')     ' || tipo_col); 
              end if;
            else
              dbms_output.put_line ('      , ' || reg_datail.COLUMNA || '            POSITION(' || reg_datail.POSITION || ':' || (lista_pos (num_column+1)-1) || ')     ' || tipo_col); 
            end if;
          END IF;
        END LOOP;
        close dtd_interfaz_detail;
        /* (20150605) Angel Ruiz. AÃADIDO PARA CHEQUEAR LA CALIDAD DEL DATO */
        --UTL_FILE.put_line(fich_salida, ', FILE_NAME CONSTANT "MY_FILE"' ); 
        /* (20150605) Fin */
        DBMS_OUTPUT.put_line('      )');
        DBMS_OUTPUT.put_line('  )');
        DBMS_OUTPUT.put_line('  LOCATION (''' || nombre_interface_a_cargar || ''')');
        DBMS_OUTPUT.put_line(')');
        DBMS_OUTPUT.put_line('PARALLEL;');        
      END IF;
    end if;
  END LOOP;
  CLOSE dtd_interfaz_summary;
  /****************************************************************/
  /* (20150717) ANGEL RUIZ. NUEVA FUNCIONALIDAD.*/
  /* Las tablas de STAGING pueden tener HISTORICO */
  /* POR LO QUE HAY QUE CREAR LAS TABLAS DE HISTORICO */
  /*****************************************************************************/
  OPEN dtd_interfaz_summary_history;
  LOOP
    FETCH dtd_interfaz_summary_history
      INTO reg_summary_history;
      EXIT WHEN dtd_interfaz_summary_history%NOTFOUND;  
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME);
      DBMS_OUTPUT.put_line('(');
      OPEN dtd_interfaz_detail (reg_summary_history.CONCEPT_NAME, reg_summary_history.SOURCE);
      primera_col := 1;
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        IF primera_col = 1 THEN /* Si es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'DATE';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'VARCHAR2 (8)';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
          END IF;
          primera_col := 0;
        ELSE  /* si no es primera columna */
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'NU' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'DE' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
          WHEN reg_datail.TYPE = 'FE' THEN
            tipo_col := 'DATE';
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
            --tipo_col := 'NUMBER (15, 3)';
          WHEN reg_datail.TYPE = 'TI' THEN
            tipo_col := 'VARCHAR2 (8)';
          END CASE;
          IF reg_datail.NULABLE = 'N'
          THEN
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
          ELSE
            DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
          END IF;
        END IF;
        IF upper(reg_datail.KEY) = 'S'  then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
        END IF;
        IF reg_datail.PARTITIONED = 'S' then
          lista_par.EXTEND;
          lista_par(lista_par.LAST) := reg_datail.COLUMNA;
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      /* Ahora miramos si he de crear un campo de particionado para la tabla historica */
      /* o por el contrario la tabla de STAGING ya tenia un campo de particionado */
      if (lista_par.count = 0) then
        /* La tabla de STAGING no esta particionada aunque su historica si debe estarlo*/
        DBMS_OUTPUT.put_line(', CVE_DIA          NUMBER(8)'); /* Anyado una columna de particionado */
        lista_par.EXTEND;
        lista_par(lista_par.LAST) := 'CVE_DIA'; /* La anyado a la lista de campos por los que particionar mi tabla historica */
        if (lista_pk.count > 0) then
          /* La tabla tiene clave primaria, asi anyadimos el campo de particionado a la clave primaria para que se pueda hacer un indice local */
          lista_pk.extend;
          lista_pk(lista_pk.LAST) := 'CVE_DIA';     /* La anyado a la lista de PKs de mi tabla historica */
        end if;
      end if;
      DBMS_OUTPUT.put_line(')'); /* Parentesis final del create*/
      DBMS_OUTPUT.put_line('NOLOGGING');
      DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_SA);
      /* tomamos el campo por el que va a estar particionada la tabla */
      if lista_par.COUNT > 0 then
        FOR indx IN lista_par.FIRST .. lista_par.LAST
        LOOP
          IF indx = lista_par.FIRST THEN
            lista_campos_particion:= lista_par (indx);
          ELSE
            lista_campos_particion:=lista_campos_particion || ',' || lista_par (indx);
          END IF;
        END LOOP;
        DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || lista_campos_particion || ')');   
        DBMS_OUTPUT.put_line('('); 
        if (length(reg_summary_history.CONCEPT_NAME) <= 18) then
          v_nombre_particion := 'SA_' || reg_summary_history.CONCEPT_NAME;
        else
          v_nombre_particion := reg_summary_history.CONCEPT_NAME;
        end if;
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-90,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-89,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-89,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-88,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-88,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-87,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-87,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-86,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-86,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-85,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-85,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-84,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-84,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-83,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-83,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-82,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-82,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-81,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-81,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-80,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-80,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-79,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-79,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-78,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-78,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-77,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-77,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-76,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-76,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-75,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-75,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-74,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-74,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-73,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-73,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-72,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-72,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-71,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-71,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-70,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-70,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-69,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-69,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-68,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-68,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-67,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-67,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-66,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-66,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-65,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-65,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-64,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-64,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-63,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-63,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-62,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-62,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-61,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-61,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-60,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-60,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-59,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-59,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-58,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-58,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-57,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-57,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-56,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-56,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-55,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-55,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-54,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-54,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-53,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-53,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-52,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-52,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-51,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-51,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-50,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-50,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-49,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-49,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-48,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-48,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-47,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-47,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-46,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-46,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-45,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-45,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-44,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-44,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-43,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-43,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-42,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-42,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-41,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-41,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-40,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-40,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-39,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-39,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-38,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-38,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-37,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-37,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-36,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-36,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-35,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-35,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-34,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-34,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-33,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-33,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-32,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-32,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-31,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-31,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-30,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-30,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-29,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-29,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-28,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-28,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-27,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-27,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-26,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-26,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-25,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-25,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-24,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-24,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-23,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-23,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-22,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-22,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-21,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-21,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-20,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-20,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-19,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-19,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-18,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-18,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-17,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-17,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-16,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-16,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-15,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-15,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-14,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-14,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-13,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-13,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-12,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-12,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-11,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-11,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-10,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-10,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-9,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-9,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-8,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-8,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-7,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-7,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-6,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-6,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-5,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-5,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-4,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-4,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-3,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-3,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-2,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-1,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+1,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+2,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+3,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+3,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+4,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+4,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+5,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+5,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+6,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+6,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+7,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+7,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+8,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+8,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+9,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+9,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+10,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+10,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+11,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+11,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+12,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+12,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+13,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+13,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+14,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+14,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+15,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+15,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+16,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+16,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+17,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+17,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+18,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+18,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+19,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+19,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+20,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+20,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+21,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+21,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+22,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+22,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+23,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+23,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+24,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+24,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+25,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+25,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+26,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+26,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+27,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+27,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+28,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+28,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+29,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+29,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+30,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+30,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+31,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+31,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+32,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+32,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+33,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+33,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+34,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+34,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+35,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+35,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+36,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+36,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+37,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+37,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+38,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+38,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+39,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+39,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+40,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+40,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+41,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+41,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+42,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+42,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+43,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+43,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+44,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+44,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+45,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+45,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+46,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+46,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+47,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+47,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+48,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+48,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+49,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+49,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+50,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+50,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+51,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+51,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+52,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+52,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+53,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+53,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+54,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+54,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+55,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+55,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+56,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+56,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+57,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+57,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+58,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+58,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+59,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+59,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+60,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+60,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+61,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+61,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+62,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+62,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+63,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+63,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+64,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+64,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+65,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+65,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+66,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+66,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+67,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+67,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+68,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+68,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+69,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+69,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+70,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+70,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+71,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+71,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+72,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+72,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+73,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+73,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+74,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+74,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+75,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+75,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+76,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+76,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+77,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+77,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+78,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+78,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+79,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+79,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+80,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+80,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+81,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+81,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+82,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+82,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+83,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+83,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+84,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+84,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+85,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+85,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+86,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+86,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+87,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+87,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+88,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+88,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+89,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+89,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+90,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+90,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+91,'YYYYMMDD') || ')');   
        
        DBMS_OUTPUT.put_line(')');
      end if;
      DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
      /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
      IF (lista_pk.COUNT > 0 and lista_par.COUNT > 0) THEN 
        /* Tenemos una tabla particionada y con PK */
        /* Buscamos si elcampo de particionado forma parte de la PK, ya que si asi es podemos crear un indice PK local*/
        no_encontrado := 'N'; /* por defecto supongo que todos los campos de particionado forman parte del indice, de ahi no_encontrado = N */
        FOR indy IN lista_par.FIRST .. lista_par.LAST
        LOOP
          /* Para cada uno de los campos de particionado. Normalmente es uno*/
          /* busco si estan en los campos del indice */
          subset := 'N';
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF (lista_par(indy) = lista_pk(indx)) THEN
              subset := 'Y';
            END IF;
          END LOOP;
          if (subset = 'N') then
            /* No he encontrado el campo de particionado en los campos que forman el indice */
            no_encontrado := 'Y';
          end if;
        END LOOP;
        IF (no_encontrado = 'Y') THEN
          /* Ocurre que hay campos de particionado que no formal parte del indice por lo que no se puede crear un indice local*/
          DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX ' || reg_summary_history.CONCEPT_NAME || '_HP ON ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME);
          DBMS_OUTPUT.put_line('(');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('GLOBAL;');
          DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SAH_'  || reg_summary_history.CONCEPT_NAME || ' ADD CONSTRAINT ' || reg_summary_history.CONCEPT_NAME || '_HP PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('USING INDEX ' || reg_summary_history.CONCEPT_NAME || '_HP;');
        ELSE
          /* Podemos crear un Indice PK local */
          DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX ' || reg_summary_history.CONCEPT_NAME || '_HP ON ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME);
          DBMS_OUTPUT.put_line('(');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
          DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SAH_'  || reg_summary_history.CONCEPT_NAME || ' ADD CONSTRAINT ' || reg_summary_history.CONCEPT_NAME || '_HP PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('USING INDEX ' || reg_summary_history.CONCEPT_NAME || '_HP;');
        END IF;
      ELSE
        if (lista_pk.COUNT = 0 and lista_par.COUNT>0) then
          /* Tenemos el caso de que la tabla no tiene PK pero si esta particionada */
          /* Creamos un indice local por el campo de particionado */
          DBMS_OUTPUT.put_line('CREATE INDEX '  || reg_summary_history.CONCEPT_NAME || '_HL ON ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME || ' (');
          FOR indy IN lista_par.FIRST .. lista_par.LAST
          LOOP
              IF indy = lista_par.LAST THEN
                DBMS_OUTPUT.put_line(lista_par (indy) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(lista_par (indy) || ',');
              END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
        end if;
      END IF;
      DBMS_OUTPUT.put_line('');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_par.DELETE;
      /*********************************************/
      /* (20151118) Angel Ruiz. NF: Tablas de inyeccion SADH_*/
      /*********************************************/
      if v_existe_tablas_RE = 1 then
        /* Existen tablas de inyeccion */
        v_encontrado:= 'N';
        for indx in v_lista_tablas_RE.FIRST .. v_lista_tablas_RE.LAST
        loop
          if (v_lista_tablas_RE(indx) = reg_summary_history.CONCEPT_NAME) then
            v_encontrado := 'Y';
          end if;
        end loop;
        if v_encontrado = 'Y' then

          DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_SA || '.SADH_' || reg_summary_history.CONCEPT_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN dtd_interfaz_detail (reg_summary_history.CONCEPT_NAME, reg_summary_history.SOURCE);
          primera_col := 1;
          LOOP
            FETCH dtd_interfaz_detail
            INTO reg_datail;
            EXIT WHEN dtd_interfaz_detail%NOTFOUND;
            IF primera_col = 1 THEN /* Si es primera columna */
              CASE 
              WHEN reg_datail.TYPE = 'AN' THEN
                tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'NU' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'DE' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'FE' THEN
                tipo_col := 'DATE';
              WHEN reg_datail.TYPE = 'IM' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
                --tipo_col := 'NUMBER (15, 3)';
              WHEN reg_datail.TYPE = 'TI' THEN
                tipo_col := 'VARCHAR2 (8)';
              END CASE;
              IF reg_datail.NULABLE = 'N'
              THEN
                DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
              ELSE
                DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
              END IF;
              primera_col := 0;
            ELSE  /* si no es primera columna */
              CASE 
              WHEN reg_datail.TYPE = 'AN' THEN
                tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'NU' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'DE' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
              WHEN reg_datail.TYPE = 'FE' THEN
                tipo_col := 'DATE';
              WHEN reg_datail.TYPE = 'IM' THEN
                tipo_col := 'NUMBER (' || reg_datail.LENGTH || ')';
                --tipo_col := 'NUMBER (15, 3)';
              WHEN reg_datail.TYPE = 'TI' THEN
                tipo_col := 'VARCHAR2 (8)';
              END CASE;
              IF reg_datail.NULABLE = 'N'
              THEN
                DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          ' || tipo_col || ' NOT NULL');
              ELSE
                DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
              END IF;
            END IF;
            IF upper(reg_datail.KEY) = 'S'  then
              lista_pk.EXTEND;
              lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
            END IF;
            IF reg_datail.PARTITIONED = 'S' then
              lista_par.EXTEND;
              lista_par(lista_par.LAST) := reg_datail.COLUMNA;
            END IF;
          END LOOP;
          CLOSE dtd_interfaz_detail;
          /*(20151123) Angel Ruiz. creo el campo BAN_DESCARTE en las tablas SADH_*/
          DBMS_OUTPUT.put_line(', BAN_DESCARTE' ||  '          '  || 'VARCHAR2(10)');
          /* Ahora miramos si he de crear un campo de particionado para la tabla historica */
          /* o por el contrario la tabla de STAGING ya tenia un campo de particionado */
          if (lista_par.count = 0) then
            /* La tabla de STAGING no esta particionada aunque su historica si debe estarlo*/
            DBMS_OUTPUT.put_line(', CVE_DIA          NUMBER(8)'); /* Anyado una columna de particionado */
            lista_par.EXTEND;
            lista_par(lista_par.LAST) := 'CVE_DIA'; /* La anyado a la lista de campos por los que particionar mi tabla historica */
            if (lista_pk.count > 0) then
              /* La tabla tiene clave primaria, asi anyadimos el campo de particionado a la clave primaria para que se pueda hacer un indice local */
              lista_pk.extend;
              lista_pk(lista_pk.LAST) := 'CVE_DIA';     /* La anyado a la lista de PKs de mi tabla historica */
            end if;
          end if;
          DBMS_OUTPUT.put_line(')'); /* Parentesis final del create*/
          DBMS_OUTPUT.put_line('NOLOGGING');
          DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_SA);
          /* tomamos el campo por el que va a estar particionada la tabla */
          if lista_par.COUNT > 0 then
            FOR indx IN lista_par.FIRST .. lista_par.LAST
            LOOP
              IF indx = lista_par.FIRST THEN
                lista_campos_particion:= lista_par (indx);
              ELSE
                lista_campos_particion:=lista_campos_particion || ',' || lista_par (indx);
              END IF;
            END LOOP;
            DBMS_OUTPUT.put_line('PARTITION BY RANGE (' || lista_campos_particion || ')');   
            DBMS_OUTPUT.put_line('('); 
            if (length(reg_summary_history.CONCEPT_NAME) <= 18) then
              v_nombre_particion := 'SA_' || reg_summary_history.CONCEPT_NAME;
            else
              v_nombre_particion := reg_summary_history.CONCEPT_NAME;
            end if;
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-90,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-89,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-89,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-88,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-88,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-87,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-87,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-86,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-86,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-85,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-85,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-84,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-84,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-83,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-83,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-82,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-82,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-81,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-81,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-80,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-80,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-79,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-79,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-78,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-78,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-77,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-77,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-76,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-76,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-75,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-75,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-74,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-74,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-73,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-73,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-72,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-72,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-71,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-71,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-70,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-70,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-69,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-69,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-68,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-68,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-67,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-67,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-66,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-66,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-65,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-65,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-64,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-64,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-63,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-63,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-62,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-62,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-61,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-61,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-60,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-60,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-59,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-59,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-58,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-58,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-57,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-57,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-56,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-56,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-55,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-55,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-54,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-54,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-53,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-53,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-52,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-52,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-51,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-51,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-50,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-50,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-49,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-49,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-48,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-48,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-47,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-47,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-46,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-46,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-45,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-45,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-44,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-44,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-43,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-43,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-42,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-42,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-41,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-41,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-40,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-40,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-39,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-39,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-38,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-38,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-37,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-37,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-36,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-36,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-35,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-35,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-34,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-34,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-33,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-33,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-32,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-32,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-31,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-31,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-30,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-30,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-29,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-29,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-28,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-28,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-27,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-27,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-26,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-26,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-25,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-25,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-24,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-24,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-23,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-23,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-22,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-22,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-21,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-21,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-20,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-20,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-19,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-19,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-18,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-18,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-17,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-17,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-16,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-16,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-15,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-15,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-14,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-14,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-13,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-13,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-12,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-12,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-11,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-11,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-10,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-10,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-9,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-9,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-8,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-8,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-7,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-7,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-6,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-6,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-5,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-5,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-4,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-4,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-3,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-3,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-2,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-1,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+1,'YYYYMMDD') || '),');
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+2,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+3,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+3,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+4,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+4,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+5,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+5,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+6,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+6,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+7,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+7,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+8,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+8,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+9,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+9,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+10,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+10,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+11,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+11,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+12,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+12,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+13,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+13,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+14,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+14,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+15,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+15,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+16,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+16,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+17,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+17,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+18,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+18,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+19,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+19,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+20,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+20,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+21,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+21,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+22,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+22,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+23,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+23,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+24,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+24,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+25,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+25,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+26,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+26,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+27,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+27,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+28,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+28,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+29,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+29,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+30,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+30,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+31,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+31,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+32,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+32,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+33,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+33,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+34,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+34,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+35,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+35,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+36,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+36,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+37,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+37,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+38,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+38,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+39,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+39,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+40,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+40,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+41,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+41,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+42,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+42,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+43,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+43,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+44,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+44,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+45,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+45,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+46,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+46,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+47,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+47,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+48,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+48,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+49,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+49,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+50,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+50,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+51,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+51,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+52,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+52,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+53,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+53,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+54,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+54,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+55,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+55,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+56,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+56,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+57,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+57,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+58,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+58,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+59,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+59,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+60,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+60,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+61,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+61,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+62,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+62,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+63,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+63,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+64,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+64,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+65,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+65,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+66,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+66,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+67,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+67,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+68,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+68,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+69,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+69,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+70,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+70,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+71,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+71,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+72,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+72,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+73,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+73,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+74,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+74,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+75,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+75,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+76,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+76,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+77,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+77,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+78,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+78,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+79,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+79,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+80,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+80,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+81,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+81,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+82,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+82,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+83,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+83,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+84,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+84,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+85,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+85,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+86,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+86,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+87,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+87,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+88,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+88,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+89,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+89,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+90,'YYYYMMDD') || '),');   
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+90,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+91,'YYYYMMDD') || ')');   
            DBMS_OUTPUT.put_line(')');
          end if;
          DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
          /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
          IF (lista_pk.COUNT > 0 and lista_par.COUNT > 0) THEN 
            /* Tenemos una tabla particionada y con PK */
            /* Buscamos si elcampo de particionado forma parte de la PK, ya que si asi es podemos crear un indice PK local*/
            no_encontrado := 'N'; /* por defecto supongo que todos los campos de particionado forman parte del indice, de ahi no_encontrado = N */
            FOR indy IN lista_par.FIRST .. lista_par.LAST
            LOOP
              /* Para cada uno de los campos de particionado. Normalmente es uno*/
              /* busco si estan en los campos del indice */
              subset := 'N';
              FOR indx IN lista_pk.FIRST .. lista_pk.LAST
              LOOP
                IF (lista_par(indy) = lista_pk(indx)) THEN
                  subset := 'Y';
                END IF;
              END LOOP;
              if (subset = 'N') then
                /* No he encontrado el campo de particionado en los campos que forman el indice */
                no_encontrado := 'Y';
              end if;
            END LOOP;
            IF (no_encontrado = 'Y') THEN
              /* Ocurre que hay campos de particionado que no formal parte del indice por lo que no se puede crear un indice local*/
              DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX ' || reg_summary_history.CONCEPT_NAME || 'DHP ON ' || OWNER_SA || '.SADH_' || reg_summary_history.CONCEPT_NAME);
              DBMS_OUTPUT.put_line('(');
              FOR indx IN lista_pk.FIRST .. lista_pk.LAST
              LOOP
                IF indx = lista_pk.LAST THEN
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
                ELSE
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
                END IF;
              END LOOP;
              DBMS_OUTPUT.put_line('GLOBAL;');
              DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SADH_'  || reg_summary_history.CONCEPT_NAME || ' ADD CONSTRAINT ' || reg_summary_history.CONCEPT_NAME || 'DHP PRIMARY KEY (');
              FOR indx IN lista_pk.FIRST .. lista_pk.LAST
              LOOP
                IF indx = lista_pk.LAST THEN
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
                ELSE
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
                END IF;
              END LOOP;
              DBMS_OUTPUT.put_line('USING INDEX ' || reg_summary_history.CONCEPT_NAME || 'DHP;');
            ELSE
              /* Podemos crear un Indice PK local */
              DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX ' || reg_summary_history.CONCEPT_NAME || 'DHP ON ' || OWNER_SA || '.SADH_' || reg_summary_history.CONCEPT_NAME);
              DBMS_OUTPUT.put_line('(');
              FOR indx IN lista_pk.FIRST .. lista_pk.LAST
              LOOP
                IF indx = lista_pk.LAST THEN
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
                ELSE
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
                END IF;
              END LOOP;
              DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
              DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SADH_'  || reg_summary_history.CONCEPT_NAME || ' ADD CONSTRAINT ' || reg_summary_history.CONCEPT_NAME || 'DHP PRIMARY KEY (');
              FOR indx IN lista_pk.FIRST .. lista_pk.LAST
              LOOP
                IF indx = lista_pk.LAST THEN
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
                ELSE
                  DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
                END IF;
              END LOOP;
              DBMS_OUTPUT.put_line('USING INDEX ' || reg_summary_history.CONCEPT_NAME || 'DHP;');
            END IF;
          ELSE
            if (lista_pk.COUNT = 0 and lista_par.COUNT>0) then
              /* Tenemos el caso de que la tabla no tiene PK pero si esta particionada */
              /* Creamos un indice local por el campo de particionado */
              DBMS_OUTPUT.put_line('CREATE INDEX '  || reg_summary_history.CONCEPT_NAME || 'DHL ON ' || OWNER_SA || '.SADH_' || reg_summary_history.CONCEPT_NAME || ' (');
              FOR indy IN lista_par.FIRST .. lista_par.LAST
              LOOP
                  IF indy = lista_par.LAST THEN
                    DBMS_OUTPUT.put_line(lista_par (indy) || ') ');
                  ELSE
                    DBMS_OUTPUT.put_line(lista_par (indy) || ',');
                  END IF;
              END LOOP;
              DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
            end if;
          END IF;
          DBMS_OUTPUT.put_line('');
          lista_pk.DELETE;      /* Borramos los elementos de la lista */
          lista_par.DELETE;
        end if;   /* Fin if v_encontrado = 'Y' then */
      end if;     /* Fin if Existen tablas de inyeccion */
      /*********************************************/
      /* (20151118) Angel Ruiz. FIN NF */      
      /*********************************************/
  END LOOP;
  CLOSE dtd_interfaz_summary_history;

  /* (20150717) ANGEL RUIZ. FIN.*/
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
END;

