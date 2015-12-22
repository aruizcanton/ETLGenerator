  DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED
  FROM MTDT_INTERFACE_SUMMARY;

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
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
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
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' CASCADE CONSTRAINTS;');
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
      IF (lista_pk.COUNT > 0 and lista_par .COUNT = 0) THEN
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
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+50,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+51,'YYYYMMDD') || ''',''YYYYMMDD''))');   
        
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+2,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+3,'YYYYMMDD') || ''',''YYYYMMDD''))');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')-2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')-1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')-1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD'),'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD'),'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')+1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')+1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')+2,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')+2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')+3,'YYYYMMDD') || ''',''YYYYMMDD''))');   
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
          DBMS_OUTPUT.put_line(', BAN_DESCARTE' ||  '          '  || 'VARCHAR2(1)');
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
        DBMS_OUTPUT.put_line(', CVE_DIA          NUMBER(8)'); /* Añado una columna de particionado */
        lista_par.EXTEND;
        lista_par(lista_par.LAST) := 'CVE_DIA'; /* La añado a la lista de campos por los que particionar mi tabla historica */
        if (lista_pk.count > 0) then
          /* La tabla tiene clave primaria, asi añadimos el campo de particionado a la clave primaria para que se pueda hacer un indice local */
          lista_pk.extend;
          lista_pk(lista_pk.LAST) := 'CVE_DIA';     /* La añado a la lista de PKs de mi tabla historica */
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
        DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+50,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+51,'YYYYMMDD') || ')');   
        
        DBMS_OUTPUT.put_line(')');
      end if;
      DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
      /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
      IF (lista_pk.COUNT > 0 and lista_par .COUNT > 0) THEN 
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
          DBMS_OUTPUT.put_line(', BAN_DESCARTE' ||  '          '  || 'VARCHAR2(1)');
          /* Ahora miramos si he de crear un campo de particionado para la tabla historica */
          /* o por el contrario la tabla de STAGING ya tenia un campo de particionado */
          if (lista_par.count = 0) then
            /* La tabla de STAGING no esta particionada aunque su historica si debe estarlo*/
            DBMS_OUTPUT.put_line(', CVE_DIA          NUMBER(8)'); /* Añado una columna de particionado */
            lista_par.EXTEND;
            lista_par(lista_par.LAST) := 'CVE_DIA'; /* La añado a la lista de campos por los que particionar mi tabla historica */
            if (lista_pk.count > 0) then
              /* La tabla tiene clave primaria, asi añadimos el campo de particionado a la clave primaria para que se pueda hacer un indice local */
              lista_pk.extend;
              lista_pk(lista_pk.LAST) := 'CVE_DIA';     /* La añado a la lista de PKs de mi tabla historica */
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
              v_nombre_particion := 'SAF' || reg_summary_history.CONCEPT_NAME;
            else
              v_nombre_particion := reg_summary_history.CONCEPT_NAME;
            end if;
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
            DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate+15,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+16,'YYYYMMDD') || ')');   
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

