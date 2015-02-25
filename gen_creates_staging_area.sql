  DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      CONCEPT_NAME,
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED
    FROM MTDT_INTERFACE_SUMMARY;
  
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
      PARTITIONED,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      CONCEPT_NAME = concep_name_in and
      SOURCE = source_in
      order by POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col INTEGER;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_columns_partitioned  IS TABLE OF VARCHAR(30);
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      tipo_col                                      VARCHAR(70);
      lista_par                                     list_columns_partitioned := list_columns_partitioned();
      lista_campos_particion            VARCHAR(250);
      no_encontrado                          VARCHAR(1);
      subset                                         VARCHAR(1);
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      TABLESPACE_SA                  VARCHAR2(60);
      


BEGIN
  /* (20150119) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  /* (20150119) FIN*/

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
            tipo_col := 'NUMBER (15, 3)';
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
            tipo_col := 'NUMBER (15, 3)';
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
        IF reg_datail.KEY = 'S' then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := reg_datail.COLUMNA;
        END IF;
        IF reg_datail.PARTITIONED = 'S' then
          lista_par.EXTEND;
          lista_par(lista_par.LAST) := reg_datail.COLUMNA;
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      --if (reg_summary.DELAYED = 'S') then
      --  /* Si la tabla admite retrasados, la particiono por la fecha de datos*/
      --  IF primera_col = 1 THEN /* Si es primera columna */
      --    DBMS_OUTPUT.put_line('CVE_DIA          NUMBER(8)');  /* anyadimos un campo que es fecha de datos */
      --    primera_col := 0;
      --  ELSE
      --    DBMS_OUTPUT.put_line(',' || 'CVE_DIA          NUMBER(8)');   /* anyadimos un campo que es fecha de datos */
      --  END IF;
      --end if;
      --IF lista_pk.COUNT > 0 THEN  /* Si hay clave primaria, a la clave primaria le meto el campo FCH_REGISTRO como clave tb */
      --    lista_pk.EXTEND;
      --    lista_pk(lista_pk.LAST) := 'FCH_REGISTRO';
      --END IF;
      IF (lista_pk.COUNT > 0 and lista_par .COUNT = 0) THEN
        /* tenemos una tabla normal no particionada */
        DBMS_OUTPUT.put_line(',' || 'CONSTRAINT "SA_' || reg_summary.CONCEPT_NAME || '_P"' || ' PRIMARY KEY (');
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
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate-1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+2,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(sysdate+3,'YYYYMMDD') || ''',''YYYYMMDD''))');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')-2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')-1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')-1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD'),'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD'),'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')+1,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')+1,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')+2,'YYYYMMDD') || ''',''YYYYMMDD'')),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')+2,'YYYYMMDD') || ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(to_date('20150113','YYYYMMDD')+3,'YYYYMMDD') || ''',''YYYYMMDD''))');   
        DBMS_OUTPUT.put_line(')');
      end if;
      DBMS_OUTPUT.put_line(';'); /* FIN CREATE */
      /* COMIENZO LA GESTION DE LA CREACION DE INDICES LOCALES O GLOBALES */
      --DBMS_OUTPUT.put_line('El valor de lista_pk es: ' || lista_pk.COUNT);
      --DBMS_OUTPUT.put_line('El valor de lista_par es: ' || lista_par.COUNT);
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
          DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX SA_' || reg_summary.CONCEPT_NAME || '_P ON ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
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
          DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SA_'  || reg_summary.CONCEPT_NAME || ' ADD CONSTRAINT SA_' || reg_summary.CONCEPT_NAME || '_P PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('USING INDEX SA_' || reg_summary.CONCEPT_NAME || '_P;');
        ELSE
          /* Podemos crear un Indice PK local */
          DBMS_OUTPUT.put_line('CREATE UNIQUE INDEX SA_' || reg_summary.CONCEPT_NAME || '_P ON ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
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
          DBMS_OUTPUT.put_line('ALTER TABLE ' || OWNER_SA || '.SA_'  || reg_summary.CONCEPT_NAME || ' ADD CONSTRAINT SA_' || reg_summary.CONCEPT_NAME || '_P PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('USING INDEX SA_' || reg_summary.CONCEPT_NAME || '_P;');
        END IF;
      ELSE
        if (lista_pk.COUNT = 0 and lista_par .COUNT>0) then
          /* Tenemos el caso de que la tabla no tiene PK pero si esta particionada */
          /* Creamos un indice local por el campo de particionado */
          DBMS_OUTPUT.put_line('CREATE INDEX SA_'  || reg_summary.CONCEPT_NAME || '_L ON SA_' || reg_summary.CONCEPT_NAME || ' (');
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
      --if (reg_summary.DELAYED = 'S') then
        --DBMS_OUTPUT.put_line(')'); /* Parentesis final del create*/ 
        --DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_DIA)');   
        --DBMS_OUTPUT.put_line('(');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-3,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-3,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-2,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-1,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+1,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+2,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || reg_summary.CONCEPT_NAME ||'_' || TO_CHAR(sysdate+3,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+3,'YYYYMMDD') || ')');   
        --DBMS_OUTPUT.put_line(');');
        --DBMS_OUTPUT.put_line('CREATE INDEX idx_SA_' || reg_summary.CONCEPT_NAME || ' on SA_' || reg_summary.CONCEPT_NAME || '(CVE_DIA) LOCAL;');
      --else
        --DBMS_OUTPUT.put_line(') TABLESPACE DWTBSP_D_MVNO_SA;');
      --end if;
      DBMS_OUTPUT.put_line('');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_par.DELETE;
  END LOOP;
  CLOSE dtd_interfaz_summary;
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
END;

