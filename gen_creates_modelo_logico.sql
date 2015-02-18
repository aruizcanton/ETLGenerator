DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      DISTINCT
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI"
    FROM METADATO.MTDT_MODELO_LOGICO
    WHERE CI <> 'P';    /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */

  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      CI,
      TRIM(VDEFAULT) "VDEFAULT",
      TRIM(TABLESPACE) "TABLESPACE"
    FROM METADATO.MTDT_MODELO_LOGICO
    WHERE
      TRIM(TABLE_NAME) = table_name_in;

  r_mtdt_modelo_logico_TABLA                                          c_mtdt_modelo_logico_TABLA%rowtype;
  r_mtdt_modelo_logico_COLUMNA                                    c_mtdt_modelo_logico_COLUMNA%rowtype;
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  primera_col INTEGER;
  cadena_values VARCHAR2(500);
  concept_name VARCHAR2 (30);
  nombre_tabla_reducido VARCHAR2(30);
  pos_abre_paren PLS_integer;
  pos_cierra_paren PLS_integer;
  longitud_des varchar2(5);
  longitud_des_numerico PLS_integer;
  
  
BEGIN
  SELECT COUNT(*) INTO num_filas FROM METADATO.MTDT_MODELO_LOGICO;
  /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_MODELO_LOGICO  */
  IF num_filas > 0 THEN
    /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
    DBMS_OUTPUT.put_line('set echo on;');
    DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
    OPEN c_mtdt_modelo_logico_TABLA;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH c_mtdt_modelo_logico_TABLA
      INTO r_mtdt_modelo_logico_TABLA;
      EXIT WHEN c_mtdt_modelo_logico_TABLA%NOTFOUND;
      nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
      DBMS_OUTPUT.put_line('DROP TABLE APP_MVNODM.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('CREATE TABLE APP_MVNODM.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      DBMS_OUTPUT.put_line('(');
      concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
      OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      primera_col := 1;
      LOOP
        FETCH c_mtdt_modelo_logico_COLUMNA
        INTO r_mtdt_modelo_logico_COLUMNA;
        EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
        /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
        IF primera_col = 1 THEN /* Si es primera columna */
          IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
            IF (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
              DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
            ELSIF (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
              DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
            ELSE  /* se trata de Fecha  */
              DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
            END IF;
          ELSE
            DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
          END IF;
            primera_col := 0;
        ELSE  /* si no es primera columna */
          IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
            IF (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
              DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
            ELSIF (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
              DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
            ELSE  /* se trata de Fecha  */
              DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
            END IF;
          ELSE
            DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
          END IF;
        END IF;
        IF r_mtdt_modelo_logico_COLUMNA.PK = 'S' then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
        END IF;
      END LOOP; 
      CLOSE c_mtdt_modelo_logico_COLUMNA;
      IF lista_pk.COUNT > 0 THEN
        DBMS_OUTPUT.put_line(',' || 'CONSTRAINT "' || nombre_tabla_reducido || '_P"' || ' PRIMARY KEY (');
        FOR indx IN lista_pk.FIRST .. lista_pk.LAST
        LOOP
          IF indx = lista_pk.LAST THEN
            DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
          ELSE
            DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
          END IF;
        END LOOP;
      END IF;
      DBMS_OUTPUT.put_line(')');  /* Parentesis final del create*/
      --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE);
      if (regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME,'^DMF_',1,'i') >0)  then  /* Se trata de una tabla de HECHOS  */
        --  /* Hay que particonarla */
        DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE);
        --  if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_PARQUE_MVNO') then
        --    DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_PARQUE');
        --    end if;
        --  if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_RECARGAS_MVNO') then
        --    DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_AJUSTES');
        --  end if;
        --  if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFD_CU_MVNO') then
        --    DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFD');
        --  end if;
        --  if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFE_CU_MVNO') then
        --    DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFE');
        --  end if;
        --  if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFV_CU_MVNO') then
        --    DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFV');
        --  end if;
        --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE);
        DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_DIA)');
        DBMS_OUTPUT.put_line('(');
        --DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(sysdate-2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-1,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(sysdate-1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(sysdate,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+1,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(sysdate+1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+2,'YYYYMMDD') || '),');   
        --DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(sysdate+2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate+3,'YYYYMMDD') || ')');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')-2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(to_date('20150113','YYYYMMDD')-1,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')-1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(to_date('20150113','YYYYMMDD'),'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD'),'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(to_date('20150113','YYYYMMDD')+1,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')+1,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(to_date('20150113','YYYYMMDD')+2,'YYYYMMDD') || '),');   
        DBMS_OUTPUT.put_line('PARTITION PA_' || nombre_tabla_reducido ||'_' || TO_CHAR(to_date('20150113','YYYYMMDD')+2,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(to_date('20150113','YYYYMMDD')+3,'YYYYMMDD') || ')');   
        DBMS_OUTPUT.put_line(');');
      else
        --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE || ';');
        --DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_DIM' || ';');
        DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
      end if;
      --DBMS_OUTPUT.put_line(';');
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      DBMS_OUTPUT.put_line('');
      /***************************/
      /* Ahora creamos la tabla TEMPORAL pero solo para aquellas que no se van a cargar como carga inicial */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        /* Aquellas que no tienen ningún tipo de carga inicial */
        DBMS_OUTPUT.put_line('DROP TABLE APP_MVNODM.T_' || nombre_tabla_reducido || ' CASCADE CONSTRAINTS;');
        DBMS_OUTPUT.put_line('CREATE TABLE APP_MVNODM.T_' || nombre_tabla_reducido);
        DBMS_OUTPUT.put_line('(');
        concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
        OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
        primera_col := 1;
        LOOP
          FETCH c_mtdt_modelo_logico_COLUMNA
          INTO r_mtdt_modelo_logico_COLUMNA;
          EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
          /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
          IF primera_col = 1 THEN /* Si es primera columna */
              DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
              primera_col := 0;
          ELSE  /* si no es primera columna */
              DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
          END IF;
          IF r_mtdt_modelo_logico_COLUMNA.PK = 'S' then
            lista_pk.EXTEND;
            lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          END IF;
        END LOOP; 
        CLOSE c_mtdt_modelo_logico_COLUMNA;
        IF lista_pk.COUNT > 0 THEN
          DBMS_OUTPUT.put_line(',' || 'CONSTRAINT "T_' || nombre_tabla_reducido || '_P"' || ' PRIMARY KEY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
        END IF;
        DBMS_OUTPUT.put_line(')');  /* Parentesis final del create */
        DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
        --if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_PARQUE_MVNO') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_PARQUE;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_RECARGAS_MVNO') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_AJUSTES;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFD_CU_MVNO;') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFD;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFE_CU_MVNO') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFE;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFV_CU_MVNO;') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFV;');
        --else
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_DIM' || ';');
        --end if;
      end if;      
      --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE || ';'); 
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      DBMS_OUTPUT.put_line('');
      
      /****************************************************************************************************/
      /* Viene la parte donde se generan los INSERTS por defecto y la SECUENCIA */
      /****************************************************************************************************/
      if (r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'I') then
        /* Generamos los inserts para aquellas tablas que no son de carga inicial */
        if (regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME,'^DMD_',1,'i') >0 or regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME,'^DMT_',1,'i') >0) then
          /* Solo si se trata de una dimension generamos los inserts por defecto y la secuencia */
          if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
            DBMS_OUTPUT.put_line('DROP SEQUENCE APP_MVNODM.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5) || ';');
            DBMS_OUTPUT.put_line('CREATE SEQUENCE APP_MVNODM.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5));
            DBMS_OUTPUT.put_line('MINVALUE 1 START WITH 1 INCREMENT BY 1;');
          end if;
          DBMS_OUTPUT.put_line('');        
          /* Primero el INSERT "NO APLICA" */
          DBMS_OUTPUT.put_line('INSERT INTO APP_MVNODM.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^CVE_',1,'i') >0 THEN
                    cadena_values := '-1';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := '-1';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := 'NULL';
                      else
                        cadena_values := '''NA#''';
                      end if;
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := '''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NA#''';
                    else
                      cadena_values := 'NULL';
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_REGISTRO',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_MODIFICACION',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  ELSE
                    cadena_values := 'NULL';
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
                DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^CVE_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', -1';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0 ) then
                      cadena_values := cadena_values || ', -1';
                    else
                        if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := cadena_values || ', NULL';
                        else
                          cadena_values := cadena_values || ', ''NA#''';
                        end if;
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := cadena_values || ', ''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NA#''';
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_REGISTRO',1,'i') >0 THEN
                      cadena_values := cadena_values || ', sysdate';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_MODIFICACION',1,'i') >0 THEN
                      cadena_values := cadena_values || ', sysdate';
                  ELSE
                    cadena_values := cadena_values || ', NULL';
                END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente insert "GENERICO" */
          DBMS_OUTPUT.put_line('INSERT INTO APP_MVNODM.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^CVE_',1,'i') >0 THEN
                    cadena_values := '-2';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := '-2';
                    else
                        if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := 'NULL';
                        else
                          cadena_values := '''GE#''';
                        end if;
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := '''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''GE#''';
                    else
                      cadena_values := 'NULL';
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_REGISTRO',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_MODIFICACION',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  ELSE
                    cadena_values := 'NULL';
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
                DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^CVE_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', -2';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := cadena_values || ', -2';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := cadena_values || ', NULL';
                      else
                        cadena_values := cadena_values || ', ''GE#''';
                      end if;
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := cadena_values || ', ''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''GE#''';
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_REGISTRO',1,'i') >0 THEN
                      cadena_values := cadena_values || ', sysdate';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_MODIFICACION',1,'i') >0 THEN
                      cadena_values := cadena_values || ', sysdate';
                  ELSE
                    cadena_values := cadena_values || ', NULL';
                END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente INSERT "NO INFORMADO" */
          DBMS_OUTPUT.put_line('INSERT INTO APP_MVNODM.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^CVE_',1,'i') >0 THEN
                    cadena_values := '-3';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values :=  '-3';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := 'NULL';
                      else
                        cadena_values := '''NI#''';
                      end if;
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := '''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NI#''';
                    else
                      cadena_values := 'NULL';
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_REGISTRO',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_MODIFICACION',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  ELSE
                    cadena_values := 'NULL';
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
                DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^CVE_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', -3';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := cadena_values || ', -3';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := cadena_values || ', NULL';
                      else                  
                        cadena_values := cadena_values || ', ''NI#''';
                      end if;
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := cadena_values || ', ''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NI#''';
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_REGISTRO',1,'i') >0 THEN
                      cadena_values := cadena_values || ', sysdate';
                  WHEN regexp_count(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME,'^FCH_MODIFICACION',1,'i') >0 THEN
                      cadena_values := cadena_values || ', sysdate';
                  ELSE
                    cadena_values := cadena_values || ', NULL';
                END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          DBMS_OUTPUT.put_line('commit;');
          DBMS_OUTPUT.put_line('');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
        end if;
      end if;
      /**********************/
      /**********************/
      
      
    END LOOP;
    CLOSE c_mtdt_modelo_logico_TABLA;
  END IF;
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
END;

