DECLARE
  /* (20150907) Angel Ruiz . NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */

    /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI",
      TRIM(PARTICIONADO) "PARTICIONADO"
    FROM MTDT_MODELO_SUMMARY
    WHERE TRIM(CI) <> 'P';    /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */
    
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      TRIM(NULABLE) "NULABLE",
      TRIM(VDEFAULT) "VDEFAULT",
      TRIM(INDICE) "INDICE"
    FROM MTDT_MODELO_DETAIL
    WHERE
      trim(TABLE_NAME) = table_name_in
    ORDER BY POSITION;
  /* (20150907) Angel Ruiz . FIN NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */

  r_mtdt_modelo_logico_TABLA                                          c_mtdt_modelo_logico_TABLA%rowtype;
  r_mtdt_modelo_logico_COLUMNA                                    c_mtdt_modelo_logico_COLUMNA%rowtype;
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  TYPE list_columns_indice  IS TABLE OF VARCHAR(30);
  TYPE list_columns_par  IS TABLE OF VARCHAR(30);
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  lista_ind                                      list_columns_indice := list_columns_indice (); 
  lista_par                                      list_columns_par := list_columns_par (); 
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  primera_col INTEGER;
  cadena_values VARCHAR2(3000);
  concept_name VARCHAR2 (30);
  nombre_tabla_reducido VARCHAR2(30);
  v_nombre_particion VARCHAR2(30);
  pos_abre_paren PLS_integer;
  pos_cierra_paren PLS_integer;
  longitud_des varchar2(5);
  longitud_des_numerico PLS_integer;
  v_tipo_particionado VARCHAR2(10);
  subset                                         VARCHAR(1);
  no_encontrado                          VARCHAR(1);

  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  TABLESPACE_DIM                VARCHAR2(60);
  
BEGIN

  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO TABLESPACE_DIM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_DIM';
  
  /* (20141219) FIN*/

  SELECT COUNT(*) INTO num_filas FROM MTDT_MODELO_SUMMARY;
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
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      DBMS_OUTPUT.put_line('(');
      concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
      OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      primera_col := 1;
      v_tipo_particionado := 'S';  /* (20150821) Angel Ruiz. Por defecto la tabla no estara particionada */
      LOOP
        FETCH c_mtdt_modelo_logico_COLUMNA
        INTO r_mtdt_modelo_logico_COLUMNA;
        EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
        /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
        IF primera_col = 1 THEN /* Si es primera columna */
          IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
            CASE
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                end if;
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                end if;
              ELSE  /* se trata de Fecha  */
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                end if;
            END CASE;
          ELSE
            if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
              DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' NOT NULL');
            else
              DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
            end if;
          END IF;
          primera_col := 0;
        ELSE  /* si no es primera columna */
          IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
            CASE 
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                end if;
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                end if;
              ELSE  /* se trata de Fecha  */
                if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                else
                  DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                end if;
              END CASE;
          ELSE
            if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
              DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' NOT NULL');
            else
              DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
            end if;
          END IF;
        END IF;
        IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' then
          lista_pk.EXTEND;
          lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
        END IF;
        /* (20160324) Angel Ruiz. NF: Indices en las tablas de modelo*/
        IF upper(trim(r_mtdt_modelo_logico_COLUMNA.INDICE)) = 'S' then
          lista_ind.EXTEND;
          lista_ind(lista_ind.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
        END IF;
        /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas de modelo*/

        /* (20150821) ANGEL RUIZ. FUNCIONALIDAD PARA PARTICIONADO */
        if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??F_',1,'i') >0 AND 
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_DIA') then 
          /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO DIARIO */
          v_tipo_particionado := 'D';   /* Particionado Diario */
          /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo*/
          lista_par.extend;
          lista_par(lista_par.last) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas del modelo*/
        end if;
        /* Gestionamos el posible particionado de la tabla */
        if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4) ,'??F_',1,'i') >0 AND
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_MES') then 
          /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO MENSUAL */
          if (r_mtdt_modelo_logico_TABLA.PARTICIONADO = 'M24') then
            /* (20150918) Angel Ruiz. NF: Se trata del particionado para BSC. Mensual pero 24 Particiones fijas.*/
            /* La filosofia cambia */
              v_tipo_particionado := 'M24';   /* Particionado Mensual */
          else
            v_tipo_particionado := 'M';   /* Particionado Mensual, aunque para una tabla de Agregados*/
          end if;
          /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo*/
          lista_par.extend;
          lista_par(lista_par.last) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas del modelo*/
        end if;
        if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??A_',1,'i') >0 AND
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_MES') then
          /* SE TRATA DE UNA TABLA DE AGREGADOS CON PARTICIONAMIENTO POR MES */
          v_tipo_particionado := 'M';   /* Particionado Mensual, aunque para una tabla de Agregados*/
          /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo*/
          lista_par.extend;
          lista_par(lista_par.last) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas del modelo*/
        end if;
        /* (20150821) ANGEL RUIZ. FIN FUNCIONALIDAD PARA PARTICIONADO */
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
      if (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4) ,'??F_',1,'i') >0)  then  /* Se trata de una tabla de HECHOS  */
        --  /* Hay que particonarla */
        if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
          DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE);
        end if;
        if (v_tipo_particionado = 'D') then
          /* Se trata de un particionado diario */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_DIA)');
          DBMS_OUTPUT.put_line('(');
          /* (20150224) Angel Ruiz. Al generar le modelo para DIST me da un error por nombre demasiado largo */
          if (length(nombre_tabla_reducido) <= 18) then
            v_nombre_particion := 'PA_' || nombre_tabla_reducido;
          else
            v_nombre_particion := nombre_tabla_reducido;
          end if;
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-208,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-207,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-207,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-206,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-206,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-205,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-205,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-204,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-204,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-203,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-203,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-202,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-202,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-201,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-201,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-200,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-200,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-199,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-199,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-198,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-198,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-197,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-197,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-196,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-196,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-195,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-195,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-194,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-194,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-193,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-193,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-192,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-192,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-191,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-191,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-190,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-190,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-189,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-189,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-188,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-188,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-187,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-187,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-186,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-186,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-185,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-185,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-184,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-184,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-183,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-183,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-182,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-182,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-181,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-181,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-180,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-180,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-179,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-179,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-178,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-178,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-177,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-177,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-176,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-176,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-175,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-175,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-174,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-174,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-173,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-173,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-172,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-172,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-171,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-171,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-170,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-170,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-169,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-169,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-168,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-168,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-167,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-167,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-166,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-166,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-165,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-165,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-164,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-164,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-163,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-163,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-162,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-162,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-161,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-161,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-160,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-160,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-159,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-159,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-158,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-158,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-157,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-157,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-156,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-156,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-155,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-155,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-154,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-154,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-153,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-153,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-152,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-152,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-151,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-151,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-150,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-150,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-149,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-149,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-148,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-148,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-147,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-147,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-146,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-146,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-145,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-145,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-144,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-144,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-143,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-143,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-142,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-142,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-141,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-141,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-140,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-140,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-139,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-139,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-138,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-138,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-137,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-137,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-136,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-136,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-135,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-135,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-134,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-134,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-133,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-133,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-132,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-132,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-131,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-131,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-130,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-130,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-129,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-129,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-128,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-128,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-127,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-127,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-126,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-126,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-125,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-125,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-124,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-124,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-123,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-123,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-122,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-122,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-121,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-121,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-120,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-120,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-119,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-119,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-118,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-118,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-117,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-117,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-116,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-116,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-115,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-115,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-114,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-114,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-113,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-113,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-112,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-112,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-111,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-111,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-110,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-110,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-109,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-109,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-108,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-108,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-107,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-107,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-106,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-106,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-105,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-105,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-104,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-104,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-103,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-103,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-102,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-102,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-101,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-101,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-100,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-100,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-99,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-99,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-98,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-98,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-97,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-97,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-96,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-96,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-95,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-95,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-94,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-94,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-93,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-93,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-92,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-92,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-91,'YYYYMMDD') || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate-91,'YYYYMMDD') || ' VALUES LESS THAN (' || TO_CHAR(sysdate-90,'YYYYMMDD') || '),');   
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
          DBMS_OUTPUT.put_line(');');
        elsif (v_tipo_particionado = 'M') then
          /* Se trata de un particionado Mensual */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_MES)');
          DBMS_OUTPUT.put_line('(');
          /* (20150224) Angel Ruiz. Al generar le modelo para DIST me da un error por nombre demasiado largo */
          if (length(nombre_tabla_reducido) <= 18) then
            v_nombre_particion := 'PA_' || nombre_tabla_reducido;
          else
            v_nombre_particion := nombre_tabla_reducido;
          end if;
          /********/
          /* (20150518) Angel Ruiz */
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate,-3),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,-2),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate,-2),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,-1),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate,-1),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(sysdate,'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate,'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,1),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate, 1),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,2),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate, 2),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,3),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate, 3),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,4),'YYYYMM')) || ')');   
          DBMS_OUTPUT.put_line(');');
        elsif (v_tipo_particionado = 'M24') then
          /* (20150918) Angel Ruiz. N.F.: Se trata de implementar el particionado para BSC donde hay 24 particiones siempre */
          /* Las particiones se crean una vez y asi permanecen ya que el espacio de analisis se extiende 24 meses */
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_MES)');
          DBMS_OUTPUT.put_line('(');
          /* (20150224) Angel Ruiz. Al generar le modelo para DIST me da un error por nombre demasiado largo */
          if (length(nombre_tabla_reducido) <= 18) then
            v_nombre_particion := 'PA_' || nombre_tabla_reducido;
          else
            v_nombre_particion := nombre_tabla_reducido;
          end if;
          /* (20150918) Angel Ruiz. Fin N.F.: Se trata de implementar el particionado para BSC donde hay 24 particiones siempre */
          /* Se cra la primera particion de analisis solamente. El resto se crea en los procesos de carga */
          /* La primera particion coincide con Enero del año anterior al sysdate */
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(TO_NUMBER(TO_CHAR(sysdate,'YYYY')) -1) || '01' || ' VALUES LESS THAN (' || TO_CHAR(TO_NUMBER(TO_CHAR(sysdate,'YYYY')) -1) || '02' || ')');   
          DBMS_OUTPUT.put_line(');');
          /* (20150918) Angel Ruiz. Fin N.F*/
        end if;
      elsif (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), '??A_',1,'i') >0)  then  /* Se trata de una tabla de HECHOS AGREGADOS  */
        if (v_tipo_particionado = 'M') then
          --  /* Hay que particonarla */
          if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
            DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE);
          end if;
          
          DBMS_OUTPUT.put_line('PARTITION BY RANGE (CVE_MES)');
          DBMS_OUTPUT.put_line('(');
          /* (20150224) Angel Ruiz. Al generar le modelo para DIST me da un error por nombre demasiado largo */
          if (length(nombre_tabla_reducido) <= 18) then
            v_nombre_particion := 'PA_' || nombre_tabla_reducido;
          else
            v_nombre_particion := nombre_tabla_reducido;
          end if;
          /********/
          /* (20150518) Angel Ruiz */
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate,-2),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,-1),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate,-1),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(sysdate,'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(sysdate,'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,1),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate, 1),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,2),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate, 2),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,3),'YYYYMM')) || '),');   
          DBMS_OUTPUT.put_line('PARTITION ' || v_nombre_particion ||'_' || TO_CHAR(add_months(sysdate, 3),'YYYYMM') || ' VALUES LESS THAN (' || TO_NUMBER(TO_CHAR(add_months(sysdate,4),'YYYYMM')) || ')');   
          DBMS_OUTPUT.put_line(');');
        end if;
      else
        --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE || ';');
        --DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_DIM' || ';');
        --DBMS_OUTPUT.put_line ('-- #' ||  r_mtdt_modelo_logico_TABLA.TABLE_NAME || '#');
        --if (TRIM(r_mtdt_modelo_logico_TABLA.table_name) = 'DMD_CAUSA_LLAMADA') then
          --DBMS_OUTPUT.put_line ('LA TABLA QUE QUIERO INVESTIGAR: #' ||  r_mtdt_modelo_logico_TABLA.TABLE_NAME || '#');
          --DBMS_OUTPUT.put_line ('El valor de TABLESPACE ES: #' ||  r_mtdt_modelo_logico_TABLA.TABLESPACE || '#');
        --end if;
        if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
          DBMS_OUTPUT.put_line ('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
        else
          DBMS_OUTPUT.put_line (';');
        end if;
      end if;
      
      --DBMS_OUTPUT.put_line(';');
      /* (20160324. Angel Ruiz. NF: Gestion de indices */
      if (lista_ind.COUNT > 0) THEN
        /* La tabla tiene indices */
        if (v_tipo_particionado = 'D' or v_tipo_particionado = 'M') then
          /* vemos la posibilidad de crear un indice LOCAL a cada particion */
          /* Para ello la clave de particionado debe formar parte del indice */
          /* por lo que hemos de buscar si la clave de particionado está en el indice */
          no_encontrado := 'N'; /* por defecto supongo que todos los campos de particionado forman parte del indice, de ahi no_encontrado = N */
          FOR indy IN lista_par.FIRST .. lista_par.LAST
          LOOP
            /* Para cada uno de los campos de particionado. Normalmente es uno*/
            /* busco si estan en los campos del indice */
            subset := 'N';
            FOR indx IN lista_ind.FIRST .. lista_ind.LAST
            LOOP
              IF (lista_par(indy) = lista_ind(indx)) THEN
                subset := 'Y';
              END IF;
            END LOOP;
            if (subset = 'N') then
              /* No he encontrado el campo de particionado en los campos que forman el indice */
              no_encontrado := 'Y';
            end if;
          END LOOP;
          if (no_encontrado = 'Y') then
            /* Ocurre que hay campos de particionado que no formal parte del indice por lo que no se puede crear un indice local*/
            DBMS_OUTPUT.put_line('CREATE INDEX ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || '_I ON ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
            DBMS_OUTPUT.put_line('(');
            FOR indx IN lista_ind.FIRST .. lista_ind.LAST
            LOOP
              IF indx = lista_ind.LAST THEN
                DBMS_OUTPUT.put_line(lista_ind (indx) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(lista_ind (indx) || ',');
              END IF;
            END LOOP;
            DBMS_OUTPUT.put_line('NOLOGGING GLOBAL;');
          else
            /* Podemos crear un Indice PK local */
            DBMS_OUTPUT.put_line('CREATE INDEX ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || '_I ON ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
            DBMS_OUTPUT.put_line('(');
            FOR indx IN lista_ind.FIRST .. lista_ind.LAST
            LOOP
              IF indx = lista_ind.LAST THEN
                DBMS_OUTPUT.put_line(lista_ind (indx) || ') ');
              ELSE
                DBMS_OUTPUT.put_line(lista_ind (indx) || ',');
              END IF;
            END LOOP;
            DBMS_OUTPUT.put_line('NOLOGGING LOCAL;');
          end if;
        end if;
      end if;
      /* (20160324. Angel Ruiz. Fin NF: Gestion de indices */
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_ind.delete;
      lista_par.delete;
      DBMS_OUTPUT.put_line('');
      /***************************/
      /* AHORA CREAMOS LA TABLA TEMPORAL PERO SOLO PARA AQUELLAS QUE NO SE VAN A CARGAR COMO CARGA INICIAL */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        /* Aquellas que no tienen ningún tipo de carga inicial */
        --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ' CASCADE CONSTRAINTS;');
        DBMS_OUTPUT.put_line('CREATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
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
              CASE
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                  end if;
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                  end if;
                ELSE  /* se trata de Fecha  */
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                  end if;
              END CASE;
            ELSE
              if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' NOT NULL');
              else
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
              end if;
            END IF;
            primera_col := 0;
          ELSE  /* si no es primera columna */
            IF (r_mtdt_modelo_logico_COLUMNA.VDEFAULT IS NOT NULL) THEN
              CASE 
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                  end if;
                WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''' || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ''' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || '''');
                  end if;
                ELSE  /* se trata de Fecha  */
                  if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT || ' NOT NULL');
                  else
                    DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' DEFAULT ' || r_mtdt_modelo_logico_COLUMNA.VDEFAULT);
                  end if;
                END CASE;
            ELSE
              if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE || ' NOT NULL');
              else
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
              end if;
            END IF;
          END IF;
          IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' then
            lista_pk.EXTEND;
            lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          END IF;
          /* (20160324) Angel Ruiz. NF: INdices en el modelo */
          IF upper(trim(r_mtdt_modelo_logico_COLUMNA.INDICE)) = 'S' then
            lista_ind.EXTEND;
            lista_ind(lista_ind.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
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
        if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
          DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
        else
          DBMS_OUTPUT.put_line(';');
        end if;
        /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo */
        if (lista_ind.COUNT > 0) then
          DBMS_OUTPUT.put_line('CREATE INDEX ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_I ON ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
          DBMS_OUTPUT.put_line('(');
          FOR indx IN lista_ind.FIRST .. lista_ind.LAST
          LOOP
            IF indx = lista_ind.LAST THEN
              DBMS_OUTPUT.put_line(lista_ind (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_ind (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('NOLOGGING;');
        end if;
        /* (20160324) Angel Ruiz. Fin NF:Indices en las tablas del modelo */
        
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
      lista_ind.DELETE;
      DBMS_OUTPUT.put_line('');
      
      /****************************************************************************************************/
      /* Viene la parte donde se generan los INSERTS por defecto y la SECUENCIA */
      /****************************************************************************************************/
      /* (20150826) ANGEL RUIZ. Cambio la creacion de la secuencia para que se cree secuencia para todas las tablas DIMENSIONES o HECHOS */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        --DBMS_OUTPUT.put_line('DROP SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5) || ';');
        DBMS_OUTPUT.put_line('CREATE SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5));
        DBMS_OUTPUT.put_line('MINVALUE 1 START WITH 1 INCREMENT BY 1;');
        DBMS_OUTPUT.put_line('');        
      end if;
      
      if (r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'I') then
        /* Generamos los inserts para aquellas tablas que no son de carga inicial */
        if (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4) ,'??D_',1,'i') >0 or regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), 'DMT_',1,'i') >0 
        or regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), 'DWD_',1,'i') >0) then
          /* Solo si se trata de una dimension generamos los inserts por defecto y la secuencia */
          --if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
            --DBMS_OUTPUT.put_line('CREATE SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5));
            --DBMS_OUTPUT.put_line('MINVALUE 1 START WITH 1 INCREMENT BY 1;');
          --end if;
          DBMS_OUTPUT.put_line('');        
          /* Primero el INSERT "NO APLICA" */
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
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
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := '-1';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := '-1';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := 'N';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := 'NA';
                        else
                          cadena_values := 'NULL';
                        end if;
                      else
                        cadena_values := '''NA#''';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := '''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NA#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := '''NA''';
                          when longitud_des_numerico = 1 then
                            cadena_values := '''N''';
                        end case;
                      else
                        cadena_values := 'NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := 'sysdate';
                  ELSE
                    /* (20150118) Angel Ruiz. BUG: Se han de incluir valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                      cadena_values := '-1';
                    elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                      cadena_values := 'sysdate';
                    else
                      /* VARCHAR */
                      pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                      pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                      longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                      longitud_des_numerico := to_number(longitud_des);
                      if (longitud_des_numerico > 8) then
                        cadena_values := '''NO APLICA''';
                      elsif (longitud_des_numerico > 2) then
                        cadena_values := '''NA#''';
                      else
                        cadena_values := '''N''';
                      end if;
                    end if;
                  --else
                    --cadena_values := 'NULL';
                  --end if;
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
              DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := cadena_values || ', -1';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3),'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0 ) then
                    cadena_values := cadena_values || ', -1';
                  else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := cadena_values || ', ''N''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := cadena_values || ', ''NA''';
                        else
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      else
                        cadena_values := cadena_values || ', ''NA#''';
                      end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 8) then
                    cadena_values := cadena_values || ', ''NO APLICA''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := cadena_values || ', ''NA#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := cadena_values || ', ''NA''';
                        when longitud_des_numerico = 1 then
                          cadena_values := cadena_values || ', ''N''';
                      end case;
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', sysdate';
                ELSE
                  /* (20150118) Angel Ruiz. BUG: Se han de incluir valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -1';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := cadena_values || ', sysdate';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := cadena_values || ', ''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NA#''';
                    else
                      cadena_values := cadena_values || ', ''N''';
                    end if;
                  end if;
                  --else
                  --cadena_values := cadena_values || ', NULL';
                  --end if;
              END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente insert "GENERICO" */
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
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
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'CVE_',1,'i') >0 THEN
                  cadena_values := '-2';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values := '-2';
                  else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := '''G''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := '''GE''';
                        else
                          cadena_values := 'NULL';
                        end if;
                      else
                        cadena_values := '''GE#''';
                      end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 7) then
                    cadena_values := '''GENERICO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := '''GE#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := '''GE''';
                        when longitud_des_numerico = 1 then
                          cadena_values := '''G''';
                      end case;
                    else
                      cadena_values := 'NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := 'sysdate';
                ELSE
                  /* (20150118) Angel Ruiz. BUG: Se han de incluir valores en lugar de NULL */
                --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := '-2';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := 'sysdate';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := '''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''GE#''';
                    else
                      cadena_values := '''G''';
                    end if;
                  end if;
                  --else
                    --cadena_values := 'NULL';
                  --end if;
              END CASE;  
              primera_col := 0;
            ELSE  /* si no es primera columna */
              DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := cadena_values || ', -2';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -2';
                  else
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                        cadena_values := cadena_values || ', ''G''';
                      elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := cadena_values || ', ''GE''';
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    else
                      cadena_values := cadena_values || ', ''GE#''';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 7) then
                    cadena_values := cadena_values || ', ''GENERICO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := cadena_values || ', ''GE#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := cadena_values || ', ''GE''';
                        when longitud_des_numerico = 1 then
                          cadena_values := cadena_values || ', ''G''';
                      end case;
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', sysdate';
                ELSE
                  /* (20160118) ANGEL RUIZ. BUG. Deben aparecer siempre valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -2';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := cadena_values || ', sysdate';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := cadena_values || ', ''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''GE#''';
                    else
                      cadena_values := cadena_values || ', ''G''';
                    end if;
                  end if;
                  --else
                  --  cadena_values := cadena_values || ', NULL';
                  --end if;
              END CASE;  
            END IF;
          END LOOP; 
          DBMS_OUTPUT.put_line(')');
          DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente INSERT "NO INFORMADO" */
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
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
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := '-3';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values :=  '-3';
                  else
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                        cadena_values := '''N''';
                      elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := '''NI''';
                      else
                        cadena_values := 'NULL';
                      end if;
                    else
                      cadena_values := '''NI#''';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 11) then
                    cadena_values := '''NO INFORMADO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := '''NI#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := '''NI''';
                        when longitud_des_numerico = 1 then
                          cadena_values := '''N''';
                      end case;
                    else
                      cadena_values := 'NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := 'sysdate';
                ELSE
                  /* (20160118) ANGEL RUIZ. BUG. Deben aparecer siempre valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := '-3';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := 'sysdate';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := '''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NI#''';
                    else
                      cadena_values := '''N''';
                    end if;
                  end if;
                  --else
                    --cadena_values := 'NULL';
                  --end if;
              END CASE;  
              primera_col := 0;
            ELSE  /* si no es primera columna */
              DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := cadena_values || ', -3';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -3';
                  else
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                        cadena_values := cadena_values || ', ''N''';
                      elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := cadena_values || ', ''NI''';
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    else                  
                      cadena_values := cadena_values || ', ''NI#''';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 11) then
                    cadena_values := cadena_values || ', ''NO INFORMADO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := cadena_values || ', ''NI#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := cadena_values || ', ''NA''';
                        when longitud_des_numerico = 1 then
                          cadena_values := cadena_values || ', ''N''';
                      end case;
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'FCH_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', sysdate';
                ELSE
                  /* (20160118) ANGEL RUIZ. BUG. Deben aparecer siempre valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -3';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := cadena_values || ', sysdate';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := cadena_values || ', ''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NI#''';
                    else
                      cadena_values := cadena_values || ', ''N''';
                    end if;
                  end if;
                  --else
                  --cadena_values := cadena_values || ', NULL';
                  --end if;
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

