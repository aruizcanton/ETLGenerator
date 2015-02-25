DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      DISTINCT
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI"
    FROM MTDT_MODELO_LOGICO
    WHERE CI <> 'P';    /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */

  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      CI
    FROM MTDT_MODELO_LOGICO
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
  cadena_values VARCHAR2(255);
  concept_name VARCHAR2 (30);
  nombre_tabla_reducido VARCHAR2(30);
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  TABLESPACE_SA                  VARCHAR2(60);
  OWNER_TC                            VARCHAR2(60);
  OWNER_DWH                         VARCHAR2(60);
  OWNER_RD                            VARCHAR2(60);
  
  
BEGIN
  /* (20150119) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO OWNER_DWH FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DWH';
  SELECT VALOR INTO OWNER_RD FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_RD';
  /* (20150119) FIN*/

  SELECT COUNT(*) INTO num_filas FROM MTDT_MODELO_LOGICO;
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
      DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' to ' || OWNER_TC || ';');
      DBMS_OUTPUT.put_line('GRANT select  on ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' to ' || OWNER_DWH || ';');
      DBMS_OUTPUT.put_line('GRANT select  on ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' to ' || OWNER_RD || ';');
      /* Ahora creamos para la tabla TEMPORAL pero solo para aquellas que no se van a cargar como carga inicial */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ' to ' || OWNER_TC || ';');
      end if;
      DBMS_OUTPUT.put_line('');
      
      /**********************/
      /**********************/
    END LOOP;
    CLOSE c_mtdt_modelo_logico_TABLA;
    DBMS_OUTPUT.put_line('set echo off;');
    DBMS_OUTPUT.put_line('exit SUCCESS;');
  END IF;
END;

