DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR dtd_permited_values
  IS
    SELECT 
      ITEM_NAME,
      ID_LIST,
      AGREGATION,
      MAX(LENGTH(VALUE)) LONGITUD,
      MAX(LENGTH(DESCRIPTION)) LONGITUD_DES      
    FROM MTDT_PERMITED_VALUES
    --WHERE ITEM_NAME not in ('TIPO_ENVIO', 'TIPO_SERVICIO')
    GROUP BY 
      ITEM_NAME,
      ID_LIST,
      AGREGATION
      ORDER BY ID_LIST;
  reg_per_val dtd_permited_values%rowtype;
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  TABLESPACE_SA                  VARCHAR2(60);
  OWNER_TC                            VARCHAR2(60);
  OWNER_DWH                         VARCHAR2(60);
  OWNER_RD                            VARCHAR2(60);
  PREFIJO_DM                            VARCHAR2(60);
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
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  /* (20150119) FIN*/

  SELECT COUNT(*) INTO num_filas FROM MTDT_PERMITED_VALUES;
  /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_PERMITED_VALUES  */
  IF num_filas > 0 THEN
    /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
    DBMS_OUTPUT.put_line('set echo on;');
    DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
    OPEN dtd_permited_values;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH dtd_permited_values
      INTO reg_per_val;
      EXIT WHEN dtd_permited_values%NOTFOUND;
      --clave_foranea :=0;
      DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ' to ' || OWNER_TC || ';');
      DBMS_OUTPUT.put_line('GRANT select  on ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ' to ' || OWNER_DWH || ';');
      DBMS_OUTPUT.put_line('GRANT select  on ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ' to ' || OWNER_RD || ';');
      DBMS_OUTPUT.put_line('');
    END LOOP;
    CLOSE dtd_permited_values;
    DBMS_OUTPUT.put_line('set echo off;');
    DBMS_OUTPUT.put_line('exit SUCCESS;');
  END IF;
END;
