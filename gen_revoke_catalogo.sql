DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR dtd_permited_values
  IS
    SELECT 
      ITEM_NAME,
      ID_LIST,
      AGREGATION,
      MAX(LENGTH(VALUE)) LONGITUD
    FROM METADATO.MTDT_PERMITED_VALUES
    GROUP BY 
      ITEM_NAME,
      ID_LIST,
      AGREGATION
      ORDER BY ID_LIST;
  reg_per_val dtd_permited_values%rowtype;
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
BEGIN

  SELECT COUNT(*) INTO num_filas FROM METADATO.MTDT_PERMITED_VALUES;
  /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_PERMITED_VALUES  */
  IF num_filas > 0 THEN
    /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
    DBMS_OUTPUT.put_line('set echo on;');
    OPEN dtd_permited_values;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH dtd_permited_values
      INTO reg_per_val;
      EXIT WHEN dtd_permited_values%NOTFOUND;
      --clave_foranea :=0;
      DBMS_OUTPUT.put_line('REVOKE select, insert, update, delete, alter on app_mvnodm.DMD_' || reg_per_val.ITEM_NAME || ' from app_mvnotc;');
    END LOOP;
    CLOSE dtd_permited_values;
    DBMS_OUTPUT.put_line('set echo off;');
    DBMS_OUTPUT.put_line('exit SUCCESS;');
  END IF;
END;
