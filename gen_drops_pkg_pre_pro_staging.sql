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
    FROM MTDT_INTERFACE_SUMMARY
    WHERE SOURCE <> 'SA';
    --where DELAYED = 'S';
    --and CONCEPT_NAME in ('CIUDAD');
  

      reg_summary dtd_interfaz_summary%rowtype;
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
  dbms_output.put_line('set echo on;');
  dbms_output.put_line('whenever sqlerror exit 1;');
  dbms_output.put_line(''); 
  OPEN dtd_interfaz_summary;
  LOOP
    
      FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;
      --nombre_fich_pkg := 'pkg_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '.sql';
      --fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
      /* Angel Ruiz (20141223) Hecho porque hay paquetes que no compilan */
       if (length(reg_summary.CONCEPT_NAME) < 24) then
        nombre_proceso := 'SA_' || reg_summary.CONCEPT_NAME;
      else
        nombre_proceso := reg_summary.CONCEPT_NAME;
      end if;
      
      dbms_output.put_line ('DROP PACKAGE ' ||  OWNER_SA || '.pkg_' || nombre_proceso || ';');
      
  END LOOP;
  CLOSE dtd_interfaz_summary;
  dbms_output.put_line(''); 
  dbms_output.put_line('set echo off;');
  dbms_output.put_line('exit SUCCESS;');

END;


