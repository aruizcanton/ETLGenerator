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
    
  CURSOR dtd_interfaz_summary_history
  IS
    SELECT 
      CONCEPT_NAME,
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED,
      HISTORY
    FROM MTDT_INTERFACE_SUMMARY
    where HISTORY is not null;
    
  

      reg_summary dtd_interfaz_summary%rowtype;
      reg_summary_history dtd_interfaz_summary_history%rowtype;
      

      
      primera_col INTEGER;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      tipo_col                                      VARCHAR(70);
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

  DBMS_OUTPUT.put_line('set echo on;');
  DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;
      DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' TO ' || OWNER_TC || ';');
      DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' TO ' || OWNER_DM || ';');
      DBMS_OUTPUT.put_line('GRANT select on ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' TO ' || OWNER_DWH || ';');
      
  END LOOP;
  CLOSE dtd_interfaz_summary;
  
  /****************************************************************/
  /* (20150717) ANGEL RUIZ. NUEVA FUNCIONALIDAD.*/
  /* Las tablas de STAGING pueden tener HISTORICO */
  /*****************************************************************************/  
  DBMS_OUTPUT.put_line('');
 
  OPEN dtd_interfaz_summary_history;
  LOOP
    FETCH dtd_interfaz_summary_history
      INTO reg_summary_history;
      EXIT WHEN dtd_interfaz_summary_history%NOTFOUND;
      DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME || ' TO ' || OWNER_TC || ';');
      DBMS_OUTPUT.put_line('GRANT select, insert, update, delete on ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME || ' TO ' || OWNER_DM || ';');
      DBMS_OUTPUT.put_line('GRANT select on ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME || ' TO ' || OWNER_DWH || ';');
  END LOOP;
  CLOSE dtd_interfaz_summary_history;
  
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
END;

