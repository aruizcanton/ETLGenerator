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
    
  

      reg_summary dtd_interfaz_summary%rowtype;
      reg_summary_history dtd_interfaz_summary_history%rowtype;
      

      
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
  DBMS_OUTPUT.put_line('');
  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;  
      DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME || ' CASCADE CONSTRAINTS;');
  END LOOP;
  CLOSE dtd_interfaz_summary;
  /****************************************************************/
  /* (20150717) ANGEL RUIZ. NUEVA FUNCIONALIDAD.*/
  /* Las tablas de STAGING pueden tener HISTORICO */
  /*****************************************************************************/  
  OPEN dtd_interfaz_summary_history;
  LOOP
    FETCH dtd_interfaz_summary_history
      INTO reg_summary_history;
      EXIT WHEN dtd_interfaz_summary_history%NOTFOUND;  
      DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_SA || '.SAH_' || reg_summary_history.CONCEPT_NAME || ' CASCADE CONSTRAINTS;');
  END LOOP;
  CLOSE dtd_interfaz_summary_history;
  DBMS_OUTPUT.put_line('');
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
END;

