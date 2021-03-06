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
    FROM METADATO.MTDT_INTERFACE_SUMMARY;
  
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
      POSITION
    FROM
      METADATO.MTDT_INTERFACE_DETAIL
    WHERE
      CONCEPT_NAME = concep_name_in and
      SOURCE = source_in
      order by POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col INTEGER;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      tipo_col                                      VARCHAR(70);

BEGIN
  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;
      DBMS_OUTPUT.put_line('REVOKE select, insert, update, delete, alter on APP_MVNOSA.SA_' || reg_summary.CONCEPT_NAME || ' from app_mvnotc;');
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;

