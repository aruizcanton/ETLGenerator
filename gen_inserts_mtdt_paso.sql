DECLARE
/* Creo el curdor que nos va a recorrer todos los procesos existentes */
CURSOR
  cursor_mtdt_paso
IS
  SELECT
  CVE_PROCESO,
  CVE_PASO,
  NOMBRE_PASO,
  ORDEN_EJECUCION,
  TIPO_IMPLEMENTACION,
  TIPO_PASO,
  RESPONSABLE,
  AUTOR,
  VERSION,
  FCH_ALTA,
  ESTADO,
  FCH_ESTADO,
  FCH_REGISTRO
  FROM MTDT_PASO
  ORDER BY CVE_PROCESO, CVE_PASO;
  
  reg cursor_mtdt_paso%rowtype;
  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  
BEGIN
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  /* (20141219) FIN*/
  DBMS_OUTPUT.put_line('REM INSERTING INTO ' || OWNER_MTDT || '.MTDT_PASO');
  DBMS_OUTPUT.put_line('SET DEFINE OFF;');
  
  OPEN cursor_mtdt_paso;
  LOOP
    FETCH cursor_mtdt_paso
    INTO reg;
    EXIT WHEN cursor_mtdt_paso%NOTFOUND;
    DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_MTDT || '.MTDT_PASO');
    DBMS_OUTPUT.put_line('(CVE_PROCESO,CVE_PASO,NOMBRE_PASO,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO)');
    DBMS_OUTPUT.put_line('VALUES (' || reg.CVE_PROCESO || ', ' || reg.CVE_PASO || ', ''' || reg.NOMBRE_PASO || ''', '''  || reg.TIPO_IMPLEMENTACION || ''', ''' || reg.TIPO_PASO || ''', ''' || reg.RESPONSABLE || ''', ''' || reg.AUTOR || ''', ''' || reg.VERSION || ''', TO_DATE(''' || to_char(reg.FCH_ALTA, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''), ''' || reg.ESTADO || ''', TO_DATE(''' || to_char(reg.FCH_ESTADO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''), TO_DATE(''' || to_char(reg.FCH_REGISTRO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')' || ');');
  END LOOP;
  DBMS_OUTPUT.put_line('commit;');
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
  
END;