DECLARE
/* Creo el curdor que nos va a recorrer todos los procesos existentes */
CURSOR
  cursor_mtdt_resultado
IS
  SELECT
  CVE_PROCESO,
  CVE_PASO,
  CVE_RESULTADO,
  DESCRIPCION,
  ACCION,
  BAN_DIRECTIVA_EJECUTIVA,
  FCH_ALTA,
  FCH_BAJA,
  FCH_REGISTRO
  FROM MTDT_RESULTADO
  ORDER BY CVE_PROCESO, CVE_PASO, CVE_RESULTADO;
  
  reg cursor_mtdt_resultado%rowtype;
  
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
  DBMS_OUTPUT.put_line('REM INSERTING INTO ' || OWNER_MTDT || '.MTDT_RESULTADO');
  DBMS_OUTPUT.put_line('SET DEFINE OFF;');
  
  OPEN cursor_mtdt_resultado;
  LOOP
    FETCH cursor_mtdt_resultado
    INTO reg;
    EXIT WHEN cursor_mtdt_resultado%NOTFOUND;
    DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_MTDT || '.MTDT_RESULTADO');
    DBMS_OUTPUT.put_line('(CVE_PROCESO,CVE_PASO,CVE_RESULTADO,DESCRIPCION,ACCION,BAN_DIRECTIVA_EJECUTIVA,FCH_ALTA,FCH_BAJA,FCH_REGISTRO)');
    DBMS_OUTPUT.put_line('VALUES (' || reg.CVE_PROCESO || ', ' || reg.CVE_PASO || ', ''' || reg.CVE_RESULTADO || ''', '''  || reg.DESCRIPCION || ''', ''' || reg.ACCION || ''', ''' || reg.BAN_DIRECTIVA_EJECUTIVA || '''' || ', TO_DATE(''' || to_char(reg.FCH_ALTA, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''), ' || 'TO_DATE(''' || to_char(reg.FCH_BAJA, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''), TO_DATE(''' || to_char(reg.FCH_REGISTRO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')' || ');');
  END LOOP;
  DBMS_OUTPUT.put_line('commit;');
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');
  
END;