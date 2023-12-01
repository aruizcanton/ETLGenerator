declare
cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE"
    FROM
      MTDT_TC_SCENARIO
    WHERE TABLE_TYPE in ('D','I')
    and TABLE_NAME in ('MDD_DEPARTAMENTOS_MEDAL', 'MDD_PARAMETROS_MEDAL')
    order by
    TABLE_TYPE;
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2)
  is
    SELECT 
      trim(TABLE_NAME) "TABLE_NAME",
      trim(TABLE_TYPE) "TABLE_TYPE",
      TABLE_COLUMNS,
      trim(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      "SELECT",
      "GROUP",
      FILTER,
      INTERFACE_COLUMNS,
      FILTER_CARGA_INI,
      trim(SCENARIO) "SCENARIO",
      trim(REINYECTION) "REINYECTION",
      DATE_CREATE,
      DATE_MODIFY
    FROM 
      MTDT_TC_SCENARIO
    WHERE
      TABLE_NAME = table_name_in
      ORDER BY SCENARIO;
      
  CURSOR MTDT_TC_DETAIL (table_name_in IN VARCHAR2, scenario_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(SCENARIO) "SCENARIO",
      TRIM(OUTER) "OUTER",
      SEVERIDAD,
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TRIM(TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(TABLE_LKUP_COND) "TABLE_LKUP_COND",      
      TRIM(IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(LKUP_COM_RULE) "LKUP_COM_RULE",
      VALUE,
      RUL,
      DATE_CREATE,
      DATE_MODIFY
  FROM
      MTDT_TC_DETAIL
  WHERE
      trim(TABLE_NAME) = table_name_in and
      trim(SCENARIO) = scenario_in;
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
      MTDT_INTERFACE_DETAIL
    WHERE
      CONCEPT_NAME = concep_name_in and
      SOURCE = source_in
    ORDER BY POSITION;
    
      
  reg_tabla MTDT_TABLA%rowtype;
      
  reg_scenario MTDT_SCENARIO%rowtype;
  
  reg_detail MTDT_TC_DETAIL%rowtype;
  
  reg_interface_detail dtd_interfaz_detail%rowtype;
  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  OWNER_TC                              VARCHAR(60);
  PREFIJO_DM                            VARCHAR2(60);
  TABLESPACE_SA                  VARCHAR2(60);
  v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;  

  v_num_procesos PLS_INTEGER:=0;
  v_num_pasos PLS_INTEGER:=0;
  
      
begin
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    if (reg_tabla.TABLE_TYPE = 'D') 
    then
      /* Generamos los registros relativos al proceso */
      v_num_procesos := v_num_procesos + 1;
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_ne_' || reg_tabla.TABLE_NAME || '.sh'', ''DIMENSIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_dh_' || reg_tabla.TABLE_NAME || '.sh'', ''DIMENSIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ''DIMENSIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
      /* GENERAMOS LOS PASOS */
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
      loop
        fetch MTDT_SCENARIO
        into reg_scenario;
        exit when MTDT_SCENARIO%NOTFOUND;
        if (reg_scenario.SCENARIO = 'N')
        then
          v_num_pasos := v_num_pasos +1;
          dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ' || to_char(v_num_pasos) || ', ''load_ne_' || reg_tabla.TABLE_NAME || '.sh'', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
        end if;
        if (reg_scenario.SCENARIO = 'E')
        then
          v_num_pasos := v_num_pasos +1;
          dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ' || to_char(v_num_pasos) || ', ''load_ne_' || reg_tabla.TABLE_NAME || '.sh'', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
        end if;
        if (reg_scenario.SCENARIO = 'H')
          dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 1' || ', ''load_dh_' || reg_tabla.TABLE_NAME || '.sh'', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
        then
        end if;
      end loop;
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 1' || ', ''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');

    end if;
    if (reg_tabla.TABLE_NAME == 'I')
    then
      /* Generamos los registros relativos al proceso */
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_' || reg_tabla.TABLE_NAME || '.sh'', ''INTEGRACIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
    end if;
  end loop;
end;