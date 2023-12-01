declare
cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE"
    FROM
      MTDT_TC_SCENARIO
    WHERE TABLE_TYPE in ('D','I','H')
    and TABLE_NAME in ('MDD_DEPARTAMENTOS_MEDAL', 'MDF_TMP_INV_MEDAL') -- , 'MDD_PARAMETROS_MEDAL'
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
    
  TYPE list_strings  IS TABLE OF VARCHAR(500);
      
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
  
  lista_scenarios_presentes list_strings := list_strings();
  v_existe_scen_nuevo BOOLEAN := false;
  v_existe_scen_exis BOOLEAN := false;
  v_existe_scen_histo BOOLEAN := false;
  nombre_tabla_reducido           VARCHAR2(30);
  nombre_proceso                        VARCHAR2(30);
  
      
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
  v_num_procesos := 0;
  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5);
    if (length(reg_tabla.TABLE_NAME) < 25) then
      /* (20221006) ANGEL RUIZ. Recorto la longitud d elos nombres.*/
      --nombre_proceso := reg_tabla.TABLE_NAME;
      nombre_proceso := nombre_tabla_reducido;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    
    if (reg_tabla.TABLE_TYPE = 'D')   /* TABLA DE DIMENSION */
    then
      lista_scenarios_presentes.delete;
      v_existe_scen_nuevo := false;
      v_existe_scen_exis := false;
      v_existe_scen_histo := false;
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
      loop
        fetch MTDT_SCENARIO
        into reg_scenario;
        exit when MTDT_SCENARIO%NOTFOUND;
        if (reg_scenario.SCENARIO = 'N') then
          --dbms_output.put_line('** ESTOY EN EL SCENARIO = N');
          v_existe_scen_nuevo := true;
          lista_scenarios_presentes.EXTEND;
          lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'N';
        end if;
        if (reg_scenario.SCENARIO = 'E') then
          --dbms_output.put_line('** ESTOY EN EL SCENARIO = E');        
          v_existe_scen_exis := true;
          lista_scenarios_presentes.EXTEND;
          lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'E';
        end if;
        if (reg_scenario.SCENARIO = 'H') then
          --dbms_output.put_line('** ESTOY EN EL SCENARIO = H');        
          v_existe_scen_histo := true;
          lista_scenarios_presentes.EXTEND;
          lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'H';
        end if;
      end loop;
      close MTDT_SCENARIO;
      /* Generamos los registros relativos al proceso */
      if (v_existe_scen_nuevo or v_existe_scen_exis) then
        v_num_procesos := v_num_procesos + 1;
        dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_ne_' || reg_tabla.TABLE_NAME || '.sh'', ''DIMENSIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
        --dbms_output.put_line('** Acabo de pasar el insert del nuevo-existente');
        --dbms_output.put_line('** El numero de escenarios es: ' || to_char(lista_scenarios_presentes.COUNT));
        /* Generamos los pasos */
        v_num_pasos := 0;
        FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
        loop
          if (lista_scenarios_presentes (indx) = 'N') then
            v_num_pasos := v_num_pasos +1;
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PASO, CVE_PROCESO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''nreg_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, ' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, ' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
          end if;
        end loop;
        FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
        loop
          if (lista_scenarios_presentes (indx) = 'E') then
            v_num_pasos := v_num_pasos +1;
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PASO, CVE_PROCESO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''ureg_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, ' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, ' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
          end if;
        end loop;
        
      end if;
      if (v_existe_scen_histo) then
        v_num_procesos := v_num_procesos + 1;
        dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_dh_' || reg_tabla.TABLE_NAME || '.sh'', ''DIMENSIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');      
        FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
        loop
          if (lista_scenarios_presentes (indx) = 'H') then
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PASO, CVE_PROCESO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (1, ' || to_char(v_num_procesos) || ', ''hreg_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, 1' || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
            dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, 1' || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
          end if;
        end loop;
      end if;
      /* Generamos el exchange */
      v_num_procesos := v_num_procesos + 1;
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ''DIMENSIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
      /* Generamos los dos pasos del exchange */
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 1' || ', ''lex_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, 1' || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, 1' || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 2' || ', ''lex_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, 2' || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, 2' || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');

    end if;
    if (reg_tabla.TABLE_TYPE = 'I') /* TABLA DE INTEGRACION */
    then
      v_num_procesos := v_num_procesos + 1;
      /* Generamos los registros relativos al proceso */
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_' || reg_tabla.TABLE_NAME || '.sh'', ''INTEGRACIÓN'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 1' || ', ''load_' || reg_tabla.TABLE_NAME || '.sh'', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');      
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, 1' || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, 1' || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
    end if;
    if (reg_tabla.TABLE_TYPE = 'H') /* TABLA DE HECHOS */
    then
      /* Generamos los registros relativos al proceso */
      v_num_procesos := v_num_procesos + 1;
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_he_' || reg_tabla.TABLE_NAME || '.sh'', ''HECHO'', SYSDATE, ''A'', SYSDATE, SYSDATE);');
      lista_scenarios_presentes.delete;
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
      loop
        fetch MTDT_SCENARIO
        into reg_scenario;
        exit when MTDT_SCENARIO%NOTFOUND;
          lista_scenarios_presentes.EXTEND;
          lista_scenarios_presentes(lista_scenarios_presentes.LAST) := reg_scenario.SCENARIO;
      end loop;
      close MTDT_SCENARIO;
      /* GENERAMOS LOS PASOS */
      v_num_pasos := 0;
      FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
      loop
        v_num_pasos := v_num_pasos +1;
        dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ' || to_char(v_num_pasos) || ', ''' ||  lista_scenarios_presentes (indx) || '_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
        dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, ' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
        dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, ' || to_char(v_num_pasos) || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');      
      end loop;
      /* Generamos los dos pasos del exchange*/
      v_num_procesos := v_num_procesos + 1;
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PROCESO (CVE_PROCESO,NOMBRE_PROCESO,TIPO_PROCESO,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', ''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ''HECHO'', SYSDATE, ''A'', SYSDATE, SYSDATE);');

      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 1' || ', ''lex_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, 1' || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, 1' || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_PASO (CVE_PROCESO,CVE_PASO,NOMBRE_PASO,ORDEN_EJECUCION,TIPO_IMPLEMENTACION,TIPO_PASO,RESPONSABLE,AUTOR,VERSION,FCH_ALTA,ESTADO,FCH_ESTADO,FCH_REGISTRO) values (' || to_char(v_num_procesos) || ', 2' || ', ''lex_' || nombre_proceso || ''', null, ''PL/SQL'', null, null, null, ''1.0'', sysdate, ''A'', sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (0, 2' || ', ' || to_char(v_num_procesos) || ', ''OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      dbms_output.put_line('insert into ' || OWNER_MTDT || '.MTDT_RESULTADO (CVE_RESULTADO, CVE_PASO, CVE_PROCESO, DESCRIPCION, ACCION, BAN_DIRECTIVA_EJECUTIVA, FCH_ALTA, FCH_BAJA, FCH_REGISTRO) values (1, 2' || ', ' || to_char(v_num_procesos) || ', ''NO OK'', NULL, NULL, sysdate, sysdate, sysdate);');
      
    end if;
  end loop;
  close MTDT_TABLA;
end;