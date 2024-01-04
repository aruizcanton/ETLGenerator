declare

cursor MTDT_TABLA
  is
SELECT
      DISTINCT TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME", /*(20150907) Angel Ruiz NF. Nuevas tablas.*/
      --TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      --TRIM(mtdt_modelo_logico.TABLESPACE) "TABLESPACE" (20150907) Angel Ruiz NF. Nuevas tablas.
      TRIM(mtdt_modelo_summary.TABLESPACE) "TABLESPACE",
      TRIM(mtdt_modelo_summary.PARTICIONADO) "PARTICIONADO"
    FROM
      --MTDT_TC_SCENARIO, mtdt_modelo_logico (20150907) Angel Ruiz NF. Nuevas tablas.
      MTDT_TC_SCENARIO, mtdt_modelo_summary
    WHERE MTDT_TC_SCENARIO.TABLE_TYPE = 'H' and
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_logico.TABLE_NAME) and (20150907) Angel Ruiz NF. Nuevas tablas.
    trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_summary.TABLE_NAME) and
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_TRAFD_CU_MVNO', 'DMF_TRAFE_CU_MVNO', 'DMF_TRAFV_CU_MVNO');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_RECARGAS_MVNO');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_TRAFV_CU_MVNO');  
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_PMP', 'DMF_PARQUE_SERIADOS', 'DMF_CLASE_VALORACION');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('BSF_ROAMN_TRAF', 'BSF_ITX_TRAFICO', 'BSF_ITX_IMPORTES');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('BSF_COMIS_TMK', 'BSF_PRE_COMIS_PROPIO', 'BSF_COMIS_CDA', 'BSF_COMIS_DIGITAL', 'BSF_CDG_PARQUE');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('BSF_COMIS_TMK', 'BSF_COMIS_DIGITAL', 'BSF_PRE_COMIS_CDA', 'BSF_PRE_COMIS_PROPIO', 'BSF_PRE_COMIS_ESP');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('BSF_ALTAS_POSTPAGO', 'BSF_ALTAS_PREPAGO');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('BSF_ITX_TRAFICO', 'BSF_ITX_IMPORTES');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_MOVIMIENTOS_SERIADOS', 'DMF_CLASE_VALORACION');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_MOVIMIENTOS_SERIADOS');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DWF_REEMBOLSO_ITSON', 'DWF_PQ_SUSCRPCN_ITSON', 'DWF_COMPRA_ITSON');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DWF_COMPRA_ITSON', 'DWF_AJUSTE_ITSON', 'DWF_REEMBOLSO_ITSON', 'DWF_PQ_SUSCRPCN_ITSON', 'DWF_CONSUMO_DETALLE_ITSON');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_MOVIMIENTOS_SERIADOS', 'DMF_FACT_SERIADOS');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_PARQUE_SERIADOS', 'DMF_MOVIMIENTOS_SERIADOS', 'DMF_FACT_SERIADOS');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_CLASE_VALORACION','DMF_FACT_SERIADOS','DMF_PMP','DMF_MOVIMIENTOS_SERIADOS');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_FACT_SERIADOS', 'DMF_PARQUE_SERIADOS', 'DMF_MOVIMIENTOS_SERIADOS');
    --'MDF_JSON_INVITACION_MEDALIA', 'MDF_FTP_COMPLEMENTARIO_PREP', 'MDF_FTP_COMPLEMENTARIO_PREP', 'MDF_FTP_COMPLEMENTARIO_MEDALI', 'MDF_COMPLEMENTARIO_MEDALI_B2B'
    -- MDF_MEDALIA_COMP_PREPAGO
    --, 'MDF_MEDAL_COMP_PREPAGO' , 'MDF_JSON_INVITACION_MEDAL_PRE'
    trim(MTDT_TC_SCENARIO.TABLE_NAME) in (
    --'MDF_TMP_COMP_MEDAL', 'MDF_FTP_COMP_MEDAL', 'MDF_MEDAL_RECARGAS_APP',
    --'MDF_MEDAL_COMP_PREPAGO'
    --, 'MDF_JSON_INV_MEDAL_PRE', 'MDF_COMP_MEDAL_B2B', 'MDF_JSON_INV_MEDAL', 'MDF_BP_TEMP_ROAMERS_SAL'
    --, 'MDF_MEDAL_TMP_ARPU_CLI', 'MDF_BP_NAVEGACION_3M', 'MDF_FTP_COMP_PREP', 
    --'MDF_INV_MEDAL_B2B', 'MDF_MEDAL_INV_PREPAGO', 'MDF_TMP_COMP_PREP', 'MDF_TMP_INV_MEDAL',
    --'MDF_TMP_BAJAS_MEDAL', 'MDF_TMP_INV_MEDAL_DIA', 'MDF_TMP_INV_MEDAL_PREP'
    --, 'MDF_PARQUE_CANAL_RS'
    --'MDF_MEDAL_INV_PREPAGO', 'MDF_TMP_INV_MEDAL', 'MDF_TMP_INV_MEDAL_PREP', 'MDF_MUESTRAS_USO'
    --'MDF_SIMCARD_TO'
    --, 'MDF_TARJETAS_TO', 
    --, 'MDF_CLIENTE_TO'
    --, 'MDF_FACTURAS_TO'
    --, 'MIG_SIMCARD_TO'
    --, 'MIG_TARJETAS_TO', 'MIG_CLIENTE_TO', 'MIG_FACTURAS_TO'
    --, 'MDF_VENDEDOR_TO'
    --, 'MDF_ABONADO_TO'
    --, 'MIG_ABONADO_TO', 'MDF_ABONADO_SS_TO'
    --, 'MIG_ABONADO_SS_TO', 'MDF_CLIENTE_BODEGA_TO', 'MIG_CLIENTE_BODEGA_TO'
    'MDF_PORTADOS_TO'
    , 'MIG_PORTADOS_TO'
    );
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_PMP_DEMO');
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2)
  is
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE",
      TRIM(TABLE_COLUMNS) "TABLE_COLUMNS",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM("SELECT") "SELECT",
      TRIM ("GROUP") "GROUP",
      TRIM(FILTER) "FILTER",
      TRIM(INTERFACE_COLUMNS) "INTERFACE_COLUMNS",
      TRIM(SCENARIO) "SCENARIO",
      DATE_CREATE,
      DATE_MODIFY
    FROM 
      MTDT_TC_SCENARIO
    WHERE
      TRIM(TABLE_NAME) = table_name_in
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
      TRIM(VALUE) "VALUE",
      TRIM(RUL) "RUL",
      DATE_CREATE,
      DATE_MODIFY
  FROM
      MTDT_TC_DETAIL
  WHERE
      TRIM(TABLE_NAME) = table_name_in and
      TRIM(SCENARIO) = scenario_in;
      
  CURSOR MTDT_TC_LOOKUP (table_name_in IN VARCHAR2)
  IS
    SELECT
      DISTINCT
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      --IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      TRIM("VALUE") "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      (trim(RUL) = 'LKUP' or trim(RUL) = 'LKUPC') and
      TRIM(TABLE_NAME) = table_name_in;

  CURSOR MTDT_TC_FUNCTION (table_name_in IN VARCHAR2)
  IS
    SELECT
      DISTINCT
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      CAST(VALUE AS VARCHAR2(2000)) "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      RUL = 'FUNCTION' and
      TRIM(TABLE_NAME) = table_name_in;
      

  reg_tabla MTDT_TABLA%rowtype;     
  reg_scenario MTDT_SCENARIO%rowtype;
  reg_detail MTDT_TC_DETAIL%rowtype;
  reg_lookup MTDT_TC_LOOKUP%rowtype;
  reg_function MTDT_TC_FUNCTION%rowtype;
  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(400);
  type lista_tablas_from is table of varchar(2000);
  type lista_condi_where is table of varchar(500);

  
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(2000);
  prototipo_fun                             VARCHAR2(2000);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
  lista_scenarios_presentes                                    list_strings := list_strings();
  campo_filter                                VARCHAR2(2000);
  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_reducido           VARCHAR2(30);
  nombre_tabla_T                        VARCHAR2(30);
  v_nombre_particion                  VARCHAR2(30);
  --nombre_tabla_base_reducido           VARCHAR2(30);
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR2(60);
  OWNER_TC                              VARCHAR2(60);
  PREFIJO_DM                            VARCHAR2(60);
  PAIS                                  VARCHAR2(60);
  
  l_FROM                                      lista_tablas_from := lista_tablas_from();
  l_WHERE                                   lista_condi_where := lista_condi_where();
  v_hay_look_up                           VARCHAR2(1):='N';
  v_nombre_seqg                          VARCHAR(120):='N';
  v_bandera                                   VARCHAR2(1):='S';
  v_nombre_tabla_agr                VARCHAR2(30):='No Existe';
  v_nombre_tabla_agr_redu           VARCHAR2(30):='No Existe';
  v_nombre_proceso_agr              VARCHAR2(30);
  nombre_tabla_T_agr                VARCHAR2(30);
  v_existen_retrasados              VARCHAR2(1) := 'N';
  v_numero_indices                  PLS_INTEGER:=0;
  v_paso_actual                      PLS_INTEGER:=0;


/************/
/*************/

/* (20161117) Angel Ruiz. */
  function extrae_campo (cadena_in in varchar2) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    /* Detecto si existen funciones SQL en el campo */
    if (regexp_instr(cadena_in, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
      if (regexp_instr(cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z0-9_\.]+ *,') > 0) then
        /* Se trata de un decode normal y corriente */
        if (instr(cadena_in, '.') > 0 ) then
          v_cadena_temp := regexp_substr (cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z0-9_\.]+ *,'); 
          v_campo := regexp_substr (v_cadena_temp,'\.[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_campo := substr(v_campo, 2); /* quito el punto */
          v_cadena_result := v_campo;
        else
          v_cadena_temp := regexp_substr (cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z_]+ *,'); 
          v_campo := regexp_substr (v_cadena_temp,'[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_cadena_result := v_campo;
        end if;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Nn][Vv][Ll]') > 0) then
      /* Se trata de que el campo de join posee la funcion NVL */
      if (regexp_instr(cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z0-9_\.]+ *,') > 0) then
        if (instr(cadena_in, '.') > 0) then
          v_cadena_temp := regexp_substr (cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z0-9_\.]+ *,');
          v_campo := regexp_substr (v_cadena_temp,'\.[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_campo := substr(v_campo, 2);
          v_cadena_result := v_campo;
        else
          v_cadena_temp := regexp_substr (cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,');
          v_campo := regexp_substr (v_cadena_temp,'[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_cadena_result := v_campo;
        end if;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
      /* Se trata de que el campo de join posee la funcion UPPER */
      if (regexp_instr(cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z0-9_\.]+ *\)') > 0) then
        if (instr(cadena_in, '.') > 0) then
          v_cadena_temp := regexp_substr (cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z0-9_\.]+ *\)');
          v_campo := regexp_substr (v_cadena_temp,'\.[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_campo := substr(v_campo, 2); /* quito el punto */
          v_cadena_result := v_campo;
        else
          v_cadena_temp := regexp_substr (cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)');
          v_campo := regexp_substr (v_cadena_temp,'[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_cadena_result := v_campo;
        end if;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_0-9\.]+ *\)') > 0) then
        if (instr(v_cadena_temp, '.') > 0) then
          /* El campo viene con ALIAS */
          v_cadena_temp := regexp_substr (cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z0-9_\.]+ *,');
          v_campo := regexp_substr (v_cadena_temp,'\.[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_campo := substr(v_campo ,2); /* quito el punto */
          v_cadena_result := v_campo;
        else
          v_cadena_temp := regexp_substr (cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *,');
          v_campo := regexp_substr (v_cadena_temp,'[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_cadena_result := v_campo;
        end if;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
      /* Se trata de que el campo de join posee la funcion LTRIM */
      if (regexp_instr(cadena_in, ' *[Ll][Tt][Rr][Ii][Mm] *\( *[A-Za-z0-9_\.]+ *,') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Ll][Tt][Rr][Ii][Mm] *\( *[A-Za-z0-9_\.]+ *,');
        if (instr(v_cadena_temp, '.') > 0) then
          /* El campo viene con ALIAS */
          v_campo := regexp_substr (v_cadena_temp,'\.[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
          v_campo := substr(v_campo, 2);  /* quito el punto */
        else
          /* El campo viene sin ALIAS */
          v_campo := regexp_substr (v_cadena_temp,'[A-Za-z0-9_]+', instr( v_cadena_temp, '('));
        end if;
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    else
      v_cadena_result := cadena_in;
    end if;
    return v_cadena_result;
  end;
/* (20150918) Angel Ruiz. NUEVA FUNCION */
  function sustituye_comillas_dinam (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(100);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco LA COMILLA */
        cadena_resul := regexp_replace(cadena_resul, '''', '''''');
      end if;
      return cadena_resul;
    end;

/************/

  function cambio_puntoYcoma_por_coma (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(1000);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      /* Busco VAR_FUN_NAME_LOOKUP */
      cadena_resul := regexp_replace(cadena_resul, ';', ',');
    end if;  
    return cadena_resul;
  end cambio_puntoYcoma_por_coma;

  function split_string_punto_coma ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (400);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ';', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                dbms_output.put_line ('Pos es:' || pos);
                dbms_output.put_line ('Pos_ant es:' || pos_ant);
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant) -1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    return lista_elementos;
  end split_string_punto_coma;  
  
  function split_string_coma ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (50);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ',', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant)-1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    return lista_elementos;
  end split_string_coma;
  /* (20170302) Angel Ruiz */
  function transformo_funcion_outer (cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    if (regexp_instr(cadena_in, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
        /* trasformamos el primer operador del LTRIM */
      if (regexp_instr(cadena_in, ' *[Ll][Tt][Rr][Ii][Mm] *\( *[A-Za-z0-9_\.]+ *,') > 0) then
        if (instr(cadena_in, '.') > 0) then
          /* el campo esta cualificado con ALIAS */
          if outer_in = 1 then
            v_cadena_temp := regexp_replace (cadena_in, ' *([Ll][Tt][Rr][Ii][Mm]) *\( *([A-Za-z0-9_\.]+) *,', '\1(' || '\2' || ' (+),');
          else
            v_cadena_temp := regexp_replace (cadena_in, ' *([Ll][Tt][Rr][Ii][Mm]) *\( *([A-Za-z0-9_\.]+) *,', '\1(' || '\2' || ' ,');
          end if;
        else
          if outer_in = 1 then
            v_cadena_temp := regexp_replace (cadena_in, ' *([Ll][Tt][Rr][Ii][Mm]) *\( *([A-Za-z0-9_]+) *,', '\1(' || alias_in || '.' || '\2' || ' (+),');
          else
            v_cadena_temp := regexp_replace (cadena_in, ' *([Ll][Tt][Rr][Ii][Mm]) *\( *([A-Za-z0-9_]+) *,', '\1(' || alias_in || '.' || '\2' || ' ,');
          end if;
        end if;
        v_cadena_result := regexp_replace(v_cadena_temp, '''', ''''''); /* retorno el resultado pero sustituyo comilla por doble comilla */
      else
        v_cadena_result := regexp_replace (cadena_in, '''', '''''');
      end if;
    else
      v_cadena_result := alias_in || '.' || cadena_in;
    end if;
    return v_cadena_result;
  end;
  
  /* (20161122) Angel Ruiz. Transforma el campo de la funcion poniendole el alias*/
  function transformo_funcion (cadena_in in varchar2, alias_in in varchar2) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    /* Detecto si existen funciones SQL en el campo */
    if (regexp_instr(cadena_in, '[Nn][Vv][Ll]') > 0) then
      /* Se trata de que el campo de join posee la funcion NVL */
      if (regexp_instr(cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,') > 0) then
        /* trasformamos el primer operador del NVL */
        v_cadena_temp := regexp_replace (cadena_in, ' *([Nn][Vv][Ll]) *\( *([A-Za-z_]+) *,', '\1(' || alias_in || '.' || '\2' || ',');
        /* trasformamos el segundo operador del NVL, en caso de que sea un campo y no un literal */
        v_cadena_temp := regexp_replace (v_cadena_temp, ', *([A-Za-z_]+) *\)', ', ' || alias_in || '.' || '\1' || ')');
        v_cadena_result := v_cadena_temp; /* retorno el resultado */
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
      /* Se trata de que el campo de join posee la funcion UPPER */
      if (regexp_instr(cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Uu][Pp][Pp][Ee][Rr]) *\( *([A-Za-z_]+) *\)', '\1(' || alias_in || '.' || '\2' || ')');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Rr][Ee][Pp][Ll][Aa][Cc][Ee]) *\( *([A-Za-z_]+) *,', '\1(' || alias_in || '.' || '\2,');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    else
      v_cadena_result := alias_in || '.' || cadena_in;
    end if;
    return v_cadena_result;
  end;

  function extrae_campo_decode (cadena_in in varchar2) return varchar2
  is
    lista_elementos list_strings := list_strings (); 

  begin
    lista_elementos := split_string_coma(cadena_in);
    return lista_elementos(lista_elementos.count - 1);
  
  end extrae_campo_decode;

  function extrae_campo_decode_sin_tabla (cadena_in in varchar2) return varchar2
  is
    lista_elementos list_strings := list_strings (); 

  begin
    lista_elementos := split_string_coma(cadena_in);
    if instr(lista_elementos((lista_elementos.count) - 1), '.') > 0 then
      return substr(lista_elementos(lista_elementos.count - 1), instr(lista_elementos((lista_elementos.count) - 1), '.') + 1);
    else
      return lista_elementos(lista_elementos.count - 1);
    end if;
  end extrae_campo_decode_sin_tabla;

--  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
--  is
--    parte_1 varchar2(100);
--    parte_2 varchar2(100);
--    parte_3 varchar2(100);
--    parte_4 varchar2(100);
--    decode_out varchar2(500);
--    lista_elementos list_strings := list_strings ();
  
--  begin
--    /* Ejemplo de Deode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
--    lista_elementos := split_string_coma(cadena_in);
--    parte_1 := trim(substr(lista_elementos(1), instr(lista_elementos(1), '(') + 1)); /* Me quedo con ID_FUENTE*/
--    parte_2 := lista_elementos(2);  /* Me quedo con 'SER' */
--    parte_3 := trim(lista_elementos(3));
--    parte_4 := lista_elementos(4);
--    if (outer_in = 1) then
--      /* En la tranformacion del DECODE es necesario ponerle el signo de OUTER */
--      decode_out := 'DECODE(' || alias_in || '.' || parte_1 || '(+), ' || sustituye_comillas_dinam(parte_2) || ', ' || alias_in || '.'|| parte_3 || '(+), ' || sustituye_comillas_dinam(parte_4);
--    else    
--      /* En la tranformacion del DECODE no es necesario ponerle el signo de OUTER */
--      decode_out := 'DECODE(' || alias_in || '.' || parte_1 || ', ' || sustituye_comillas_dinam(parte_2) || ', ' || alias_in || '.'|| parte_3 || ', ' || sustituye_comillas_dinam(parte_4);
--    end if;
--    return decode_out;
--  end transformo_decode;
--  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
--  is
--    parte_1 varchar2(100);
--    parte_2 varchar2(100);
--    parte_3 varchar2(100);
--    parte_4 varchar2(100);
--    decode_out varchar2(500);
--    lista_elementos list_strings := list_strings ();
  
--  begin
    /* Ejemplo de Decode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
--    lista_elementos := split_string_coma(cadena_in);
--    parte_1 := trim(substr(lista_elementos(1), instr(lista_elementos(1), '(') + 1)); /* Me quedo con ID_FUENTE*/
--    parte_2 := lista_elementos(2);  /* Me quedo con 'SER' */
--    parte_3 := trim(lista_elementos(3));  /* Me quedo con ID_CANAL */
--    parte_4 := trim(substr(lista_elementos(4), 1, instr(lista_elementos(4), ')') - 1));  /* Me quedo con '1' */
--    if (instr(parte_1, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_1 := alias_in || '.' || parte_1 || '(+)';
--      else
--        parte_1 := alias_in || '.' || parte_1;
--      end if;
--    end if;
--    if (instr(parte_2, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_2 := alias_in || '.' || parte_2 || '(+)';
--      else
--        parte_2 := alias_in || '.' || parte_2;
--      end if;
--    end if;
--    if (instr(parte_3, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_3 := alias_in || '.' || parte_3 || '(+)';
--      else
--        parte_3 := alias_in || '.' || parte_3;
--      end if;
--    end if;
--    if (instr(parte_4, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
--      if (outer_in = 1) then
--        parte_4 := alias_in || '.' || parte_4 || '(+)';
--      else
--        parte_4 := alias_in || '.' || parte_4;
--      end if;
--    end if;
    /* Puede ocurrir que alguna parte del decode tanga el signo ' como seria el caso de los campos literales */
    /* como estamos generando querys dinamicas, tenemos que escapar las comillas */
--    if (instr(parte_1, '''') > 0) then
--      parte_1 := sustituye_comillas_dinam(parte_1);
--    end if;
--    if (instr(parte_2, '''') > 0) then
--      parte_2 := sustituye_comillas_dinam(parte_2);
--    end if;
--    if (instr(parte_3, '''') > 0) then
--      parte_3 := sustituye_comillas_dinam(parte_3);
--    end if;
--    if (instr(parte_4, '''') > 0) then
--      parte_4 := sustituye_comillas_dinam(parte_4);
--    end if;
--    decode_out := 'DECODE(' || parte_1 || ', ' || parte_2 || ', ' || parte_3 || ', ' || parte_4 || ')';
--    return decode_out;
--  end transformo_decode;
  /* (20161118) Angel Ruiz. Nueva version de la funcion que transforma los decodes*/
  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
  is
    parte_1 varchar2(100);
    parte_2 varchar2(100);
    parte_3 varchar2(100);
    parte_4 varchar2(100);
    decode_out varchar2(500);
    lista_elementos list_strings := list_strings ();
    v_cadena_temp VARCHAR2(500):='';

  begin
    /* Ejemplo de Decode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
    if (regexp_instr(cadena_in, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
      lista_elementos := split_string_coma(cadena_in);
      if (lista_elementos.COUNT > 0) then
        FOR indx IN lista_elementos.FIRST .. lista_elementos.LAST
        LOOP
          if (indx = 1) then
            /* Se trata del primer elemento: DECODE (ID_FUENTE */
            v_cadena_temp := trim(regexp_substr(lista_elementos(indx), ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\('));  /* Me quedo con DECODE ( */
            parte_1 := trim(substr(lista_elementos(indx), instr(lista_elementos(indx), '(') +1)); /* DETECTO EL ( */
            if (outer_in = 1) then
              v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
            else
              v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
            end if;
            v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), " */
          elsif (indx = lista_elementos.LAST) then
            /* Se trata del ultimo elemento '1') */
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemnto tipo ID_CANAL pero situado al final del DECODE */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.\1' || ' (+) )'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.\1'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              end if;
            else
              /* Se trata de un elemento literal situado como ultimo elemento del decode, tipo '1' */
              /* Le ponemos doble comillas ya que estamos generando una query deinamica */
              v_cadena_temp := v_cadena_temp || sustituye_comillas_dinam(lista_elementos(indx));
            end if;
          else
            /* Se trata del resto de elmentos 'SER', ID_CANAL*/
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemento que no es un literal, tipo ID_CANAL */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1' || ' (+)');
              else
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1');
              end if;
              v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), ..., alias_in.ID_CANAL, ... "*/
            else
              /* Se trata de un elemento que es un literal, tipo 'SER' */
              /* Le ponemos doble comillas ya que estamos generando una query deinamica */
              v_cadena_temp := v_cadena_temp || sustituye_comillas_dinam(lista_elementos(indx)) || ', ';
            end if; 
          end if;
        END LOOP;
      end if;
    else
      if (outer_in = 1) then
        v_cadena_temp := alias_in || '.' || cadena_in || ' (+)';
      else
        v_cadena_temp := alias_in || '.' || cadena_in;
      end if;
    end if;
    return v_cadena_temp;
  end;
  
  function proceso_campo_value (cadena_in in varchar2, alias_in in varchar) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  v_pos_ini_corchete_ab PLS_integer;
  v_pos_fin_corchete_ce PLS_integer;
  v_cadena_a_buscar varchar2(100);
  cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      v_pos_ini_corchete_ab := instr(cadena_in, '[');
      v_pos_fin_corchete_ce := instr(cadena_in, ']');
      v_cadena_a_buscar := substr(cadena_in, v_pos_ini_corchete_ab, (v_pos_fin_corchete_ce - v_pos_ini_corchete_ab) + 1);
      sustituto := alias_in || '.' || substr (cadena_in, v_pos_ini_corchete_ab + 1, (v_pos_fin_corchete_ce - v_pos_ini_corchete_ab) - 1);
      loop
        pos := instr(cadena_resul, v_cadena_a_buscar, pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (v_cadena_a_buscar));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
      end loop;
    end if;
    return cadena_resul;
  end;


  function proc_campo_value_condicion (cadena_in in varchar2, nombre_funcion_lookup in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(100);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      /* Busco VAR_FUN_NAME_LOOKUP */
      sustituto := nombre_funcion_lookup;
      loop
        dbms_output.put_line ('Entro en el LOOP de proc_campo_value_condicion. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, 'VAR_FUN_NAME_LOOKUP', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length ('VAR_FUN_NAME_LOOKUP'));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
        --pos := pos_ant;
      end loop;
      /* Busco LA COMILLA */
      pos := 0;
      posicion_ant := 0;
      sustituto := '''''';
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, '''', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (''''));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + length ('''''');
        pos := pos_ant;
      end loop;
    end if;  
    return cadena_resul;
  end;

  function procesa_COM_RULE_lookup (cadena_in in varchar2, v_alias_in varchar2 := NULL) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_COM_RULE_lookup');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco LA COMILLA */
      pos := 0;
      posicion_ant := 0;
      sustituto := '''''';
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, '''', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (''''));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + length ('''''');
        pos := pos_ant;
      end loop;
      /* Sustituyo el nombre de Tabla generico por el nombre que le paso como parametro */
      if (v_alias_in is not null) then
        /* Existe un alias que sustituir */
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#TABLE_OWNER#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#TABLE_OWNER#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;

      end if;
    end if;
    
    return cadena_resul;
  end;


  function procesa_condicion_lookup (cadena_in in varchar2, v_alias_in varchar2 := NULL) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_condicion_lookup');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco el signo = o el simbolo != */
      if (instr(cadena_resul, '!=') > 0) then
        /* Busco el signo != */
        sustituto := ' (+)!= ';
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '!=', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('!='));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          pos_ant := pos + (length (' (+)!= '));
          dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
          pos := pos_ant;
        end loop;
      else
        if (instr(cadena_resul, '=') > 0) then
          sustituto := ' (+)= ';
          loop
            dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
            pos := instr(cadena_resul, '=', pos+1);
            exit when pos = 0;
            dbms_output.put_line ('Pos es mayor que 0');
            dbms_output.put_line ('Primer valor de Pos: ' || pos);
            cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
            dbms_output.put_line ('La cabeza es: ' || cabeza);
            dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
            cola := substr(cadena_resul, pos + length ('='));
            dbms_output.put_line ('La cola es: ' || cola);
            cadena_resul := cabeza || sustituto || cola;
            pos_ant := pos + (length (' (+)= '));
            dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
            pos := pos_ant;
          end loop;
        end if;
      end if;
      /* Busco LA COMILLA */
      pos := 0;
      posicion_ant := 0;
      sustituto := '''''';
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, '''', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (''''));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + length ('''''');
        pos := pos_ant;
      end loop;
      /* Sustituyo el nombre de Tabla generico por el nombre que le paso como parametro */
      if (v_alias_in is not null) then
        /* Existe un alias que sustituir */
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#TABLE_OWNER#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#TABLE_OWNER#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;

      end if;
    end if;
    
    return cadena_resul;
  end;
  
/************/
/*************/
  function procesa_campo_filter_dinam (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (2000);
    sustituto              varchar2(100);
    cola                      varchar2(2000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(5000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* (20150914) Angel Ruiz. BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
        /* Busco LA COMILLA para poner comillas dobles */
        /*(20161118) Angel Ruiz. Modifico la forma de cambiar la ' por '' usando regexp_replace */
        cadena_resul := regexp_replace(cadena_resul, '''', '''''');
        /* (20150914) Angel Ruiz. FIN BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */


        /* Busco #VAR_FCH_CARGA# */
        --sustituto := ' to_date ('''' ||  fch_datos_in || '''', ''yyyymmdd'') ';
        --sustituto := ' TO_DATE('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''')';
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_CARGA#', ' TO_DATE('''''' || fch_carga_in || '''''', ''''YYYYMMDD'''') ');
        /* Busco VAR_FCH_INICIO */
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INICIO#', ' TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDD'''') ');

        /* Busco VAR_PROFUNDIDAD_BAJAS */
        cadena_resul := regexp_replace(cadena_resul, '#VAR_PROFUNDIDAD_BAJAS#', ' 90 ');
        /* Busco OWNER_DM */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_DM#', OWNER_DM);
        /* Busco OWNER_SA */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_SA#', OWNER_SA);
        /* Busco OWNER_T */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_T#', OWNER_T);
        /* Busco OWNER_MTDT */
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_MTDT#', OWNER_MTDT);
      end if;
      return cadena_resul;
    end;

/************/
  

  function procesa_campo_filter (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(100);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco #VAR_FCH_CARGA# */
        sustituto := ' to_date ( fch_datos_in, ''yyyymmdd'') ';
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#VAR_FCH_CARGA#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#VAR_FCH_CARGA#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
          --pos := pos_ant;
        end loop;
        /* Busco #VAR_PROFUNDIDAD_BAJAS# */
        sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de #VAR_PROFUNDIDAD_BAJAS#. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#VAR_PROFUNDIDAD_BAJAS#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#VAR_PROFUNDIDAD_BAJAS#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_DM */
        sustituto := OWNER_DM;
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_DM#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_DM#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_SA */
        sustituto := OWNER_SA; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_SA#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_SA#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_T */
        sustituto := OWNER_T; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_T#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_T#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_MTDT */
        sustituto := OWNER_MTDT; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_MTDT#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_MTDT#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;        
      end if;
      return cadena_resul;
    end;

  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (2000);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(500);
    cad_seg         VARCHAR(500);
    cadena            VARCHAR(200);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(200);
    condicion_pro         VARCHAR2(200);
    constante         VARCHAR2(100);
    posicion_ant    PLS_integer;
    pos                    PLS_integer;
    cadena_resul  VARCHAR(2000);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(2000);
    cola                   VARCHAR2(2000);
    pos_ant            PLS_integer;
    v_encontrado  VARCHAR2(1);
    v_alias             VARCHAR2(10000);
    table_columns_lkup  list_strings := list_strings();
    ie_column_lkup    list_strings := list_strings();
    tipo_columna  VARCHAR2(30);
    mitabla_look_up VARCHAR2(2000);
    l_registro          ALL_TAB_COLUMNS%rowtype;
    --l_registro1         ALL_TAB_COLUMNS%rowtype;
    l_registro1         v_MTDT_CAMPOS_DETAIL%rowtype;
    v_value VARCHAR(200);
    nombre_campo  VARCHAR2(300);
    v_alias_incluido PLS_Integer:=0;
    v_table_look_up varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_reg_table_lkup varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_alias_table_look_up varchar2(10000);  /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_no_se_generara_case             BOOLEAN:=false;
    v_existe_valor  BOOLEAN;
    v_numero_campos PLS_integer;
    v_nombre_tabla_reducido varchar2(50);
    v_nombre_paquete varchar2(50);
    
  begin
    /* Seleccionamos el escenario primero */
      case reg_detalle_in.RUL
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
      when 'LKUPC' then
        /* (20150626) Angel Ruiz.  Se trata de hacer el LOOK UP con la tabla dimension de manera condicional */
        l_FROM.extend;
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          v_alias := 'LKUP_' || l_FROM.count;
          mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
          /* (20161111) Angel Ruiz. NF. Puede haber ALIAS EN LA TABLA DE LOOUP */
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_incluido := 1;
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            v_alias_incluido := 0;
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
          end if;
            
          
          /* (20161111) Angel Ruiz. NF FIN. Puede haber ALIAS EN LA TABLA DE LOOKUP */
        
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          --v_encontrado:='N';
          --FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          --LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              --v_encontrado:='Y';
            --end if;
          --END LOOP;
          --if (v_encontrado='Y') then
            --v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          --else
            --v_alias := reg_detalle_in.TABLE_LKUP;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP;
          --end if;
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);

        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        
        /* Construyo el campo de SELECT */
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          valor_retorno := 'CASE WHEN (';
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
          
            if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
              if (indx = 1) then
                valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
              else
                valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
              end if;
            else 
              if (indx = 1) then
                valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
              else
                valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
              end if;
            end if;
          END LOOP;
          valor_retorno := valor_retorno || ') THEN -3 ELSE ' || proc_campo_value_condicion(reg_detalle_in.LKUP_COM_RULE, 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)') || ' END';
        else
          valor_retorno :=  proc_campo_value_condicion (reg_detalle_in.LKUP_COM_RULE, 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)');
        end if;
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
            if (l_WHERE.count = 1) then
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = reg_detalle_in.IE_COLUMN_LKUP;
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
      when 'LKUP' then
        /* Se trata de hacer el LOOK UP con la tabla dimension */
        /* (20150126) Angel Ruiz. Primero recojo la tabla del modelo con la que se hace LookUp. NO puede ser tablas T_* sino su equivalesnte del modelo */
        dbms_output.put_line('ESTOY EN EL LOOKUP. Al principio');
        dbms_output.put_line('El campo es: ' || reg_detalle_in.TABLE_COLUMN);
        l_FROM.extend;
        /* (20150130) Angel Ruiz */
        /* Nueva incidencia. */
        if (regexp_instr (reg_detalle_in.TABLE_LKUP,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$')) then
          /* (20160629) Angel Ruiz. NF: Se aceptan tablas de LKUP que son SELECT que ademas tienen un ALIAS */
            v_alias := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            --mitabla_look_up := reg_detalle_in.TABLE_LKUP;
            mitabla_look_up := procesa_campo_filter_dinam(reg_detalle_in.TABLE_LKUP);
            v_alias_incluido := 1;
            dbms_output.put_line('EXISTE ALIAS EN LA QUERY TABLE_LKUP');
          else
            v_alias := 'LKUP_' || l_FROM.count;
            mitabla_look_up := '(' || procesa_campo_filter_dinam(reg_detalle_in.TABLE_LKUP) || ') "LKUP_' || l_FROM.count || '"';
            --mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
            v_alias_incluido := 0;
            dbms_output.put_line('NO EXISTE ALIAS EN LA QUERY TABLE_LKUP');
          end if;
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else

          /* (20161111) Angel Ruiz. NF. Puede haber ALIAS EN LA TABLA DE LOOUP */
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter_dinam(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_incluido := 1;
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            v_alias_incluido := 0;
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
          end if;
            
          
          /* (20161111) Angel Ruiz. NF FIN. Puede haber ALIAS EN LA TABLA DE LOOKUP */
        
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM */
          --v_encontrado:='N';
          --FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          --LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              --v_encontrado:='Y';
            --end if;
          --END LOOP;
          --if (v_encontrado='Y') then
            --v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          --else
            --v_alias := reg_detalle_in.TABLE_LKUP;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP;
          --end if;
        end if;

        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        /* (20160302) Angel Ruiz. NF: Campos separados por ; */
        --table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        --ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        
        /*************************************************************************/
        /* (20170109) Angel Ruiz. BUG. Existen ocasiones en las que no es posible */
        /* hacer el CASE WHEN para comprobar si los campos vienen NO INFORMADO */
        /* porque las columnas por las que se hacen JOIN poseen muchas funciones */
        /* Compruebo antes si sera posible generar un CASE WHEN */
        /*************************************************************************/
        v_no_se_generara_case:=false;
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0)
            then
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              v_existe_valor:=false;
              /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
              --for registro in (SELECT * FROM ALL_TAB_COLUMNS
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=false) then
                v_no_se_generara_case:=true;
              end if;
            else
              v_existe_valor:=false;
              /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
              --for registro in (SELECT * FROM ALL_TAB_COLUMNS
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=false) then
                v_no_se_generara_case:=true;
              end if;
            end if;
          END LOOP;
        else
          v_existe_valor:=false;
          /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
          --for registro in (SELECT * FROM ALL_TAB_COLUMNS
          for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
          WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
          UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN)))
          loop
            v_existe_valor:=true;
          end loop;
          if (v_existe_valor=false) then
            v_no_se_generara_case:=true;
          end if;
        end if;
        /* (20170109) Angel Ruiz. FIN BUG.*/
        

        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
            if (v_no_se_generara_case = false) then /*(20170207) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
              if ((regexp_like (trim(reg_detalle_in.TABLE_COLUMN), '^CVE_') = true) or (regexp_like (trim(reg_detalle_in.TABLE_COLUMN), '^ID_') = true)) then
                /* (20221017) Angel Ruiz. NF: Solo se generan NVL si el campo es CVE_ o es ID_ */
                valor_retorno := 'CASE WHEN (';
                v_numero_campos:=0;
                FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
                LOOP
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
                  (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
                  (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
                  (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
                  (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0)
                  then
                    --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
                    /* (20161117) Angel Ruiz. NF: Pueden venir funciones en los campos de join como */
                    /* UPPER, NVL, DECODE, ... */
                    nombre_campo := extrae_campo (ie_column_lkup(indx));
                    v_existe_valor:=false;
                    /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                    --for registro in (SELECT * FROM ALL_TAB_COLUMNS
                    for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
                    WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
                    UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
                    loop
                      v_existe_valor:=true;
                    end loop;
                    if (v_existe_valor=true) then
                      /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                      SELECT * INTO l_registro1
                      FROM v_MTDT_CAMPOS_DETAIL
                      WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                      COLUMN_NAME = TRIM(nombre_campo);
                    end if;
                  else
                    dbms_output.put_line ('El campo por el que voy a hacer LookUp de la TABLE_BASE es: ' || TRIM(ie_column_lkup(indx)));
                    v_existe_valor:=false;
                    /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                    for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
                    WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
                    UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
                    loop
                      v_existe_valor:=true;
                    end loop;
                    if (v_existe_valor=true) then
                      /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                      SELECT * INTO l_registro1
                      FROM v_MTDT_CAMPOS_DETAIL
                      WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                      COLUMN_NAME = TRIM(ie_column_lkup(indx));
                    end if;
                  end if;
                  if (v_existe_valor=true) then
                    v_numero_campos:=v_numero_campos+1;
                    if (instr(trim(l_registro1.TYPE), 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
                      if (v_numero_campos = 1) then
                        /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                        if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                          valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                        else
                          /* (20190723) Angel Ruiz. Meto una excepcion por una cosa que arregla Stephany en producción*/
                          if ((reg_detalle_in.TABLE_NAME = 'DMF_MOVIMIENTOS_SERIADOS' and reg_detalle_in.TABLE_COLUMN = 'CVE_ALMACEN' and l_registro.COLUMN_NAME = 'COD_CENTRO')
                          OR (reg_detalle_in.TABLE_NAME = 'DMF_MOVIMIENTOS_SERIADOS' and reg_detalle_in.TABLE_COLUMN = 'CVE_ALMACEN_DESTINO' and l_registro.COLUMN_NAME = 'COD_CENTRO_DESTINO')) then
                            valor_retorno := valor_retorno || 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ', -1) IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                          else
                            valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                          end if;
                        end if;
                      else
                        /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                        if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                          valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                        else
                          /* (20190723) Angel Ruiz. Meto una excepcion por una cosa que arregla Stephany en producción*/
                          if ((reg_detalle_in.TABLE_NAME = 'DMF_MOVIMIENTOS_SERIADOS' and reg_detalle_in.TABLE_COLUMN = 'CVE_ALMACEN' and l_registro.COLUMN_NAME = 'COD_CENTRO')
                          OR (reg_detalle_in.TABLE_NAME = 'DMF_MOVIMIENTOS_SERIADOS' and reg_detalle_in.TABLE_COLUMN = 'CVE_ALMACEN_DESTINO' and l_registro.COLUMN_NAME = 'COD_CENTRO_DESTINO')) then
                            valor_retorno := valor_retorno || 'OR NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ', -1) IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                          else                        
                            valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                          end if;
                        end if;
                      end if;
                    else 
                      if (v_numero_campos = 1) then
                        /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                        if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                          valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                        else
                          valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                        end if;
                      else
                        /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                        if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                          valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                        else
                          valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                        end if;
                      end if;
                    end if;
                  end if; /* if (v_existe_valor=true) then */
                END LOOP;
                /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
                SELECT * INTO l_registro1
                --FROM ALL_TAB_COLUMNS
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(TRIM(reg_detalle_in.TABLE_NAME)) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN));
                dbms_output.put_line ('Estoy donde quiero.');
                dbms_output.put_line ('El nombre de TABLE_NAME ES: ' || reg_detalle_in.TABLE_NAME);
                dbms_output.put_line ('El nombre de TABLE_COLUMN ES: ' || reg_detalle_in.TABLE_COLUMN);
                dbms_output.put_line ('El tipo de DATOS es: ' || l_registro1.TYPE);
                if (trim(l_registro1.TYPE) = 'NUMBER') then
                  if (v_alias_incluido = 1) then
                  /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                    valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', -2) END';
                  else
                    if (instr(reg_detalle_in.VALUE, '.') = 0) then
                      valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) END';
                    else
                      valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || reg_detalle_in.VALUE || ', -2) END';
                    end if;
                  end if;
                else
                  if (v_alias_incluido = 1) then
                  /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                    valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''') END';
                  else
                    if (instr(reg_detalle_in.VALUE, '.') = 0) then
                      valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''') END';
                    else
                      valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || reg_detalle_in.VALUE || ', ''''GENERICO'''') END';
                    end if;
                  end if;
                end if;              
              
              else
                valor_retorno := procesa_campo_filter_dinam(reg_detalle_in.VALUE);
              end if;
            else /* (20170207) Angel Ruiz. if (v_no_se_generara_case = false) then */
              valor_retorno := procesa_campo_filter_dinam(reg_detalle_in.VALUE);
            end if; /* if (v_no_se_generara_case = false) then */
          else /* if (table_columns_lkup.COUNT > 1) then */
            /* (20221017) Angel Ruiz. NF: Solo se generan NVL si el campo es CVE_ o es ID_ */
            if ((regexp_like (trim(reg_detalle_in.TABLE_COLUMN), '^CVE_') = true) or (regexp_like (trim(reg_detalle_in.TABLE_COLUMN), '^ID_') = true)) then
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              SELECT * INTO l_registro1
              --FROM ALL_TAB_COLUMNS
              FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(TRIM(reg_detalle_in.TABLE_NAME)) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN));
              dbms_output.put_line ('==> ==> El valor del tipo del campo (COLUMN_NAME) de: ' || l_registro1.COLUMN_NAME || ' es: ' || l_registro1.TYPE );
              dbms_output.put_line ('==> ==> El valor del tipo del campo (TABLE_COLUMN) de: ' || reg_detalle_in.TABLE_COLUMN || ' es: ' || l_registro1.TYPE );
              if (trim(l_registro1.TYPE) = 'NUMBER') then
                dbms_output.put_line ('==> ==> Entro en el caso en que lregistro1.TYPE=''NUMBER''');
                if (v_alias_incluido = 1) then
                  valor_retorno :=  '    NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', -2)';
                else
                  if (regexp_instr(reg_detalle_in.VALUE, '[Cc][Aa][Ss][Ee]') > 0) then
                    valor_retorno :=  '    NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', -2)';
                  elsif (instr(reg_detalle_in.VALUE, '.') = 0) then
                    valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)';
                  else
                    valor_retorno :=  '    NVL(' || reg_detalle_in.VALUE || ', -2)';
                  end if;
                end if;
                dbms_output.put_line ('==> ==> EL VALOR DE RETORNO ES: ' || valor_retorno);
              elsif (trim(l_registro1.TYPE) = 'DATE') then
                /* (20190520) Angel Ruiz. Hay un BUG. Es el caso de que el tipo de campo sea una fecha. En ese caso no debe poner NVL*/
                if (v_alias_incluido = 1) then
                  valor_retorno :=  '    ' || procesa_campo_filter_dinam(reg_detalle_in.VALUE);
                else
                  if (regexp_instr(reg_detalle_in.VALUE, '[Cc][Aa][Ss][Ee]') > 0) then
                    valor_retorno :=  '    ' || procesa_campo_filter_dinam(reg_detalle_in.VALUE);
                  elsif (instr(reg_detalle_in.VALUE, '.') = 0) then
                    valor_retorno :=  '    ' || v_alias || '.' || reg_detalle_in.VALUE;
                  else
                    valor_retorno :=  '    ' || reg_detalle_in.VALUE;
                  end if;
                end if;              
              else
                if (v_alias_incluido = 1) then
                  if (trim(l_registro1.LENGTH) = '1') then
                    valor_retorno :=  '    NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', ''''G'''')';
                  elsif (trim(l_registro1.LENGTH) = '2') then
                    valor_retorno :=  '    NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', ''''GE'''')';
                  elsif (trim(l_registro1.LENGTH) = '3') then
                    valor_retorno :=  '    NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', ''''GE#'''')';
                  else
                    valor_retorno :=  '    NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''')';
                  end if;
                else
                  if (instr(reg_detalle_in.VALUE, '.') = 0) then
                    if (trim(l_registro1.LENGTH) = '1') then
                      valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''G'''')';
                    elsif (trim(l_registro1.LENGTH) = '2') then
                      valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GE'''')';
                    elsif (trim(l_registro1.LENGTH) = '3') then
                      valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GE#'''')';
                    else
                      valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''')';
                    end if;
                  else
                    if (trim(l_registro1.LENGTH) = '1') then
                      valor_retorno :=  '    NVL(' || reg_detalle_in.VALUE || ', ''''G'''')';
                    elsif(trim(l_registro1.LENGTH) = '2') then
                      valor_retorno :=  '    NVL(' || reg_detalle_in.VALUE || ', ''''GE'''')';
                    elsif (trim(l_registro1.LENGTH) = '3') then
                      valor_retorno :=  '    NVL(' || reg_detalle_in.VALUE || ', ''''GE#'''')';
                    else
                      valor_retorno :=  '    NVL(' || reg_detalle_in.VALUE || ', ''''GENERICO'''')';
                    end if;
                  end if;
                end if;
              end if;
            else
              valor_retorno := procesa_campo_filter_dinam(reg_detalle_in.VALUE);
            end if;
          end if;

        end if;
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. En la parte del Where. Varias Columnas. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. En la parte del Where. Varias Columnas. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            /************************/
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0)
            then
              --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
              /* (20161117) Angel Ruiz. NF: Pueden venir funciones en los campos de join como */
              /* UPPER, NVL, DECODE, ... */
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              dbms_output.put_line ('El campo del WHERE ES #####: ' || nombre_campo);
              /****************************************/
              /* (20170207) Angel Ruiz. BUG. Hay campos de los q no se puede hayar su tipo pq tienen muchas funciones */
              /****************************************/
              v_existe_valor:=false;
              /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                SELECT * INTO l_registro1
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                COLUMN_NAME = TRIM(nombre_campo);
              end if;
            else
              v_existe_valor:=false;
              /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor = true) then
                /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                SELECT * INTO l_registro1
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                COLUMN_NAME = TRIM(ie_column_lkup(indx));
              end if;
            end if;
            if (l_WHERE.count = 1) then
              if (v_existe_valor = true) then /* (20170207) Angel Ruiz. BUG. Pueden venir campos que no esten en el diccionario */
                if (instr(l_registro1.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (l_registro1.LENGTH <3 and l_registro1.NULABLE = 'Y') then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''''NI#'''')' || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''''NI#'''')' || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  'NVL(' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) := transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                    else
                      l_WHERE(l_WHERE.last) := transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    end if;
                  elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    end if;
                  elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    end if;
                  elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                    end if;
                  elsif (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) := transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                    else
                      l_WHERE(l_WHERE.last) := transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                    end if;
                  else
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                if (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                  else
                    l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                  end if;
                else
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  sustituye_comillas_dinam(ie_column_lkup(indx)) || ' = ' || sustituye_comillas_dinam(table_columns_lkup(indx)) || ' (+)';
                  else
                    l_WHERE(l_WHERE.last) :=  sustituye_comillas_dinam(ie_column_lkup(indx)) || ' = ' || sustituye_comillas_dinam(table_columns_lkup(indx));
                  end if;
                end if;
              end if; /* if (v_existe_valor = true) then */
            else /* if (l_WHERE.count = 1) then */
              if (v_existe_valor = true) then /* (20170207) Angel Ruiz. BUG. Pueden venir campos que no esten en el diccionario */
                if (instr(l_registro1.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (l_registro1.LENGTH <3 and l_registro1.NULABLE = 'Y') then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  end if;
                else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */                
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(table_columns_lkup(indx), v_alias);
                      end if;
                    elsif (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                end if;
              else
                if (regexp_instr(ie_column_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(table_columns_lkup(indx), '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 1);
                  else
                    l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(table_columns_lkup(indx), v_alias, 0);
                  end if;
                else
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  ' AND ' || sustituye_comillas_dinam(ie_column_lkup(indx)) || ' = ' || sustituye_comillas_dinam(table_columns_lkup(indx)) || ' (+)';
                  else
                    l_WHERE(l_WHERE.last) :=  ' AND ' || sustituye_comillas_dinam(ie_column_lkup(indx)) || ' = ' || sustituye_comillas_dinam(table_columns_lkup(indx));
                  end if;
                end if;
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('#ESTOY EN EL LOOKUP. En la parte del Where. Una sola columna. La Tabla es: $' || reg_detalle_in.TABLE_BASE_NAME || '$');
            dbms_output.put_line('#ESTOY EN EL LOOKUP. En la parte del Where. UNa sola columna. La Columna es: $' || reg_detalle_in.IE_COLUMN_LKUP || '$');
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0)
            then
            --if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
              --nombre_campo := extrae_campo_decode (reg_detalle_in.IE_COLUMN_LKUP);
              nombre_campo := extrae_campo (reg_detalle_in.IE_COLUMN_LKUP);
              v_existe_valor:=false;
              /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                SELECT * INTO l_registro1
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                COLUMN_NAME = trim(nombre_campo);
              end if;
            else
              v_existe_valor:=false;
              /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_BASE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.IE_COLUMN_LKUP)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                /* (20220923) Angel Ruiz. Sustituyo ALL_TAB_COLUMNS por v_MTDT_CAMPOS_DETAIL */
                SELECT * INTO l_registro1
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                COLUMN_NAME = reg_detalle_in.IE_COLUMN_LKUP;
              end if;
            end if;
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (v_existe_valor = true) then /* (20170207) Angel Ruiz. BUG. Hay columnas que no se encuentran en el metadato */
                if (instr(l_registro1.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (l_registro1.LENGTH <3 and l_registro1.NULABLE = 'Y') then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) := 'NVL(' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) := transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) := transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                    else
                      l_WHERE(l_WHERE.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                    else
                      l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    end if;
                  else
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                end if;
              else  /* if (v_existe_valor = true) then */
                if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                  else
                    l_WHERE(l_WHERE.last) :=  transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  end if;
                else
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    /* (202210) Angel Ruiz. BUG: no tiene en cuenta si está reyeno el campo OUTER o no*/
                    l_WHERE(l_WHERE.last) := sustituye_comillas_dinam(reg_detalle_in.IE_COLUMN_LKUP) ||  ' = ' || sustituye_comillas_dinam(reg_detalle_in.TABLE_COLUMN_LKUP) || ' (+)';
                  else
                    l_WHERE(l_WHERE.last) := sustituye_comillas_dinam(reg_detalle_in.IE_COLUMN_LKUP) ||  ' = ' || sustituye_comillas_dinam(reg_detalle_in.TABLE_COLUMN_LKUP);
                  end if;
                end if;
              end if;
            else  /* sino es el primer campo del Where  */
              if (v_existe_valor = true) then /* (20170207) Angel Ruiz. BUG. Hay columnas que no se encuentran en el metadato */
                if (instr(l_registro1.TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (l_registro1.LENGTH <3 and l_registro1.NULABLE = 'Y') then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                      end if;
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                      end if;
                    else
                      if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                        l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                      else
                        l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  end if;
                else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[D][E][C][O][D][E]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[D][E][C][O][D][E]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias) || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ' = ' || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    end if;
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    end if;
                  else
                    if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                      l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                end if;
              else  /* if (v_existe_valor = true) then */
                if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Ll][Tt][Rr][Ii][Mm]') > 0) then
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                  else
                    l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_funcion_outer(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ' = ' || transformo_funcion_outer(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  end if;
                else
                  if (upper(trim(reg_detalle_in.OUTER)) = 'Y') then
                    l_WHERE(l_WHERE.last) :=  ' AND ' || sustituye_comillas_dinam(reg_detalle_in.IE_COLUMN_LKUP) || ' = ' || sustituye_comillas_dinam(reg_detalle_in.TABLE_COLUMN_LKUP) || ' (+)';
                  else
                    l_WHERE(l_WHERE.last) :=  ' AND ' || sustituye_comillas_dinam(reg_detalle_in.IE_COLUMN_LKUP) || ' = ' || sustituye_comillas_dinam(reg_detalle_in.TABLE_COLUMN_LKUP);
                  end if;
                end if;
              end if;   /* if (v_existe_valor = true) then */
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
        /* (20150306) ANGEL RUIZ. Hay un error que corrijo */
        v_nombre_tabla_reducido := substr(reg_detalle_in.TABLE_NAME, 5);
        if (length(reg_detalle_in.TABLE_NAME) < 25) then
        v_nombre_paquete := reg_detalle_in.TABLE_NAME;
        else
        v_nombre_paquete := v_nombre_tabla_reducido;
        end if;
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
        else
          valor_retorno :=  '    ' || 'PKG_' || v_nombre_paquete || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
        end if;
      when 'DLOAD' then
        valor_retorno :=  '    ' || ''' || ''TO_DATE ('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''';
      when 'DSYS' then
        valor_retorno :=  '    ' || 'SYSDATE';
      when 'CODE' then
        /* 20141204 Angel Ruiz. Como es codigo dinamico he de detectar si hay una comilla para poner dos */
        /* Esto lo añado nuevo y solo en este generador pq genera procesos que soportan retrasados */
        pos := 0;
        posicion_ant := 0;
        cadena_resul:= trim(reg_detalle_in.VALUE);
        lon_cadena := length (cadena_resul);
        if lon_cadena > 0 then
          valor_retorno := procesa_campo_filter_dinam (cadena_resul);
          /* Busco LA COMILLA */
          --sustituto := '''''';
          --loop
            --dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
            --pos := instr(cadena_resul, '''', pos+1);
            --exit when pos = 0;
            --dbms_output.put_line ('Pos es mayor que 0');
            --dbms_output.put_line ('Primer valor de Pos: ' || pos);
            --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
            --dbms_output.put_line ('La cabeza es: ' || cabeza);
            --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
            --cola := substr(cadena_resul, pos + length (''''));
            --dbms_output.put_line ('La cola es: ' || cola);
            --cadena_resul := cabeza || sustituto || cola;
            --pos_ant := pos + length ('''''');
            --pos := pos_ant;
          --end loop;
        end if;
          /************/
        --valor_retorno := '    ' || trim(reg_detalle_in.VALUE);
        --valor_retorno := cadena_resul;
        --posicion := instr(valor_retorno, 'VAR_IVA');
        --if (posicion >0) then
          --cad_pri := substr(valor_retorno, 1, posicion-1);
          --cad_seg := substr(valor_retorno, posicion + length('VAR_IVA'));
          --valor_retorno :=  cad_pri || '21' || cad_seg;
        --end if;
        --posicion := instr(valor_retorno, '#VAR_FCH_CARGA#');
        --if (posicion >0) then
          --cad_pri := substr(valor_retorno, 1, posicion-1);
          --cad_seg := substr(valor_retorno, posicion + length('#VAR_FCH_CARGA#'));
          --valor_retorno :=  cad_pri || ''' || ''TO_DATE ('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''' || cad_seg;
        --end if;
      when 'HARDC' then
        valor_retorno :=  '    ' || sustituye_comillas_dinam(reg_detalle_in.VALUE);
      when 'SEQ' then
        valor_retorno := '    ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
        --  valor_retorno := '    ' || reg_detalle_in.VALUE;
        --else
        --  valor_retorno := '    ' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
      when 'SEQG' then
        valor_retorno := '    ' || ''' || var_seqg || ''';
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;      
      when 'VAR_FCH_INICIO' then
        --valor_retorno :=  '    ' || ''' || var_fch_inicio || ''';
        --valor_retorno :=  '    SYSDATE';
        valor_retorno :=  '    TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')'; /*(20151221) Angel Ruiz BUG. Debe insertarse la fecha de inicio del proceso de insercion */
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          valor_retorno := '    ' || ''' || fch_datos_in || ''';        
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '    ' ||  '1';
        end if;
      when 'LKUPN' then
        /* (20150824) ANGEL RUIZ. Nueva Regla. Permite rescatar un campo numerico de la tabla de look up y hacer operaciones con el */
        l_FROM.extend;
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          v_alias := 'LKUP_' || l_FROM.count;
          mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
          /* (20161111) Angel Ruiz. NF. Puede haber ALIAS EN LA TABLA DE LOOUP */
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_incluido := 1;
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            v_alias_incluido := 0;
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
            end if;
          end if;
            
          
          /* (20161111) Angel Ruiz. NF FIN. Puede haber ALIAS EN LA TABLA DE LOOKUP */
        
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          --v_encontrado:='N';
          --FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          --LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              --v_encontrado:='Y';
            --end if;
          --END LOOP;
          --if (v_encontrado='Y') then
            --v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          --else
            --v_alias := reg_detalle_in.TABLE_LKUP;
            --l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP;
          --end if;
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        /* Le añadimos al nombre del campo de la tabla de LookUp su Alias */
        v_value := proceso_campo_value (reg_detalle_in.VALUE, v_alias);
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_value || ', -2) ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
            valor_retorno := 'CASE WHEN (';
            FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
            LOOP
              SELECT * INTO l_registro
              FROM ALL_TAB_COLUMNS
              WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              COLUMN_NAME = TRIM(ie_column_lkup(indx));
            
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                end if;
              else 
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                end if;
              end if;
            END LOOP;
            valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || v_value || ', -2) END';
          else
            valor_retorno :=  '    NVL(' || v_value || ', -2)';
          end if;

        end if;

        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/

        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
            if (l_WHERE.count = 1) then
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = reg_detalle_in.IE_COLUMN_LKUP;
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
        when 'LKUPD' then
          if (reg_detalle_in.LKUP_COM_RULE is not null) then
            /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
            cadena := trim(reg_detalle_in.LKUP_COM_RULE);
            pos_del_si := instr(cadena, 'SI');
            pos_del_then := instr(cadena, 'THEN');
            pos_del_else := instr(cadena, 'ELSE');
            pos_del_end := instr(cadena, 'END');  
            condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
            condicion_pro := procesa_COM_RULE_lookup(condicion);
            constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
            valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || procesa_campo_filter_dinam(reg_detalle_in.VALUE) || ', '' '') ELSE ' || trim(constante) || ' END';
          else
            valor_retorno := procesa_campo_filter_dinam(reg_detalle_in.VALUE);
          end if;
      end case;
    return valor_retorno;
  end;

/*************/
  function genera_encabezado_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    dbms_output.put_line('Estoy en genera_encabezado_funcion_pkg. Antes de llamar a string coma');
    lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
    dbms_output.put_line('Estoy en genera_encabezado_funcion_pkg. Despues de llamar a string coma');
    if (lkup_columns.COUNT > 1)
    then
      valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (';
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        else
          valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        end if;
      END LOOP;
      valor_retorno := valor_retorno || ') return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE RESULT_CACHE;';
    else        
      valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE RESULT_CACHE;';
    end if;
    dbms_output.put_line('Justo antes de retornar');
    dbms_output.put_line('Retorno es: ' || valor_retorno);
    return valor_retorno;
  end;

/************/

  function gen_encabe_regla_function (reg_function_in in MTDT_TC_FUNCTION%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (250);
    lkup_columns                list_strings := list_strings();
  begin
    valor_retorno := '  FUNCTION ' || 'LK_' || reg_function_in.VALUE || ';';
    return valor_retorno;
  end gen_encabe_regla_function;

  procedure genera_cuerpo_regla_function (reg_function_in in MTDT_TC_FUNCTION%rowtype) is
  begin
    UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || 'LK_' || reg_function_in.VALUE);
    --UTL_FILE.put_line (fich_salida_pkg, '  RESULT_CACHE');
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    dbms_output.put_line (''Aqui iria el cuerpo de la funcion'');');
    UTL_FILE.put_line (fich_salida_pkg, '    /* AQUI IRIA EL CUERPO DE LA FUNCION */');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_function_in.TABLE_LKUP || ';');
  end genera_cuerpo_regla_function;

/************/
  procedure genera_cuerpo_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();

  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
    if (lkup_columns.COUNT > 1)
    then
      valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (';
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        else
          valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        end if;
      END LOOP;
      valor_retorno := valor_retorno || ') ';
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else        
      UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE)'); 
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '    return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE');
    UTL_FILE.put_line (fich_salida_pkg, '    RESULT_CACHE RELIES_ON (' || reg_lookup_in.TABLE_LKUP || ')');
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    UTL_FILE.put_line (fich_salida_pkg, '    SELECT nvl(' || reg_lookup_in.VALUE || ', -2) INTO l_row'); 
    UTL_FILE.put_line (fich_salida_pkg, '    FROM ' || reg_lookup_in.TABLE_LKUP);
    if (lkup_columns.COUNT > 1) then
      valor_retorno := '    WHERE ' ;
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        else
          valor_retorno := valor_retorno || ' and ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        end if;
      END LOOP;
      if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        valor_retorno := valor_retorno || ';';
      else
        valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP_COND || ';';
      end if;
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else
      /* 20141204 Angel Ruiz - Añadido para las tablas de LOOK UP que son un rango */
      if (instr (reg_lookup_in.TABLE_LKUP,'RANGO') > 0) then
        /* Se trata de una tabla de Rango y la trato diferente */
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ';' );
        else
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ' and ' || reg_lookup_in.TABLE_LKUP_COND || ';');
        end if;
      else 
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
        else
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
        end if;
      end if;
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN l_row;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  exception');
    UTL_FILE.put_line (fich_salida_pkg, '  when NO_DATA_FOUND then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '  when others then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '');
 
  end genera_cuerpo_funcion_pkg;



begin
  /* (20141223) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';  
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  SELECT VALOR INTO PAIS FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PAIS_DM';
  /* (20141223) FIN*/

  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    dbms_output.put_line ('Estoy en el primero LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);
    nombre_fich_carga := 'load_he_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql';
    fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
    fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    --nombre_tabla_base_reducido := substr(reg_tabla.TABLE_BASE_NAME, 4); /* Le quito al nombre de la tabla los caracteres SA_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_tabla.TABLE_NAME) < 25) then
      /* (20221006) ANGEL RUIZ. Recorto la longitud d elos nombres.*/
      nombre_proceso := reg_tabla.TABLE_NAME;
      --nombre_proceso := nombre_tabla_reducido;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    /* (20150414) Angel Ruiz. Incidencia. El nombre de la partición es demasiado largo */
    if (length(nombre_tabla_reducido) <= 18) then
      v_nombre_particion := 'PA_' || nombre_tabla_reducido;
    else
      v_nombre_particion := nombre_tabla_reducido;
    end if;
    /* (20151112) Angel Ruiz. BUG. Si el nombre de la tabla es superior a los 19 caracteres*/
    /* El nombre d ela tabla que se crea T_*_YYYYMMDD supera los 30 caracteres y da error*/
    if (length(nombre_tabla_reducido) > 19) then
      nombre_tabla_T := substr(nombre_tabla_reducido,1, length(nombre_tabla_reducido) - (length(nombre_tabla_reducido) - 19));
    else
      nombre_tabla_T := nombre_tabla_reducido;
    end if;
    
    UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AUTHID CURRENT_USER AS');
    lista_scenarios_presentes.delete;
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    dbms_output.put_line ('Comienzo la generacion del PACKAGE DEFINITION');
    dbms_output.put_line ('Antes de mirar funciones para hacer regla FUNCTION');
    /* Segundo miro si hay funciones de la regla FUNCTION para crear */
    
    open MTDT_TC_FUNCTION (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_FUNCTION
      into reg_function;
      exit when MTDT_TC_FUNCTION%NOTFOUND;
      prototipo_fun := gen_encabe_regla_function (reg_function);
      UTL_FILE.put_line(fich_salida_pkg,'');
      UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
    end loop;
    close MTDT_TC_FUNCTION;

    dbms_output.put_line ('Despues de mirar funciones para hacer regla FUNCTION');

    /* Tercero genero los metodos para los escenarios */
    dbms_output.put_line ('Comienzo a generar los metodos para los escenarios');
    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
      /* Elaboramos la implementacion de las funciones de LOOK UP antes de nada */
      
      /* (20161117) Angel Ruiz. Tenemos cualquier otro escenario */
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
      lista_scenarios_presentes.EXTEND;
      lista_scenarios_presentes(lista_scenarios_presentes.LAST) := reg_scenario.SCENARIO;
      
    end loop; /* fin del LOOP MTDT_SCENARIO  */
    close MTDT_SCENARIO;
    
    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lhe_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');

    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lex_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
    
    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
    UTL_FILE.put_line(fich_salida_pkg, '/' );

    /* GENERACION DEL PACKAGE BODY */
    UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    UTL_FILE.put_line(fich_salida_pkg,'');

    dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION. :-)');
    
    UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_tabla (table_name_in IN VARCHAR2) return number');
    UTL_FILE.put_line(fich_salida_pkg,'  IS');
    UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
    UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_tabla varchar(30);BEGIN select table_name into nombre_tabla from all_tables where table_name = '''''' || table_name_in || '''''' and owner = '''''' || ''' || OWNER_DM || ''' || ''''''; END;'';');
    UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
    UTL_FILE.put_line(fich_salida_pkg,'  exception');
    UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
    UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
    UTL_FILE.put_line(fich_salida_pkg,'  END existe_tabla;');
    UTL_FILE.put_line(fich_salida_pkg,'');
    UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_particion (partition_name_in IN VARCHAR2, table_name_in IN VARCHAR2) return number');
    UTL_FILE.put_line(fich_salida_pkg,'  IS');
    UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
    UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_particion varchar(30);BEGIN select partition_name into nombre_particion from all_tab_partitions where partition_name = '''''' || partition_name_in || '''''' and table_name = '''''' || table_name_in || '''''' and table_owner = '''''' || ''' || OWNER_DM || ''' || ''''''; END;'';');
    UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
    UTL_FILE.put_line(fich_salida_pkg,'  exception');
    UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
    UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
    UTL_FILE.put_line(fich_salida_pkg,'  END existe_particion;');
    /* (20150825) Angel Ruiz. N.F.: Particionado Controlado con un parametro en la tabla MTDT_MODELO_SUMMARY */
    if (reg_tabla.PARTICIONADO = 'M24') then
      UTL_FILE.put_line(fich_salida_pkg,'  PROCEDURE pre_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)'); 
      UTL_FILE.put_line(fich_salida_pkg,'  is'); 
      UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
      UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
      UTL_FILE.put_line(fich_salida_pkg,'   fch_particion number(8);');
      UTL_FILE.put_line(fich_salida_pkg,'   l_anyo_datos PLS_INTEGER;');
      UTL_FILE.put_line(fich_salida_pkg,'   l_num_meses PLS_INTEGER := 12;');
      UTL_FILE.put_line(fich_salida_pkg,'   v_fch_datos VARCHAR2(6);');
      UTL_FILE.put_line(fich_salida_pkg,'   v_fch_particion PLS_INTEGER;');
      UTL_FILE.put_line(fich_salida_pkg,'  begin'); 
      UTL_FILE.put_line(fich_salida_pkg,'    l_anyo_datos := to_number(substr(fch_datos_in, 1, 4));'); 
      /*(20151112) Angel Ruiz BUG*/
      --UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla :=  existe_tabla (' || '''T_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in);');
      UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla :=  existe_tabla (' || '''T_' || nombre_tabla_T || '_' || ''' || fch_datos_in);');
      /* (20151112) Angel Ruiz FIN BUG*/
      UTL_FILE.put_line(fich_salida_pkg,'    if (exis_tabla = 0) then' );      
      UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la tabla */'); 
      UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''CREATE TABLE  ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in || '' TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''' || '' AS SELECT * FROM ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''';');
      UTL_FILE.put_line(fich_salida_pkg,'    else'); 
      UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in;');
      UTL_FILE.put_line(fich_salida_pkg,'    end if;'); 
      UTL_FILE.put_line(fich_salida_pkg,'    /* CREO LAS PARTICIONES PARA LOS DOS ANYOS DE ANALISIS */'); 
      UTL_FILE.put_line(fich_salida_pkg,'    for l_anyo_actu in l_anyo_datos - 1 .. l_anyo_datos'); 
      UTL_FILE.put_line(fich_salida_pkg,'    LOOP');
      UTL_FILE.put_line(fich_salida_pkg,'      for l_mes_actu in 1 .. l_num_meses');
      UTL_FILE.put_line(fich_salida_pkg,'      LOOP');
      UTL_FILE.put_line(fich_salida_pkg,'        v_fch_datos := TO_CHAR(l_anyo_actu) || TRIM(TO_CHAR(l_mes_actu, ''00''));');      
      UTL_FILE.put_line(fich_salida_pkg,'        v_fch_particion := TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(v_fch_datos, ''YYYYMM''), 1), ''YYYYMM''));');      
      UTL_FILE.put_line(fich_salida_pkg,'        exis_partition :=  existe_particion (''' ||  v_nombre_particion || '_' || ''' || v_fch_datos, ''' || reg_tabla.TABLE_NAME || ''');');
      UTL_FILE.put_line(fich_salida_pkg,'        if (exis_partition = 0) then' );      
      UTL_FILE.put_line(fich_salida_pkg,'          /* Creo la particion */'); 
      UTL_FILE.put_line(fich_salida_pkg, '         EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || v_fch_datos || '' VALUES LESS THAN ('' || v_fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
      UTL_FILE.put_line(fich_salida_pkg,'        end if;'); 
      UTL_FILE.put_line(fich_salida_pkg,'      END LOOP;'); 
      UTL_FILE.put_line(fich_salida_pkg,'    END LOOP;'); 
      UTL_FILE.put_line(fich_salida_pkg,'  exception'); 
      UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
      UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
      UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
      UTL_FILE.put_line(fich_salida_pkg,'  end pre_proceso;');      
      /* (20150825) Angel Ruiz. FIN N.F.: */
    else
      /* Miramos si existe escenario de desagregacion para la tabla de hechos */
      /* que estamos cargando. Para de este modo hay que llevar a cabo la desagregacion*/
      v_nombre_tabla_agr := 'No Existe';
      for v_EXISTE_DESAGREGACION in (
        select TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME"
        FROM
          MTDT_TC_SCENARIO
        WHERE MTDT_TC_SCENARIO.TABLE_TYPE = 'A' and
        MTDT_TC_SCENARIO.SCENARIO = 'AGR' and
        instr(trim(MTDT_TC_SCENARIO.TABLE_BASE_NAME), 'T_' || nombre_tabla_reducido) >0)
      loop
        v_nombre_tabla_agr := v_EXISTE_DESAGREGACION.TABLE_NAME;
      end loop;
      if (v_nombre_tabla_agr = 'No Existe') then
        /* Si no existe escenario de desagregacion usamos un prototipo diferente de la funcion que si existe */
        UTL_FILE.put_line(fich_salida_pkg,'  PROCEDURE pre_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
      else
        UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION pre_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2) return number');      
      end if;
      UTL_FILE.put_line(fich_salida_pkg,'  is'); 
      UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla number(1);');
      UTL_FILE.put_line(fich_salida_pkg,'    exis_partition number(1);');
      UTL_FILE.put_line(fich_salida_pkg,'    fch_particion number(8);');
      if (v_nombre_tabla_agr <> 'No Existe') then
        /* Existe escenario de desagregacion */
        /* Hay que definir las variables donde se almacenan los registros de desagregacion */
        UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_dsg NUMBER:=0;');
        UTL_FILE.put_line(fich_salida_pkg, '    numero_reg_del NUMBER:=0;');
      end if;
      UTL_FILE.put_line(fich_salida_pkg,'  begin'); 
      /*(20151112) Angel Ruiz BUG*/
      --UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla :=  existe_tabla (' || '''T_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in);');
      UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla :=  existe_tabla (' || '''T_' || nombre_tabla_T || '_' || ''' || fch_datos_in);');
      /* (20151112) Angel Ruiz FIN BUG*/
      UTL_FILE.put_line(fich_salida_pkg,'    if (exis_tabla = 0) then' );      
      UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la tabla */'); 
      --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''CREATE TABLE  ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in || '' TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''' || '' AS SELECT * FROM ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''';');
      UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''CREATE TABLE  ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in || '' TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''' || '' AS SELECT * FROM ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''';');
      UTL_FILE.put_line(fich_salida_pkg,'    else'); 
      --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in;');
      UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in;');
      UTL_FILE.put_line(fich_salida_pkg,'    end if;'); 
      UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_NUMBER(TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD'')+1,''YYYYMMDD''));'); 
      --UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''PA_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in, ''' || reg_tabla.TABLE_NAME || ''');');
      UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (''' ||  v_nombre_particion || '_' || ''' || fch_datos_in, ''' || reg_tabla.TABLE_NAME || ''');');
      UTL_FILE.put_line(fich_salida_pkg,'    if (exis_partition = 0) then' );      
      UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la particion */'); 
      --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') UPDATE INDEXES'';');
      --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
      UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
      if (v_nombre_tabla_agr <> 'No Existe') then
        UTL_FILE.put_line(fich_salida_pkg,'    else'); 
        UTL_FILE.put_line(fich_salida_pkg,'      if (forzado_in = ''F'') then' );
        --UTL_FILE.put_line(fich_salida_pkg,'        EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' TRUNCATE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in;');
        /* (20160108) Angel Ruiz NF: AGREGACION */
        /* ------------------------------------ */
        UTL_FILE.put_line(fich_salida_pkg,'        /* Estamos ejecutando la transformacion en modo forzado */');
        UTL_FILE.put_line(fich_salida_pkg,'        FOR fecha_datos_cargada IN (');
        UTL_FILE.put_line(fich_salida_pkg,'          SELECT AGREGADO.FCH_DATOS FCH_DATOS, AGREGADO.FCH_REGISTRO FCH_REGISTRO'); 
        UTL_FILE.put_line(fich_salida_pkg,'          FROM');
        UTL_FILE.put_line(fich_salida_pkg,'          (');
        UTL_FILE.put_line(fich_salida_pkg,'            SELECT TO_CHAR(MTDT_MONITOREO.FCH_DATOS, ''YYYYMMDD'') FCH_DATOS, TO_CHAR(MTDT_MONITOREO.FCH_REGISTRO, ''YYYYMMDDHH24MISS'') FCH_REGISTRO , ROW_NUMBER() OVER (PARTITION BY MTDT_MONITOREO.FCH_DATOS ORDER BY MTDT_MONITOREO.FCH_REGISTRO DESC) RN');
        UTL_FILE.put_line(fich_salida_pkg,'            FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' );
        UTL_FILE.put_line(fich_salida_pkg,'              ' || OWNER_MTDT || '.MTDT_PASO, ' || OWNER_MTDT || '.MTDT_RESULTADO' );
        UTL_FILE.put_line(fich_salida_pkg,'            WHERE');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_he_' || reg_tabla.TABLE_NAME || '.sh'' AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.CVE_PASO = 1 AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_PROCESO.CVE_PROCESO = MTDT_PASO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_PASO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_RESULTADO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_RESULTADO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_RESULTADO.CVE_RESULTADO = MTDT_MONITOREO.CVE_RESULTADO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'') AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.FCH_DATOS = TO_DATE(' || 'fch_datos_in, ''yyyymmdd'')');
        UTL_FILE.put_line(fich_salida_pkg,'          ) AGREGADO');
        UTL_FILE.put_line(fich_salida_pkg,'          WHERE AGREGADO.RN = 1');
        /* (20160222) Angel Ruiz. Hago esta modificacion para excluir aquellas desagregaciones que ya esten hechas */
        UTL_FILE.put_line(fich_salida_pkg,'          AND NOT EXISTS (');
        UTL_FILE.put_line(fich_salida_pkg,'          SELECT AGREGADO_2.FCH_DATOS FCH_DATOS, AGREGADO_2.FCH_REGISTRO FCH_REGISTRO'); 
        UTL_FILE.put_line(fich_salida_pkg,'          FROM');
        UTL_FILE.put_line(fich_salida_pkg,'          (');
        UTL_FILE.put_line(fich_salida_pkg,'            SELECT TO_CHAR(MTDT_MONITOREO.FCH_DATOS, ''YYYYMMDD'') FCH_DATOS, TO_CHAR(MTDT_MONITOREO.FCH_REGISTRO, ''YYYYMMDDHH24MISS'') FCH_REGISTRO , ROW_NUMBER() OVER (PARTITION BY MTDT_MONITOREO.FCH_DATOS ORDER BY MTDT_MONITOREO.FCH_REGISTRO DESC) RN');
        UTL_FILE.put_line(fich_salida_pkg,'            FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' );
        UTL_FILE.put_line(fich_salida_pkg,'              ' || OWNER_MTDT || '.MTDT_PASO, ' || OWNER_MTDT || '.MTDT_RESULTADO' );
        UTL_FILE.put_line(fich_salida_pkg,'            WHERE');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_ds_' || v_nombre_tabla_agr || '.sh'' AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.CVE_PASO = 1 AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_PROCESO.CVE_PROCESO = MTDT_PASO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_PASO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_RESULTADO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_RESULTADO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_RESULTADO.CVE_RESULTADO = MTDT_MONITOREO.CVE_RESULTADO AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'') AND');
        UTL_FILE.put_line(fich_salida_pkg,'              MTDT_MONITOREO.FCH_DATOS = TO_DATE(' || 'fch_datos_in, ''yyyymmdd'')');
        UTL_FILE.put_line(fich_salida_pkg,'          ) AGREGADO_2');
        UTL_FILE.put_line(fich_salida_pkg,'          WHERE AGREGADO_2.RN = 1 AND AGREGADO.FCH_DATOS = AGREGADO_2.FCH_DATOS');
        UTL_FILE.put_line(fich_salida_pkg,'          AND AGREGADO.FCH_REGISTRO = AGREGADO_2.FCH_REGISTRO))');
        /* (20160222) Angel Ruiz. FIN MODIFICACION */
        UTL_FILE.put_line(fich_salida_pkg,'        LOOP' );
        /* La tabla de hechos de la que estamos generando la transformacion posee desagregacion */
        /* Por lo que al ser ejecutada en modo Forzado, debemos generar el codigo para desagregar */
        v_nombre_tabla_agr_redu := substr(v_nombre_tabla_agr, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
        /* (20151112) Angel Ruiz. BUG. Si el nombre de la tabla es superior a los 19 caracteres*/
        /* El nombre de la tabla que se crea T_*_YYYYMMDD supera los 30 caracteres y da error*/
        if (length(v_nombre_tabla_agr_redu) > 19) then
          nombre_tabla_T_agr := substr(v_nombre_tabla_agr_redu,1, length(v_nombre_tabla_agr_redu) - (length(v_nombre_tabla_agr_redu) - 19));
        else
          nombre_tabla_T_agr := v_nombre_tabla_agr_redu;
        end if;
        if (length(v_nombre_tabla_agr) < 25) then
          v_nombre_proceso_agr := v_nombre_tabla_agr;
        else
          v_nombre_proceso_agr := v_nombre_tabla_agr_redu;
        end if;
        UTL_FILE.put_line(fich_salida_pkg,'          /* Hago la desagregacion */');
        UTL_FILE.put_line(fich_salida_pkg,'');
        UTL_FILE.put_line(fich_salida_pkg,'          /* Creo la tabla temporal sobre la voy a copiar los reg. a desagregar */');
        UTL_FILE.put_line(fich_salida_pkg,'          exis_tabla :=  existe_tabla (' || '''T_DSG_' || nombre_tabla_T_agr || ''' );');
        UTL_FILE.put_line(fich_salida_pkg,'          if (exis_tabla = 0) then' );      
        UTL_FILE.put_line(fich_salida_pkg,'            /* Creo la tabla */'); 
        UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE '''); 
        UTL_FILE.put_line(fich_salida_pkg,'            CREATE TABLE  ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T_agr || ' TABLESPACE ' || reg_tabla.TABLESPACE);
        UTL_FILE.put_line(fich_salida_pkg,'            AS SELECT * FROM ' ||  OWNER_DM || '.' || reg_tabla.TABLE_NAME);
        UTL_FILE.put_line(fich_salida_pkg,'            WHERE CVE_DIA =  '' || fecha_datos_cargada.FCH_DATOS || '' AND FCH_REGISTRO =  TO_DATE('''''' || fecha_datos_cargada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');  
        UTL_FILE.put_line(fich_salida_pkg,'          else'); 
        UTL_FILE.put_line(fich_salida_pkg,'            /* Borro la tabla */'); 
        UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T_agr || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE '''); 
        UTL_FILE.put_line(fich_salida_pkg,'            CREATE TABLE  ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T_agr || ' TABLESPACE ' || reg_tabla.TABLESPACE);
        UTL_FILE.put_line(fich_salida_pkg,'            AS SELECT * FROM ' ||  OWNER_DM || '.' || reg_tabla.TABLE_NAME);
        UTL_FILE.put_line(fich_salida_pkg,'            WHERE CVE_DIA =  '' || fecha_datos_cargada.FCH_DATOS || '' AND FCH_REGISTRO =  TO_DATE('''''' || fecha_datos_cargada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');  
        UTL_FILE.put_line(fich_salida_pkg,'          end if;'); 
      
        UTL_FILE.put_line(fich_salida_pkg,'          numero_reg_dsg := ' || OWNER_DM || '.pkg_' || v_nombre_proceso_agr || '.' || 'dsg_' || v_nombre_proceso_agr || ' (fch_carga_in, fecha_datos_cargada.FCH_DATOS, fecha_datos_cargada.FCH_REGISTRO);');        

        UTL_FILE.put_line(fich_salida_pkg,'          ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ds_' || v_nombre_tabla_agr || '.sh'',' || '1, 0, to_date(fecha_datos_cargada.FCH_REGISTRO, ''yyyymmddhh24miss''), systimestamp, to_date(fecha_datos_cargada.FCH_DATOS, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), 0, numero_reg_dsg);');
        UTL_FILE.put_line(fich_salida_pkg,'          COMMIT;'); /* (20160222) Angel Ruiz. Añadido */
        UTL_FILE.put_line(fich_salida_pkg,'          /* Borro la tabla temporal para desagregacion */');
        UTL_FILE.put_line(fich_salida_pkg,'          EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T_agr || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'');      
        UTL_FILE.put_line(fich_salida_pkg,'          /* Borramos los registros una vez desagregados */');
        UTL_FILE.put_line(fich_salida_pkg,'          EXECUTE IMMEDIATE ''DELETE ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' WHERE CVE_DIA = '' || fecha_datos_cargada.FCH_DATOS || '' AND FCH_REGISTRO = TO_DATE('''''' || fecha_datos_cargada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');
        UTL_FILE.put_line(fich_salida_pkg,'          numero_reg_del := sql%rowcount;');
        UTL_FILE.put_line(fich_salida_pkg,'          dbms_output.put_line (''El numero de registros borrados es: '' || numero_reg_del);'); 
        UTL_FILE.put_line(fich_salida_pkg,'        END LOOP;' );
        UTL_FILE.put_line(fich_salida_pkg,'      end if;');
      end if;
      UTL_FILE.put_line(fich_salida_pkg,'    end if;');
      if (v_nombre_tabla_agr <> 'No Existe') then
        UTL_FILE.put_line(fich_salida_pkg,'    return numero_reg_del;');
      end if;
      /* (20160108) Angel Ruiz FIN NF: AGREGACION */
      /* ------------------------------------ */

      UTL_FILE.put_line(fich_salida_pkg,'  exception');
      UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
      UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
      UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
      UTL_FILE.put_line(fich_salida_pkg,'  end pre_proceso;');
      /* (20160308) Angel Ruiz. NF: Preparo el script para generar cualquier tipo de procesos para cargar */
      /* tablas de hechos, tanto del tipo con retrasados como del tipo sin retrasados */
      v_existen_retrasados := 'N';  /* Por defecto las tablas de Staging no tienen retrasados */
      /* Me quedo con el nombre de la tabla base sin el calificador de propietario */
      for v_EXISTEN_DELAYED in (
        select
        TRIM(MTDT_INTERFACE_SUMMARY.DELAYED) "DELAYED"
        from
          MTDT_INTERFACE_SUMMARY, MTDT_TC_SCENARIO
        WHERE
          INSTR(MTDT_TC_SCENARIO.TABLE_BASE_NAME, 'SELECT') = 0 and /* No uso las tablas q tienen un SELECT */
          INSTR(MTDT_TC_SCENARIO.TABLE_BASE_NAME, 'select') = 0 and /* ya que no podemos hayar la conexion */
          MTDT_TC_SCENARIO.TABLE_TYPE = 'H' and /* entre MTDT_TC_SCENARIO y MTDT_INTERFACE_SUMMARY */
          MTDT_INTERFACE_SUMMARY.CONCEPT_NAME = CAST(substr(substr(MTDT_TC_SCENARIO.TABLE_BASE_NAME, instr(MTDT_TC_SCENARIO.TABLE_BASE_NAME, '.') + 1), instr(substr(MTDT_TC_SCENARIO.TABLE_BASE_NAME, instr(MTDT_TC_SCENARIO.TABLE_BASE_NAME, '.') + 1), '_')+1) AS VARCHAR2(40)) and
          MTDT_TC_SCENARIO.TABLE_NAME = reg_tabla.TABLE_NAME
      )
      loop
        v_existen_retrasados := v_EXISTEN_DELAYED.DELAYED;
      end loop;
      if (v_existen_retrasados = 'S') then
        /* Tenemos un proceso de hechos que va a cargar una tabla que podra tener retarasados */
        UTL_FILE.put_line(fich_salida_pkg,'  function pos_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2) return number'); 
        UTL_FILE.put_line(fich_salida_pkg,'  is'); 
        UTL_FILE.put_line(fich_salida_pkg,'    valor_retorno number;'); 
        UTL_FILE.put_line(fich_salida_pkg,'  begin'); 
        UTL_FILE.put_line(fich_salida_pkg,'    /* Proceso que se va ha encargar de hacer el pos-procesado despues de insertar */'); 
        UTL_FILE.put_line(fich_salida_pkg,'    /* consistente en comprobar si la particion de ' || reg_tabla.TABLE_NAME || ' de fecha de datos ya tenia datos, para salvaguardarlos si los tenia */');
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND, PARALLEL (T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '') */ INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' select * from ' || OWNER_DM || '.'' || ''' || reg_tabla.TABLE_NAME || ''' || '' where CVE_DIA = '' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND, PARALLEL (T_' || nombre_tabla_T || ''' || ''_'' || fch_datos_in || '') */ INTO ' || OWNER_DM || '.T_' || nombre_tabla_T || ''' || ''_'' || fch_datos_in || '' select * from ' || OWNER_DM || '.'' || ''' || reg_tabla.TABLE_NAME || ''' || '' where CVE_DIA = '' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''El numero de filas salvaguardadas es: '' || SQL%ROWCOUNT);'); 
        UTL_FILE.put_line(fich_salida_pkg,'    valor_retorno := SQL%ROWCOUNT;'); 
        UTL_FILE.put_line(fich_salida_pkg,'    commit;'); 
        UTL_FILE.put_line(fich_salida_pkg,'    return valor_retorno;'); 
        UTL_FILE.put_line(fich_salida_pkg,'  exception'); 
        UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
        UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
        UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
        UTL_FILE.put_line(fich_salida_pkg,'  end pos_proceso;'); 
      end if;
    end if;

    /* (20150825) Angel Ruiz. N.F.: Se trata de una nueva regla SEQG */
    /* Este tipo de Regla solo puede existir una para cada tabla por su propia definicion */
    for var_seq_generales in (
      select value from MTDT_TC_DETAIL
      where rul = 'SEQG' and TRIM(TABLE_NAME) = reg_tabla.TABLE_NAME)
    --select VALUE into v_nombre_seqg from MTDT_TC_DETAIL
    --where RUL = 'SEQG' and TRIM(TABLE_NAME) = reg_tabla.TABLE_NAME;
    loop
      v_nombre_seqg := var_seq_generales.value;
    end loop;
    /* (20150825) Angel Ruiz. FIN N.F.*/
    
    dbms_output.put_line ('Antes de generar las funciones de FUNCTION');
    /* Segundo de todo miro si tengo que generar los cuerpos de las funciones de FUNCTION */
    open MTDT_TC_FUNCTION (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_FUNCTION
      into reg_function;
      exit when MTDT_TC_FUNCTION%NOTFOUND;
      
      genera_cuerpo_regla_function (reg_function);      
    end loop;
    close MTDT_TC_FUNCTION;
    
    /* Tercero genero los cuerpos de los metodos que implementan los escenarios */
    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
      
      
      
      
      
        /* (20161117) Angel Ruiz. NF: Puede venir cualquier escenario */
        /* CUALQUIER OTRO SCENARIO */
          dbms_output.put_line ('Estoy dentro del scenario $' || reg_scenario.SCENARIO || '$');
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  var_seqg number;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
          UTL_FILE.put_line(fich_salida_pkg, '');
          /* (20150825) Angel Ruiz. N.F.: SEQG */
          if (v_nombre_seqg <> 'N') then
            /* existe una regla para esta tabla de SEQG */
            UTL_FILE.put_line(fich_salida_pkg, '    /* Recupero el valor de la secuencia general para esta tabla */');
            UTL_FILE.put_line(fich_salida_pkg, '    SELECT ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL' || ' into var_seqg FROM DUAL;');
            UTL_FILE.put_line(fich_salida_pkg, '');
          end if;
          /* (20150825) Angel Ruiz. FIN N.F.: SEQG */
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_T || '_'' || fch_datos_in ||');
          /****/
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          UTL_FILE.put_line(fich_salida_pkg,'    ''(');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if primera_col = 1 then
              UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
              primera_col := 0;
            else
              UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_detail.TABLE_COLUMN);
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,')');
          dbms_output.put_line ('Despues del INTO');
          /****/
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          /****/
          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
          l_FROM.delete;
          l_WHERE.delete;
          /* Fin de la inicialización */
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT ');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            columna := genera_campo_select (reg_detail);
            if primera_col = 1 then
              UTL_FILE.put_line(fich_salida_pkg,columna);
              primera_col := 0;
            else
              UTL_FILE.put_line(fich_salida_pkg,',' || columna);
            end if;        
          end loop;
          close MTDT_TC_DETAIL;
          /****/
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/      
          /****/
          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          /****/    
          dbms_output.put_line ('Despues del SELECT');
          --dbms_output.put_line ('El valor que han cogifo v_FROM:' || v_FROM);
          --dbms_output.put_line ('El valor que han cogifo v_WHERE:' || v_WHERE);
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          --UTL_FILE.put_line(fich_salida_pkg, '   app_mvnosa.'  || reg_scenario.TABLE_BASE_NAME || ''' || ''_'' || fch_datos_in;');
          UTL_FILE.put_line(fich_salida_pkg, '   ' || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));
          /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
          v_hay_look_up:='N';
          /* (20150311) ANGEL RUIZ. se produce un error al generar ya que la tabla de hechos no tiene tablas de LookUp */
          if l_FROM.count > 0 then
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
              v_hay_look_up := 'Y';
            END LOOP;
          end if;
          /* FIN */
          --UTL_FILE.put_line(fich_salida_pkg,'    ' || v_FROM);
          dbms_output.put_line ('Despues del FROM');
          if (reg_scenario.FILTER is not null) then
            /* Procesamos el campo FILTER */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            dbms_output.put_line ('Antes de procesar el campo FILTER');
            campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
            dbms_output.put_line ('Despues de procesar el campo FILTER');
            if (v_hay_look_up = 'Y') then
            /* Hay tablas de LookUp. Hay que poner las condiciones de los Where*/
              dbms_output.put_line ('Entro en el que hay Tablas de LookUp');          
              /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
              UTL_FILE.put_line(fich_salida_pkg, '   ' || 'AND');
              FOR indx IN l_WHERE.FIRST .. l_WHERE.LAST
              LOOP
                UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx));
              END LOOP;
              /* FIN */
            end if;
          else
            if (v_hay_look_up = 'Y') then
              UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
              /* Hay tablas de LookUp. Hay que poner las condiciones de los Where*/
              dbms_output.put_line ('Entro en el que hay Tablas de LookUp');          
              /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
              FOR indx IN l_WHERE.FIRST .. l_WHERE.LAST
              LOOP
                UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx));
              END LOOP;
              /* FIN */
            end if;
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ''';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          
          --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.CVE_DIA = '' || fch_datos ;');
          UTL_FILE.put_line(fich_salida_pkg,'');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END ' || reg_scenario.SCENARIO || '_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
        /* (20161117) Angel Ruiz. FIN NF: Puede venir cualquier escenario */






      
      /**************/
      /**************/
    
    end loop;
    close MTDT_SCENARIO;

    /* Generamos los procedimientos que llaman a las funciones escenarios */
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lhe_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
    UTL_FILE.put_line(fich_salida_pkg, '  IS');
    --UTL_FILE.put_line(fich_salida_pkg, '  cursor MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '  is');
    --UTL_FILE.put_line(fich_salida_pkg, '  select');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_CARGA,');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_DATOS,');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_REGISTRO');
    --UTL_FILE.put_line(fich_salida_pkg, '  from');
    --UTL_FILE.put_line(fich_salida_pkg, '    app_mvnomt.MTDT_PROCESO, app_mvnomt.MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '  where');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_PROCESO.NOMBRE_PROCESO = ''load_SA_' || nombre_tabla_base_reducido || '.sh'' AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_PROCESO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_DATOS =  to_date(fch_datos_in, ''yyyymmdd'')  AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_CARGA =  to_date(fch_carga_in, ''yyyymmdd'')  AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.CVE_PASO = 1 AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.CVE_RESULTADO = 0;');
    UTL_FILE.put_line(fich_salida_pkg, '  /* LLEVO A CABO LA DECLARACION DE LAS VARIABLES */');
    --UTL_FILE.put_line(fich_salida_pkg, '  reg_monitoreo MTDT_MONITOREO%rowtype;');
    /* (20161117) Angel Ruiz. NF: Pueden aparecer cualquier tipo de escenario */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_' || lista_scenarios_presentes (indx) || ' NUMBER;');
    END LOOP;
    /* (20161117) Angel Ruiz. FIN NF: Pueden aparecer cualquier tipo de escenario */
    if (v_nombre_tabla_agr <> 'No Existe') then
      /* Ocurre que como existe un escenario de desagregacion en el pre-proceso hemos desagregado */
      /* y necesitamos una variable para almacenar los registros borrados */
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_del NUMBER;');
      --UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_agr NUMBER;');
    end if;
    /* (20150918) Angel Ruiz. NF: Si se trata  de un particionado tipo BSC, M24, no hay salvaguarda de información */
    if (reg_tabla.PARTICIONADO <> 'M24' or reg_tabla.PARTICIONADO is null) then
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_salvaguardados NUMBER:=0;');
    end if;
    /* (20150918) Angel Ruiz. Fin NF */
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_tot NUMBER;');
    UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');
    UTL_FILE.put_line(fich_salida_pkg, '  ult_paso_ejecutado PLS_integer;');
    UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
    UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
    UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
    --UTL_FILE.put_line(fich_salida_pkg, '    open MTDT_MONITOREO;');
    --UTL_FILE.put_line(fich_salida_pkg, '    loop');
    --UTL_FILE.put_line(fich_salida_pkg, '      fetch MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '      into reg_monitoreo;');
    --UTL_FILE.put_line(fich_salida_pkg, '      exit when MTDT_MONITOREO%NOTFOUND;');
    UTL_FILE.put_line(fich_salida_pkg, '      numero_reg_tot := 0;');
    --UTL_FILE.put_line(fich_salida_pkg, '      /* INICIAMOS EL BUCLE POR CADA UNA DE LAS INSERCIONES EN LA TABLA DE STAGING */');
    UTL_FILE.put_line(fich_salida_pkg, '      /* Este proceso solo tiene un paso, por lo que o se ejecuta todo el o no sejecuta nada porque ya se ejecuto OK */');
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_he_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '      if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    
    /**********************/
    /**********************/
    /* Generamos las llamadas a los procedimientos para realizar las cargas */
    
    /****************************************************************************************/
    /* (20220930) ANGEL RUIZ. NUEVA FUNCIONALIDAD. Cada ESCENARIO será un PASO de EJECUCIÓN */
    /****************************************************************************************/
    v_paso_actual := 0;
    while (v_paso_actual < (lista_scenarios_presentes.COUNT))
    LOOP
      UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = ' || to_char(v_paso_actual +1) || ') then');
      if (v_paso_actual + 1 = 1) then
        /* Solo en el primer paso es cuando generamos el codigo para le preproceso */
        if (v_nombre_tabla_agr <> 'No Existe') then
          /* Ocurre que como existe un escenario de desagregacion en el pre-proceso hemos desagregado */
          /* lo que significa que usamos un prototipo diferente de la funcion pre-proceso */
          UTL_FILE.put_line(fich_salida_pkg, '        numero_reg_del := pkg_' || nombre_proceso || '.' || 'pre_proceso (fch_carga_in, fch_datos_in, forzado_in);');
        else
          UTL_FILE.put_line(fich_salida_pkg, '        pkg_' || nombre_proceso || '.' || 'pre_proceso (fch_carga_in, fch_datos_in, forzado_in);');
        end if;
        UTL_FILE.put_line(fich_salida_pkg, '');
      end if;
      FOR indx IN (lista_scenarios_presentes.FIRST + v_paso_actual)  .. lista_scenarios_presentes.LAST
      LOOP
      
        UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
        UTL_FILE.put_line(fich_salida_pkg, '        commit;');
        UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.insert_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || to_char(indx) || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
        UTL_FILE.put_line(fich_salida_pkg, '        numero_reg_' || lista_scenarios_presentes (indx) || ' := ' || 'pkg_' || nombre_proceso || '.' || lista_scenarios_presentes (indx) || '_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr,''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg, '        numero_reg_tot := numero_reg_tot + numero_reg_' || lista_scenarios_presentes (indx) || ';');
        UTL_FILE.put_line(fich_salida_pkg, '        dbms_output.put_line (''El numero de registros ' || lista_scenarios_presentes (indx) || ' es: '' || numero_reg_' || lista_scenarios_presentes (indx) || ' || ''.'');');
        /* (20150918) Angel Ruiz. NF: Si se trata  de un particionado tipo BSC, M24, no hay salvaguarda de información */
        if (reg_tabla.PARTICIONADO = 'M24') then
          UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || to_char(indx) || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || 'numero_reg_' || lista_scenarios_presentes (indx) || ', 0, 0, 0);');
        else
          if (v_nombre_tabla_agr <> 'No Existe') then
            /* Hay un escenario de desagregacion. Hemos borrado registros despues de desagregar y los reflejamos en el monitoreo */
            if (v_existen_retrasados = 'S') then
              /* (20160309) Angel Ruiz. Si existen retrasados, quiere decir que hay reg. salvaguardados */
              UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || to_char(indx) || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || 'numero_reg_' || lista_scenarios_presentes (indx) || ', 0, numero_reg_del, numero_reg_salvaguardados);');
            else  /* (20160309) Angel Ruiz. Si no hay retrasados, no hay reg. salvaguardados */
              UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || to_char(indx) || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || 'numero_reg_' || lista_scenarios_presentes (indx) || ', 0, numero_reg_del);');
            end if;
          else
            if (v_existen_retrasados = 'S') then
              /* (20160309) Angel Ruiz. Si existen retrasados, quiere decir que hay reg. salvaguardados */
              UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || to_char(indx) || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || 'numero_reg_' || lista_scenarios_presentes (indx) || ', 0, 0, numero_reg_salvaguardados);');
            else
              UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || to_char(indx) || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || 'numero_reg_' || lista_scenarios_presentes (indx) || ', 0, 0, 0);');
            end if;
          end if;
        end if;
        UTL_FILE.put_line(fich_salida_pkg, '        commit;');
        UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');
      END LOOP;
      
      UTL_FILE.put_line(fich_salida_pkg, '      end if;');
      UTL_FILE.put_line(fich_salida_pkg, '');
      v_paso_actual := v_paso_actual + 1;
    END LOOP;
    
    UTL_FILE.put_line(fich_salida_pkg, '');

    /* (20220930) ANGEL RUIZ. FIN NUEVA FUNCIONALIDAD. Cada ESCENARIO será un PASO de EJECUCIÓN */

    /* (20150918) Angel Ruiz. NF: Si se trata  de un particionado tipo BSC, M24, no hay salvaguarda de información */
    DBMS_OUTPUT.PUT_LINE('--ANTES DE HOLA HOLA HOLA');
    DBMS_OUTPUT.PUT_LINE('El valor de PARTICIONADO ES: #' || reg_tabla.PARTICIONADO || '#');
    if (reg_tabla.PARTICIONADO is null or reg_tabla.PARTICIONADO <> 'M24') then
      /* (20160308) Angel Ruiz. NF: Preparo el script para generar cualquier tipo de procesos de hechos */
      if (v_existen_retrasados = 'S') then
        UTL_FILE.put_line(fich_salida_pkg, '        /* Salvaguardamos la informacion que ya estaba grabada en la particion */');
        UTL_FILE.put_line(fich_salida_pkg, '        numero_reg_salvaguardados := pkg_' || nombre_proceso || '.' || 'pos_proceso (fch_carga_in, fch_datos_in);');
      end if;
    end if;    
    /* (20150918) Angel Ruiz. Fin NF */


    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
    UTL_FILE.put_line(fich_salida_pkg,'        COMMIT;');
    
    UTL_FILE.put_line(fich_salida_pkg,'    exception');
    --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
    --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
    UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
    UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
    UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' ||  NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');    
    UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
    UTL_FILE.put_line(fich_salida_pkg,'');
    UTL_FILE.put_line(fich_salida_pkg,'  END lhe_' || nombre_proceso || ';');
    UTL_FILE.put_line(fich_salida_pkg,'');
  
    /**************/
    
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lex_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
    UTL_FILE.put_line(fich_salida_pkg, '  IS');
    --UTL_FILE.put_line(fich_salida_pkg, '  cursor MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '  is');
    --UTL_FILE.put_line(fich_salida_pkg, '  select');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_CARGA,');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_DATOS,');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_REGISTRO');
    --UTL_FILE.put_line(fich_salida_pkg, '  from');
    --UTL_FILE.put_line(fich_salida_pkg, '    app_mvnomt.MTDT_PROCESO, app_mvnomt.MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '  where');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_PROCESO.NOMBRE_PROCESO = ''load_he_' || reg_tabla.TABLE_NAME || '.sh'' AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_PROCESO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.FCH_CARGA =  to_date(fch_carga_in, ''yyyymmdd'')  AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.CVE_PASO = 1 AND');
    --UTL_FILE.put_line(fich_salida_pkg, '    MTDT_MONITOREO.CVE_RESULTADO = 0;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg, '  reg_monitoreo MTDT_MONITOREO%rowtype;');
    
    UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
    UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
    if (reg_tabla.PARTICIONADO = 'M24') then
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_num NUMBER;');
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
    --UTL_FILE.put_line(fich_salida_pkg, '    open MTDT_MONITOREO;');
    --UTL_FILE.put_line(fich_salida_pkg, '    loop');
    --UTL_FILE.put_line(fich_salida_pkg, '      fetch MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '      into reg_monitoreo;');
    --UTL_FILE.put_line(fich_salida_pkg, '      exit when MTDT_MONITOREO%NOTFOUND;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '      /* Lo primero que se hace es mirar que paso es el primero a ejecutar */');
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '      if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if;');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 1) then');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Este tipo de procesos ex solo tienen dos pasos */');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Comienza en el primer paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    /* (20150904) Angel Ruiz. NF: No hay exchange_partition ya que se vienen datos de todas las particiones */
    if (reg_tabla.PARTICIONADO = 'M24') then
      UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.insert_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', 1' || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg, '        commit;');
      UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''MERGE INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' A');    
      UTL_FILE.put_line(fich_salida_pkg, '        USING ' || OWNER_DM || '.T_' || nombre_tabla_T || ''' || ''_'' || fch_datos_in || '' B');
      UTL_FILE.put_line(fich_salida_pkg, '        ON (');
      primera_col := 1;
      FOR nombre_campo_pk IN (
        select column_name
        from mtdt_modelo_detail
        where table_name = reg_tabla.TABLE_NAME and upper(trim(PK)) = 'S')
      LOOP
        if primera_col = 1 then
          UTL_FILE.put_line(fich_salida_pkg, '        A.' || nombre_campo_pk.column_name || ' = ' || 'B.' || nombre_campo_pk.column_name);
        else
          UTL_FILE.put_line(fich_salida_pkg, '        AND A.' || nombre_campo_pk.column_name || ' = ' || 'B.' || nombre_campo_pk.column_name);
        end if;
        primera_col := primera_col +1;
      END LOOP;
      UTL_FILE.put_line(fich_salida_pkg, '        )');
      UTL_FILE.put_line(fich_salida_pkg, '        WHEN MATCHED THEN UPDATE SET');
      primera_col := 1;
      FOR nombre_campo_nopk IN (
        select column_name
        from mtdt_modelo_detail
        where table_name = reg_tabla.TABLE_NAME and PK is null)
      LOOP
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg, '          A.' || nombre_campo_nopk.column_name || ' = ' || 'B.' || nombre_campo_nopk.column_name);
          else
            UTL_FILE.put_line(fich_salida_pkg, '          , A.' || nombre_campo_nopk.column_name || ' = ' || 'B.' || nombre_campo_nopk.column_name);
          end if;
          primera_col := primera_col +1;
      END LOOP;
      UTL_FILE.put_line(fich_salida_pkg, '        WHEN NOT MATCHED THEN');
      UTL_FILE.put_line(fich_salida_pkg, '          INSERT (');
      primera_col := 1;
      FOR nombre_campo_to IN (
        select column_name
        from mtdt_modelo_detail
        where table_name = reg_tabla.TABLE_NAME)
      LOOP
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg, '          ' || nombre_campo_to.column_name);
          else
            UTL_FILE.put_line(fich_salida_pkg, '          ,' || nombre_campo_to.column_name);
          end if;
          primera_col := primera_col +1;
      END LOOP;
      UTL_FILE.put_line(fich_salida_pkg, '          ) VALUES (');
      primera_col := 1;
      FOR nombre_campo_to IN (
        select column_name
        from mtdt_modelo_detail
        where table_name = reg_tabla.TABLE_NAME)
      LOOP
          if primera_col = 1 then
            UTL_FILE.put_line(fich_salida_pkg, '         B. ' || nombre_campo_to.column_name);
          else
            UTL_FILE.put_line(fich_salida_pkg, '          ,B.' || nombre_campo_to.column_name);
          end if;
          primera_col := primera_col +1;
      END LOOP;
      UTL_FILE.put_line(fich_salida_pkg, '          )');
      UTL_FILE.put_line(fich_salida_pkg, ''';');
      if (reg_tabla.PARTICIONADO = 'M24') then
        UTL_FILE.put_line(fich_salida_pkg, '         numero_reg_num := sql%rowcount;');
      end if;
    /* (20150904) Angel Ruiz. FIN NF: No hay exchange_partition ya que se vienen datos de todas las particiones */
    else
      UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.insert_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', 1' || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg, '        commit;');
      UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME);    
      --UTL_FILE.put_line(fich_salida_pkg, '        EXCHANGE PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' ');    
      UTL_FILE.put_line(fich_salida_pkg, '        EXCHANGE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in || '' ');    
      UTL_FILE.put_line(fich_salida_pkg, '        WITH TABLE ' || OWNER_DM || '.T_' || nombre_tabla_T || ''' || ''_'' || fch_datos_in || '' ');
      /* (20160412) Angel Ruiz. NF: Las tablas del modelo pueden tener indices */
      /* con lo que debemos incluir o no los indices en el exchange */
      v_numero_indices := 0;
      select count(*) into v_numero_indices from MTDT_MODELO_DETAIL where TRIM(TABLE_NAME) = reg_scenario.TABLE_NAME AND UPPER(TRIM(INDICE)) = 'S';
      if (v_numero_indices > 0) then
        UTL_FILE.put_line(fich_salida_pkg, '      INCLUDING INDEXES'' || '' ');    
      end if;
      /* (20160412) Angel Ruiz. FIN NF */
      UTL_FILE.put_line(fich_salida_pkg, '        WITHOUT VALIDATION'';');    
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '');
    if (reg_tabla.PARTICIONADO = 'M24') then
      UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', 1' || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || ' numero_reg_num, 0, 0, 0);');      
      --UTL_FILE.put_line(fich_salida_pkg, '         ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_num, 0, 0, 0);');    
    else
      UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', 1' || ', 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || ' 0, 0, 0, 0);');      
      --UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');
    UTL_FILE.put_line(fich_salida_pkg, '        /* comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.insert_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_T || ''' || ''_'' || fch_datos_in || '' CASCADE CONSTRAINTS PURGE'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || ' 0, 0, 0, 0);');    
    --UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if; ');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 2) then');
    UTL_FILE.put_line(fich_salida_pkg, '        /* comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    --UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE APP_MVNODM.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in;');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.insert_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');    
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_T || ''' || ''_'' || fch_datos_in || '' CASCADE CONSTRAINTS PURGE'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.upd_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''),' || ' 0, 0, 0, 0);');    
    --UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if; ');
    --UTL_FILE.put_line(fich_salida_pkg, '    end loop;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg,'    exception');
    --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
    --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
    UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
    UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
    UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_' || PREFIJO_DM || 'F_MONITOREO_' ||  NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');    
    UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '  END lex_' || nombre_proceso || ';');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
    UTL_FILE.put_line(fich_salida_pkg, '/' );
    /******/
    /* FIN DE LA GENERACION DEL PACKAGE */
    /******/
    /******/    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, 'grant execute on ' || OWNER_DM || '.pkg_' || nombre_proceso || ' to ' || OWNER_TC || ';');
    UTL_FILE.put_line(fich_salida_pkg, '/');
    UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');

    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/
    
    UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_load, '#############################################################################');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Archivo    :       load_he_' ||  reg_tabla.TABLE_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Autor      : Angel Ruiz Canton. <SYNAPSYS>.                               #');
    UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || '.        #');
    UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Historia : 31-Octubre-2014 -> Creacion                                    #');
    UTL_FILE.put_line(fich_salida_load, '# Caja de Control - M :                                                     #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
    UTL_FILE.put_line(fich_salida_load, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Caducidad del Requerimiento :                                             #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Dependencias :                                                            #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Usuario:                                                                  #');   
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Telefono:                                                                 #');   
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '#############################################################################');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '#Obtiene los password de base de datos                                         #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_load, '{');
    UTL_FILE.put_line(fich_salida_load, '   #Se especifican parametros usuario y la BD');
    UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_load, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_load, '   then');
    UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
    UTL_FILE.put_line(fich_salida_load, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_load, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '      exit 1;');
    UTL_FILE.put_line(fich_salida_load, '   fi');
    UTL_FILE.put_line(fich_salida_load, '   return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_load, '{');
    UTL_FILE.put_line(fich_salida_load, '   #Se especifican parametros usuario y la BD');
    UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_load, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_load, '   then');
    UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
    UTL_FILE.put_line(fich_salida_load, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_load, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '      exit 1;');
    UTL_FILE.put_line(fich_salida_load, '   fi');
    UTL_FILE.put_line(fich_salida_load, '   return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_' || PAIS ||'.sh');
    /* (20180319) Angel Ruiz. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    if reg_tabla.TABLE_NAME = 'DMF_ESTATUS_ENTREGAS' then
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# LIBRERIAS                                                                    #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
      UTL_FILE.put_line(fich_salida_load, '');
      UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
      UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE=${PASSWORD}');
      UTL_FILE.put_line(fich_salida_load, 'if [ $# -eq 0 ] ; then');
      UTL_FILE.put_line(fich_salida_load, '  # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha del sistema.');
      UTL_FILE.put_line(fich_salida_load, '  FCH_CARGA=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof');
      UTL_FILE.put_line(fich_salida_load, '    whenever sqlerror exit 1');
      UTL_FILE.put_line(fich_salida_load, '    set pagesize 0');
      UTL_FILE.put_line(fich_salida_load, '    set heading off');
      UTL_FILE.put_line(fich_salida_load, '    select');
      UTL_FILE.put_line(fich_salida_load, '      to_char(SYSDATE-1,''YYYYMMDD'')');
      UTL_FILE.put_line(fich_salida_load, '    from dual;');
      UTL_FILE.put_line(fich_salida_load, '    quit');
      UTL_FILE.put_line(fich_salida_load, '  !eof`');
      UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
      UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      UTL_FILE.put_line(fich_salida_load, '    echo `date`');
      UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_load, '    exit 1');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      UTL_FILE.put_line(fich_salida_load, '  # Recogida de parametros');
      UTL_FILE.put_line(fich_salida_load, '  FCH_DATOS=${FCH_CARGA}');
      UTL_FILE.put_line(fich_salida_load, '  BAN_FORZADO=N');
      UTL_FILE.put_line(fich_salida_load, 'else');
      UTL_FILE.put_line(fich_salida_load, '  # Comprobamos si el numero de parametros es el correcto');
      UTL_FILE.put_line(fich_salida_load, '  if [ $# -ne 3 ] ; then');
      UTL_FILE.put_line(fich_salida_load, '    SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
      UTL_FILE.put_line(fich_salida_load, '    echo ${SUBJECT}');        
      UTL_FILE.put_line(fich_salida_load, '    exit 1');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      UTL_FILE.put_line(fich_salida_load, '  # Recogida de parametros');
      UTL_FILE.put_line(fich_salida_load, '  FCH_CARGA=${1}');
      UTL_FILE.put_line(fich_salida_load, '  FCH_DATOS=${2}');
      UTL_FILE.put_line(fich_salida_load, '  BAN_FORZADO=${3}');
      UTL_FILE.put_line(fich_salida_load, 'fi');
    else
      UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
      UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 3 ] ; then');
      UTL_FILE.put_line(fich_salida_load, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
      UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT}');        
      UTL_FILE.put_line(fich_salida_load, '  exit 1');
      UTL_FILE.put_line(fich_salida_load, 'fi');
      UTL_FILE.put_line(fich_salida_load, '# Recogida de parametros');
      UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA=${1}');
      UTL_FILE.put_line(fich_salida_load, 'FCH_DATOS=${2}');
      UTL_FILE.put_line(fich_salida_load, 'BAN_FORZADO=${3}');
    end if;
    /* (20180319) Angel Ruiz. FIN. */
    UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');    
    --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    --UTL_FILE.put_line(fich_salida_sh, 'set -x');
    UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
    UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
    UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="Req89208"');
    UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=Req89208_load_he_' || reg_tabla.TABLE_NAME);
    UTL_FILE.put_line(fich_salida_load, '');
    /* (20180319) Angel Ruiz. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    if (reg_tabla.TABLE_NAME <> 'DMF_ESTATUS_ENTREGAS') then
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# LIBRERIAS                                                                    #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
      UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
    end if;
    /* (20180319) Angel Ruiz. FIN. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_load, 'else');
    UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '');
    /* (20180319) Angel Ruiz. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    if (reg_tabla.TABLE_NAME <> 'DMF_ESTATUS_ENTREGAS') then
      UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
      UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE=${PASSWORD}');
    end if;
    /* (20180319) Angel Ruiz. FIN. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    /*************************/
    /* (20141211) Anyadido a posteriori al darme cuenta de que si falla el proceso pq no se invoque el procedimiento del paquete*/
    /* Fallara pero no se tendrá ni fecha ni hora de inicio */
    UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER SQLERROR EXIT 1;');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER OSERROR EXIT 2;');
    UTL_FILE.put_line(fich_salida_load, 'SET PAGESIZE 0;');
    UTL_FILE.put_line(fich_salida_load, 'SET HEADING OFF;');
    UTL_FILE.put_line(fich_salida_load, 'SELECT cast (systimestamp as timestamp) from dual;');
    UTL_FILE.put_line(fich_salida_load, 'QUIT;');
    UTL_FILE.put_line(fich_salida_load, 'EOF`');
    
    /***********************************************************************************/
    UTL_FILE.put_line(fich_salida_load, '# Llamada a sql_plus');
    UTL_FILE.put_line(fich_salida_load, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
    UTL_FILE.put_line(fich_salida_load, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
    UTL_FILE.put_line(fich_salida_load, 'whenever sqlerror exit 1;');
    UTL_FILE.put_line(fich_salida_load, 'whenever oserror exit 2;');
    UTL_FILE.put_line(fich_salida_load, 'set feedback off;');
    UTL_FILE.put_line(fich_salida_load, 'set serveroutput on;');
    UTL_FILE.put_line(fich_salida_load, 'set echo on;');
    UTL_FILE.put_line(fich_salida_load, 'set pagesize 0;');
    UTL_FILE.put_line(fich_salida_load, 'set verify off;');
    UTL_FILE.put_line(fich_salida_load, '');
    --UTL_FILE.put_line(fich_salida_load, 'declare');
    --UTL_FILE.put_line(fich_salida_load, '  num_filas_insertadas number;');
    UTL_FILE.put_line(fich_salida_load, 'begin');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'lhe_' || nombre_proceso || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
    UTL_FILE.put_line(fich_salida_load, 'end;');
    UTL_FILE.put_line(fich_salida_load, '/');
    UTL_FILE.put_line(fich_salida_load, 'exit 0;');
    UTL_FILE.put_line(fich_salida_load, 'EOF');
    UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_he_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_he' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_he' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'echo "El proceso load_' ||  'he_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'exit 0');
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
    
    /*************************/
    /******/
    /* INICIO DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    UTL_FILE.put_line(fich_salida_exchange, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_exchange, '#############################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Telefonica Moviles Mexico SA DE CV                                        #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Archivo    :       load_ex_' ||  reg_tabla.TABLE_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Autor      : Angel Ruiz Canton. <SYNAPSYS>.                               #');
    UTL_FILE.put_line(fich_salida_exchange, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || '.        #');
    UTL_FILE.put_line(fich_salida_exchange, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Historia : 31-Octubre-2014 -> Creacion                                    #');
    UTL_FILE.put_line(fich_salida_exchange, '# Caja de Control - M :                                                     #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
    UTL_FILE.put_line(fich_salida_exchange, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Caducidad del Requerimiento :                                             #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Dependencias :                                                            #');
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Usuario:                                                                  #');   
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '# Telefono:                                                                 #');   
    UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_exchange, '#############################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '#Obtiene los password de base de datos                                         #');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_exchange, '{');
    UTL_FILE.put_line(fich_salida_exchange, '   #Se especifican parametros usuario y la BD');
    UTL_FILE.put_line(fich_salida_exchange, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_exchange, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_exchange, '   then');
    UTL_FILE.put_line(fich_salida_exchange, '      SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
    UTL_FILE.put_line(fich_salida_exchange, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_exchange, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_exchange, '      exit 1;');
    UTL_FILE.put_line(fich_salida_exchange, '   fi');
    UTL_FILE.put_line(fich_salida_exchange, '   return 0');
    UTL_FILE.put_line(fich_salida_exchange, '}');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_exchange, '{');
    UTL_FILE.put_line(fich_salida_exchange, '   #Se especifican parametros usuario y la BD');
    UTL_FILE.put_line(fich_salida_exchange, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_exchange, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_exchange, '   then');
    UTL_FILE.put_line(fich_salida_exchange, '      SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
    UTL_FILE.put_line(fich_salida_exchange, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_exchange, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_exchange, '      exit 1;');
    UTL_FILE.put_line(fich_salida_exchange, '   fi');
    UTL_FILE.put_line(fich_salida_exchange, '   return 0');
    UTL_FILE.put_line(fich_salida_exchange, '}');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_' || PAIS ||'.sh');

    /* (20180319) Angel Ruiz. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    if (reg_tabla.TABLE_NAME = 'DMF_ESTATUS_ENTREGAS') then
      UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '# LIBRERIAS                                                                    #');
      UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
      UTL_FILE.put_line(fich_salida_exchange, '');
      UTL_FILE.put_line(fich_salida_exchange, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
      UTL_FILE.put_line(fich_salida_exchange, 'BD_CLAVE=${PASSWORD}');
      UTL_FILE.put_line(fich_salida_exchange, 'if [ $# -eq 0 ] ; then');
      UTL_FILE.put_line(fich_salida_exchange, '  # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha del sistema.');
      UTL_FILE.put_line(fich_salida_exchange, '  FCH_CARGA=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof');
      UTL_FILE.put_line(fich_salida_exchange, '    whenever sqlerror exit 1');
      UTL_FILE.put_line(fich_salida_exchange, '    set pagesize 0');
      UTL_FILE.put_line(fich_salida_exchange, '    set heading off');
      UTL_FILE.put_line(fich_salida_exchange, '    select');
      UTL_FILE.put_line(fich_salida_exchange, '      to_char(SYSDATE-1,''YYYYMMDD'')');
      UTL_FILE.put_line(fich_salida_exchange, '    from dual;');
      UTL_FILE.put_line(fich_salida_exchange, '    quit');
      UTL_FILE.put_line(fich_salida_exchange, '  !eof`');
      UTL_FILE.put_line(fich_salida_exchange, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_exchange, '    SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
      UTL_FILE.put_line(fich_salida_exchange, '    echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      UTL_FILE.put_line(fich_salida_exchange, '    echo `date`');
      UTL_FILE.put_line(fich_salida_exchange, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_exchange, '    exit 1');
      UTL_FILE.put_line(fich_salida_exchange, '  fi');
      UTL_FILE.put_line(fich_salida_exchange, '  # Recogida de parametros');
      UTL_FILE.put_line(fich_salida_exchange, '  FCH_DATOS=${FCH_CARGA}');
      UTL_FILE.put_line(fich_salida_exchange, '  BAN_FORZADO=N');
      UTL_FILE.put_line(fich_salida_exchange, 'else');
      UTL_FILE.put_line(fich_salida_exchange, '  # Comprobamos si el numero de parametros es el correcto');
      UTL_FILE.put_line(fich_salida_exchange, '  if [ $# -ne 3 ] ; then');
      UTL_FILE.put_line(fich_salida_exchange, '    SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
      UTL_FILE.put_line(fich_salida_exchange, '    echo ${SUBJECT}');        
      UTL_FILE.put_line(fich_salida_exchange, '    exit 1');
      UTL_FILE.put_line(fich_salida_exchange, '  fi');
      UTL_FILE.put_line(fich_salida_exchange, '  # Recogida de parametros');
      UTL_FILE.put_line(fich_salida_exchange, '  FCH_CARGA=${1}');
      UTL_FILE.put_line(fich_salida_exchange, '  FCH_DATOS=${2}');
      UTL_FILE.put_line(fich_salida_exchange, '  BAN_FORZADO=${3}');
      UTL_FILE.put_line(fich_salida_exchange, 'fi');
    else
      UTL_FILE.put_line(fich_salida_exchange, '# Comprobamos si el numero de parametros es el correcto');
      UTL_FILE.put_line(fich_salida_exchange, 'if [ $# -ne 3 ] ; then');
      UTL_FILE.put_line(fich_salida_exchange, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
      UTL_FILE.put_line(fich_salida_exchange, '  echo ${SUBJECT}');        
      UTL_FILE.put_line(fich_salida_exchange, '  exit 1');
      UTL_FILE.put_line(fich_salida_exchange, 'fi');
      UTL_FILE.put_line(fich_salida_exchange, '# Recogida de parametros');
      UTL_FILE.put_line(fich_salida_exchange, 'FCH_CARGA=${1}');
      UTL_FILE.put_line(fich_salida_exchange, 'FCH_DATOS=${2}');
      UTL_FILE.put_line(fich_salida_exchange, 'BAN_FORZADO=${3}');
    end if;
    /* (20180319) Angel Ruiz. FIN. */

    UTL_FILE.put_line(fich_salida_exchange, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
    --UTL_FILE.put_line(fich_salida_exchange, 'echo "load_ex_' || reg_tabla.TABLE_NAME || '" > ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_exchange, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_exchange, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_exchange, 'fi');
    UTL_FILE.put_line(fich_salida_exchange, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    --UTL_FILE.put_line(fich_salida_sh, 'set -x');
    UTL_FILE.put_line(fich_salida_exchange, '#Permite los acentos y U');
    UTL_FILE.put_line(fich_salida_exchange, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
    UTL_FILE.put_line(fich_salida_exchange, 'export NLS_LANG');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, 'REQ_NUM="Req89208"');
    UTL_FILE.put_line(fich_salida_exchange, 'INTERFAZ=Req89208_load_ex_' || reg_tabla.TABLE_NAME);
    UTL_FILE.put_line(fich_salida_exchange, '');
    /* (20180319) Angel Ruiz. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    if (reg_tabla.TABLE_NAME <> 'DMF_ESTATUS_ENTREGAS') then
      UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '# LIBRERIAS                                                                    #');
      UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
    end if;
    /* (20180319) Angel Ruiz. FIN. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    UTL_FILE.put_line(fich_salida_exchange, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_exchange, 'else');
    UTL_FILE.put_line(fich_salida_exchange, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_exchange, 'fi');
    UTL_FILE.put_line(fich_salida_exchange, '');
    /* (20180319) Angel Ruiz. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    if (reg_tabla.TABLE_NAME <> 'DMF_ESTATUS_ENTREGAS') then
      UTL_FILE.put_line(fich_salida_exchange, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
      UTL_FILE.put_line(fich_salida_exchange, 'BD_CLAVE=${PASSWORD}');
    end if;
    /* (20180319) Angel Ruiz. FIN. Introduzco una excepcion para DMF_ESTATUS_ENTREGAS. */
    
    /*****************************************************/
    UTL_FILE.put_line(fich_salida_exchange, '# Llamada a sql_plus');
    UTL_FILE.put_line(fich_salida_exchange, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
    UTL_FILE.put_line(fich_salida_exchange, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
    UTL_FILE.put_line(fich_salida_exchange, 'whenever sqlerror exit 1;');
    UTL_FILE.put_line(fich_salida_exchange, 'whenever oserror exit 2;');
    UTL_FILE.put_line(fich_salida_exchange, 'set feedback off;');
    UTL_FILE.put_line(fich_salida_exchange, 'set serveroutput on;');
    UTL_FILE.put_line(fich_salida_exchange, 'set echo on;');
    UTL_FILE.put_line(fich_salida_exchange, 'set pagesize 0;');
    UTL_FILE.put_line(fich_salida_exchange, 'set verify off;');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, 'begin');
    UTL_FILE.put_line(fich_salida_exchange, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'lex_' || nombre_proceso || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
    UTL_FILE.put_line(fich_salida_exchange, 'end;');
    UTL_FILE.put_line(fich_salida_exchange, '/');
    UTL_FILE.put_line(fich_salida_exchange, 'exit 0;');
    UTL_FILE.put_line(fich_salida_exchange, 'EOF');
    UTL_FILE.put_line(fich_salida_exchange, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_exchange, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_exchange, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_ex_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_exchange, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_exchange, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ex' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_exchange, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ex' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_exchange, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_exchange, '  exit 1');
    UTL_FILE.put_line(fich_salida_exchange, 'fi');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "El proceso de exchange load_' ||  'ex_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, 'exit 0');
    /******/
    /* FIN DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    
    /*************************/
    UTL_FILE.FCLOSE (fich_salida_load);
    UTL_FILE.FCLOSE (fich_salida_exchange);
    UTL_FILE.FCLOSE (fich_salida_pkg);
  end loop;
  close MTDT_TABLA;
end;

