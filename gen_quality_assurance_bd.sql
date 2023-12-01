declare

cursor MTDT_QUALITY_ASSURANCE
  is
    SELECT 
      TRIM(QUALITY_NAME) "QUALITY_NAME",
      TRIM(QUALITY_TYPE) "QUALITY_TYPE",
      TRIM("SELECT_CLAUSE") "PARTE_SELECT",
      TRIM("GROUP_CLAUSE") "PARTE_GROUP",
      TRIM(WHERE_CLAUSE) "FILTER",
      TRIM(FILE_SPOOL) "FILE_SPOOL",
      TRIM(SCENARIO) "SCENARIO",
      TABLE_BASE_NAME
    FROM
      MTDT_QUALITY_ASSURANCE
    WHERE
      (TRIM(STATUS) = 'P' or TRIM(STATUS) = 'D')
      --and TRIM(QUALITY_NAME) in ('NGQ_CHTFTR_DURACION_VINCULACION'
      --, 'NGQ_CHTFDC_PERIODO_VINCULACION'
      --, 'NGQ_CHTFTR_SIN_ITEM'
      --)
      ;
      
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
    trim(MTDT_TC_SCENARIO.TABLE_NAME) in (
    --'NGA_PARQUE_ABO_MES', 'NGA_PARQUE_SVA_MES', 'NGA_PARQUE_BENEF_MES', 
    --'NGG_TRANSACCIONES_DETAIL', 'NGA_COMIS_POS_ABO_MES', 'NGA_AJUSTE_ABO_MES', 'NGA_NOSTNDR_CONTRATOS_MES', 
    --'NGA_ALTAS_CANAL_MES', 'NGF_PERIMETRO', 'NGA_NOSTNDR_PLANTA_MES', 'NGA_PARQUE_ABO_DESA_MES',
    --'NGA_DESC_ADQR_ABO_MES', 'NGA_DESC_EJEC_ABO_MES', 'NGA_PRECIO_UNITARIO_SIM', 'NGA_VIDA_MEDIA_ABO_PRE', 'NGA_MATERIALIDAD_SIM'
    'NGQ_CONCILIATODO_CDG'
    );
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('NGA_PARQUE_ABO_DESA_MES');
    
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
      TRIM(TABLE_NAME) = table_name_in;
  
  CURSOR MTDT_TC_DETAIL (table_name_in IN VARCHAR2, scenario_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_TC_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_TC_DETAIL.TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(MTDT_TC_DETAIL.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_TC_DETAIL.SCENARIO) "SCENARIO",
      TRIM(MTDT_TC_DETAIL.OUTER) "OUTER",
      MTDT_TC_DETAIL.SEVERIDAD,
      TRIM(MTDT_TC_DETAIL.TABLE_LKUP) "TABLE_LKUP",
      TRIM(MTDT_TC_DETAIL.TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(MTDT_TC_DETAIL.TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(MTDT_TC_DETAIL.IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(MTDT_TC_DETAIL.LKUP_COM_RULE) "LKUP_COM_RULE",
      TRIM(MTDT_TC_DETAIL.VALUE) "VALUE",
      TRIM(MTDT_TC_DETAIL.RUL) "RUL",
      MTDT_TC_DETAIL.DATE_CREATE,
      MTDT_TC_DETAIL.DATE_MODIFY
  FROM
      MTDT_TC_DETAIL, MTDT_MODELO_DETAIL
  WHERE
      TRIM(MTDT_TC_DETAIL.TABLE_NAME) = table_name_in and
      TRIM(MTDT_TC_DETAIL.SCENARIO) = scenario_in and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_NAME)) = UPPER(trim(MTDT_MODELO_DETAIL.TABLE_NAME)) and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_COLUMN)) = UPPER(trim(MTDT_MODELO_DETAIL.COLUMN_NAME))
  ORDER BY MTDT_MODELO_DETAIL.POSITION ASC;

  /* (20161228) Angel Ruiz. */
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_MODELO_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_MODELO_DETAIL.COLUMN_NAME) "COLUMN_NAME",
      MTDT_MODELO_DETAIL.DATA_TYPE,
      MTDT_MODELO_DETAIL.PK,
      TRIM(MTDT_MODELO_DETAIL.NULABLE) "NULABLE",
      TRIM(MTDT_MODELO_DETAIL.VDEFAULT) "VDEFAULT",
      TRIM(MTDT_MODELO_DETAIL.INDICE) "INDICE"
    FROM MTDT_MODELO_DETAIL
    WHERE
      MTDT_MODELO_DETAIL.TABLE_NAME = table_name_in
    ORDER BY POSITION ASC;
      
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
      TRIM("VALUE") "VALUE"
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
  reg_modelo_logico_col c_mtdt_modelo_logico_COLUMNA%rowtype;
      
      
      
  reg_quality MTDT_QUALITY_ASSURANCE%rowtype;      


  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(400);
  type lista_tablas_from is table of varchar(4000);
  type lista_condi_where is table of varchar(500);
  type list_columns_par  IS TABLE OF VARCHAR(30);

  
  lista_pk                                      list_columns_primary := list_columns_primary ();
  lista_par                                      list_columns_par := list_columns_par ();
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(25000);
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
  l_FROM_solo_tablas                               lista_tablas_from := lista_tablas_from();  
  l_WHERE                                   lista_condi_where := lista_condi_where();
  l_WHERE_ON_clause                         lista_condi_where := lista_condi_where();
  v_hay_look_up                           VARCHAR2(1):='N';
  v_nombre_seqg                          VARCHAR(120):='N';
  v_bandera                                   VARCHAR2(1):='S';
  v_nombre_tabla_agr                VARCHAR2(30):='No Existe';
  v_nombre_tabla_agr_redu           VARCHAR2(30):='No Existe';
  v_nombre_proceso_agr              VARCHAR2(30);
  nombre_tabla_T_agr                VARCHAR2(30);
  v_existen_retrasados              VARCHAR2(1) := 'N';
  v_numero_indices                  PLS_INTEGER:=0;
  v_MULTIPLICADOR_PROC                   VARCHAR2(60);
  v_tipo_particionado               VARCHAR2(10);
  v_alias                           VARCHAR2(40);
  v_hay_regla_seq                   BOOLEAN:=false; /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_nombre_seq                      VARCHAR2(50); /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_nombre_campo_seq                VARCHAR2(50); /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_poner_paren_cierre boolean:=false;

/************/
/*************/
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
        v_cadena_temp := regexp_replace (cadena_in, ' *([Nn][Vv][Ll]) *\( *([A-Za-z_]+) *,', ' \1(' || alias_in || '.' || '\2' || ',');
        /* trasformamos el segundo operador del NVL, en caso de que sea un campo y no un literal */
        if (regexp_instr(v_cadena_temp, '[Cc][Uu][Rr][Rr][Ee][Nn][Tt]') = 0) then
          /* si aunque no es un literal si es un current_date entonces no se anyade alias */
          v_cadena_temp := regexp_replace (v_cadena_temp, ', *([A-Za-z_]+) *\)', ', ' || alias_in || '.' || '\1' || ')');
        end if;
        v_cadena_result := v_cadena_temp; /* retorno el resultado */
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
      /* Se trata de que el campo de join posee la funcion UPPER */
      if (regexp_instr(cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Uu][Pp][Pp][Ee][Rr]) *\( *([A-Za-z_]+) *\)', ' \1(' || alias_in || '.' || '\2' || ')');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *') > 0) then
        v_cadena_temp := regexp_replace (cadena_in, ' *([Rr][Ee][Pp][Ll][Aa][Cc][Ee]) *\( *([A-Za-z_]+) *,', ' \1(' || alias_in || '.' || '\2,');
        v_cadena_result := v_cadena_temp;
      else
        v_cadena_result := cadena_in;
      end if;
    else
      v_cadena_result := alias_in || '.' || cadena_in;
    end if;
    return v_cadena_result;
  end;


/**************/

/* (20161117) Angel Ruiz. */
  function extrae_campo (cadena_in in varchar2) return varchar2
  is
    v_campo varchar2(200);
    v_cadena_temp varchar2(200);
    v_cadena_result varchar2(200);
  begin
    /* Detecto si existen funciones SQL en el campo */
    if (regexp_instr(cadena_in, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 ) then
      if (regexp_instr(cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z_]+ *,') > 0) then
        /* Se trata de un decode normal y corriente */
        v_cadena_temp := regexp_substr (cadena_in, ' *[Dd][Ee][Cc][Oo][Dd][Ee] *\( *[A-Za-z_]+ *,'); 
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Nn][Vv][Ll]') > 0) then
      /* Se trata de que el campo de join posee la funcion NVL */
      if (regexp_instr(cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Nn][Vv][Ll] *\( *[A-Za-z_]+ *,');
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
      /* Se trata de que el campo de join posee la funcion UPPER */
      if (regexp_instr(cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Uu][Pp][Pp][Ee][Rr] *\( *[A-Za-z_]+ *\)');
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
        v_cadena_result := v_campo;
      else
        v_cadena_result := cadena_in;
      end if;
    elsif (regexp_instr(cadena_in, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
      /* Se trata de que el campo de join posee la funcion REPLACE */
      if (regexp_instr(cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *\)') > 0) then
        v_cadena_temp := regexp_substr (cadena_in, ' *[Rr][Ee][Pp][Ll][Aa][Cc][Ee] *\( *[A-Za-z_]+ *,');
        v_campo := regexp_substr (v_cadena_temp,'[A-Za-z_]+', instr( v_cadena_temp, '('));
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
      sustituto := ',';
      loop
        dbms_output.put_line ('Entro en el LOOP de cambio_puntoYcoma_por_coma. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, ';', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (';'));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
        --pos := pos_ant;
      end loop;
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
            if (instr(parte_1, '.') = 0) then /* (20170221) Angel Ruiz. BUG */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_]+) *', alias_in || '.\1'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              end if;
            else  /* Viene el campo ya con el alias por lo que no lo pongo */
              if (outer_in = 1) then
                v_cadena_temp := v_cadena_temp || regexp_replace(parte_1, ' *([A-Za-z_.]+) *', '\1' || ' (+)'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
              else
                v_cadena_temp := v_cadena_temp || parte_1;
              end if;
            end if;
            v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), " */
          elsif (indx = lista_elementos.LAST) then
            /* Se trata del ultimo elemento '1') */
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemnto tipo ID_CANAL pero situado al final del DECODE */
              if (instr(lista_elementos(indx), '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene alias */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.\1' || ' (+))'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *\)', alias_in || '.\1' || ')'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                end if;
              else  /* (20170221) Angel Ruiz. Si viene alias no lo pongo*/
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *\)', '\1' || ' (+))'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                else
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *\)', '\1' || ')'); /* cambio ID_FUENTE por ALIAS.ID_FUENTE */
                end if;
              end if;
            else
              /* Se trata de un elemento literal situado como ultimo elemento del decode, tipo '1' */
              /* Le ponemos doble comillas ya que estamos generando una query deinamica */
              v_cadena_temp := v_cadena_temp || lista_elementos(indx) || ')';
            end if;
          else
            /* Se trata del resto de elmentos 'SER', ID_CANAL*/
            if (instr(lista_elementos(indx), '''') = 0) then
              /* Se trata de un elemento que no es un literal, tipo ID_CANAL */
              if (instr(lista_elementos(indx), '.') = 0) then /* (20170221) Angel Ruiz. BUG. Si no viene alias */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *', alias_in || '.\1' || ' (+)');
                else  
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_]+) *', alias_in || '.\1');
                end if;
              else /* (20170221) Angel Ruiz. BUG. Si viene alias */
                if (outer_in = 1) then
                  v_cadena_temp := v_cadena_temp || regexp_replace(lista_elementos(indx), ' *([A-Za-z_.]+) *', '\1' || ' (+)');
                else  
                  v_cadena_temp := v_cadena_temp || lista_elementos(indx);
                end if;
              end if;
              v_cadena_temp := v_cadena_temp || ', '; /* Tengo LA CADENA: "DECODE (alias_in.ID_FUENTE (+), ..., alias_in.ID_CANAL, ... "*/
            else
              /* Se trata de un elemento que es un literal, tipo 'SER' */
              /* Le ponemos doble comillas ya que estamos generando una query deinamica */
              v_cadena_temp := v_cadena_temp || lista_elementos(indx) || ', ';
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
      --if (instr(cadena_resul, '!=') > 0) then
        /* Busco el signo != */
        --sustituto := ' (+)!= ';
        --loop
          --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '!=', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('!='));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + (length (' (+)!= '));
          --dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
          --pos := pos_ant;
        --end loop;
      --else
        --if (instr(cadena_resul, '=') > 0) then
          --sustituto := ' (+)= ';
          --loop
            --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
            --pos := instr(cadena_resul, '=', pos+1);
            --exit when pos = 0;
            --dbms_output.put_line ('Pos es mayor que 0');
            --dbms_output.put_line ('Primer valor de Pos: ' || pos);
            --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
            --dbms_output.put_line ('La cabeza es: ' || cabeza);
            --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
            --cola := substr(cadena_resul, pos + length ('='));
            --dbms_output.put_line ('La cola es: ' || cola);
            --cadena_resul := cabeza || sustituto || cola;
            --pos_ant := pos + (length (' (+)= '));
            --dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
            --pos := pos_ant;
          --end loop;
        --end if;
      --end if;
      /* Busco LA COMILLA */
      --pos := 0;
      --posicion_ant := 0;
      --sustituto := '''''';
      --loop
        --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
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
    cabeza                varchar2 (4000);
    sustituto              varchar2(100);
    cola                      varchar2(4000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(4000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        sustituto := ' to_date ('''' ||  fch_datos_in || '''', ''yyyymmdd'') ';
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_FCH_CARGA', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_FCH_CARGA'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
          --pos := pos_ant;
        end loop;
        /* Busco VAR_FCH_INICIO */
        sustituto := ' to_date ('''' ||  fch_registro_in || '''', ''yyyymmdd'') ';
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_FCH_INICIO', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_FCH_INICIO'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
          --pos := pos_ant;
        end loop;
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de VAR_PROFUNDIDAD_BAJAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_PROFUNDIDAD_BAJAS'));
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
        /* (20150914) Angel Ruiz. BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
        /* Busco LA COMILLA para poner comillas dobles */
        /*(20161118) Angel Ruiz. Modifico la forma de cambiar la ' por '' usando regexp_replace */
        cadena_resul := regexp_replace(cadena_resul, '''', '''''');
        --pos := 0;
        --posicion_ant := 0;
        --sustituto := '''''';
        --loop
          --dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '''', pos);
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
        /* (20150914) Angel Ruiz. FIN BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
      end if;
      return cadena_resul;
    end;

/************/
  

  function procesa_campo_filter (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (25000);
    sustituto              varchar2(100);
    cola                      varchar2(25000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(25000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        --sustituto := ' to_date ( fch_datos_in, ''yyyymmdd'') ';
        --sustituto := ' date_format (''#VAR_FCH_DATOS#'', ''yyyy-MM-dd'') '; /* (20161208) Angel Ruiz */
        --sustituto := '''#VAR_FCH_DATOS#'''; /* (20161208) Angel Ruiz */
        --loop
          --dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '#VAR_FCH_CARGA#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#VAR_FCH_CARGA#'));
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_CARGA#', '''#VAR_FCH_DATOS#''');
        /* Busco VAR_FCH_INICIO */
        --sustituto := ' date_format (''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd'') ';
        --loop
          --dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '#VAR_FCH_INICIO#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#VAR_FCH_INICIO#'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#VAR_FCH_INICIO#', ' date_format (''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd'') ');
        
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        --sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        --pos := 0;
        --loop
          --dbms_output.put_line ('Entro en el LOOP de VAR_PROFUNDIDAD_BAJAS. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('VAR_PROFUNDIDAD_BAJAS'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', ' 90 ');
        
        /* Busco OWNER_DM */
        --sustituto := OWNER_DM;
        --pos := 0;
        --loop
          --dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '#OWNER_DM#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#OWNER_DM#'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_DM#', OWNER_DM);
        /* Busco OWNER_SA */
        --sustituto := OWNER_SA; 
        --pos := 0;
        --loop
          --dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '#OWNER_SA#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#OWNER_SA#'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_SA#', OWNER_SA);
        /* Busco OWNER_T */
        --sustituto := OWNER_T; 
        --pos := 0;
        --loop
          --dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '#OWNER_T#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#OWNER_T#'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_T#', OWNER_T);
        
        /* Busco OWNER_MTDT */
        --sustituto := OWNER_MTDT; 
        --pos := 0;
        --loop
          --pos := instr(cadena_resul, '#OWNER_MTDT#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#OWNER_MTDT#'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#OWNER_MTDT#', OWNER_MTDT);
        /* Busco VAR_MARGEN_COMISION */
        --sustituto := ' 0.3 ';  /* Temporalmente pongo 90 dias */
        --pos := 0;
        --loop
          --dbms_output.put_line ('Entro en el LOOP de VAR_MARGEN_COMISION. La cadena es: ' || cadena_resul);
          --pos := instr(cadena_resul, '#VAR_MARGEN_COMISION#', pos+1);
          --exit when pos = 0;
          --dbms_output.put_line ('Pos es mayor que 0');
          --dbms_output.put_line ('Primer valor de Pos: ' || pos);
          --cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          --dbms_output.put_line ('La cabeza es: ' || cabeza);
          --dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          --cola := substr(cadena_resul, pos + length ('#VAR_MARGEN_COMISION#'));
          --dbms_output.put_line ('La cola es: ' || cola);
          --cadena_resul := cabeza || sustituto || cola;
        --end loop;
        cadena_resul := regexp_replace(cadena_resul, '#VAR_MARGEN_COMISION#', ' 0.3 ');

      end if;
      return cadena_resul;
    end;

  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (25000);
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
    cadena_resul  VARCHAR(20000);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(2000);
    cola                   VARCHAR2(2000);
    pos_ant            PLS_integer;
    v_encontrado  VARCHAR2(1);
    v_alias             VARCHAR2(40);
    v_alias_table_base  VARCHAR2(40);/* (20161227) Angel Ruiz */
    table_columns_lkup  list_strings := list_strings();
    ie_column_lkup    list_strings := list_strings();
    tipo_columna  VARCHAR2(30);
    mitabla_look_up VARCHAR2(4000);
    v_tabla_base_name VARCHAR2(800);
    mi_tabla_base_name VARCHAR2(50);
    l_registro          ALL_TAB_COLUMNS%rowtype;
    l_registro1         ALL_TAB_COLUMNS%rowtype;
    l_registro2         v_MTDT_CAMPOS_DETAIL%rowtype;
    v_value VARCHAR(200);
    nombre_campo  VARCHAR2(200);
    v_alias_incluido PLS_Integer:=0;
    v_alias_incluido_table_base PLS_Integer:=0;
    v_es_query_table_base PLS_Integer:=0;
    v_table_look_up varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_reg_table_lkup varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_alias_table_look_up varchar2(10000);  /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_tipo_campo  VARCHAR2(30);
    v_existe_valor  BOOLEAN;
    v_table_lkup_prima varchar2(10000);  /*(20170109) Angel Ruiz. BUG.*/
    v_no_se_generara_case             BOOLEAN:=false;
    v_operador_para_join  VARCHAR2(3);

    
  begin
    /* Seleccionamos el escenario primero */
      dbms_output.put_line ('REGLA RUL: #' || reg_detalle_in.RUL || '#');
      dbms_output.put_line ('REGLA RUL: #' || reg_detalle_in.TABLE_COLUMN || '#');
      case trim(reg_detalle_in.RUL)
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
        l_FROM_solo_tablas.extend;  /*(20161222) Angel Ruiz */
        /* (20150130) Angel Ruiz */
        /* Nueva incidencia. */
        if (regexp_instr (reg_detalle_in.TABLE_LKUP,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$')) then
          --if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+ *[-]* *([a-zA-Z_0-9]* *)+$')) then
          /* (20160629) Angel Ruiz. NF: Se aceptan tablas de LKUP que son SELECT que ademas tienen un ALIAS */
            v_alias := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            --v_alias := REGEXP_SUBSTR(trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+ *[-]* *([a-zA-Z_0-9]* *)+$'), 2)), '^[a-zA-Z_0-9]+');
            --mitabla_look_up := reg_detalle_in.TABLE_LKUP;
            mitabla_look_up := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
            v_alias_incluido := 1;
            dbms_output.put_line('EXISTE ALIAS EN LA QUERY TABLE_LKUP');
            v_table_lkup_prima := v_alias; /*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
          else
            v_alias := 'LKUP_' || l_FROM.count;
            mitabla_look_up := '(' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ') "LKUP_' || l_FROM.count || '"';
            --mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
            v_alias_incluido := 0;
            dbms_output.put_line('NO EXISTE ALIAS EN LA QUERY TABLE_LKUP');
            v_table_lkup_prima := v_alias; /*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
          end if;
          --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
          l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
          l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
          
        else  /* La TABLA_LKUP no es una query */

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
              v_table_lkup_prima := substr(regexp_substr(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);/*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
              v_table_look_up := v_table_look_up;
              
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              v_table_lkup_prima:= v_table_look_up; /*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM_solo_tablas(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
              l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
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
              v_table_lkup_prima := v_alias_table_look_up; /*(20170109) Angel Ruiz. BUG.Despues la uso para buscar en el metadato*/
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              v_table_lkup_prima := v_table_look_up; /*(20170109) Angel Ruiz. BUG.Despues la uso para buscar en el metadato*/
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              v_table_look_up := OWNER_DM || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: $$' || v_alias_table_look_up || '$$');
            dbms_output.put_line('La tabla de LKUP es: $$' || v_table_look_up || '$$');
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
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
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
              l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
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
        /*********************************/
        /* (20161227) Angel Ruiz. Ocurre que pueden venir Queries en la columna TABLE_BASE_NAME */
        /*********************************/
        if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt] ') > 0) then
          /* Tenemos una query en TABLE_BASE_NAME DEL SCENARIO */
          v_es_query_table_base:=1;
          /* Calculo el TABLE_BASE_NAME a partir del Scenario mejor que a partir del detail */
          if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$')) then
            v_alias_table_base := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
            v_tabla_base_name := procesa_campo_filter(reg_scenario.TABLE_BASE_NAME);
            v_alias_incluido_table_base:=1;
          end if;
        else
          /* NO es una query lo que viene en TABLE_BASE_NAME*/
          v_es_query_table_base:=0;
          if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* Posee un alias */
            v_alias_incluido_table_base:=1;
            v_alias_table_base := trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, ' +[a-zA-Z_0-9]+$'));
            v_tabla_base_name := trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, '^+[a-zA-Z_0-9\.#&]+ '));
            if (REGEXP_LIKE(v_tabla_base_name, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* TABLE_BASE_NAME esta calificada */
              /* me quedo solo con el nombre de la tabla base name */
              v_tabla_base_name := substr(trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, '\.+[a-zA-Z_0-9]+')),2);
            end if;
          else
            /* No posee un alias */
            v_alias_incluido_table_base:=0;
            /* NO viene ALIAS en TABLE_BASE_NAME */
            if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* TABLE_BASE_NAME esta calificada */
              /* me quedo solo con el nombre de la tabla base name */
              v_tabla_base_name := substr(trim(REGEXP_SUBSTR(reg_scenario.TABLE_BASE_NAME, '\.+[a-zA-Z_0-9]+')),2);
            else
              v_tabla_base_name:=reg_scenario.TABLE_BASE_NAME;
            end if;
          end if;
        end if;
        if (v_alias_incluido_table_base = 1) then
          /* TABLE_BASE_NAME viene con alias */
          if (v_es_query_table_base = 1) then
            mi_tabla_base_name := v_alias_table_base;
          else
            mi_tabla_base_name := v_tabla_base_name;
          end if;
        else
          mi_tabla_base_name := v_tabla_base_name;
        end if;
        /* (20161227) Angel Ruiz. */

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
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
            then
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=false) then
                v_no_se_generara_case:=true;
              end if;
            else
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
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
          if (v_no_se_generara_case = false) then /*(20170109) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
            if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
              valor_retorno := 'CASE WHEN (';
              FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
              LOOP
                /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
                (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
                then
                  --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
                  /* (20161117) Angel Ruiz. NF: Pueden venir funciones en los campos de join como */
                  /* UPPER, NVL, DECODE, ... */
                  dbms_output.put_line ('El campo por el que voy a hacer LookUp de la TABLE_BASE es: ' || TRIM(ie_column_lkup(indx)));
                  dbms_output.put_line ('La TABLE_BASE es: ' || TRIM(mi_tabla_base_name));
                  nombre_campo := extrae_campo (ie_column_lkup(indx));
                  dbms_output.put_line ('El campo por el que voy a hacer LooUp de la TABLE_BASE es: ' || TRIM(nombre_campo));
                  --SELECT * INTO l_registro
                  --FROM ALL_TAB_COLUMNS
                  --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                  --COLUMN_NAME = TRIM(nombre_campo);
                  SELECT * INTO l_registro2
                  FROM v_MTDT_CAMPOS_DETAIL
                  WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                  UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
                else
                  dbms_output.put_line ('El campo por el que voy a hacer LookUp de la TABLE_BASE es: ' || TRIM(ie_column_lkup(indx)));
                  dbms_output.put_line ('La TABLE_BASE es: ' || TRIM(mi_tabla_base_name));
                  --SELECT * INTO l_registro
                  --FROM ALL_TAB_COLUMNS
                  --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
                  --COLUMN_NAME = TRIM(ie_column_lkup(indx));
                  SELECT * INTO l_registro2
                  FROM V_MTDT_CAMPOS_DETAIL
                  WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                  UPPER(trim(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
                end if;
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
                  if (indx = 1) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IN (''NI#'', ''NO INFORMADO'') ';
                    else
                      valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' IN (''NI#'', ''NO INFORMADO'') ';
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IN (''NI#'''', ''NO INFORMADO'') ';
                    else
                      valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' IN (''NI#'', ''NO INFORMADO'') ';
                    end if;
                  end if;
                else 
                  if (indx = 1) then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                    else
                      valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' = -3 ';
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (instr(ie_column_lkup(indx), 'DECODE') > 0 or instr(ie_column_lkup(indx), 'decode') > 0) then
                      valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || nombre_campo || ' = -3 ';
                    else
                      valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro2.COLUMN_NAME || ' = -3 ';
                    end if;
                  end if;
                end if;
              END LOOP;
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              --SELECT * INTO l_registro1
              --FROM ALL_TAB_COLUMNS
              --WHERE TABLE_NAME =  reg_detalle_in.TABLE_NAME and
              --COLUMN_NAME = reg_detalle_in.TABLE_COLUMN;
              SELECT * INTO l_registro2
              FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.TABLE_COLUMN);
              dbms_output.put_line ('Estoy donde quiero.');
              dbms_output.put_line ('El nombre de TABLE_NAME ES: ' || reg_detalle_in.TABLE_NAME);
              dbms_output.put_line ('El nombre de TABLE_COLUMN ES: ' || reg_detalle_in.TABLE_COLUMN);
              dbms_output.put_line ('El tipo de DATOS es: ' || l_registro2.TYPE);
              if (l_registro2.TYPE = 'NUMBER') then
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  --valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', -2) END';
                  valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2) END';
                else
                  valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) END';
                end if;
              elsif (UPPER(TRIM(l_registro2.TYPE)) = 'DATE') then
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE)) END';
                else
                  valor_retorno := valor_retorno || ') THEN CAST(''1970-01-01'' AS DATE) ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', CAST(''2000-01-01'' AS DATE)) END';
                end if;
              else
                if (v_alias_incluido = 1) then
                /* (20160629) Angel Ruiz. NF: Se incluye la posibilidad de incluir el ALIAS en tablas de LKUP que sean SELECT */
                  --valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''') END';
                  valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'') END';
                else
                  --valor_retorno := valor_retorno || ') THEN ''''NO INFORMADO'''' ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''') END';
                  valor_retorno := valor_retorno || ') THEN ''NO INFORMADO'' ELSE ' || 'NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'') END';
                end if;
              end if;
            else  /* (20170109) Angel Ruiz. if table_columns_lkup.COUNT > 1 */
              /* (20160630) Angel Ruiz. NF: Se admiten Queries como tablas de LookUp y con ALIAS */
              SELECT * INTO l_registro2
              FROM v_MTDT_CAMPOS_DETAIL
              WHERE TABLE_NAME =  reg_detalle_in.TABLE_NAME and
              COLUMN_NAME = reg_detalle_in.TABLE_COLUMN;
              if (l_registro2.TYPE = 'NUMBER') then
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', -2)';
                  valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', -2)';
                else
                  valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)';
                end if;
              elsif (l_registro2.TYPE = 'DATE') then
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''')';
                  valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', CAST(''2000-01-01'' AS DATE))';
                else
                  --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''')';
                  valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', CAST(''2000-01-01'' AS DATE))';
                end if;
              else
                if (v_alias_incluido = 1) then
                  --valor_retorno :=  '    NVL(' || sustituye_comillas_dinam(reg_detalle_in.VALUE) || ', ''''GENERICO'''')';
                  valor_retorno :=  '    NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', ''GENERICO'')';
                else
                  --valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''''GENERICO'''')';
                  valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', ''GENERICO'')';
                end if;
              end if;
            end if;
          else  /* (20170109) Angel Ruiz. if (v_no_se_generara_case = false) then */
            /*(20170109) Angel Ruiz. BUG: Hay campos con JOIN en los que no se va a generar CASE WHEN */
            valor_retorno :=  '    ' || procesa_campo_filter(reg_detalle_in.VALUE);
          end if;
        end if;
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        l_WHERE_ON_clause.delete;   /* (20161222) Angel Ruiz */
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            --l_WHERE.extend;
            l_WHERE_ON_clause.extend;
            /* (20170127) Angel Ruiz. BUG. Cuando en TABLE_COLUMN_LKUP viene un campo con BETWEEN */
            /* entonces el operador de JOIN no sera el = sino ese BETWEEN que nos viene */
            if (regexp_instr(table_columns_lkup(indx), '[Bb][Ee][Tt][Ww][Ee][Ee][Nn]') = 0) then
              /* si no hay un between entonces el operador de join sera el operador por defecto, el = */
              v_operador_para_join := ' = ';
            else
              /* Si lo hay, entonces sera el mismo between, por lo que nuestro operador solo sera un blanco */
              v_operador_para_join := ' ';
            end if;
            /* (20170127) Angel Ruiz. Fin */
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna TABLE_COLUMN es: ' || reg_detalle_in.TABLE_COLUMN);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Table_Base_Name es: ' || mi_tabla_base_name);
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            /************************/
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
            then
              --nombre_campo := extrae_campo_decode (ie_column_lkup(indx));
              /* (20161117) Angel Ruiz. NF: Pueden venir funciones en los campos de join como */
              /* UPPER, NVL, DECODE, ... */
              nombre_campo := extrae_campo (ie_column_lkup(indx));
              /****************************************/
              /* (20170109) Angel Ruiz. BUG. Hay campos de los q no se puede hayar su tipo pq tienen muchas funciones */
              /****************************************/
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor = true) then
                SELECT * INTO l_registro2
                FROM V_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
              end if;
            else  /* NO poseen funciones el campo por el que se va a hacer JOIN */
              --SELECT * INTO l_registro
              --FROM ALL_TAB_COLUMNS
              --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              --COLUMN_NAME = TRIM(ie_column_lkup(indx));
              /****************************************/
              /* (20170109) Angel Ruiz. BUG. Hay campos de los q no se puede hayar su tipo pq tienen muchas funciones */
              /****************************************/
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx))))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                /* podemos encontrar el campo en el diccionario de datos */
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(ie_column_lkup(indx)));
              end if;
            end if;
            if (l_WHERE_ON_clause.count = 1) then
              if (v_existe_valor = true) then
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number((l_registro2.LENGTH)) < 3 and l_registro2.NULABLE = 'Y') then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'NVL(' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'NVL(' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) ||  v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ie_column_lkup(indx) ||  v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(ie_column_lkup(indx), '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                /* No podemos encontar el campo en el diccionario de datos */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                elsif (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ie_column_lkup(indx) || v_operador_para_join || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                elsif (instr(ie_column_lkup(indx), '.') = 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                end if;
              end if;
            else  /* if (l_WHERE_ON_clause.count = 1) then */
              if (v_existe_valor = true) then
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number(l_registro2.LENGTH) < 3 and l_registro2.NULABLE = 'Y') then
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  else
                    /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
                    if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion (ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(ie_column_lkup(indx), '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      else                    
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                      end if;
                    end if;
                  end if;
                else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                  /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */                
                  if (regexp_instr(ie_column_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME, 0) || v_operador_para_join || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Nn][Vv][Ll]') > 0 or regexp_instr(table_columns_lkup(indx), '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(table_columns_lkup(indx), '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  elsif (regexp_instr(ie_column_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(table_columns_lkup(indx), '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(ie_column_lkup(indx), reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(table_columns_lkup(indx), v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(ie_column_lkup(indx), '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                elsif (instr(ie_column_lkup(indx), '.') > 0 and instr(table_columns_lkup(indx), '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' || table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                  end if;
                elsif (instr(ie_column_lkup(indx), '.') = 0 and instr(table_columns_lkup(indx), '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || table_columns_lkup(indx);
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias || '.' ||table_columns_lkup(indx);
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || v_operador_para_join || v_alias_table_look_up || '.' || table_columns_lkup(indx);
                  end if;
                end if;
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          --l_WHERE.extend;
          l_WHERE_ON_clause.extend;
          /* (20170127) Angel Ruiz. BUG. Cuando en TABLE_COLUMN_LKUP viene un campo con BETWEEN */
          /* entonces el operador de JOIN no sera el = sino ese BETWEEN que nos viene */
          if (regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Bb][Ee][Tt][Ww][Ee][Ee][Nn]') = 0) then
            /* si no hay un between entonces el operador de join sera el operador por defecto, el = */
            v_operador_para_join := ' = ';
          else
            /* Si lo hay, entonces sera el mismo between, por lo que nuestro operador solo sera un blanco */
            v_operador_para_join := ' ';
          end if;
          /* (20170127) Angel Ruiz. Fin */
          
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0 and instr(reg_detalle_in.TABLE_LKUP, 'SELECT') = 0 )then
            if (l_WHERE_ON_clause.count = 1) then
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
              l_WHERE_ON_clause.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            else
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP;
              l_WHERE_ON_clause.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('#ESTOY EN EL LOOKUP. La Tabla es: $' || mi_tabla_base_name || '$');
            dbms_output.put_line('#ESTOY EN EL LOOKUP. La Columna es: $' || reg_detalle_in.IE_COLUMN_LKUP || '$');
            /* (20161117) Angel Ruiz NF: Pueden venir funciones en los campos de JOIN */
            if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) or
            (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0)
            then
            --if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
              --nombre_campo := extrae_campo_decode (reg_detalle_in.IE_COLUMN_LKUP);
              nombre_campo := extrae_campo (reg_detalle_in.IE_COLUMN_LKUP);
              --SELECT * INTO l_registro2
              --FROM ALL_TAB_COLUMNS
              --WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              --COLUMN_NAME = trim(nombre_campo);
              /* (20170109) Angel Ruiz. BUG: Hay campos que no se encuentran en el diccionario de datos */
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                /* podemos encontrar el campo en el diccionario de datos */
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(nombre_campo));
              end if;
            else  /* (20170109) Angel Ruiz. NO hay funciones en las columnas de JOIN */
              /* (20170109) Angel Ruiz. BUG: Hay campos que no se encuentran en el diccionario de datos */
              v_existe_valor:=false;
              for registro in (SELECT * FROM v_MTDT_CAMPOS_DETAIL
              WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(mi_tabla_base_name) and
              UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.IE_COLUMN_LKUP)))
              loop
                v_existe_valor:=true;
              end loop;
              if (v_existe_valor=true) then
                SELECT * INTO l_registro2
                FROM v_MTDT_CAMPOS_DETAIL
                WHERE UPPER(TRIM((TABLE_NAME))) =  UPPER(mi_tabla_base_name) and
                UPPER(TRIM(COLUMN_NAME)) = UPPER(reg_detalle_in.IE_COLUMN_LKUP);
              end if;
            end if;
            if (l_WHERE_ON_clause.count = 1) then /* si es el primer campo del WHERE */
              if (v_existe_valor = true) then /* (20170110) Angel Ruiz. BUG. Hay columnas que no se encuentran en el metadato */
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number(l_registro2.LENGTH) <3 and l_registro2.NULABLE = 'Y') then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' ||  v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else                      
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 and regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else                    
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  end if;
                else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) ||  v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) ||  v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                end if;
              else /* if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              end if;
            else  /* sino es el primer campo del Where  */
              if (v_existe_valor = true) then
                if (instr(l_registro2.TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                  if (to_number(l_registro2.LENGTH) <3 and l_registro2.NULABLE = 'Y') then
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || ', ''NI#'')' || v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || ', ''NI#'')' || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''NI#'')' || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  else
                    if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                    else
                      /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                      if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      else
                        l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                      end if;
                    end if;
                  end if;
                else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                  --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  if (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Dd][Ee][Cc][Oo][Dd][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME, 0) || v_operador_para_join || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Nn][Vv][Ll]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Uu][Pp][Pp][Ee][Rr]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  elsif (regexp_instr(reg_detalle_in.IE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0 or regexp_instr(reg_detalle_in.TABLE_COLUMN_LKUP, '[Rr][Ee][Pp][Ll][Aa][Cc][Ee]') > 0) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_funcion(reg_detalle_in.IE_COLUMN_LKUP, reg_detalle_in.TABLE_BASE_NAME) || v_operador_para_join || transformo_funcion(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias);
                  else
                    /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                    if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0) then
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    else
                      l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  end if;
                end if;
              else  /* if (v_existe_valor = true) then */
                /* (20170316) Angel Ruiz. BUG. Proceso si he de anyadir la calificacion al campo where */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') = 0) then
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, '.') > 0) then                  
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  if (v_alias_incluido = 1) then
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || v_operador_para_join || v_alias_table_look_up || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE_ON_clause.extend;
          l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
        end if;
        /* (20161223) Angel Ruiz */
        /* Modifico esta parte para HIVE */
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || chr(10) || ' ON (';
        FOR indx IN l_WHERE_ON_clause.FIRST .. l_WHERE_ON_clause.LAST
        LOOP
          l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || l_WHERE_ON_clause(indx);
        END LOOP;
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || ')';
        
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
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
          valor_retorno :=  '    ' || 'PKG_' || reg_detalle_in.TABLE_NAME || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
        end if;
      when 'DLOAD' then
        --valor_retorno :=  '    ' || ''' || ''TO_DATE ('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''';
        --valor_retorno := '    ' || 'date_format (''#VAR_FCH_CARGA#'', ''yyyy-MM-dd'')'; /* (20161223) Angel Ruiz */
        valor_retorno := '    ' || 'TO_DATE (''#VAR_FCH_CARGA#'', ''YYYYMMDD'')'; /* (20230724)*/
      when 'DSYS' then
        valor_retorno :=  '    ' || 'SYSDATE';
        --valor_retorno := '    ' || 'current_date'; /* (20161223) Angel Ruiz */
      when 'CODE' then
        /* 20141204 Angel Ruiz. Como es codigo dinamico he de detectar si hay una comilla para poner dos */
        /* Esto lo añado nuevo y solo en este generador pq genera procesos que soportan retrasados */
        pos := 0;
        posicion_ant := 0;
        cadena_resul:= trim(reg_detalle_in.VALUE);
        lon_cadena := length (cadena_resul);
        if lon_cadena > 0 then
          valor_retorno := procesa_campo_filter (cadena_resul);
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
          --valor_retorno :=  cad_pri || ''' || ''TO_DATE ('' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''' || cad_seg;
          --valor_retorno :=  '    ' || cad_pri || ' date_format(''#VAR_FCH_DATOS#'', ''yyyy-MM-dd'') ' || cad_seg; /* (20161208) Angel Ruiz */
        --end if;
      when 'HARDC' then
        /* (20170105) Angel Ruiz */
        if reg_detalle_in.VALUE <> 'NULL' then
          SELECT * INTO l_registro2
          FROM v_MTDT_CAMPOS_DETAIL
          WHERE UPPER(TRIM(TABLE_NAME)) =  UPPER(reg_detalle_in.TABLE_NAME) and
          UPPER(TRIM(COLUMN_NAME)) = UPPER(TRIM(reg_detalle_in.TABLE_COLUMN));
          if (l_registro2.type <> 'NUMBER') then
            valor_retorno := '    ''' || reg_detalle_in.VALUE || '''';
          else
            valor_retorno := '    ' || reg_detalle_in.VALUE;
          end if;
        else
            valor_retorno := '    ' || reg_detalle_in.VALUE;
        end if;
        --valor_retorno :=  '    ' || sustituye_comillas_dinam(reg_detalle_in.VALUE);
        --valor_retorno := '    ' || reg_detalle_in.VALUE;
      when 'SEQ' then
        --valor_retorno := '    ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
        --  valor_retorno := '    ' || reg_detalle_in.VALUE;
        --else
        --  valor_retorno := '    ' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
        --valor_retorno := '    ' || ''' || var_seqg || ''';
        /*(20170107) Angel Ruiz. NF: Secuencias en tablas de hechos */
        dbms_output.put_line('ESTOY EN LA REGLA SEQ.');
        l_FROM.extend;
        --l_FROM (l_FROM.LAST) := ' LEFT OUTER JOIN (SELECT NVL(MAX(' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN || '), 0) maximo from ' || OWNER_DM || '.' || reg_detalle_in.TABLE_NAME || ') ' || reg_detalle_in.value || ' ';
        l_FROM (l_FROM.LAST) := ' LEFT OUTER JOIN (SELECT ULT_VAL maximo from ' || OWNER_MTDT || '.MTDT_SEQUENCIAS WHERE ID_SEQ=''SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5) || ''') ' || 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5);
        --valor_retorno := '    ' || reg_detalle_in.value || '.' || 'maximo + (row_number() over (order by ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE || '))' ;
        valor_retorno := '    ' || 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5) || '.' || 'maximo + (row_number() over (order by ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE || '))' ;
        v_hay_regla_seq := true;
        v_nombre_seq := 'SEQ_' || substr(reg_detalle_in.TABLE_COLUMN, 5);
        v_nombre_campo_seq := reg_detalle_in.TABLE_COLUMN;
        
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        /* (20170127) Angel Ruiz. BUG. Si ya lleva punto es que se le ha puesto el propietario */
        /* por lo que no se le pone */
        if (instr(reg_detalle_in.VALUE, '.') = 0) then
          valor_retorno :=  '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;
        else
          valor_retorno :=  '    ' || reg_detalle_in.VALUE;
        end if;
      when 'VAR_FCH_INICIO' then
        --valor_retorno :=  '    ' || ''' || var_fch_inicio || ''';
        --valor_retorno :=  '    SYSDATE';
        --valor_retorno :=  '    TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')'; /*(20151221) Angel Ruiz BUG. Debe insertarse la fecha de inicio del proceso de insercion */
        /* (20161223) Angel Ruiz. Ocurre que esta regla la podemos usar tanto en */
        /* campos DATE como en campos DATETIME, por lo que hay que saber de que tipo de campo se trata */
        select type into v_tipo_campo from v_MTDT_CAMPOS_DETAIL where table_name = reg_detalle_in.TABLE_NAME and column_name = reg_detalle_in.TABLE_COLUMN;
        /* (20230724). Angel Ruiz */
        --if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
          --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd hh:mm:ss'')';
        --else
          --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd'')';
        --end if;
        /* (20230724). Angel Ruiz Fin*/
        if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
          valor_retorno := '    ' || 'TO_DATE(''#VAR_FCH_REGISTRO#'', ''YYMMDD HH24:MI:SS'')';
        else
          valor_retorno := '    ' || 'TO_DATE(''#VAR_FCH_REGISTRO#'', ''YYYYMMDD'')';
        end if;
      when 'VAR_FCH_CARGA' then
          valor_retorno := '     ' || '''#VAR_FCH_DATOS#'''; /* (20161219) Angel Ruiz */
      when 'VAR_FIN_DEFAULT' then
          --valor_retorno := '    ' || '''9999-12-31''';
          valor_retorno := '    ' || '''31/12/99''';
      when 'VAR_USER' then
          valor_retorno := '    ' || '''#VAR_USER#''';
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        --if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '    ' || ''' || fch_datos_in || ''';        
        --end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_DATOS' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '     ' || 'fch_datos_in';        
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#'''; /* (20161208) Angel Ruiz */
          valor_retorno := '     ' || 'TO_DATE(''#VAR_FCH_DATOS#'', ''YYYYMMDD'')'; /* (20230724) Angel Ruiz */
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno := '     ' || 'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          --valor_retorno := '     ' || 'fch_datos_in';        
          --valor_retorno := '     ' || '''#VAR_FCH_DATOS#''';  /* (20161208) Angel Ruiz */      
          valor_retorno := '     ' || 'TO_DATE(''#VAR_FCH_DATOS#'', ''YYYYMMDD'')';  /* (20230724) Angel Ruiz */      
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '    ' || '1';
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_INICIO' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          /* (20161208) Angel Ruiz. Ocurre que esta regla la podemos usar tanto en */
          /* campos DATE como en campos DATETIME, por lo que hay que saber de que tipo de campo se trata */
          /* (2023)*/
          select trim(data_type) into v_tipo_campo from mtdt_modelo_detail where trim(table_name) = reg_detalle_in.TABLE_NAME and trim(column_name) = reg_detalle_in.TABLE_COLUMN;
          --if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
            --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd hh:mm:ss'')';
          --else
            --valor_retorno := '    ' || 'date_format(''#VAR_FCH_REGISTRO#'', ''yyyy-MM-dd'')';
          --end if;
          if (instr(upper(v_tipo_campo), 'TIMESTAMP') > 0) then
            valor_retorno := '    ' || 'TO_DATE(''#VAR_FCH_REGISTRO#'', ''YYYYMMDD HH24:MI:SS'')';
          else
            valor_retorno := '    ' || 'TO_DATE(''#VAR_FCH_REGISTRO#'', ''YYYYMMDD'')';
          end if;
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
            valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || procesa_campo_filter(reg_detalle_in.VALUE) || ', '' '') ELSE ' || trim(constante) || ' END';
          else
            valor_retorno := procesa_campo_filter(reg_detalle_in.VALUE);
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
  SELECT VALOR INTO v_MULTIPLICADOR_PROC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'MULTIPLICADOR_PROC';
  SELECT VALOR INTO PAIS FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PAIS_DM';
  
  /* (20141223) FIN*/

  open MTDT_QUALITY_ASSURANCE;
  loop
    fetch MTDT_QUALITY_ASSURANCE
    into reg_quality;
    exit when MTDT_QUALITY_ASSURANCE%NOTFOUND;
    dbms_output.put_line ('Estoy en el primer LOOP. La VALIDACION que tengo es: ' || reg_quality.QUALITY_NAME);
    nombre_fich_carga := reg_quality.QUALITY_NAME || '.sh';
    --nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_pkg := reg_quality.QUALITY_NAME || '.sql';
    fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
    fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 10); /* Le quito al nombre de la validacion los caracteres iniciales NGQ_XXXX_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_quality.QUALITY_NAME) < 25) then
      nombre_proceso := reg_quality.QUALITY_NAME;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    
    --UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    lista_scenarios_presentes.delete;
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    dbms_output.put_line ('Comienzo la generacion del PACKAGE DEFINITION');
    v_poner_paren_cierre := false;

    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '-- ### INICIO DEL SCRIPT');
    UTL_FILE.put_line(fich_salida_pkg, '');
    
    if (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is null and reg_quality.PARTE_GROUP is null) then
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, 'COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        v_poner_paren_cierre := true;
      end if;
      --UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      --UTL_FILE.put_line(fich_salida_pkg, '*');
      --UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
    elsif (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is not null and reg_quality.PARTE_GROUP is null) then
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, 'COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      UTL_FILE.put_line(fich_salida_pkg, '(');
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.PARTE_SELECT));
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
        UTL_FILE.put_line(fich_salida_pkg, ') ' || reg_quality.QUALITY_NAME || '_IN');
      else
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
      end if;
    elsif (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is null and reg_quality.PARTE_GROUP is not null) then
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, 'COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      UTL_FILE.put_line(fich_salida_pkg, '(');
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.PARTE_GROUP));
      UTL_FILE.put_line(fich_salida_pkg, ', COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
        UTL_FILE.put_line(fich_salida_pkg, ') ' || reg_quality.QUALITY_NAME || '_IN');
      else
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
      end if;
    elsif (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is not null and reg_quality.PARTE_GROUP is not null) then
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, 'COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      UTL_FILE.put_line(fich_salida_pkg, '(');
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.PARTE_GROUP));
      UTL_FILE.put_line(fich_salida_pkg, ', COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
        UTL_FILE.put_line(fich_salida_pkg, ') ' || reg_quality.QUALITY_NAME || '_IN');
      else
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
      end if;
    elsif (reg_quality.FILE_SPOOL = 'S' and reg_quality.PARTE_SELECT is null and reg_quality.PARTE_GROUP is null) then
      /* Si no hay parte SELECT ni GROUP y ademas se genera fichero, FILE_SPOOL='S' */
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, '*');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
        UTL_FILE.put_line(fich_salida_pkg, ') ' || reg_quality.QUALITY_NAME || '_IN');
      else
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
      end if;
    elsif (reg_quality.FILE_SPOOL = 'S' and reg_quality.PARTE_SELECT is not null and reg_quality.PARTE_GROUP is null) then
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.PARTE_SELECT));
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
        UTL_FILE.put_line(fich_salida_pkg, ') ' || reg_quality.QUALITY_NAME || '_IN');
      else
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
      end if;
    elsif (reg_quality.FILE_SPOOL = 'S' and reg_quality.PARTE_SELECT is null and reg_quality.PARTE_GROUP is not null) then
      UTL_FILE.put_line(fich_salida_pkg, 'SELECT');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.PARTE_GROUP));
      UTL_FILE.put_line(fich_salida_pkg, ', COUNT(1)');
      UTL_FILE.put_line(fich_salida_pkg, 'FROM');
      if (regexp_instr(reg_quality.TABLE_BASE_NAME, '^ *\(') = 0 and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        /* Aunque TABLE_BASE_NAME es una SELECT no tiene el ( inicial. Se lo ponemos */
        UTL_FILE.put_line(fich_salida_pkg, '(');
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
        UTL_FILE.put_line(fich_salida_pkg, ') ' || reg_quality.QUALITY_NAME || '_IN');
      else
        UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.TABLE_BASE_NAME));
      end if;
    end if;
    /* (20170906) Angel Ruiz. Hay que cerrar en caso*/
    if (reg_quality.FILTER is not null) then
      UTL_FILE.put_line(fich_salida_pkg,'WHERE');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.FILTER));
    end if;
    if (reg_quality.PARTE_GROUP is not null) then
      /* Tenemos una subquery con un Group By */
      UTL_FILE.put_line(fich_salida_pkg,'GROUP BY');
      UTL_FILE.put_line(fich_salida_pkg, procesa_campo_filter(reg_quality.PARTE_GROUP));
    end if;
    if (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is null and reg_quality.PARTE_GROUP is null) then
      /* En este caso tenemos una subquery por lo que hay que cerrar el parentesis si no se cierra*/
      /* ya en el propio campo TABLE_BASE_NAME*/
      if v_poner_paren_cierre = true then
      --if (not REGEXP_LIKE(reg_quality.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$') and regexp_instr(reg_quality.TABLE_BASE_NAME, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
        --if (regexp_instr(reg_quality.TABLE_BASE_NAME, '\) *$') = 0) then
        UTL_FILE.put_line(fich_salida_pkg,') ' || reg_quality.QUALITY_NAME);
        --else
          --UTL_FILE.put_line(fich_salida_pkg,' ' || reg_quality.QUALITY_NAME  );
        --end if;
      end if;
    elsif (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is not null and reg_quality.PARTE_GROUP is null) then
      UTL_FILE.put_line(fich_salida_pkg,') ' || reg_quality.QUALITY_NAME);
    elsif (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is null and reg_quality.PARTE_GROUP is not null) then
      UTL_FILE.put_line(fich_salida_pkg,') ' || reg_quality.QUALITY_NAME);
    elsif (reg_quality.FILE_SPOOL = 'N' and reg_quality.PARTE_SELECT is not null and reg_quality.PARTE_GROUP is not null) then
      UTL_FILE.put_line(fich_salida_pkg,') ' || reg_quality.QUALITY_NAME);
    end if;      
    UTL_FILE.put_line(fich_salida_pkg,';');
    UTL_FILE.put_line(fich_salida_pkg,'');
    UTL_FILE.put_line(fich_salida_pkg,'');
    
        
  
    /**************/
    
    /******/
    /* FIN DE LA GENERACION DEL PACKAGE */
    /******/
    /******/    
    --UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg, 'grant execute on ' || OWNER_DM || '.pkg_' || nombre_proceso || ' to ' || OWNER_TC || ';');
    --UTL_FILE.put_line(fich_salida_pkg, '/');
    --UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg, '!quit');
    UTL_FILE.put_line(fich_salida_pkg, '');
    

    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/
    
    UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_load, '#############################################################################');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Archivo    :       ' ||  reg_quality.QUALITY_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                               #');
    UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta un proceso del módulo QUALITY ASSURANCE.   #');
    UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Historia : 05-Mayo-2017 -> Creacion                                    #');
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
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || reg_quality.QUALITY_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" current_timestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || reg_quality.QUALITY_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" current_timestamp ${FCH_DATOS} ${FCH_CARGA} ${TOT_INSERTADOS} 0 0 ${TOT_LEIDOS} ${TOT_RECHAZADOS}' || ' >> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_' || PAIS || '.sh');
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
    UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
    UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA_MES=`echo ${FCH_CARGA} | cut -c 1-6`');
    UTL_FILE.put_line(fich_salida_load, 'TOT_HISTO=0');
    UTL_FILE.put_line(fich_salida_load, 'TOT_INSERTADOS=0');
    UTL_FILE.put_line(fich_salida_load, 'TOT_RECHAZADOS=0');
    UTL_FILE.put_line(fich_salida_load, 'TOT_MODIFICADOS=0');
    UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO=1');
    
    --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}' || '.log ');
    --UTL_FILE.put_line(fich_salida_sh, 'set -x');
    UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
    UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
    UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    --UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="' || v_REQ_NUMER || '"');
    --UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || v_REQ_NUMER || '_load_' || reg_tabla.TABLE_NAME);
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# LIBRERIAS                                                                    #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
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
    UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
    UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE=${PASSWORD}');

    UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER SQLERROR EXIT 1;');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER OSERROR EXIT 2;');
    UTL_FILE.put_line(fich_salida_load, 'SET PAGESIZE 0;');
    UTL_FILE.put_line(fich_salida_load, 'SET HEADING OFF;');
    UTL_FILE.put_line(fich_salida_load, '  SELECT nvl(MAX(' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PASO),0)');
    UTL_FILE.put_line(fich_salida_load, '  FROM');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' || OWNER_MTDT || '.MTDT_PASO');
    UTL_FILE.put_line(fich_salida_load, '  WHERE');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.FCH_CARGA = to_date(''${FCH_CARGA}'', ''yyyymmdd'') AND');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.FCH_DATOS = to_date(''${FCH_DATOS}'', ''yyyymmdd'') AND');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_PROCESO.NOMBRE_PROCESO = ''${0}'' AND');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_PROCESO.CVE_PROCESO = ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PROCESO AND');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_PROCESO.CVE_PROCESO = ' || OWNER_MTDT || '.MTDT_PASO.CVE_PROCESO AND');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_PASO.CVE_PASO = ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PASO AND');
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_RESULTADO = 0;');
    UTL_FILE.put_line(fich_salida_load, 'QUIT;');
    UTL_FILE.put_line(fich_salida_load, 'EOF`');
    UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER SQLERROR EXIT 1;');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER OSERROR EXIT 2;');
    UTL_FILE.put_line(fich_salida_load, 'SET PAGESIZE 0;');
    UTL_FILE.put_line(fich_salida_load, 'SET HEADING OFF;');
    UTL_FILE.put_line(fich_salida_load, 'SELECT cast (systimestamp as timestamp) from dual;');
    UTL_FILE.put_line(fich_salida_load, 'QUIT;');
    UTL_FILE.put_line(fich_salida_load, 'EOF`');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER SQLERROR EXIT 1;');
    UTL_FILE.put_line(fich_salida_load, 'WHENEVER OSERROR EXIT 2;');
    UTL_FILE.put_line(fich_salida_load, 'SET PAGESIZE 0;');
    UTL_FILE.put_line(fich_salida_load, 'SET HEADING OFF;');
    UTL_FILE.put_line(fich_salida_load, 'SELECT cast (systimestamp as timestamp) from dual;');
    UTL_FILE.put_line(fich_salida_load, 'QUIT;');
    UTL_FILE.put_line(fich_salida_load, 'EOF`');

    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'sed -e "s/#VAR_FCH_REGISTRO#/sysdate/g" -e "s/#VAR_FCH_CARGA#/${FCH_CARGA}/g" -e "s/#VAR_FCH_DATOS#/${FCH_DATOS}/g" -e "s/#VAR_USER#/${BD_USER}/g" -e "s/#VAR_CVE_MES#/${FCH_CARGA_MES}/g" -e "s/#VAR_CVE_DIA#/${FCH_CARGA}/g" ${' || NAME_DM || '_SQL}/' || reg_quality.QUALITY_NAME || '.sql > ${' || NAME_DM || '_SQL}/' || reg_quality.QUALITY_NAME || '_tmp.sql');

    /***********************************************************************************/
    --UTL_FILE.put_line(fich_salida_load, 'beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_ML}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} -f ' || '${' || NAME_DM || '_SQL}/pkg_' || reg_tabla.TABLE_NAME || '_tmp.sql >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_he' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');
    /* (20170509) Angel Ruiz. Dependiendo de si estamos generando un script que a su vez genera un fichero de salida */
    /* o en cambio no lo hace */
    if (reg_quality.FILE_SPOOL = 'S') then
      /* (20170509) Angel Ruiz. Se tarta de un proceso de calidad que genera un fichero de contraste */
      UTL_FILE.put_line(fich_salida_load, 'sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} << EOF ${' || NAME_DM || '_TMP}/' || reg_quality.QUALITY_NAME || '_${FCH_DATOS}.dat 2>> ' || '${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, 'WHENEVER SQLERROR EXIT 1;');
      UTL_FILE.put_line(fich_salida_load, 'WHENEVER OSERROR EXIT 2;');
      UTL_FILE.put_line(fich_salida_load, 'SET PAGESIZE 0;');
      UTL_FILE.put_line(fich_salida_load, 'SET HEADING OFF;');
      UTL_FILE.put_line(fich_salida_load, '@${' || NAME_DM || '_SQL}/' || reg_quality.QUALITY_NAME || '_tmp.sql');
      UTL_FILE.put_line(fich_salida_load, 'QUIT;');
      UTL_FILE.put_line(fich_salida_load, 'EOF');
    else
      UTL_FILE.put_line(fich_salida_load, 'TOT_INSERTADOS=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} << EOF 2>> ${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, 'WHENEVER SQLERROR EXIT 1;');
      UTL_FILE.put_line(fich_salida_load, 'WHENEVER OSERROR EXIT 2;');
      UTL_FILE.put_line(fich_salida_load, 'SET PAGESIZE 0;');
      UTL_FILE.put_line(fich_salida_load, 'SET HEADING OFF;');
      UTL_FILE.put_line(fich_salida_load, '@${' || NAME_DM || '_SQL}/' || reg_quality.QUALITY_NAME || '_tmp.sql');
      UTL_FILE.put_line(fich_salida_load, 'QUIT;');
      UTL_FILE.put_line(fich_salida_load, 'EOF`');
    end if;
    UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a pre_' || nombre_proceso || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '# Borro el fichero temporal .sql generado en vuelo');
    UTL_FILE.put_line(fich_salida_load, 'rm ${' || NAME_DM || '_SQL}/' || reg_quality.QUALITY_NAME || '_tmp.sql');
    UTL_FILE.put_line(fich_salida_load, '');
    if (reg_quality.FILE_SPOOL = 'S') then
      /* (20170509) Angel Ruiz. Se tarta de un proceso de calidad que genera un fichero de contraste */
      UTL_FILE.put_line(fich_salida_load, '# Obtenemos el numero de registros para despues grabarlo en el metadato');
      UTL_FILE.put_line(fich_salida_load, 'TOT_INSERTADOS=`cat ${' || NAME_DM || '_TMP}/' || reg_quality.QUALITY_NAME || '_${FCH_DATOS}.dat | wc -l`');
    end if;
    UTL_FILE.put_line(fich_salida_load, '# Insertamos que el proceso se ha Ejecutado Correctamente');
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'echo "El proceso ' || reg_quality.QUALITY_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || reg_quality.QUALITY_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'exit 0');
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
    
    /*************************/
    /******/
    /* INICIO DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    /******/
    /* FIN DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    
    /*************************/
    UTL_FILE.FCLOSE (fich_salida_load);
    --UTL_FILE.FCLOSE (fich_salida_exchange);
    UTL_FILE.FCLOSE (fich_salida_pkg);
  end loop;
  close MTDT_QUALITY_ASSURANCE;
end;

