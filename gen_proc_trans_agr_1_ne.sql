declare

cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(mtdt_modelo_summary.TABLESPACE) "TABLESPACE"
    FROM
      MTDT_TC_SCENARIO, mtdt_modelo_summary
    WHERE MTDT_TC_SCENARIO.TABLE_TYPE = 'A' and
    trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_summary.TABLE_NAME)
    and
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMA_TRAFD_CU_MES_DN', 'DMA_TRAFE_CU_MES_DN', 'DMA_TRAFV_CU_MES_DN', 'DMA_PARQUE_MVNO_MES_DN');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_MOVIMIENTOS_MVNO', 'DMF_RECARGAS_MVNO', 'DMF_PARQUE_MVNO');  
    trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMA_PARQUE_SERIADOS_MES_DN');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_PMP', 'DMF_PARQUE_SERIADOS', 'DMF_FACT_SERIADOS', 'DMF_MOVIMIENTOS_SERIADOS');  

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
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(SCENARIO) "SCENARIO",
      TRIM("OUTER") "OUTER",
      SEVERIDAD,
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TRIM(TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(LKUP_COM_RULE) "LKUP_COM_RULE",
      TRIM(VALUE) "VALUE",
      TRIM(RUL) "RUL",
      TRIM(CRUCE) "CRUCE",
      TRIM(INSERT_OPE) "INSERT_OPE",
      TRIM(UPDATE_OPE) "UPDATE_OPE",
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
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      TRIM("VALUE") "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      RUL = 'LKUP' and
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
  
  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(30);
  type list_campos_group_by is table of varchar(100);  /* (20151123) Angel Ruiz. NF: AGREGADOS */
  type list_campos_where_ori is table of varchar(30);  /* (20151123) Angel Ruiz. NF: AGREGADOS */
  type list_campos_where_des is table of varchar(30);  /* (20151123) Angel Ruiz. NF: AGREGADOS */
  
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  v_list_campos_group_by            list_campos_group_by := list_campos_group_by();
  v_list_campos_where_ori           list_campos_where_ori := list_campos_where_ori();
  v_list_campos_where_des         list_campos_where_des := list_campos_where_des();
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(2000);
  prototipo_fun                             VARCHAR2(2000);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_desagrega             UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
  nombre_fich_desagrega           VARCHAR2(60);
  lista_scenarios_presentes                                    list_strings := list_strings();
  lista_table_base_name                                    list_strings := list_strings();
  lista_lkup                                    list_strings := list_strings();
  campo_filter                                VARCHAR2(2000);
  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_reducido							VARCHAR2(30);
  v_nombre_particion                  VARCHAR2(30);  
  nombre_tabla_T                        VARCHAR2(30);
  nombre_tabla_T_agr         VARCHAR2(30);
  OWNER_SA                         VARCHAR2(60);
  OWNER_T                          VARCHAR2(60);
  OWNER_DM                         VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  OWNER_TC                              VARCHAR(60);
  PREFIJO_DM                            VARCHAR(60);
  
  nombre_funcion                   VARCHAR2(100);
  v_encontrado											VARCHAR2(1):= 'N';
  v_contador                        PLS_INTEGER:=0;
  v_existe_scn_AGR          PLS_INTEGER:=0;
  v_existe_scn_DSG          PLS_INTEGER:=0;
	
  v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;
  v_indice_DSG              PLS_INTEGER:=0;

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
        /* Busco VAR_FCH_CARGA */
        sustituto := ' to_date ( fch_datos_in, ''yyyymmdd'') ';
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
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        pos := 0;
        posicion_ant := 0;
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
        
      end if;
      return cadena_resul;
    end;
/************/
/*************/
  function procesa_campo_filter_dinam (cadena_in in varchar2) return varchar2
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
        /* Busco VAR_FCH_CARGA */
        sustituto := ' to_date ('''''' ||  fch_datos_in || '''''', ''''yyyymmdd'''') ';
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
        
      end if;
      return cadena_resul;
    end;

  function genera_campo_insert_merge ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (2000);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(2000);
    cad_seg         VARCHAR(2000);
    cadena            VARCHAR(2000);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(2000);
    constante         VARCHAR2(2000);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_paquete                    VARCHAR2(40);
    v_nombre_tabla_reducido         VARCHAR2(40);    
    v_IE_COLUMN_LKUP              VARCHAR(400);
    
  begin
    /* Seleccionamos el escenario primero */
    dbms_output.put_line('El valor de reg_detalle_in.UPDATE_OPE es: ' || reg_detalle_in.UPDATE_OPE);
    dbms_output.put_line('El valor de reg_detalle_in.INSERT_OPE es: ' || reg_detalle_in.INSERT_OPE);
    dbms_output.put_line('El valor de reg_detalle_in.RUL es: ' || reg_detalle_in.RUL);
    dbms_output.put_line('El valor de value es: ' || reg_detalle_in.VALUE);
    case reg_detalle_in.INSERT_OPE
      when 'ASIGNA' then
        case reg_detalle_in.RUL
          when 'SELECT' then
            /* Se mantienen el valor del campo de la tabla que estamos cargando */
            valor_retorno := reg_detalle_in.VALUE;
          when 'VAR_FCH_INICIO' then
            --valor_retorno := ''' || fch_registro_in || ''';
            valor_retorno := 'TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')';
          else
            valor_retorno := 'ORIGEN.' || reg_detalle_in.TABLE_COLUMN;
        end case;
      when 'INCREMENTA' then
        valor_retorno := 'DESTINO.' || reg_detalle_in.TABLE_COLUMN || ' + ORIGEN.' || reg_detalle_in.TABLE_COLUMN;
      when 'DECREMENTA' then
        valor_retorno := 'DESTINO.' || reg_detalle_in.TABLE_COLUMN || ' - ORIGEN.' || reg_detalle_in.TABLE_COLUMN;
      end case;
      
    return valor_retorno;  
  end;


  function genera_campo_update_merge ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (2000);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(2000);
    cad_seg         VARCHAR(2000);
    cadena            VARCHAR(2000);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(2000);
    constante         VARCHAR2(2000);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_paquete                    VARCHAR2(40);
    v_nombre_tabla_reducido         VARCHAR2(40);    
    v_IE_COLUMN_LKUP              VARCHAR(400);
    
  begin
    /* Seleccionamos el escenario primero */
    dbms_output.put_line('El valor de reg_detalle_in.UPDATE_OPE es: ' || reg_detalle_in.UPDATE_OPE);
    dbms_output.put_line('El valor de reg_detalle_in.RUL es: ' || reg_detalle_in.RUL);
    dbms_output.put_line('El valor de value es: ' || reg_detalle_in.VALUE);
    case reg_detalle_in.UPDATE_OPE
      when 'ASIGNA' then
        case reg_detalle_in.RUL
          when 'SELECT' then
            /* Se mantienen el valor del campo de la tabla que estamos cargando */
            valor_retorno := reg_detalle_in.VALUE;
          when 'VAR_FCH_INICIO' then
            --valor_retorno := ''' || fch_registro_in || ''';
            valor_retorno := 'TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')';
          else
            valor_retorno := 'ORIGEN.' || reg_detalle_in.TABLE_COLUMN;
        end case;
      when 'INCREMENTA' then
        valor_retorno := 'DESTINO.' || reg_detalle_in.TABLE_COLUMN || ' + ORIGEN.' || reg_detalle_in.TABLE_COLUMN;
      when 'DECREMENTA' then
        valor_retorno := 'DESTINO.' || reg_detalle_in.TABLE_COLUMN || ' - ORIGEN.' || reg_detalle_in.TABLE_COLUMN;
      end case;
      
    return valor_retorno;  
  end;


  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (2000);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(2000);
    cad_seg         VARCHAR(2000);
    cadena            VARCHAR(2000);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(2000);
    constante         VARCHAR2(2000);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_paquete                    VARCHAR2(40);
    v_nombre_tabla_reducido         VARCHAR2(40);    
    v_IE_COLUMN_LKUP              VARCHAR(400);
    
  begin
    /* Seleccionamos el escenario primero */
      case reg_detalle_in.RUL
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;      
      when 'LKUP' then
        /* Se trata de hacer el LOOK UP con la tabla dimension */
        --if (trim(reg_detalle_in.LKUP_COM_RULE) <> "") then
        
        /* (20150309) Angel Ruiz. Anyado esta nueva funcionalidad */
        /*Puede ocurrir que en el campo VALUE de la llamada a LOOKUP se use la variable VAR_FCH_CARGA */
        v_IE_COLUMN_LKUP := procesa_campo_filter (reg_detalle_in.IE_COLUMN_LKUP);
        /****************************/
        /* (20150306) ANGEL RUIZ. Hay un error que corrijo */
        v_nombre_tabla_reducido := substr(reg_detalle_in.TABLE_NAME, 5);
        if (length(reg_detalle_in.TABLE_NAME) < 25) then
        v_nombre_paquete := reg_detalle_in.TABLE_NAME;
        else
        v_nombre_paquete := v_nombre_tabla_reducido;
        end if;        
        
        /* (20150130) Angel Ruiz. Nueva Incidencia. */
        /* La tabla de LookUp puede ser una SELECT y no solo una tabla */
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas para LookUp */
          v_nombre_func_lookup := 'LK_' || reg_detalle_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion con el nombre del campo resultado del LookUp */
        else
            v_nombre_func_lookup := 'LK_' || reg_detalle_in.TABLE_LKUP;  /* Llamo a mi funcion de LookUp esta concatenacion */
        end if;
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion) || 'THEN ' || 'PKG_' || v_nombre_paquete || '.' || v_nombre_func_lookup || ' (' || v_IE_COLUMN_LKUP || ') ELSE ' || trim(constante);
        else
          valor_retorno :=  '    ' || 'PKG_' || v_nombre_paquete || '.' || v_nombre_func_lookup || ' (' || v_IE_COLUMN_LKUP || ')';
        end if;
        --valor_retorno :=  '    ' || 'PKG_' || reg_detalle_in.TABLE_NAME || '.' || 'LKUP_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
        valor_retorno :=  '    ' || 'PKG_' || reg_detalle_in.TABLE_NAME || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
      when 'DLOAD' then
        valor_retorno :=  '    ' || 'TO_DATE (fch_datos_in, ''YYYYMMDD'')';
      when 'DSYS' then
        valor_retorno :=  '    ' || 'SYSDATE';
      when 'CODE' then
        valor_retorno := '    ' || trim(reg_detalle_in.VALUE);
        posicion := instr(valor_retorno, 'VAR_IVA');
        if (posicion >0) then
          cad_pri := substr(valor_retorno, 1, posicion-1);
          cad_seg := substr(valor_retorno, posicion + length('VAR_IVA'));
          valor_retorno :=  cad_pri || '16' || cad_seg;
        end if;
        posicion := instr(valor_retorno, 'VAR_FCH_CARGA');
        if (posicion >0) then
          cad_pri := substr(valor_retorno, 1, posicion-1);
          cad_seg := substr(valor_retorno, posicion + length('VAR_FCH_CARGA'));
          valor_retorno :=  cad_pri || ' to_date(fch_datos_in, ''yyyymmdd'') ' || cad_seg;
        end if;
      when 'HARDC' then
        valor_retorno :=  '    ' || reg_detalle_in.VALUE;
      when 'SEQ' then
        valor_retorno := '    ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
      --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
          --valor_retorno := '    ' || reg_detalle_in.VALUE;
        --else
          --valor_retorno := '    ' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;      
      when 'SUM' then
        /* (20151123) Angel Ruiz. NF: AGREGADOS */
        valor_retorno := '    SUM(' ||  reg_detalle_in.VALUE || ')';
      when 'GROUP_BY' then
        /* (20151123) Angel Ruiz. NF: AGREGADOS */
        valor_retorno := '    ' ||  reg_detalle_in.VALUE;
        v_list_campos_group_by.EXTEND;
        v_list_campos_group_by (v_list_campos_group_by.LAST) := reg_detalle_in.VALUE;
      when 'VAR_FCH_INICIO' then
        /* (20160113) Angel Ruiz. BUG. */
        --valor_retorno :=  '    ' || 'var_fch_inicio';
        valor_retorno :=  '    TO_DATE(fch_registro_in, ''YYYYMMDDHH24MISS'')'; /*(20151221) Angel Ruiz BUG. Debe insertarse la fecha de inicio del proceso de insercion */        
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        if reg_detalle_in.VALUE =  'VAR_FCH_DATOS' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          valor_retorno := '     ' || 'fch_datos_in';        
        end if;
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno := '     ' || 'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          valor_retorno := '     ' || 'fch_datos_in';        
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '    ' ||  '1';
        end if;
      end case;
    return valor_retorno;
  end;

/*************/
  function genera_encabezado_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                      list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
  begin
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
      /* Aparecen queries en lugar de tablas para LookUp */
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.VALUE;  /* Llamo a mi funcion de LookUp esta concatenacion con el nombre del campo resultado del LookUp */
      v_nombre_tabla := reg_lookup_in.TABLE_BASE_NAME;  /* Si lo que tengo es una SELECT tengo que recurrir al nombre de la tabla BASE para posteriormente saber el tipo de campo  */
    else
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_LKUP;  /* Llamo a mi funcion de LookUp esta concatenacion */
      v_nombre_tabla := reg_lookup_in.TABLE_LKUP;     /* Como no tengo una SELECT uso la tabla de LookUp para posteriormente saber el tipo de campo  */
    end if;
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then    /* (20150102) Angel Ruiz . Nueva incidencia. Hay una SELECT en lugar de una tabla para hacer LookUp */
      /* Para hacer el prototipo de la funcion he de usar la tabla base y los campos ie_olumn_lookup ya que no tenemos los campos de LookUp al ser una select */
      lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      ie_lkup_columns := split_string_coma (reg_lookup_in.IE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
        FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
        LOOP
          if indx = 1 then
              valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          else
            valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          end if;
        END LOOP;
        valor_retorno := valor_retorno || ') return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.VALUE || '%TYPE RESULT_CACHE;';
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || reg_lookup_in.IE_COLUMN_LKUP || '%TYPE) return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.value || '%TYPE RESULT_CACHE;';
      end if;
        
    else  /* (20150102) Angel Ruiz . Nueva incidencia. Hay una tabla de LookUp normal. No SELECT */
      lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
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
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE RESULT_CACHE;';
      end if;
    end if;
    return valor_retorno;
  end;

/************/
  function gen_encabe_regla_function (reg_function_in in MTDT_TC_FUNCTION%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (300);
    lkup_columns                list_strings := list_strings();
  begin
    valor_retorno := '  FUNCTION ' || 'LK_' || reg_function_in.VALUE;
    return valor_retorno;
  end gen_encabe_regla_function;

  procedure genera_cuerpo_regla_function (reg_function_in in MTDT_TC_FUNCTION%rowtype) is
  begin
    UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || 'LK_' || reg_function_in.VALUE);
    UTL_FILE.put_line (fich_salida_pkg, '  RESULT_CACHE');
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    dbms_output.put_line (''Aqui iria el cuerpo de la funcion'');');
    UTL_FILE.put_line (fich_salida_pkg, '    /* AQUI IRIA EL CUERPO DE LA FUNCION */');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_function_in.TABLE_LKUP);
  end genera_cuerpo_regla_function;

/************/
  procedure genera_cuerpo_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_alias             VARCHAR2(40);
    mitabla_look_up VARCHAR2(200);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
    l_registro          ALL_TAB_COLUMNS%rowtype;

  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
      /* Aparecen queries en lugar de tablas para LookUp */
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion con el nombre del campo destino del LookUp */
      v_nombre_tabla := reg_lookup_in.TABLE_BASE_NAME;  /* Si lo que tengo es una SELECT tengo que recurrir al nombre de la tabla BASE para posteriormente saber el tipo de campo  */
    else
      v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_LKUP;  /* Llamo a mi funcion de LookUp esta concatenacion */
      v_nombre_tabla := reg_lookup_in.TABLE_LKUP;     /* Como no tengo una SELECT uso la tabla de LookUp para posteriormente saber el tipo de campo  */
    end if;
    
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then    /* (20150102) Angel Ruiz . Nueva incidencia. Hay una SELECT en lugar de una tabla para hacer LookUp */
      /* Para hacer el prototipo de la funcion he de usar la tabla base y los campos ie_olumn_lookup ya que no tenemos los campos de LookUp al ser una select */
      /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
      lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      ie_lkup_columns := split_string_coma (reg_lookup_in.IE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
        FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
        LOOP
          if indx = 1 then
              valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          else
            valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || ie_lkup_columns(indx) || '%TYPE';
          end if;
        END LOOP;
        valor_retorno := valor_retorno || ') return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE';
        UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || reg_lookup_in.IE_COLUMN_LKUP || '%TYPE) return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE';
        UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
      end if;
    else
      /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
      lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
      if (lkup_columns.COUNT > 1)
      then
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (';
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
        UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE)'); 
      end if;
      UTL_FILE.put_line (fich_salida_pkg, '    return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE');
      UTL_FILE.put_line (fich_salida_pkg, '    RESULT_CACHE RELIES_ON (' || reg_lookup_in.TABLE_LKUP || ')');
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
      UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE;');
    else
      UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    /**********************************************************/
    /* (20150217) Angel Ruiz. Incidencia debido a que no esta retornando bien el valor de LookUp cuando se hace LookUp por varios campos */
    if (lkup_columns.COUNT > 1) then
      UTL_FILE.put_line (fich_salida_pkg, '');
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        SELECT * INTO l_registro
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME =  reg_lookup_in.TABLE_LKUP and
        COLUMN_NAME = trim(lkup_columns(indx));

        if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
          if (indx = 1) then
            UTL_FILE.put_line (fich_salida_pkg, '  if (' || lkup_columns(indx) || '_IN ' || 'IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO''');
          else
            if (indx = lkup_columns.LAST) then
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' ||lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO'') then');
            else
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' ||lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = ''NI#'' OR ' || lkup_columns(indx) || '_IN' || ' = ''NO INFORMADO''');
            end if;
          end if;
        else
          if (indx = 1) then
            UTL_FILE.put_line (fich_salida_pkg, '  if (' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3');
          else
            if (indx = lkup_columns.LAST) then
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3) then');
            else
              UTL_FILE.put_line (fich_salida_pkg, '    ' || 'OR ' || lkup_columns(indx) || '_IN' || ' IS NULL OR ' || lkup_columns(indx) || '_IN' || ' = -3');
            end if;
          end if;
        end if;
      END LOOP;
      UTL_FILE.put_line (fich_salida_pkg, '      l_row := -3;');
      UTL_FILE.put_line (fich_salida_pkg, '  else');
    end if;

    UTL_FILE.put_line (fich_salida_pkg, '');

    /*********************************************************/
    
    
    UTL_FILE.put_line (fich_salida_pkg, '    SELECT nvl(' || reg_lookup_in.VALUE || ', -2) INTO l_row');
    /*(20150130) Angel Ruiz */
    /* Nueva Incidencia. */
    if (instr (reg_lookup_in.TABLE_LKUP, 'SELECT ') > 0) then
      /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
      v_alias := 'LKUP';
      mitabla_look_up := '(' || reg_lookup_in.TABLE_LKUP || ') "LKUP"';
      UTL_FILE.put_line (fich_salida_pkg, '    FROM ');
      UTL_FILE.put_line (fich_salida_pkg, '    ' || mitabla_look_up);
    else
      v_alias := reg_lookup_in.TABLE_LKUP;
      UTL_FILE.put_line (fich_salida_pkg, '    FROM ' || reg_lookup_in.TABLE_LKUP);
    end if;
    
    if (lkup_columns.COUNT > 1) then
      valor_retorno := '    WHERE ' ;
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || v_alias || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        else
          valor_retorno := valor_retorno || ' and ' || v_alias || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
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
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || v_alias || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
        else
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || v_alias || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
        end if;
      end if;
      --if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
      --  UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
      --else
      --  UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
      --end if;
    end if;
    /* (20150217) Angel Ruiz. Incidencia debido a que no esta retornando bien el valor de LookUp cuando se hace LookUp por varios campos */
    if (lkup_columns.COUNT > 1) then
      UTL_FILE.put_line (fich_salida_pkg, '');
      UTL_FILE.put_line (fich_salida_pkg, '  end if;');
    end if;
    /***********************************/
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN l_row;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  exception');
    UTL_FILE.put_line (fich_salida_pkg, '  when NO_DATA_FOUND then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '  when others then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' ||  v_nombre_func_lookup || ';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '');
 
  end genera_cuerpo_funcion_pkg;

/************/


begin
  /* (20141223) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  
  /* (20141223) FIN*/

  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    v_existe_scn_AGR := 0;
    v_existe_scn_DSG := 0;
    dbms_output.put_line ('Estoy en el primero LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);    
    nombre_fich_carga := 'load_he_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_desagrega := 'load_ds_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql';
    fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    --fich_salida_desagrega := UTL_FILE.FOPEN ('SALIDA',nombre_fich_desagrega,'W');
    --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
    fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_tabla.TABLE_NAME) < 25) then
      nombre_proceso := reg_tabla.TABLE_NAME;
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
    
    UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    lista_scenarios_presentes.delete;
    lista_table_base_name.delete; /*(20151126) Angel Ruiz. NF: AGREGACION */
    v_list_campos_group_by.delete;    /*(20151123) Angel Ruiz. NF: AGREGADOS */
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    
    /* Primero de todo miro si hay funciones de LOOKUP para crear */
    lista_lkup.delete;
    v_contador:=0;
    open MTDT_TC_LOOKUP (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_LOOKUP
      into reg_lookup;
      exit when MTDT_TC_LOOKUP%NOTFOUND;
      if (instr(reg_lookup.TABLE_LKUP, 'SELECT ') > 0) then
      	/* Se trata de una LookUp con una SELECT en lugar de una Tabla */
      	nombre_funcion := 'LK_' || reg_lookup.TABLE_COLUMN;
      else
      	nombre_funcion := 'LK_' || reg_lookup.TABLE_LKUP;
      end if;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      /* Buscamos si la funcion de lookup ya la hemos generado, ya que si ya esta generada no hay que generarla de nuevo */
    	v_encontrado := 'N';
      if (v_contador = 0) then
      	lista_lkup.EXTEND;
      	lista_lkup (lista_lkup.last) := nombre_funcion;
      	prototipo_fun := genera_encabezado_funcion_pkg (reg_lookup);
	      UTL_FILE.put_line(fich_salida_pkg,'');
	      UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
        v_contador:=v_contador+1;
      else
        for indx in lista_lkup.FIRST .. lista_lkup.LAST
        loop
          if (lista_lkup(indx) = nombre_funcion) then
            v_encontrado := 'Y';
          end if;
        end loop;
        if (v_encontrado = 'N') then
          lista_lkup.EXTEND;
          lista_lkup (lista_lkup.last) := nombre_funcion;
          prototipo_fun := genera_encabezado_funcion_pkg (reg_lookup);
          UTL_FILE.put_line(fich_salida_pkg,'');
          UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
        end if;
        v_contador:=v_contador+1;
      end if;
    end loop;
    close MTDT_TC_LOOKUP;

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
    
    /* Tercero genero los metodos para los escenarios */

    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
      /* Elaboramos la implementacion de las funciones de LOOK UP antes de nada */
      
      if (reg_scenario.SCENARIO = 'N')      /*  Procesamos el escenario NUEVO  */
      then
        /* Tenemos el escenario Nuevo */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'N';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;
      end if;
      
      /************************/
      if (reg_scenario.SCENARIO = 'OPE')    /*  Procesamos el escenario OPE  */
      then
        /* Tenemos el escenario OPE */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ope_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'OPE';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;

      end if;
      /************************/

      if (reg_scenario.SCENARIO = 'ALT')    /*  Procesamos el escenario ALT  */
      then
        /* Tenemos el escenario ALT */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION alt_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'ALT';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;
        
      end if;

      /************************/

      if (reg_scenario.SCENARIO = 'ICC')    /*  Procesamos el escenario ICC  */
      then
        /* Tenemos el escenario ICC */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION icc_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'ICC';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;
        
      end if;

      /************************/

      if (reg_scenario.SCENARIO = 'NUM')    /*  Procesamos el escenario NUM  */
      then
        /* Tenemos el escenario NUM */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION num_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'NUM';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;
        
      end if;

      /************************/

      if (reg_scenario.SCENARIO = 'AGR')    /*  Procesamos el escenario de AGREGACION  */
      then
        /* Tenemos el escenario AGR */
        v_existe_scn_AGR:=1;
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION agr_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'AGR';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;
        nombre_tabla_T_agr := reg_scenario.TABLE_BASE_NAME;
        
      end if;

      /************************/

      if (reg_scenario.SCENARIO = 'DSG')    /*  Procesamos el escenario de AGREGACION  */
      then
        /* Tenemos el escenario DSG */
        v_existe_scn_DSG:=1;
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION dsg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'DSG';
        /* (20151126) Angel Ruiz. NF: AGREGADOS */
        lista_table_base_name.EXTEND; 
        lista_table_base_name(lista_table_base_name.LAST):= reg_scenario.TABLE_BASE_NAME;
        v_indice_DSG := lista_table_base_name.LAST;
        
      end if;
      
    end loop; /* fin del LOOP MTDT_SCENARIO  */
    close MTDT_SCENARIO;
    
    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lhe_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');

    if (v_existe_scn_AGR=0 and v_existe_scn_DSG = 0) then
      /*(20151126) Angel Ruiz. NF: AGREGACION */
      /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes posee un exchange*/
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lex_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
    end if;
    /*(20160105) Angel Ruiz. NF Agregacion */
    /* Existe agregacion que no es la agregacion sencilla */
    /* Hay que generar un proceso para DESAGREGAR de manera independiente */
    if (v_existe_scn_AGR = 1 and v_existe_scn_DSG = 1) then
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lds_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
    end if;    
    /*(20160105) Angel Ruiz. Fin NF */
    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
    UTL_FILE.put_line(fich_salida_pkg, '/' );

    /* GENERACION DEL PACKAGE BODY */
    
    UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    UTL_FILE.put_line(fich_salida_pkg,'');

    dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION');
    
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
    UTL_FILE.put_line(fich_salida_pkg,'  PROCEDURE pre_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2)'); 
    UTL_FILE.put_line(fich_salida_pkg,'  is'); 
    UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
    UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
    UTL_FILE.put_line(fich_salida_pkg,'   fch_particion number(8);');
    UTL_FILE.put_line(fich_salida_pkg,'  begin');     
    /*(20150520) Angel Ruiz. Deteccion de Bug */
    --UTL_FILE.put_line(fich_salida_pkg,'  fch_particion := TO_NUMBER(TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD''), ''YYYYMM''))+1;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  fch_particion := TO_CHAR(ADD_MONTHS(TO_DATE(fch_datos_in,''YYYYMMDD''), 1), ''YYYYMM'');');     
    --UTL_FILE.put_line(fich_salida_pkg, '  exis_partition :=  existe_particion (' || '''PA_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in, ''' || reg_tabla.TABLE_NAME || ''');');
    UTL_FILE.put_line(fich_salida_pkg, '  exis_partition :=  existe_particion (''' || v_nombre_particion || '_' || ''' || SUBSTR(fch_datos_in, 1, 6), ''' || reg_tabla.TABLE_NAME || ''');');
    UTL_FILE.put_line(fich_salida_pkg, '  if (exis_partition = 0) then' );      
    UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la particion */'); 
    --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
    UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || SUBSTR(fch_datos_in, 1, 6) || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
    UTL_FILE.put_line(fich_salida_pkg,'   end if;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  exception'); 
    UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
    UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
    UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  end pre_proceso;'); 
  
    /* Primero de todo miro si tengo que generar los cuerpos de las funciones de LOOKUP */
    lista_lkup.delete;
    v_contador:=0;
    open MTDT_TC_LOOKUP (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_LOOKUP
      into reg_lookup;
      exit when MTDT_TC_LOOKUP%NOTFOUND;
      if (instr(reg_lookup.TABLE_LKUP, 'SELECT ') > 0) then
      	/* Se trata de una LookUp con una SELECT en lugar de una Tabla */
      	nombre_funcion := 'LK_' || reg_lookup.TABLE_COLUMN;
      else
      	nombre_funcion := 'LK_' || reg_lookup.TABLE_LKUP;
      end if;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      /* Buscamos si la funcion de lookup ya la hemos generado, ya que si ya esta generada no hay que generarla de nuevo */
    	v_encontrado := 'N';
      if (v_contador = 0) then
      	lista_lkup.EXTEND;
      	lista_lkup (lista_lkup.last) := nombre_funcion;
        genera_cuerpo_funcion_pkg (reg_lookup);
        v_contador:=v_contador+1;
      else
        for indx in lista_lkup.FIRST .. lista_lkup.LAST
        loop
          if (lista_lkup(indx) = nombre_funcion) then
            v_encontrado := 'Y';
          end if;
        end loop;
        if (v_encontrado = 'N') then
          lista_lkup.EXTEND;
          lista_lkup (lista_lkup.last) := nombre_funcion;
          genera_cuerpo_funcion_pkg (reg_lookup);
        end if;
        v_contador:=v_contador+1;
      end if;
    end loop;
    close MTDT_TC_LOOKUP;
    /********************************************/

    /********************************************/
    
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
      if (reg_scenario.SCENARIO = 'N')  /* Proceso el escenario NEW */
      then
        /* SCENARIO NUEVO */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND, PARALLEL (' || reg_scenario.TABLE_NAME || ') */');
          /* (20170313) Angel Ruiz. Debido a que ahora solo generamos la carga de los hechos de manera dinamica */
          /* ya que ya no usamos los generadores que generan codigo en dinamico sin usar funciones de LookUp  */
          /* ocurre que las tablas temporales T_* en las que se insertan registros, su nombre se forma de manera dinamica */ 
          /* con la fecha del dia */ 
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
      
          /****/
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          UTL_FILE.put_line(fich_salida_pkg,'    (');
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
          /****/
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          /****/
          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
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
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
            /* (20170313) Angel Ruiz. Ocurre que la table_base_name puede tener propietario */
            UTL_FILE.put_line(fich_salida_pkg, '   ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_'' || fch_datos_in || '' ' || substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\.[a-zA-Z_0-9&]+'), 2) || '''');
          else
            UTL_FILE.put_line(fich_salida_pkg, '   ' || reg_scenario.TABLE_BASE_NAME || '_'' || fch_datos_in || '' ' || reg_scenario.TABLE_BASE_NAME || '''');
          end if;
          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    || ''WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter || '''');
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ';');
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
          UTL_FILE.put_line(fich_salida_pkg, '  END new_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion new */
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'OPE')  /* Proceso el escenario OPE */
      then
        /* SCENARIO OPE */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ope_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND, PARALLEL (' || reg_scenario.TABLE_NAME || ') */');
          UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
          UTL_FILE.put_line(fich_salida_pkg,'    (');
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
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */

          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
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
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */

          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          UTL_FILE.put_line(fich_salida_pkg,'    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME));
          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros OPE.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END ope_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion ope */
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'ALT')  /* Proceso el escenario ALT */
      then
        /* SCENARIO ALT */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION alt_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
          UTL_FILE.put_line(fich_salida_pkg,'    (');
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
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */

          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
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
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */

          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          UTL_FILE.put_line(fich_salida_pkg, '    ' ||  procesa_campo_filter(reg_scenario.TABLE_BASE_NAME));
          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros ALT.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END alt_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion ope */
      
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'ICC')  /* Proceso el escenario ICC */
      then
        /* SCENARIO ICC */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION icc_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
          UTL_FILE.put_line(fich_salida_pkg,'    (');
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
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */

          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
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
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */

          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          UTL_FILE.put_line(fich_salida_pkg, '    '  || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME));
          if (reg_scenario.FILTER is not null) then

            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros ICC.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END icc_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion ICC */
      
      /**************/
      /**************/
      if (reg_scenario.SCENARIO = 'NUM')  /* Proceso el escenario NUM */
      then
        /* SCENARIO NUM */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION num_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
          UTL_FILE.put_line(fich_salida_pkg,'    (');
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
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */

          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
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
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */

          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME));
          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros NUM.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END num_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion num */
      
      /**************/
      /**************/
      if (reg_scenario.SCENARIO = 'AGR')  /* Proceso el escenario AGR */
      then
        /* SCENARIO NUM */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION agr_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');

          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''');
          /* (20160210) Angel Ruiz. Se introducen HINTS por rendimientos */
          --UTL_FILE.put_line(fich_salida_pkg,'    MERGE');
          UTL_FILE.put_line(fich_salida_pkg,'    MERGE /*+ parallel(DESTINO,10) append use_hash(DESTINO ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || '') */');          
          --UTL_FILE.put_line(fich_salida_pkg,'    MERGE /*+ use_hash(DESTINO ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || '') */');          
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' DESTINO');
          UTL_FILE.put_line(fich_salida_pkg,'    USING ');
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
          UTL_FILE.put_line(fich_salida_pkg,'    (');

          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          v_list_campos_group_by.delete;  /* (20151124) Angel Ruiz. NF: AGREGADOS */
          /* (20160210) Angel Ruiz. Se introducen HINTS por rendimientos */
          --UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT /*+ parallel(' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || '',10) */');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if (trim(reg_detail.RUL) = 'SUM' or trim(reg_detail.RUL) = 'GROUP_BY') then
            --if (reg_detail.RUL <> 'SELECT') then
              /*(20151125) Angel Ruiz. NF: AGREGADOS.*/
              /* Se trata de que la operación en el campo origen no entra en el SELECT */
              /* lo que quiere decir que es normalmente un sysdate u otra cosa */
              /* con lo que no entra en el select del MERGE */
              columna := genera_campo_select (reg_detail);
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg,columna || ' ' || reg_detail.TABLE_COLUMN);
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg,',' || columna || ' ' || reg_detail.TABLE_COLUMN);
              end if;
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */

          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          UTL_FILE.put_line(fich_salida_pkg, '    ' || OWNER_DM || '.' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || ''');
          /* (20151222) Angel Ruiz. BUG: Se estaban agregando dos veces los registros que venian retrasados */
          UTL_FILE.put_line(fich_salida_pkg,'    WHERE ');
          UTL_FILE.put_line(fich_salida_pkg,'    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || ''.FCH_REGISTRO = TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')');
          --UTL_FILE.put_line(fich_salida_pkg,'    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || ''.FCH_REGISTRO = (');
          --UTL_FILE.put_line(fich_salida_pkg,'      SELECT MAX(MTDT_MONITOREO.FCH_REGISTRO)');
          --UTL_FILE.put_line(fich_salida_pkg,'        FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' );
          --UTL_FILE.put_line(fich_salida_pkg,'          ' || OWNER_MTDT || '.MTDT_PASO, ' || OWNER_MTDT || '.MTDT_RESULTADO' );
          --UTL_FILE.put_line(fich_salida_pkg,'        WHERE');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_PROCESO.NOMBRE_PROCESO =  '''''' || ' || '''load_he_' || PREFIJO_DM || 'F_' || SUBSTR(reg_scenario.TABLE_BASE_NAME, 3) || '.sh'' || '''''' AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_MONITOREO.FCH_DATOS = TO_DATE('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''') AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_MONITOREO.CVE_PASO = 1 AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_PROCESO.CVE_PROCESO = MTDT_PASO.CVE_PROCESO AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_PASO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_RESULTADO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_RESULTADO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
          --UTL_FILE.put_line(fich_salida_pkg,'          MTDT_RESULTADO.CVE_RESULTADO = MTDT_MONITOREO.CVE_RESULTADO)');
          /* (20151222) Angel Ruiz. FIN BUG */          
          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            --UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          /* (20151123) Angel Ruiz. NF: AGREGADOS */
          UTL_FILE.put_line(fich_salida_pkg,'    GROUP BY');
          FOR indx IN v_list_campos_group_by.FIRST .. v_list_campos_group_by.LAST
          LOOP
            if indx = 1 then
              /* Se trata del primer elemento */
              UTL_FILE.put_line(fich_salida_pkg, '      ' || v_list_campos_group_by(indx));
            else
              /* Se trata de los siguientes campos */
              UTL_FILE.put_line(fich_salida_pkg, '      ,' || v_list_campos_group_by(indx));
            end if;
          END LOOP;
          UTL_FILE.put_line(fich_salida_pkg,'    ) ORIGEN');
          UTL_FILE.put_line(fich_salida_pkg,'    ON (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if (reg_detail.CRUCE = 'JOIN') then
              /* Se trata de un campo por el que hay que hacer JOIN */
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg,'        ORIGEN.' || reg_detail.TABLE_COLUMN || ' = ' || 'DESTINO.' || reg_detail.TABLE_COLUMN);
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg,'        AND ORIGEN.' || reg_detail.TABLE_COLUMN || ' = ' || 'DESTINO.' || reg_detail.TABLE_COLUMN);
              end if;        
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'    )');
          UTL_FILE.put_line(fich_salida_pkg,'    WHEN MATCHED THEN UPDATE SET');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            /* Se trata de un campo por el que hay que hacer JOIN */
            if reg_detail.UPDATE_OPE <> 'MANTIENE' then
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg,'        DESTINO.' || reg_detail.TABLE_COLUMN || ' = ' || genera_campo_update_merge(reg_detail));
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg,'        ,DESTINO.' || reg_detail.TABLE_COLUMN || ' = ' || genera_campo_update_merge(reg_detail));
              end if;
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'    WHEN NOT MATCHED THEN INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if primera_col = 1 then
              UTL_FILE.put_line(fich_salida_pkg,'        DESTINO.' || reg_detail.TABLE_COLUMN);
              primera_col := 0;
            else
              UTL_FILE.put_line(fich_salida_pkg,'        , DESTINO.' || reg_detail.TABLE_COLUMN);
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'    )');
          UTL_FILE.put_line(fich_salida_pkg,'    VALUES (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if primera_col = 1 then
              UTL_FILE.put_line(fich_salida_pkg,'        ' || genera_campo_insert_merge(reg_detail));
              primera_col := 0;
            else
              UTL_FILE.put_line(fich_salida_pkg,'        ,' || genera_campo_insert_merge(reg_detail));
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg, '    )');          
          UTL_FILE.put_line(fich_salida_pkg, ''';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros NUM.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END agr_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion agr */
      
      /**************/
      /**************/
      if (reg_scenario.SCENARIO = 'DSG')  /* Proceso el escenario NUM */
      then
        /* SCENARIO NUM */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION dsg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''');
          /* (20160210) Angel Ruiz. Se introducen HINTS por rendimientos */
          --UTL_FILE.put_line(fich_salida_pkg,'    MERGE');
          UTL_FILE.put_line(fich_salida_pkg,'    MERGE /*+ parallel(DESTINO,10) append use_hash(DESTINO ' || 'T_DSG_' || nombre_tabla_T || ') */');
          --UTL_FILE.put_line(fich_salida_pkg,'    MERGE /*+ use_hash(DESTINO ' || 'T_DSG_' || nombre_tabla_T || ') */');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' DESTINO');
          UTL_FILE.put_line(fich_salida_pkg,'    USING ');
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
          UTL_FILE.put_line(fich_salida_pkg,'    (');

          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          v_list_campos_group_by.delete;  /* (20151130) Angel Ruiz. Borro la lista de elementos */
          --UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT /*+ parallel(T_DSG_' || nombre_tabla_T || ') */');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if (reg_detail.RUL = 'GROUP_BY' or reg_detail.RUL = 'SUM') then
            --if (reg_detail.RUL <> 'SELECT') then
              /*(20151125) Angel Ruiz. NF: AGREGADOS.*/
              /* Se trata de que la operación en el campo origen es SELECT */
              /* lo que quiere decir que es normalmente un sysdate */
              /* con lo que no entra en el select del MERGE */
              columna := genera_campo_select (reg_detail);
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg,columna || ' ' || reg_detail.TABLE_COLUMN);
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg,',' || columna || ' ' || reg_detail.TABLE_COLUMN);
              end if;
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */

          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          --UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || '_' || ''' || fch_datos_in || ''');
          UTL_FILE.put_line(fich_salida_pkg, '    ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T);
          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          /* (20151123) Angel Ruiz. NF: AGREGADOS */
          UTL_FILE.put_line(fich_salida_pkg,'    GROUP BY');
          FOR indx IN v_list_campos_group_by.FIRST .. v_list_campos_group_by.LAST
          LOOP
            if indx = 1 then
              /* Se trata del primer elemento */
              UTL_FILE.put_line(fich_salida_pkg, '      ' || v_list_campos_group_by(indx));
            else
              /* Se trata de los siguientes campos */
              UTL_FILE.put_line(fich_salida_pkg, '      ,' || v_list_campos_group_by(indx));
            end if;
          END LOOP;
          UTL_FILE.put_line(fich_salida_pkg,'    ) ORIGEN');
          UTL_FILE.put_line(fich_salida_pkg,'    ON (');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
            if (reg_detail.CRUCE = 'JOIN') then
              /* Se trata de un campo por el que hay que hacer JOIN */
              if primera_col = 1 then
                UTL_FILE.put_line(fich_salida_pkg,'        ORIGEN.' || reg_detail.TABLE_COLUMN || ' = ' || 'DESTINO.' || reg_detail.TABLE_COLUMN);
                primera_col := 0;
              else
                UTL_FILE.put_line(fich_salida_pkg,'        AND ORIGEN.' || reg_detail.TABLE_COLUMN || ' = ' || 'DESTINO.' || reg_detail.TABLE_COLUMN);
              end if;        
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg,'    )');
          UTL_FILE.put_line(fich_salida_pkg,'    WHEN MATCHED THEN UPDATE SET');
          open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
          primera_col := 1;
          loop
            fetch MTDT_TC_DETAIL
            into reg_detail;
            exit when MTDT_TC_DETAIL%NOTFOUND;
              if reg_detail.UPDATE_OPE <> 'MANTIENE' then
                if primera_col = 1 then
                  UTL_FILE.put_line(fich_salida_pkg,'        DESTINO.' || reg_detail.TABLE_COLUMN || ' = ' ||  genera_campo_update_merge(reg_detail));
                  primera_col := 0;
                else
                  UTL_FILE.put_line(fich_salida_pkg,'        ,DESTINO.' || reg_detail.TABLE_COLUMN || ' = ' || genera_campo_update_merge(reg_detail));
                end if;
              end if;
          end loop;
          close MTDT_TC_DETAIL;
          UTL_FILE.put_line(fich_salida_pkg, ''';');
          UTL_FILE.put_line(fich_salida_pkg, '');
          UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    commit;');
          UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
      
          UTL_FILE.put_line(fich_salida_pkg,'    exception');
          UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
          UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
          UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
          --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
          --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros NUM.'');');
          UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
          UTL_FILE.put_line(fich_salida_pkg,'      raise;');
          UTL_FILE.put_line(fich_salida_pkg, '  END dsg_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion dsg */
      
      /**************/
      /**************/
    
    end loop;
    close MTDT_SCENARIO;
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lhe_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
    UTL_FILE.put_line(fich_salida_pkg, '  IS');
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'N' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_new NUMBER:=0;');
      end if;
      if lista_scenarios_presentes (indx) = 'OPE' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_ope NUMBER:=0;');
      end if;
      if lista_scenarios_presentes (indx) = 'ALT' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_alt NUMBER:=0;');
      end if;
      if lista_scenarios_presentes (indx) = 'ICC' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_icc NUMBER:=0;');
      end if;
      if lista_scenarios_presentes (indx) = 'NUM' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_num NUMBER:=0;');    
      end if;
      if lista_scenarios_presentes (indx) = 'AGR' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_agr NUMBER:=0;');    
      end if;
      if lista_scenarios_presentes (indx) = 'DSG' then
        UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_dsg NUMBER:=0;');    
        UTL_FILE.put_line(fich_salida_pkg, '  exis_tabla PLS_integer;');    
      end if;
    END LOOP;    
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_tot NUMBER:=0;');    
    UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');
    UTL_FILE.put_line(fich_salida_pkg, '  ult_paso_ejecutado PLS_integer;');
    UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
    UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
    UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
    UTL_FILE.put_line(fich_salida_pkg, '');
    /**********************/
    UTL_FILE.put_line(fich_salida_pkg, '    /* Este proceso solo tiene un paso, por lo que o se ejecuta todo el o no sejecuta nada porque ya se ejecuto OK */');
    UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_he_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '    end if;');
    UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '      dbms_output.put_line (''Inicio de la pasada del bucle del proceso de carga: ''' || ' || ''' || 'load_he_' || reg_tabla.TABLE_NAME || ''' || ''.'');');
    /*(20151126) Angel Ruiz. NF: AGREGACION */
    /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
    UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
    if (v_existe_scn_AGR= 0 AND v_existe_scn_DSG = 0) then
      /*(20151126) Angel Ruiz. NF: AGREGACION */
      /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
      UTL_FILE.put_line(fich_salida_pkg, '    /* Truncamos la tabla antes de insertar los nuevos registros por si se lanza dos veces*/');
      UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''';');    
      UTL_FILE.put_line(fich_salida_pkg, '      ' || 'pkg_' || nombre_proceso || '.' || 'pre_proceso (fch_carga_in, fch_datos_in);');
      --UTL_FILE.put_line(fich_salida_pkg, '      SET TRANSACTION NAME ''TRAN_' || reg_tabla.TABLE_NAME || ''';');
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '');
    /* Generamos las llamadas a los procedimientos para realizar las cargas */
    /* Generamos la llamada para cargar los registros NUEVOS */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'N'
      then
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_new := ' || 'pkg_' || nombre_proceso || '.' || 'new_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_tot := numero_reg_tot + numero_reg_new;');
        UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros insertados es: '' || numero_reg_new || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros OPE */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'OPE'
      then
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_ope := ' || 'pkg_' || nombre_proceso || '.' || 'ope_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_tot := numero_reg_tot + numero_reg_ope;');
        UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros ope es: '' || numero_reg_ope || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros ALT */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'ALT'
      then
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_alt := ' || 'pkg_' || nombre_proceso || '.' || 'alt_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_tot := numero_reg_tot + numero_reg_alt;');
        UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros alt es: '' || numero_reg_alt || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros ALT */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'ICC'
      then
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_icc := ' || 'pkg_' || nombre_proceso || '.' || 'icc_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_tot := numero_reg_tot + numero_reg_icc;');
        UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros icc es: '' || numero_reg_icc || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros NUM */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'NUM'
      then
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_num := ' || 'pkg_' || nombre_proceso || '.' || 'num_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_tot := numero_reg_tot + numero_reg_num;');
        UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros num es: '' || numero_reg_num || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros AGR */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'AGR'
      then
        /* Se trata de una ejecución Forzada */
        --UTL_FILE.put_line(fich_salida_pkg,'    /* Este tip de procesos posee dos pasos de ejecucion */');
        --UTL_FILE.put_line(fich_salida_pkg,'    if (siguiente_paso_a_ejecutar = 1) then');
        --UTL_FILE.put_line(fich_salida_pkg,'      inicio_paso_tmr := cast (systimestamp as timestamp);');
        --UTL_FILE.put_line(fich_salida_pkg,'      if (forzado_in = ''F'') then');
        --UTL_FILE.put_line(fich_salida_pkg,'        /* EJECUCION EN MODO FORZADO */');
        --UTL_FILE.put_line(fich_salida_pkg,'        /* Calculamos las diferentes fechas de datos que se agregaron en la ultima agregacion */');
        --UTL_FILE.put_line(fich_salida_pkg,'        FOR fecha_datos_agregada IN (');
        --UTL_FILE.put_line(fich_salida_pkg,'          SELECT AGREGADO.FCH_DATOS FCH_DATOS, AGREGADO.FCH_REGISTRO FCH_REGISTRO'); 
        --UTL_FILE.put_line(fich_salida_pkg,'          FROM');
        --UTL_FILE.put_line(fich_salida_pkg,'          (');
        --UTL_FILE.put_line(fich_salida_pkg,'            SELECT TO_CHAR(FCH_DATOS, ''YYYYMMDD'') FCH_DATOS, TO_CHAR(MTDT_MONITOREO.FCH_REGISTRO, ''YYYYMMDDHH24MISS'') FCH_REGISTRO, ROW_NUMBER() OVER (PARTITION BY MTDT_MONITOREO.FCH_DATOS ORDER BY MTDT_MONITOREO.FCH_REGISTRO DESC) RN');
        --UTL_FILE.put_line(fich_salida_pkg,'            FROM APP_MVNOMT.MTDT_MONITOREO, APP_MVNOMT.MTDT_PROCESO');
        --UTL_FILE.put_line(fich_salida_pkg,'            WHERE');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_he_' || reg_tabla.TABLE_NAME || '.sh'' AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PASO = 2 AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'')');
        --UTL_FILE.put_line(fich_salida_pkg,'          ) AGREGADO');
        --UTL_FILE.put_line(fich_salida_pkg,'          WHERE AGREGADO.RN = 1)');
        --UTL_FILE.put_line(fich_salida_pkg,'          FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO' );
        --UTL_FILE.put_line(fich_salida_pkg,'          WHERE');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_he_' || reg_tabla.TABLE_NAME || '.sh'' AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PASO = 2 AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'') AND');
        --UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.FCH_REGISTRO IN (');
        --UTL_FILE.put_line(fich_salida_pkg,'              SELECT MAX(MTDT_MONITOREO.FCH_REGISTRO)' );
        --UTL_FILE.put_line(fich_salida_pkg,'              FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO' );
        --UTL_FILE.put_line(fich_salida_pkg,'              WHERE');
        --UTL_FILE.put_line(fich_salida_pkg,'                MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_he_' || reg_tabla.TABLE_NAME || '.sh'' AND');
        --UTL_FILE.put_line(fich_salida_pkg,'                MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        --UTL_FILE.put_line(fich_salida_pkg,'                MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        --UTL_FILE.put_line(fich_salida_pkg,'                MTDT_MONITOREO.CVE_PASO = 2 AND');
        --UTL_FILE.put_line(fich_salida_pkg,'                MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'')))');
        --UTL_FILE.put_line(fich_salida_pkg,'        LOOP' );
        --UTL_FILE.put_line(fich_salida_pkg,'          /* Creo la tabla temporal sobre la voy a copiar los reg. a desagregar */');
        --UTL_FILE.put_line(fich_salida_pkg,'          exis_tabla :=  existe_tabla (' || '''T_DSG_' || nombre_tabla_T || ''' );');
        --UTL_FILE.put_line(fich_salida_pkg,'          if (exis_tabla = 0) then' );      
        --UTL_FILE.put_line(fich_salida_pkg,'            /* Creo la tabla */'); 
        --UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE '''); 
        --UTL_FILE.put_line(fich_salida_pkg,'            CREATE TABLE  ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ' TABLESPACE ' || reg_tabla.TABLESPACE);
        --UTL_FILE.put_line(fich_salida_pkg,'            AS SELECT * FROM ' ||  OWNER_DM || '.' || PREFIJO_DM || 'F_' || SUBSTR(lista_table_base_name (indx), 3));
        --UTL_FILE.put_line(fich_salida_pkg,'            WHERE CVE_DIA =  '' || fecha_datos_agregada.FCH_DATOS || '' AND FCH_REGISTRO =  TO_DATE('''''' || fecha_datos_agregada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');  
        --UTL_FILE.put_line(fich_salida_pkg,'          else'); 
        --UTL_FILE.put_line(fich_salida_pkg,'            /* Borro la tabla */'); 
        --UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ''';');
        --UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE '''); 
        --UTL_FILE.put_line(fich_salida_pkg,'            CREATE TABLE  ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ' TABLESPACE ' || reg_tabla.TABLESPACE);
        --UTL_FILE.put_line(fich_salida_pkg,'            AS SELECT * FROM ' ||  OWNER_DM || '.' || PREFIJO_DM || 'F_' || SUBSTR(lista_table_base_name (indx), 3));
        --UTL_FILE.put_line(fich_salida_pkg,'            WHERE CVE_DIA =  '' || fecha_datos_agregada.FCH_DATOS || '' AND FCH_REGISTRO =  TO_DATE('''''' || fecha_datos_agregada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');  
        --UTL_FILE.put_line(fich_salida_pkg,'          end if;'); 
        --UTL_FILE.put_line(fich_salida_pkg,'          /* Hago la desagregacion */');
        --UTL_FILE.put_line(fich_salida_pkg,'          numero_reg_dsg := ' || 'pkg_' || nombre_proceso || '.' || 'dsg_' || nombre_proceso || ' (fch_carga_in, fecha_datos_agregada.FCH_DATOS, fecha_datos_agregada.FCH_REGISTRO);');        
        --UTL_FILE.put_line(fich_salida_pkg,'          numero_reg_tot := numero_reg_tot + numero_reg_dsg;');
        --UTL_FILE.put_line(fich_salida_pkg,'          ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_tot);');
        --UTL_FILE.put_line(fich_salida_pkg,'          /* Borro la tabla */'); 
        --UTL_FILE.put_line(fich_salida_pkg,'          EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ''';');
        --UTL_FILE.put_line(fich_salida_pkg,'        END LOOP;' );
        --UTL_FILE.put_line(fich_salida_pkg,'        COMMIT;');
        --UTL_FILE.put_line(fich_salida_pkg,'      else');
        --UTL_FILE.put_line(fich_salida_pkg,'        /* Si no es una ejecucion forzada no hago desagregacion */');
        --UTL_FILE.put_line(fich_salida_pkg,'        /* Aunque si que inserto que le paso a terminado correctamente ya que asi ha sido. Como no ha habido */');
        --UTL_FILE.put_line(fich_salida_pkg,'        /* desagregacion, termina bien con el numero de registros desagregados igual a 0 */');
        --UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_dsg := 0;');        
        --UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_tot := numero_reg_tot + numero_reg_dsg;');
        --UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_tot);');
        --UTL_FILE.put_line(fich_salida_pkg,'      end if;');
        --UTL_FILE.put_line(fich_salida_pkg,'      siguiente_paso_a_ejecutar := 2;');
        --UTL_FILE.put_line(fich_salida_pkg,'    end if;');
      
        --UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 2) then');
        --UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
        UTL_FILE.put_line(fich_salida_pkg, '');
        /*(20151126) Angel Ruiz. NF: AGREGACION */
        /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
        UTL_FILE.put_line(fich_salida_pkg,'      /* Es posible que haya datos de varias fechas ya que se admiten retrasados */');
        UTL_FILE.put_line(fich_salida_pkg,'      FOR fecha_datos_cargada IN (');
        UTL_FILE.put_line(fich_salida_pkg,'        SELECT AGREGADO_1.FCH_DATOS FCH_DATOS, AGREGADO_1.FCH_REGISTRO FCH_REGISTRO');
        UTL_FILE.put_line(fich_salida_pkg,'        FROM');
        UTL_FILE.put_line(fich_salida_pkg,'        (');
        UTL_FILE.put_line(fich_salida_pkg,'          SELECT TO_CHAR(MTDT_MONITOREO.FCH_DATOS, ''YYYYMMDD'') FCH_DATOS, TO_CHAR(MTDT_MONITOREO.FCH_REGISTRO, ''YYYYMMDDHH24MISS'') FCH_REGISTRO, ROW_NUMBER() OVER (PARTITION BY MTDT_MONITOREO.FCH_DATOS ORDER BY MTDT_MONITOREO.FCH_REGISTRO DESC) RN');
        UTL_FILE.put_line(fich_salida_pkg,'          FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' );
        UTL_FILE.put_line(fich_salida_pkg,'          ' || OWNER_MTDT || '.MTDT_PASO, ' || OWNER_MTDT || '.MTDT_RESULTADO' );
        UTL_FILE.put_line(fich_salida_pkg,'          WHERE');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_he_' || PREFIJO_DM || 'F_' || SUBSTR(lista_table_base_name (indx), 3) || '.sh'' AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PASO = 1 AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.CVE_PROCESO = MTDT_PASO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PASO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_RESULTADO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_RESULTADO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_RESULTADO.CVE_RESULTADO = MTDT_MONITOREO.CVE_RESULTADO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'')');
        UTL_FILE.put_line(fich_salida_pkg,'        ) AGREGADO_1');
        UTL_FILE.put_line(fich_salida_pkg,'        WHERE AGREGADO_1.RN=1');
        UTL_FILE.put_line(fich_salida_pkg,'        AND NOT EXISTS (');
        UTL_FILE.put_line(fich_salida_pkg,'        SELECT AGREGADO_2.FCH_DATOS FCH_DATOS, AGREGADO_2.FCH_REGISTRO FCH_REGISTRO');
        UTL_FILE.put_line(fich_salida_pkg,'        FROM');
        UTL_FILE.put_line(fich_salida_pkg,'        (');
        UTL_FILE.put_line(fich_salida_pkg,'          SELECT TO_CHAR(MTDT_MONITOREO.FCH_DATOS, ''YYYYMMDD'') FCH_DATOS, TO_CHAR(MTDT_MONITOREO.FCH_REGISTRO, ''YYYYMMDDHH24MISS'') FCH_REGISTRO, ROW_NUMBER() OVER (PARTITION BY MTDT_MONITOREO.FCH_DATOS ORDER BY MTDT_MONITOREO.FCH_REGISTRO DESC) RN');
        UTL_FILE.put_line(fich_salida_pkg,'          FROM ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' );
        UTL_FILE.put_line(fich_salida_pkg,'          ' || OWNER_MTDT || '.MTDT_PASO, ' || OWNER_MTDT || '.MTDT_RESULTADO' );
        UTL_FILE.put_line(fich_salida_pkg,'          WHERE');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.NOMBRE_PROCESO =  ''' || nombre_fich_carga || ''' AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PASO = 1 AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.CVE_PROCESO = MTDT_PASO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PASO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_RESULTADO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_RESULTADO.CVE_PASO = MTDT_MONITOREO.CVE_PASO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_RESULTADO.CVE_RESULTADO = MTDT_MONITOREO.CVE_RESULTADO AND');
        UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'')');
        UTL_FILE.put_line(fich_salida_pkg,'        ) AGREGADO_2');
        UTL_FILE.put_line(fich_salida_pkg,'        WHERE AGREGADO_2.RN=1 AND AGREGADO_1.FCH_DATOS = AGREGADO_2.FCH_DATOS');
        UTL_FILE.put_line(fich_salida_pkg,'        AND AGREGADO_1.FCH_REGISTRO = AGREGADO_2.FCH_REGISTRO');
        UTL_FILE.put_line(fich_salida_pkg,'        ))');
        UTL_FILE.put_line(fich_salida_pkg,'      LOOP' );
        UTL_FILE.put_line(fich_salida_pkg,'        ' || 'pkg_' || nombre_proceso || '.' || 'pre_proceso (fch_carga_in, fecha_datos_cargada.FCH_DATOS);');
        --UTL_FILE.put_line(fich_salida_pkg,'        numero_reg_agr := ' || 'pkg_' || nombre_proceso || '.' || 'agr_' || nombre_proceso || ' (fch_carga_in, fecha_datos_cargada.FCH_DATOS, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'        numero_reg_agr := ' || 'pkg_' || nombre_proceso || '.' || 'agr_' || nombre_proceso || ' (fch_carga_in, fecha_datos_cargada.FCH_DATOS, fecha_datos_cargada.FCH_REGISTRO);');
        UTL_FILE.put_line(fich_salida_pkg,'        numero_reg_tot := numero_reg_tot + numero_reg_agr;');
        UTL_FILE.put_line(fich_salida_pkg,'        dbms_output.put_line (''El numero de registros agr es: '' || numero_reg_agr || ''.'');');
        --UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_tot);');
        UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, TO_DATE(fecha_datos_cargada.FCH_REGISTRO, ''YYYYMMDDHH24MISS''), systimestamp, to_date(fecha_datos_cargada.FCH_DATOS, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_agr);');
        UTL_FILE.put_line(fich_salida_pkg,'        COMMIT;');
        UTL_FILE.put_line(fich_salida_pkg,'      END LOOP;' );        
        UTL_FILE.put_line(fich_salida_pkg,'      COMMIT;');
        UTL_FILE.put_line(fich_salida_pkg,'    end if;');
      end if;
    END LOOP;
    if (v_existe_scn_AGR = 0 AND v_existe_scn_DSG = 0) then
      /*(20151126) Angel Ruiz. NF: AGREGACION */
      /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
      UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros totales es: '' || numero_reg_tot || ''.'');');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
      UTL_FILE.put_line(fich_salida_pkg, '       /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
      UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_tot);');
      UTL_FILE.put_line(fich_salida_pkg, '      COMMIT;');
      UTL_FILE.put_line(fich_salida_pkg, '       end if;');
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg,'    RETURN 0;');
    
    UTL_FILE.put_line(fich_salida_pkg,'    exception');
    --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
    --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
    UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
    UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
    UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg,'      COMMIT;');
    UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '  END lhe_' || nombre_proceso || ';');
    UTL_FILE.put_line(fich_salida_pkg, '');
    /**************/
    /*(20160105) Angel Ruiz. NF Agregacion */
    /* Existe agregacion que no es la agregacion sencilla */
    /* Hay que generar un proceso para DESAGREGAR de manera independiente */
    if (v_existe_scn_AGR = 1 and v_existe_scn_DSG = 1) then
      UTL_FILE.put_line(fich_salida_pkg, '' ); 
      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lds_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
      UTL_FILE.put_line(fich_salida_pkg, '  IS');
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_tot NUMBER:=0;');    
      UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');
      UTL_FILE.put_line(fich_salida_pkg, '  ult_paso_ejecutado PLS_integer;');
      UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
      UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
      UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_dsg NUMBER:=0;');    
      UTL_FILE.put_line(fich_salida_pkg, '  exis_tabla PLS_integer;');    
      UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, '    /* Este proceso solo tiene un paso, por lo que o se ejecuta todo el o no sejecuta nada porque ya se ejecuto OK */');
      UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_ds_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
      UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
      UTL_FILE.put_line(fich_salida_pkg, '    end if;');
      UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg,'        inicio_paso_tmr := cast (systimestamp as timestamp);');
      UTL_FILE.put_line(fich_salida_pkg,'        /* Calculamos las diferentes fechas de datos que se agregaron en la ultima agregacion */');
      UTL_FILE.put_line(fich_salida_pkg,'        FOR fecha_datos_agregada IN (');
      UTL_FILE.put_line(fich_salida_pkg,'          SELECT AGREGADO.FCH_DATOS FCH_DATOS, AGREGADO.FCH_REGISTRO FCH_REGISTRO'); 
      UTL_FILE.put_line(fich_salida_pkg,'          FROM');
      UTL_FILE.put_line(fich_salida_pkg,'          (');
      UTL_FILE.put_line(fich_salida_pkg,'            SELECT TO_CHAR(FCH_DATOS, ''YYYYMMDD'') FCH_DATOS, TO_CHAR(MTDT_MONITOREO.FCH_REGISTRO, ''YYYYMMDDHH24MISS'') FCH_REGISTRO, ROW_NUMBER() OVER (PARTITION BY MTDT_MONITOREO.FCH_DATOS ORDER BY MTDT_MONITOREO.FCH_REGISTRO DESC) RN');
      UTL_FILE.put_line(fich_salida_pkg,'            FROM APP_MVNOMT.MTDT_MONITOREO, APP_MVNOMT.MTDT_PROCESO');
      UTL_FILE.put_line(fich_salida_pkg,'            WHERE');
      UTL_FILE.put_line(fich_salida_pkg,'            MTDT_PROCESO.NOMBRE_PROCESO =  ' || '''load_he_' || reg_tabla.TABLE_NAME || '.sh'' AND');
      UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PROCESO = MTDT_PROCESO.CVE_PROCESO AND');
      UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_RESULTADO = 0 AND');
      UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.CVE_PASO = 1 AND');
      UTL_FILE.put_line(fich_salida_pkg,'            MTDT_MONITOREO.FCH_CARGA = TO_DATE(' || 'fch_carga_in, ''yyyymmdd'')');
      UTL_FILE.put_line(fich_salida_pkg,'          ) AGREGADO');
      UTL_FILE.put_line(fich_salida_pkg,'          WHERE AGREGADO.RN = 1)');
      UTL_FILE.put_line(fich_salida_pkg,'        LOOP' );
      UTL_FILE.put_line(fich_salida_pkg,'          /* Creo la tabla temporal sobre la voy a copiar los reg. a desagregar */');
      UTL_FILE.put_line(fich_salida_pkg,'          exis_tabla :=  existe_tabla (' || '''T_DSG_' || nombre_tabla_T || ''' );');
      UTL_FILE.put_line(fich_salida_pkg,'          if (exis_tabla = 0) then' );      
      UTL_FILE.put_line(fich_salida_pkg,'            /* Creo la tabla */'); 
      UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE '''); 
      UTL_FILE.put_line(fich_salida_pkg,'            CREATE TABLE  ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ' TABLESPACE ' || reg_tabla.TABLESPACE);
      UTL_FILE.put_line(fich_salida_pkg,'            AS SELECT * FROM ' ||  OWNER_DM || '.' || PREFIJO_DM || 'F_' || SUBSTR(lista_table_base_name (v_indice_DSG), 3));
      UTL_FILE.put_line(fich_salida_pkg,'            WHERE CVE_DIA =  '' || fecha_datos_agregada.FCH_DATOS || '' AND FCH_REGISTRO =  TO_DATE('''''' || fecha_datos_agregada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');  
      UTL_FILE.put_line(fich_salida_pkg,'          else'); 
      UTL_FILE.put_line(fich_salida_pkg,'            /* Borro la tabla */'); 
      UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ''';');
      UTL_FILE.put_line(fich_salida_pkg,'            EXECUTE IMMEDIATE '''); 
      UTL_FILE.put_line(fich_salida_pkg,'            CREATE TABLE  ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ' TABLESPACE ' || reg_tabla.TABLESPACE);
      UTL_FILE.put_line(fich_salida_pkg,'            AS SELECT * FROM ' ||  OWNER_DM || '.' || PREFIJO_DM || 'F_' || SUBSTR(lista_table_base_name (v_indice_DSG), 3));
      UTL_FILE.put_line(fich_salida_pkg,'            WHERE CVE_DIA =  '' || fecha_datos_agregada.FCH_DATOS || '' AND FCH_REGISTRO =  TO_DATE('''''' || fecha_datos_agregada.FCH_REGISTRO || '''''', ''''YYYYMMDDHH24MISS'''')'';');  
      UTL_FILE.put_line(fich_salida_pkg,'          end if;'); 
      UTL_FILE.put_line(fich_salida_pkg,'          /* Hago la desagregacion */');
      UTL_FILE.put_line(fich_salida_pkg,'          numero_reg_dsg := ' || 'pkg_' || nombre_proceso || '.' || 'dsg_' || nombre_proceso || ' (fch_carga_in, fecha_datos_agregada.FCH_DATOS, fecha_datos_agregada.FCH_REGISTRO);');        
      UTL_FILE.put_line(fich_salida_pkg,'          numero_reg_tot := numero_reg_tot + numero_reg_dsg;');
      UTL_FILE.put_line(fich_salida_pkg,'          ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ds_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, to_date(fecha_datos_agregada.FCH_REGISTRO, ''yyyymmddhh24miss''), systimestamp, to_date(fecha_datos_agregada.FCH_DATOS, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), 0, numero_reg_dsg);');
      UTL_FILE.put_line(fich_salida_pkg,'          /* Borro la tabla */'); 
      UTL_FILE.put_line(fich_salida_pkg,'          EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_DSG_' || nombre_tabla_T || ''';');
      UTL_FILE.put_line(fich_salida_pkg,'        END LOOP;' );
      UTL_FILE.put_line(fich_salida_pkg,'        COMMIT;');
      UTL_FILE.put_line(fich_salida_pkg, '    end if;');
      UTL_FILE.put_line(fich_salida_pkg, '');
      --UTL_FILE.put_line(fich_salida_pkg,'    RETURN 0;');
      
      UTL_FILE.put_line(fich_salida_pkg,'    exception');
      --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
      --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
      UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
      UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
      UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
      UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
      UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ds_' || reg_tabla.TABLE_NAME || '.sh'',' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg,'      COMMIT;');
      UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, '  END lds_' || nombre_proceso || ';');
      UTL_FILE.put_line(fich_salida_pkg, '');
    end if;    
    
    /**************/
    if (v_existe_scn_AGR=0 AND v_existe_scn_DSG = 0) then
      /*(20151126) Angel Ruiz. NF: AGREGACION */
      /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
      UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lex_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
      UTL_FILE.put_line(fich_salida_pkg, '  IS');
  
      UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer := 0;');
      UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
      UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, '    /* Lo primero que se hace es mirar que paso es el primero a ejecutar */');
      UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
      UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
      UTL_FILE.put_line(fich_salida_pkg, '    end if;');
      UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
      UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos ex solo tienen dos pasos */');
      UTL_FILE.put_line(fich_salida_pkg, '      /* Comienza en el primer paso */');
      UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
      UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME);    
      --UTL_FILE.put_line(fich_salida_pkg, '      EXCHANGE PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' ');    
      UTL_FILE.put_line(fich_salida_pkg, '      EXCHANGE PARTITION ' || v_nombre_particion || ''' || ''_'' || SUBSTR(fch_datos_in, 1, 6) || '' ');    
      UTL_FILE.put_line(fich_salida_pkg, '      WITH TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || '' ');    
      UTL_FILE.put_line(fich_salida_pkg, '      WITHOUT VALIDATION'';');    
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg, '      commit;');
      --UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');    
      --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
      --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE  T_' || nombre_tabla_reducido || ''';');    
      --UTL_FILE.put_line(fich_salida_pkg, '');
      --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
      UTL_FILE.put_line(fich_salida_pkg, '    end if; ');
      --UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 2) then');
      --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el segundo paso */');
      --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
      --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE  T_' || nombre_tabla_reducido || ''';');    
      --UTL_FILE.put_line(fich_salida_pkg, '');
      --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
      --UTL_FILE.put_line(fich_salida_pkg, '    end if; ');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg,'    exception');
      --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
      --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
      UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
      UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
      UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
      UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
      UTL_FILE.put_line(fich_salida_pkg, '     ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || 'siguiente_paso_a_ejecutar, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
      UTL_FILE.put_line(fich_salida_pkg,'      commit;');
      UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
      UTL_FILE.put_line(fich_salida_pkg, '');
      UTL_FILE.put_line(fich_salida_pkg, '  END lex_' || nombre_proceso || ';');
    end if;
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
    UTL_FILE.put_line(fich_salida_pkg, '/' );
    /******/
    /* FIN DE LA GENERACION DEL PACKAGE */
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
    UTL_FILE.put_line(fich_salida_load, '# Archivo    :       load_he_ ' ||  reg_tabla.TABLE_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                                                  #');
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
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
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
    UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
    --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
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
    UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="' || v_REQ_NUMER || '"');
    --UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="Req89208"');
    UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || v_REQ_NUMER || '_load_he_' || reg_tabla.TABLE_NAME);
    --UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=Req89208_load_he_' || reg_tabla.TABLE_NAME);
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
    if (v_existe_scn_AGR=0 and v_existe_scn_DSG = 0) then
      /*(20151126) Angel Ruiz. NF: AGREGACION */
      /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
      fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W'); /* Abro aqui el fichero porque antes no se si va a existir o no */

      UTL_FILE.put_line(fich_salida_exchange, '#!/bin/bash');
      UTL_FILE.put_line(fich_salida_exchange, '#############################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_exchange, '# Telefonica Moviles Mexico SA DE CV                                        #');
      UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_exchange, '# Archivo    :       load_ex_ ' ||  reg_tabla.TABLE_NAME || '.sh                            #');
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
      UTL_FILE.put_line(fich_salida_exchange, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
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
      UTL_FILE.put_line(fich_salida_exchange, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
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
      UTL_FILE.put_line(fich_salida_exchange, 'REQ_NUM="' || v_REQ_NUMER || '"');
      --UTL_FILE.put_line(fich_salida_exchange, 'REQ_NUM="Req89208"');
      UTL_FILE.put_line(fich_salida_exchange, 'INTERFAZ=' || v_REQ_NUMER || '_load_ex_' || reg_tabla.TABLE_NAME);
      --UTL_FILE.put_line(fich_salida_exchange, 'INTERFAZ=Req89208_load_ex_' || reg_tabla.TABLE_NAME);
      UTL_FILE.put_line(fich_salida_exchange, '');
      UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '# LIBRERIAS                                                                    #');
      UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
      UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
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
      UTL_FILE.put_line(fich_salida_exchange, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
      UTL_FILE.put_line(fich_salida_exchange, 'BD_CLAVE=${PASSWORD}');
      
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
    end if;
    /******/
    /* FIN DE LA GENERACION DEL sh de EXCHANGE */
    /******/
    /*(20160105) Angel Ruiz. NF Agregacion */
    /* Existe agregacion que no es la agregacion sencilla */
    /* Hay que generar un proceso para DESAGREGAR de manera independiente */
    if (v_existe_scn_AGR = 1 and v_existe_scn_DSG = 1) then
      fich_salida_desagrega := UTL_FILE.FOPEN ('SALIDA',nombre_fich_desagrega,'W'); /* Abro aqui el fichero porque antes no se si va a existir o no */

      UTL_FILE.put_line(fich_salida_desagrega, '#!/bin/bash');
      UTL_FILE.put_line(fich_salida_desagrega, '#############################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Telefonica Moviles Mexico SA DE CV                                        #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Archivo    :       load_ds_ ' ||  reg_tabla.TABLE_NAME || '.sh                            #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Autor      : Angel Ruiz Canton. <SYNAPSYS>.                               #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || '.        #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Parametros :                                                              #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Ejecucion  :                                                              #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Historia : 31-Octubre-2014 -> Creacion                                    #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Caja de Control - M :                                                     #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Caducidad del Requerimiento :                                             #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Dependencias :                                                            #');
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Usuario:                                                                  #');   
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '# Telefono:                                                                 #');   
      UTL_FILE.put_line(fich_salida_desagrega, '#                                                                           #');
      UTL_FILE.put_line(fich_salida_desagrega, '#############################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '#Obtiene los password de base de datos                                         #');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, 'InsertaFinFallido()');
      UTL_FILE.put_line(fich_salida_desagrega, '{');
      UTL_FILE.put_line(fich_salida_desagrega, '   #Se especifican parametros usuario y la BD');
      UTL_FILE.put_line(fich_salida_desagrega, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ds_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
      UTL_FILE.put_line(fich_salida_desagrega, '   if [ $? -ne 0 ]');
      UTL_FILE.put_line(fich_salida_desagrega, '   then');
      UTL_FILE.put_line(fich_salida_desagrega, '      SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
      UTL_FILE.put_line(fich_salida_desagrega, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      UTL_FILE.put_line(fich_salida_desagrega, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_desagrega, '      exit 1;');
      UTL_FILE.put_line(fich_salida_desagrega, '   fi');
      UTL_FILE.put_line(fich_salida_desagrega, '   return 0');
      UTL_FILE.put_line(fich_salida_desagrega, '}');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, 'InsertaFinOK()');
      UTL_FILE.put_line(fich_salida_desagrega, '{');
      UTL_FILE.put_line(fich_salida_desagrega, '   #Se especifican parametros usuario y la BD');
      UTL_FILE.put_line(fich_salida_desagrega, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ds_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
      UTL_FILE.put_line(fich_salida_desagrega, '   if [ $? -ne 0 ]');
      UTL_FILE.put_line(fich_salida_desagrega, '   then');
      UTL_FILE.put_line(fich_salida_desagrega, '      SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
      UTL_FILE.put_line(fich_salida_desagrega, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      UTL_FILE.put_line(fich_salida_desagrega, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_desagrega, '      exit 1;');
      UTL_FILE.put_line(fich_salida_desagrega, '   fi');
      UTL_FILE.put_line(fich_salida_desagrega, '   return 0');
      UTL_FILE.put_line(fich_salida_desagrega, '}');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
      UTL_FILE.put_line(fich_salida_desagrega, '# Comprobamos si el numero de parametros es el correcto');
      UTL_FILE.put_line(fich_salida_desagrega, 'if [ $# -ne 3 ] ; then');
      UTL_FILE.put_line(fich_salida_desagrega, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
      UTL_FILE.put_line(fich_salida_desagrega, '  echo ${SUBJECT}');        
      UTL_FILE.put_line(fich_salida_desagrega, '  exit 1');
      UTL_FILE.put_line(fich_salida_desagrega, 'fi');
      UTL_FILE.put_line(fich_salida_desagrega, '# Recogida de parametros');
      UTL_FILE.put_line(fich_salida_desagrega, 'FCH_CARGA=${1}');
      UTL_FILE.put_line(fich_salida_desagrega, 'FCH_DATOS=${2}');
      UTL_FILE.put_line(fich_salida_desagrega, 'BAN_FORZADO=${3}');
      UTL_FILE.put_line(fich_salida_desagrega, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
      --UTL_FILE.put_line(fich_salida_desagrega, 'echo "load_ex_' || reg_tabla.TABLE_NAME || '" > ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
      UTL_FILE.put_line(fich_salida_desagrega, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
      UTL_FILE.put_line(fich_salida_desagrega, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
      UTL_FILE.put_line(fich_salida_desagrega, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
      UTL_FILE.put_line(fich_salida_desagrega, 'fi');
      UTL_FILE.put_line(fich_salida_desagrega, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');    
      UTL_FILE.put_line(fich_salida_desagrega, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
      UTL_FILE.put_line(fich_salida_desagrega, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
      UTL_FILE.put_line(fich_salida_desagrega, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
      UTL_FILE.put_line(fich_salida_desagrega, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
      UTL_FILE.put_line(fich_salida_desagrega, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
      --UTL_FILE.put_line(fich_salida_sh, 'set -x');
      UTL_FILE.put_line(fich_salida_desagrega, '#Permite los acentos y U');
      UTL_FILE.put_line(fich_salida_desagrega, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
      UTL_FILE.put_line(fich_salida_desagrega, 'export NLS_LANG');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, 'REQ_NUM="' || v_REQ_NUMER || '"');
      --UTL_FILE.put_line(fich_salida_desagrega, 'REQ_NUM="Req89208"');
      UTL_FILE.put_line(fich_salida_desagrega, 'INTERFAZ=' || v_REQ_NUMER || '_load_ds_' || reg_tabla.TABLE_NAME);
      --UTL_FILE.put_line(fich_salida_desagrega, 'INTERFAZ=Req89208_load_ex_' || reg_tabla.TABLE_NAME);
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '# LIBRERIAS                                                                    #');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
      UTL_FILE.put_line(fich_salida_desagrega, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
      UTL_FILE.put_line(fich_salida_desagrega, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
      UTL_FILE.put_line(fich_salida_desagrega, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, '# Cuentas  Produccion / Desarrollo                                             #');
      UTL_FILE.put_line(fich_salida_desagrega, '################################################################################');
      UTL_FILE.put_line(fich_salida_desagrega, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
      UTL_FILE.put_line(fich_salida_desagrega, '  ### Cuentas para mantenimiento');
      UTL_FILE.put_line(fich_salida_desagrega, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, 'else');
      UTL_FILE.put_line(fich_salida_desagrega, '  ### Cuentas para mantenimiento');
      UTL_FILE.put_line(fich_salida_desagrega, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
      UTL_FILE.put_line(fich_salida_desagrega, 'fi');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
      UTL_FILE.put_line(fich_salida_desagrega, 'BD_CLAVE=${PASSWORD}');
      
      /*****************************************************/
      UTL_FILE.put_line(fich_salida_desagrega, '# Llamada a sql_plus');
      UTL_FILE.put_line(fich_salida_desagrega, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
      UTL_FILE.put_line(fich_salida_desagrega, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
      UTL_FILE.put_line(fich_salida_desagrega, 'whenever sqlerror exit 1;');
      UTL_FILE.put_line(fich_salida_desagrega, 'whenever oserror exit 2;');
      UTL_FILE.put_line(fich_salida_desagrega, 'set feedback off;');
      UTL_FILE.put_line(fich_salida_desagrega, 'set serveroutput on;');
      UTL_FILE.put_line(fich_salida_desagrega, 'set echo on;');
      UTL_FILE.put_line(fich_salida_desagrega, 'set pagesize 0;');
      UTL_FILE.put_line(fich_salida_desagrega, 'set verify off;');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, 'begin');
      UTL_FILE.put_line(fich_salida_desagrega, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'lds_' || nombre_proceso || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
      UTL_FILE.put_line(fich_salida_desagrega, 'end;');
      UTL_FILE.put_line(fich_salida_desagrega, '/');
      UTL_FILE.put_line(fich_salida_desagrega, 'EOF');
      UTL_FILE.put_line(fich_salida_desagrega, 'err_salida=$?');
      UTL_FILE.put_line(fich_salida_desagrega, 'if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_desagrega, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_ds_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_desagrega, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_desagrega, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ds' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
      UTL_FILE.put_line(fich_salida_desagrega, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ds' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      --UTL_FILE.put_line(fich_salida_desagrega, '  InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_desagrega, '  exit 1');
      UTL_FILE.put_line(fich_salida_desagrega, 'fi');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, 'echo "El proceso load_' ||  'ds_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ds_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_desagrega, '');
      UTL_FILE.put_line(fich_salida_desagrega, 'exit 0');
      UTL_FILE.FCLOSE(fich_salida_desagrega);
    end if;
    /*(20160105) Angel Ruiz. FIN NF Agregacion */

    /*************************/
    UTL_FILE.FCLOSE (fich_salida_load);
    if (v_existe_scn_AGR=0 OR v_existe_scn_DSG = 0) then
      /*(20151126) Angel Ruiz. NF: AGREGACION */
      /* Solo en el agregado facil, el que posee los registros del ultimo dia del mes se hace un truncate de la tabla temporal */
      UTL_FILE.FCLOSE (fich_salida_exchange);
    end if;
    UTL_FILE.FCLOSE (fich_salida_pkg);
  end loop;
  close MTDT_TABLA;
end;

