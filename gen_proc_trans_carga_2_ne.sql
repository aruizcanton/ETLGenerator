declare

cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(mtdt_modelo_logico.TABLESPACE) "TABLESPACE"
    FROM
      MTDT_TC_SCENARIO, mtdt_modelo_logico
    WHERE MTDT_TC_SCENARIO.TABLE_TYPE = 'H' and
    trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_logico.TABLE_NAME) and
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_TRAFD_CU_MVNO', 'DMF_TRAFE_CU_MVNO', 'DMF_TRAFV_CU_MVNO');
    trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_PMP', 'DMF_PARQUE_SERIADOS', 'DMF_FACT_SERIADOS');  
    
    --SELECT
      --DISTINCT TRIM(TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      --TRIM(TABLESPACE) "TABLESPACE"
    --FROM
      --MTDT_TC_SCENARIO
    --WHERE TABLE_TYPE = 'H' and
    --trim(TABLE_NAME) in ('DMF_TRAFD_CU_MVNO', 'DMF_TRAFE_CU_MVNO', 'DMF_TRAFV_CU_MVNO');
    --trim(TABLE_NAME) in ('DMF_TRAFD_CU_MVNO');
    --trim(TABLE_NAME) in ('DMF_MOVIMIENTOS_MVNO');


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
      (trim(RUL) = 'LKUP' or trim(RUL) = 'LKUPC') and
      TRIM(TABLE_NAME) = table_name_in;



  CURSOR MTDT_TC_LKUPD (table_name_in IN VARCHAR2)
  IS
    SELECT
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TRIM(TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      trim(RUL) = 'LKUPD' and
      trim(TABLE_NAME) = table_name_in;


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
  reg_lookupd MTDT_TC_LKUPD%rowtype;
  
  
  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(30);

  
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
  lista_lkup                                    list_strings := list_strings();
  lista_lkupd                                  list_strings := list_strings();
  
  campo_filter                                VARCHAR2(2000);
  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_reducido           VARCHAR2(30);
  v_nombre_particion                  VARCHAR2(30);    
  --nombre_tabla_base_reducido           VARCHAR2(30);
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  OWNER_TC                              VARCHAR(60);  
  
  nombre_funcion                   VARCHAR2(100);
  v_encontrado											VARCHAR2(1):= 'N';
  v_contador                        PLS_INTEGER:=0;
	  


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
    end if;  
    return cadena_resul;
  end;

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


  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(500);
    cad_seg         VARCHAR(500);
    cadena            VARCHAR(100);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(100);
    constante         VARCHAR2(100);
    posicion_ant    PLS_integer;
    pos                    PLS_integer;
    cadena_resul  VARCHAR(500);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(500);
    cola                   VARCHAR2(500);
    pos_ant            PLS_integer;
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_paquete                    VARCHAR2(40);
    v_nombre_tabla_reducido         VARCHAR2(40);
    v_IE_COLUMN_LKUP              VARCHAR(400);
    v_prototipo_func                        VARCHAR2(500);
    
  begin
    /* Seleccionamos el escenario primero */
      case reg_detalle_in.RUL
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
        
      when 'LKUPC' then
        /* (20150619) Angel Ruiz. Anyado esta nueva funcionalidad */
        /* Se trata de que pueda aplicar una funcion de LookUp pero dentro de un CASE WHEN */
        /* para hacer esta aplicacion condicional */

        /*Puede ocurrir que en el campo VALUE de la llamada a LOOKUP se use la variable VAR_FCH_CARGA */
        v_IE_COLUMN_LKUP := procesa_campo_filter (reg_detalle_in.IE_COLUMN_LKUP);
        
        /****************************/
        v_nombre_tabla_reducido := substr(reg_detalle_in.TABLE_NAME, 5);
        if (length(reg_detalle_in.TABLE_NAME) < 25) then
        v_nombre_paquete := reg_detalle_in.TABLE_NAME;
        else
        v_nombre_paquete := v_nombre_tabla_reducido;
        end if;        
        /* La tabla de LookUp puede ser una SELECT y no solo una tabla */
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas para LookUp */
          v_nombre_func_lookup := 'LK_' || reg_detalle_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion con el nombre del campo resultado del LookUp */
        else
          v_nombre_func_lookup := 'LK_' || reg_detalle_in.TABLE_LKUP;  /* Llamo a mi funcion de LookUp esta concatenacion */
        end if;
        
        /* Procesamos el campo LKUP_COM_RUL que es donde esta la condicion CASE WHEN*/
        v_prototipo_func := 'PKG_' || v_nombre_paquete || '.' || v_nombre_func_lookup || ' (' || v_IE_COLUMN_LKUP || ')';
        valor_retorno := proc_campo_value_condicion (reg_detalle_in.LKUP_COM_RULE, v_prototipo_func);
        
      when 'LKUP' then
        /* Se trata de hacer el LOOK UP con la tabla dimension */
        --if (reg_detalle_in.LKUP_COM_RULE <> "") then

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
      when 'LKUPD' then
        /* (20150430) Angel Ruiz */
        /* Se trata de hacer el LOOK UP con la tabla dimension */

        /*Puede ocurrir que en el campo VALUE de la llamada a LOOKUP se use la variable VAR_FCH_CARGA */
        v_IE_COLUMN_LKUP := procesa_campo_filter (reg_detalle_in.IE_COLUMN_LKUP);

        v_nombre_tabla_reducido := substr(reg_detalle_in.TABLE_NAME, 5);
        if (length(reg_detalle_in.TABLE_NAME) < 25) then
        v_nombre_paquete := reg_detalle_in.TABLE_NAME;
        else
        v_nombre_paquete := v_nombre_tabla_reducido;
        end if;        

        v_nombre_func_lookup := 'LK_' || reg_detalle_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion */

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
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
        valor_retorno :=  '    ' || 'PKG_' || reg_detalle_in.TABLE_NAME || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
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
          /* Busco LA COMILLA */
          sustituto := '''''';
          loop
            dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
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
          /************/
        --valor_retorno := '    ' || trim(reg_detalle_in.VALUE);
        valor_retorno := cadena_resul;
        posicion := instr(valor_retorno, 'VAR_IVA');
        if (posicion >0) then
          cad_pri := substr(valor_retorno, 1, posicion-1);
          cad_seg := substr(valor_retorno, posicion + length('VAR_IVA'));
          valor_retorno :=  cad_pri || '21' || cad_seg;
        end if;
        posicion := instr(valor_retorno, 'VAR_FCH_CARGA');
        if (posicion >0) then
          cad_pri := substr(valor_retorno, 1, posicion-1);
          cad_seg := substr(valor_retorno, posicion + length('VAR_FCH_CARGA'));
          valor_retorno :=  cad_pri || ''' || ''TO_DATE ('''''' || fch_datos_in || '''''', ''''YYYYMMDD'''') '' || ''' || cad_seg;
        end if;
      when 'HARDC' then
        valor_retorno :=  '    ' || reg_detalle_in.VALUE;
      when 'SEQ' then
        valor_retorno := '    ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
        --  valor_retorno := '    ' || reg_detalle_in.VALUE;
        --else
        --  valor_retorno := '    ' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;      
      when 'VAR_FCH_INICIO' then
        --valor_retorno :=  '    ' || ''' || var_fch_inicio || ''';
        --valor_retorno :=  '    SYSDATE';
        /*(20160203) Angel Ruiz BUG. Debe insertarse la fecha de inicio del proceso de insercion */
        valor_retorno :=  '    TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')';
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
--          valor_retorno :=  '     ' ||  'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
          valor_retorno := '     ' || ''' || fch_datos_in || ''';        
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

  function gen_enca_funcion_LKUPD (reg_lookup_in in MTDT_TC_LKUPD%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
    ie_lkup_columns                list_strings := list_strings();
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_tabla                          VARCHAR2(30);
    
  begin
    /* (20150430) Angel Ruiz .  */
    v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion */
    v_nombre_tabla := reg_lookup_in.TABLE_LKUP;
    /* Se trata de hacer el LOOK UP con la tabla dimension */
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
      valor_retorno := valor_retorno || ') return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE RESULT_CACHE;';
    else        
      valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE RESULT_CACHE;';
    end if;
    return valor_retorno;
  end;

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


/************/

  procedure gen_cuer_funcion_LKUPD (reg_lookup_in in MTDT_TC_LKUPD%rowtype) is
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
    /* (20150430) Angel Ruiz . */
    v_nombre_func_lookup := 'LK_' || reg_lookup_in.TABLE_COLUMN;  /* Llamo a mi funcion de LookUp esta concatenacion */
    v_nombre_tabla := reg_lookup_in.TABLE_LKUP;
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
    UTL_FILE.put_line (fich_salida_pkg, '  IS');

    UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    /**********************************************************/
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
      UTL_FILE.put_line (fich_salida_pkg, '      l_row := ''NI#'';');
      UTL_FILE.put_line (fich_salida_pkg, '  else');
    end if;

    UTL_FILE.put_line (fich_salida_pkg, '');

    /*********************************************************/
    
    UTL_FILE.put_line (fich_salida_pkg, '    SELECT nvl(' || reg_lookup_in.VALUE || ', ''GE#'') INTO l_row'); 
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
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN ''GE#'';');
    UTL_FILE.put_line (fich_salida_pkg, '  when others then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN ''GE#'';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || v_nombre_func_lookup || ';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '');
 
  end gen_cuer_funcion_LKUPD;

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
    
    --nombre_tabla_base_reducido := substr(reg_tabla.TABLE_BASE_NAME, 4); /* Le quito al nombre de la tabla los caracteres SA_ */
    UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    lista_scenarios_presentes.delete;
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    dbms_output.put_line ('Comienzo la generacion del PACKAGE DEFINITION');
    /* Primero de todo miro si hay funciones de LOOKUP para crear */
    dbms_output.put_line ('Antes de mirar funciones para hacer LOOKUP');
    open MTDT_TC_LOOKUP (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_LOOKUP
      into reg_lookup;
      exit when MTDT_TC_LOOKUP%NOTFOUND;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      prototipo_fun := genera_encabezado_funcion_pkg (reg_lookup);
      UTL_FILE.put_line(fich_salida_pkg,'');
      UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
      
    end loop;
    close MTDT_TC_LOOKUP;
    dbms_output.put_line ('Despues de mirar funciones para hacer LOOKUP');
    dbms_output.put_line ('Antes de mirar funciones para hacer regla FUNCTION');
    
    /* (20150430) Angel Ruiz */
    /* Segundo de todo miro si hay funciones de LOOKUPD para crear */
    lista_lkupd.delete;
    v_contador:=0;
    open MTDT_TC_LKUPD (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_LKUPD
      into reg_lookupd;
      exit when MTDT_TC_LKUPD%NOTFOUND;
      nombre_funcion := 'LK_' || reg_lookupd.TABLE_COLUMN;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      /* Buscamos si la funcion de lookup ya la hemos generado, ya que si ya esta generada no hay que generarla de nuevo */
      v_encontrado := 'N';
      if (v_contador = 0) then
        lista_lkupd.EXTEND;
        lista_lkupd (lista_lkupd.last) := nombre_funcion;
        prototipo_fun := gen_enca_funcion_LKUPD (reg_lookupd);
        UTL_FILE.put_line(fich_salida_pkg,'');
        UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
        v_contador:=v_contador+1;
      else
        for indx in lista_lkupd.FIRST .. lista_lkupd.LAST
        loop
          if (lista_lkupd(indx) = nombre_funcion) then
            v_encontrado := 'Y';
          end if;
        end loop;
        if (v_encontrado = 'N') then
          lista_lkupd.EXTEND;
          lista_lkupd (lista_lkupd.last) := nombre_funcion;
          prototipo_fun := gen_enca_funcion_LKUPD (reg_lookupd);
          UTL_FILE.put_line(fich_salida_pkg,'');
          UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
        end if;
        v_contador:=v_contador+1;
      end if;
    end loop;
    close MTDT_TC_LKUPD;
    
    /* Tercero miro si hay funciones de la regla FUNCTION para crear */

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
      
      if (reg_scenario.SCENARIO = 'N')      /*  Procesamos el escenario NUEVO  */
      then
        /* Tenemos el escenario Nuevo */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'N';
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
      end if;

      /************************/

      if (reg_scenario.SCENARIO = 'NUM')    /*  Procesamos el escenario NUM  */
      then
        /* Tenemos el escenario NUM */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION num_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'NUM';
      end if;
      
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
    UTL_FILE.put_line(fich_salida_pkg,'  PROCEDURE pre_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2)'); 
    UTL_FILE.put_line(fich_salida_pkg,'  is'); 
    UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
    UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
    UTL_FILE.put_line(fich_salida_pkg,'   fch_particion number(8);');
    UTL_FILE.put_line(fich_salida_pkg,'  begin'); 
    UTL_FILE.put_line(fich_salida_pkg,'    exis_tabla :=  existe_tabla (' || '''T_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in);');
    UTL_FILE.put_line(fich_salida_pkg,'    if (exis_tabla = 0) then' );      
    UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la tabla */'); 
    UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''CREATE TABLE  ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in || '' TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''' || '' AS SELECT * FROM ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''';');
    UTL_FILE.put_line(fich_salida_pkg,'    else'); 
    UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in;');
    UTL_FILE.put_line(fich_salida_pkg,'    end if;'); 
    UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_NUMBER(TO_CHAR(TO_DATE(fch_datos_in,''YYYYMMDD'')+1,''YYYYMMDD''));'); 
    --UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''PA_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in, ''' || reg_tabla.TABLE_NAME || ''');');
    UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (''' || v_nombre_particion || '_' || ''' || fch_datos_in, ''' || reg_tabla.TABLE_NAME || ''');');
    UTL_FILE.put_line(fich_salida_pkg,'    if (exis_partition = 0) then' );      
    UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la particion */'); 
    --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') UPDATE INDEXES'';');
    --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
    UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
    UTL_FILE.put_line(fich_salida_pkg,'   end if;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  exception'); 
    UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
    UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
    UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  end pre_proceso;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  function pos_proceso (fch_carga_in IN VARCHAR2,  fch_datos_in IN VARCHAR2) return number'); 
    UTL_FILE.put_line(fich_salida_pkg,'  is'); 
    UTL_FILE.put_line(fich_salida_pkg,'    valor_retorno number;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  begin'); 
    UTL_FILE.put_line(fich_salida_pkg,'    /* Proceso que se va ha encargar de hacer el pos-procesado despues de insertar */'); 
    UTL_FILE.put_line(fich_salida_pkg,'    /* consistente en comprobar si la particion de ' || reg_tabla.TABLE_NAME || ' de fecha de datos ya tenia datos, para salvaguardarlos si los tenia */');
    UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND, PARALLEL (T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '') */ INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' select * from ' || OWNER_DM || '.'' || ''' || reg_tabla.TABLE_NAME || ''' || '' where CVE_DIA = '' || fch_datos_in;');
    UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''El numero de filas salvaguardadas es: '' || SQL%ROWCOUNT);'); 
    UTL_FILE.put_line(fich_salida_pkg,'    valor_retorno := SQL%ROWCOUNT;'); 
    UTL_FILE.put_line(fich_salida_pkg,'    commit;'); 
    UTL_FILE.put_line(fich_salida_pkg,'    return valor_retorno;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  exception'); 
    UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then'); 
    UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);'); 
    UTL_FILE.put_line(fich_salida_pkg,'    raise;'); 
    UTL_FILE.put_line(fich_salida_pkg,'  end pos_proceso;'); 

  
    /* Primero de todo miro si tengo que generar los cuerpos de las funciones de LOOKUP */
    lista_lkup.delete;
    v_contador:=0;
    dbms_output.put_line ('Antes de generar las funciones de LOOKUP');
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
    /* Segundo de todo miro si tengo que generar los cuerpos de las funciones de LOOKUPD */
    
    lista_lkupd.delete;
    v_contador:=0;
    open MTDT_TC_LKUPD (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_TC_LKUPD
      into reg_lookupd;
      exit when MTDT_TC_LKUPD%NOTFOUND;
      nombre_funcion := 'LK_' || reg_lookupd.TABLE_COLUMN;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      /* Buscamos si la funcion de lookup ya la hemos generado, ya que si ya esta generada no hay que generarla de nuevo */
      v_encontrado := 'N';
      if (v_contador = 0) then
        lista_lkupd.EXTEND;
        lista_lkupd (lista_lkupd.last) := nombre_funcion;
        gen_cuer_funcion_LKUPD (reg_lookupd);
        v_contador:=v_contador+1;
      else
        for indx in lista_lkupd.FIRST .. lista_lkupd.LAST
        loop
          if (lista_lkupd(indx) = nombre_funcion) then
            v_encontrado := 'Y';
          end if;
        end loop;
        if (v_encontrado = 'N') then
          lista_lkupd.EXTEND;
          lista_lkupd (lista_lkupd.last) := nombre_funcion;
          gen_cuer_funcion_LKUPD (reg_lookupd);
        end if;
        v_contador:=v_contador+1;
      end if;
    end loop;
    close MTDT_TC_LKUPD;
    /********************************************/
    
    
    dbms_output.put_line ('Antes de generar las funciones de FUNCTION');
    /* Tercero de todo miro si tengo que generar los cuerpos de las funciones de FUNCTION */
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
          dbms_output.put_line ('Estoy dentro del scenario NUEVO');
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
  
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND*/');
          --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND, PARALLEL (T_' || nombre_tabla_reducido || '_'' || fch_datos_in || '') */');
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in ||');
      
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
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          --UTL_FILE.put_line(fich_salida_pkg, '   app_mvnosa.'  || reg_scenario.TABLE_BASE_NAME || ''' || ''_'' || fch_datos_in;');
          UTL_FILE.put_line(fich_salida_pkg, '   ' || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));
          dbms_output.put_line ('Despues del FROM');
          if (reg_scenario.FILTER is not null) then
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            /* Procesamos el campo FILTER */
            dbms_output.put_line ('Antes de procesar el campo FILTER');
            campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
            dbms_output.put_line ('Despues de procesar el campo FILTER');
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
          UTL_FILE.put_line(fich_salida_pkg, '  END new_' || nombre_proceso || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion new */
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'OPE')  /* Proceso el escenario OPE */
      then
        /* SCENARIO OPE */
          dbms_output.put_line ('Dentro de OPE');
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ope_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, fch_registro_in IN VARCHAR2) return NUMBER');
          UTL_FILE.put_line(fich_salida_pkg, '  IS');
          UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas NUMBER;');
          UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');          
          UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');

          --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND*/');
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in ||');
          
  
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND, PARALLEL (' || reg_scenario.TABLE_NAME || ') */');
          --UTL_FILE.put_line(fich_salida_pkg,'    INTO TMP_' || reg_scenario.TABLE_NAME);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
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
          /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          dbms_output.put_line ('Despues del INTO');
          /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
          dbms_output.put_line ('Entro en el SELECT');
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
          dbms_output.put_line ('Despues de generar el SELECT');
          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          UTL_FILE.put_line(fich_salida_pkg,'    FROM');
          UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));
          dbms_output.put_line ('Despues de generar el FROM');
          /* INICIO generacion parte  WHERE  */
          if (reg_scenario.FILTER is not null) then
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.CVE_DIA = '' || fch_datos || '' and '' || ''');
          
            /* Procesamos el campo FILTER */
            dbms_output.put_line ('Antes de procesar el campo FILTER');
            campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
            dbms_output.put_line ('Despues de procesar el campo FILTER');
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ''';');
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

          --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND*/');
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in ||');
  
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND, PARALLEL (' || reg_scenario.TABLE_NAME || ') */');
          --UTL_FILE.put_line(fich_salida_pkg,'    INTO TMP_' || reg_scenario.TABLE_NAME);
     
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
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
          UTL_FILE.put_line(fich_salida_pkg, '    '  || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));

          /* INICIO generacion parte  WHERE  */
          if (reg_scenario.FILTER is not null) then
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.CVE_DIA = '' || fch_datos || '' and '' || ''');
            
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ''';');
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
  
          --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND*/');
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in ||');
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND, PARALLEL (' || reg_scenario.TABLE_NAME || ') */');
          --UTL_FILE.put_line(fich_salida_pkg,'    INTO TMP_' || reg_scenario.TABLE_NAME);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
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
          UTL_FILE.put_line(fich_salida_pkg, '    '  || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));

          /* INICIO generacion parte  WHERE  */
          if (reg_scenario.FILTER is not null) then
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.CVE_DIA = '' || fch_datos || '' and '' || ''');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ''';');
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
  
          --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT /*+ APPEND*/');
          UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''INSERT');
          UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido || '_'' || fch_datos_in ||');
  
          --UTL_FILE.put_line(fich_salida_pkg,'    INSERT /*+ APPEND, PARALLEL (' || reg_scenario.TABLE_NAME || ') */');
          --UTL_FILE.put_line(fich_salida_pkg,'    INTO TMP_' || reg_scenario.TABLE_NAME);
      
          /* genero la parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
          
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
          UTL_FILE.put_line(fich_salida_pkg, '    '  || procesa_campo_filter_dinam(reg_scenario.TABLE_BASE_NAME));

          if (reg_scenario.FILTER is not null) then
            /* INICIO generacion parte  WHERE  */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
            --UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.CVE_DIA = '' || fch_datos || '' and '' || ''');
            /* Procesamos el campo FILTER */
            campo_filter := procesa_campo_filter_dinam(reg_scenario.FILTER);
            UTL_FILE.put_line(fich_salida_pkg, campo_filter);
          end if;
          UTL_FILE.put_line(fich_salida_pkg, ''';');
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
    UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg, '  reg_monitoreo MTDT_MONITOREO%rowtype;');
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_new NUMBER;');
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_ope NUMBER;');
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_alt NUMBER;');
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_icc NUMBER;');
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_num NUMBER;');    
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_tot NUMBER;');    
    UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_salvaguardados NUMBER:=0;');    
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
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_he_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '      if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if;');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 1) then');
    UTL_FILE.put_line(fich_salida_pkg, '');
    
    /**********************/
    /**********************/
    --UTL_FILE.put_line(fich_salida_pkg, '        dbms_output.put_line (''Inicio de la pasada del bucle del proceso de carga: ''' || ' || ''' || 'load_he_' || reg_tabla.TABLE_NAME || ''' || ''.'');');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    --UTL_FILE.put_line(fich_salida_pkg, '        pkg_' || reg_tabla.TABLE_NAME || '.' || 'pre_proceso (fch_carga_in, to_char(reg_monitoreo.FCH_DATOS,''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        pkg_' || nombre_proceso || '.' || 'pre_proceso (fch_carga_in, fch_datos_in);');
    --UTL_FILE.put_line(fich_salida_pkg, '        SET TRANSACTION NAME ''TRAN_' || reg_tabla.TABLE_NAME || ''';');
    UTL_FILE.put_line(fich_salida_pkg, '');
    /* Generamos las llamadas a los procedimientos para realizar las cargas */
    /* Generamos la llamada para cargar los registros NUEVOS */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'N'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_new := ' || 'pkg_' || nombre_proceso || '.' || 'new_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_new;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros insertados es: '' || numero_reg_new || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros OPE */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'OPE'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_ope := ' || 'pkg_' || nombre_proceso || '.' || 'ope_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_ope;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros ope es: '' || numero_reg_ope || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros ALT */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'ALT'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_alt := ' || 'pkg_' || nombre_proceso || '.' || 'alt_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_alt;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros ope es: '' || numero_reg_alt || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros ALT */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'ICC'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_icc := ' || 'pkg_' || nombre_proceso || '.' || 'icc_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_icc;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros icc es: '' || numero_reg_icc || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros NUM */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'NUM'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_num := ' || 'pkg_' || nombre_proceso || '.' || 'num_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in, TO_CHAR(inicio_paso_tmr, ''YYYYMMDDHH24MISS''));');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_num;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros num es: '' || numero_reg_num || ''.'');');
      end if;
    END LOOP;
    UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg, '         pkg_' || reg_tabla.TABLE_NAME || '.' || 'pos_proceso (fch_carga_in, to_char(reg_monitoreo.FCH_DATOS, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '         numero_reg_salvaguardados := pkg_' || nombre_proceso || '.' || 'pos_proceso (fch_carga_in, fch_datos_in);');
    UTL_FILE.put_line(fich_salida_pkg, '         /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
    UTL_FILE.put_line(fich_salida_pkg, '         /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
    UTL_FILE.put_line(fich_salida_pkg, '         ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_tot, 0, 0, numero_reg_salvaguardados);');
    UTL_FILE.put_line(fich_salida_pkg, '         COMMIT;');
    UTL_FILE.put_line(fich_salida_pkg, '       end if;');
    --UTL_FILE.put_line(fich_salida_pkg, '     end loop;');
    --UTL_FILE.put_line(fich_salida_pkg,'    RETURN 0;');
    
    UTL_FILE.put_line(fich_salida_pkg,'    exception');
    --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
    --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
    UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
    UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
    UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
    UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '  END lhe_' || nombre_proceso || ';');
    UTL_FILE.put_line(fich_salida_pkg, '');
  
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
    UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
    --UTL_FILE.put_line(fich_salida_pkg, '    open MTDT_MONITOREO;');
    --UTL_FILE.put_line(fich_salida_pkg, '    loop');
    --UTL_FILE.put_line(fich_salida_pkg, '      fetch MTDT_MONITOREO');
    --UTL_FILE.put_line(fich_salida_pkg, '      into reg_monitoreo;');
    --UTL_FILE.put_line(fich_salida_pkg, '      exit when MTDT_MONITOREO%NOTFOUND;');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '      /* Lo primero que se hace es mirar que paso es el primero a ejecutar */');
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '      if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if;');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 1) then');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Este tipo de procesos ex solo tienen dos pasos */');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Comienza en el primer paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME);    
    --UTL_FILE.put_line(fich_salida_pkg, '        EXCHANGE PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' ');    
    UTL_FILE.put_line(fich_salida_pkg, '        EXCHANGE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_datos_in || '' ');    
    UTL_FILE.put_line(fich_salida_pkg, '        WITH TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' ');    
    UTL_FILE.put_line(fich_salida_pkg, '        WITHOUT VALIDATION'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '        /* comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' CASCADE CONSTRAINTS PURGE'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if; ');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 2) then');
    UTL_FILE.put_line(fich_salida_pkg, '        /* comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    --UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE APP_MVNODM.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in;');    
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' CASCADE CONSTRAINTS PURGE'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
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
    UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');    
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
    UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="Req89208"');
    UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=Req89208_load_he_' || reg_tabla.TABLE_NAME);
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
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'lhe_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
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
    UTL_FILE.put_line(fich_salida_exchange, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
    --UTL_FILE.put_line(fich_salida_exchange, 'echo "load_ex_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
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
    UTL_FILE.put_line(fich_salida_exchange, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'lex_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
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

