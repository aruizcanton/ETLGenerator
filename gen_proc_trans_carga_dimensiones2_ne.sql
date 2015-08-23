declare

cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE"
    FROM
      MTDT_TC_SCENARIO
    WHERE TABLE_TYPE in ('D','I')
    and TABLE_NAME in ('SA_MOVIMIENTOS_SERIADOS', 'SA_FACT_SERIADOS1')
    order by
    TABLE_TYPE;
    --and TRIM(TABLE_NAME) not in;
    --and 
    --TRIM(TABLE_NAME) in ('DMD_CAUSA_TERMINACION_LLAMADA', 'DMD_EMPRESA');
    --TRIM(TABLE_NAME) not in ('DMD_ESTADO_CELDA', 'DMD_FINALIZACION_LLAMADA', 'DMD_EMPRESA', 'DMD_POSICION_TRAZO_LLAMADA', 'DMD_TRONCAL', 'DMD_TIPO_REGISTRO', 'DMD_MSC');
    --and
    --TABLE_NAME = 'DMD_OPERADOR_VIRTUAL';
      
      
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
      DATE_CREATE,
      DATE_MODIFY
    FROM 
      MTDT_TC_SCENARIO
    WHERE
      TABLE_NAME = table_name_in;
      
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
      
  CURSOR MTDT_TC_LOOKUP (table_name_in IN VARCHAR2)
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
      (trim(RUL) = 'LKUP' or trim(RUL) = 'LKUPC') and
      trim(TABLE_NAME) = table_name_in;

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
      trim(RUL) = 'FUNCTION' and
      TRIM(TABLE_NAME) = table_name_in;
  
      
  reg_tabla MTDT_TABLA%rowtype;
      
  reg_scenario MTDT_SCENARIO%rowtype;
  
  reg_detail MTDT_TC_DETAIL%rowtype;
  
  reg_interface_detail dtd_interfaz_detail%rowtype;
  
  reg_lookup MTDT_TC_LOOKUP%rowtype;
  reg_lookupd MTDT_TC_LKUPD%rowtype;
  
  reg_function MTDT_TC_FUNCTION%rowtype;

  v_nombre_particion VARCHAR2(30);
  v_history MTDT_INTERFACE_SUMMARY.HISTORY%TYPE;
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  TYPE list_strings  IS TABLE OF VARCHAR(30);
  
  lista_pk                                                                    list_columns_primary := list_columns_primary (); 
  where_interface_columns                list_strings := list_strings();
  where_table_columns                      list_strings := list_strings();
  lista_scenarios_presentes                                    list_strings := list_strings();
  lista_lkup                                    list_strings := list_strings();
  lista_lkupd                                  list_strings := list_strings();
  
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(500);
  prototipo_fun                             VARCHAR2(500);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_hist                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
  nombre_fich_hist                      VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_tabla_reducido           VARCHAR2(30);
  campo_filter                                VARCHAR2(250);
  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_base_redu        VARCHAR2(30);
  nombre_tabla_base_sp_redu  VARCHAR2(30);
  num_sce_integra number(2) := 0;
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  OWNER_TC                              VARCHAR(60);
  nombre_funcion                   VARCHAR2(100);
  v_encontrado											VARCHAR2(1):= 'N';
  v_contador                        PLS_INTEGER:=0;
  v_concept_name                MTDT_INTERFACE_SUMMARY.CONCEPT_NAME%TYPE;
  TABLESPACE_SA                  VARCHAR2(60);
  v_num_meses                          VARCHAR2(2);
  v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;
  
  



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
    cabeza                varchar2 (3000);
    sustituto              varchar2(100);
    cola                      varchar2(3000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(3000);
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
    cadena            VARCHAR(500);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(500);
    constante         VARCHAR2(500);
    v_nombre_func_lookup             VARCHAR2(40);
    v_nombre_paquete                    VARCHAR2(40);
    v_nombre_tabla_reducido         VARCHAR2(40);
    v_IE_COLUMN_LKUP              VARCHAR(400);
    v_prototipo_func                        VARCHAR2(500);
    
  begin
      case trim(reg_detalle_in.RUL)
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno := '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
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
        /*********************/
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
        /* (20150306) ANGEL RUIZ. Hay un error que corrijo */
        v_nombre_tabla_reducido := substr(reg_detalle_in.TABLE_NAME, 5);
        if (length(reg_detalle_in.TABLE_NAME) < 25) then
        v_nombre_paquete := reg_detalle_in.TABLE_NAME;
        else
        v_nombre_paquete := v_nombre_tabla_reducido;
        end if;        
        valor_retorno :=  '    ' || 'PKG_' || v_nombre_paquete || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
      when 'DLOAD' then
        valor_retorno := '    ' || 'TO_DATE (fch_carga_in, ''YYYYMMDD'')';
      when 'DSYS' then
        valor_retorno := '    ' || 'SYSDATE';
      when 'CODE' then
        posicion := instr(reg_detalle_in.VALUE, 'VAR_IVA');
        if (posicion >0) then
          cad_pri := substr(reg_detalle_in.VALUE, 1, posicion-1);
          cad_seg := substr(reg_detalle_in.VALUE, posicion + length('VAR_IVA'));
          valor_retorno :=  '    ' || cad_pri || '21' || cad_seg;
        else
          valor_retorno :=  '    ' || reg_detalle_in.VALUE;
        end if;
        posicion := instr(valor_retorno, 'VAR_FCH_CARGA');
        if (posicion >0) then
          cad_pri := substr(valor_retorno, 1, posicion-1);
          cad_seg := substr(valor_retorno, posicion + length('VAR_FCH_CARGA'));
          valor_retorno :=  '    ' || cad_pri || ' to_date(fch_datos_in, ''yyyymmdd'') ' || cad_seg;
        end if;
      when 'HARDC' then
        valor_retorno := '    ' || reg_detalle_in.VALUE;
      when 'SEQ' then
        valor_retorno := '    ' || OWNER_DM || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
          --valor_retorno := '    app_mvnodm.' || reg_detalle_in.VALUE;
          --valor_retorno := '    app_mvnodm.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --else
          --valor_retorno := '    app_mvnodm.' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        valor_retorno := '    ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;      
      when 'VAR_FCH_INICIO' then
        valor_retorno := '    ' || 'var_fch_inicio';
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
          valor_retorno := '    ' || '1';
        end if;
      end case;
    return valor_retorno;
  end;

  function genera_encabezado_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
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
        valor_retorno := valor_retorno || ') return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || OWNER_SA || '.' || v_nombre_tabla || '.' || reg_lookup_in.IE_COLUMN_LKUP || '%TYPE) return ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
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
        valor_retorno := valor_retorno || ') return ' || reg_lookup_in.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      else        
        valor_retorno := '  FUNCTION ' || v_nombre_func_lookup || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE RESULT_CACHE;';
      end if;
    end if;
    return valor_retorno;
  end;

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
    valor_retorno := '  FUNCTION ' || 'LK_' || reg_function_in.VALUE;
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
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_function_in.TABLE_LKUP);
  end genera_cuerpo_regla_function;

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
      UTL_FILE.put_line (fich_salida_pkg, '    return ' || reg_lookup_in.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE');
      UTL_FILE.put_line (fich_salida_pkg, '    RESULT_CACHE RELIES_ON (' || reg_lookup_in.TABLE_LKUP || ')');
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    /* (20150130) Angel Ruiz . Nueva incidencia. */
    --if (instr (reg_lookup_in.TABLE_LKUP,'SELECT ') > 0) then
    --  UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE;');
    --else
    --  UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    --end if;
    /* (20150619) Angel Ruiz. Nueva funcionalidad. Cambio la obtencion del tipo de dato de retorno para que sea mas coherente*/
    UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_tabla.TABLE_NAME || '.' || reg_lookup_in.TABLE_COLUMN || '%TYPE;');
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
  /* (20141222) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  /* (20141222) FIN*/

  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    if (reg_tabla.TABLE_TYPE = 'D') 
    then
            dbms_output.put_line ('Estoy en el primer LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);
            nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
            /* Angel Ruiz (20141201) Hecho porque hay paquetes que no compilan */
             if (length(reg_tabla.TABLE_NAME) < 25) then
              nombre_proceso := reg_tabla.TABLE_NAME;
            else
              nombre_proceso := nombre_tabla_reducido;
            end if;
            nombre_fich_carga := 'load_ne_' || reg_tabla.TABLE_NAME || '.sh';
            --nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql';
            nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql'; /* Angel Ruiz (20141201) Hecho porque hay paquetes que no compilan */
            nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
            nombre_fich_hist := 'load_dh_' || reg_tabla.TABLE_NAME || '.sh';
            fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
            fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
            fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
            fich_salida_hist := UTL_FILE.FOPEN ('SALIDA',nombre_fich_hist,'W');
            dbms_output.put_line ('El nombre del PAQUETE es: ' || '.pkg_' || nombre_proceso);
            UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
            lista_scenarios_presentes.delete;
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
            
            /* Tercero genero los metodos para los escenarios */
            open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
            loop
              fetch MTDT_SCENARIO
              into reg_scenario;
              exit when MTDT_SCENARIO%NOTFOUND;
              dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
              /* Elaboramos los prototipos de la funciones que cargaran los distintos escenarios */
              if (reg_scenario.SCENARIO = 'N')
              then
                /* Tenemos el escenario Nuevo */
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION nreg_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION nreg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
                --UTL_FILE.put_line(fich_salida_pkg,'');
                /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
                lista_scenarios_presentes.EXTEND;
                lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'N';
              end if;
              if (reg_scenario.SCENARIO = 'E')
              then
              /* Tenemos el escenario Existente */
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION upt_reg_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ureg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
                
                --UTL_FILE.put_line(fich_salida_pkg,'');
                /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
                lista_scenarios_presentes.EXTEND;
                lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'E';
              end if;
              if (reg_scenario.SCENARIO = 'H')
              then
              /* Tenemos el escenario Historico */
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION hst_reg_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION hreg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
                --UTL_FILE.put_line(fich_salida_pkg,'');
                /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
                lista_scenarios_presentes.EXTEND;
                lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'H';
              end if;
            end loop;   /* Fin del loop MTDT_SCENARIO */
            close MTDT_SCENARIO;
            UTL_FILE.put_line(fich_salida_pkg,'');
            --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_ne_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lne_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg,'');
            --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_dh_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE ldh_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg,'');
            --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_ex_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lex_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg,'');
            --UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || reg_scenario.TABLE_NAME || ';' );
            UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
            UTL_FILE.put_line(fich_salida_pkg, '/' );
            /******/
            /* FIN DEL PACKAGE DEFINITION */
            /******/
            /******/
            /* INICIO DEL PACKGE BODY */
            /******/
            --UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY APP_MVNODM.pkg_' || reg_scenario.TABLE_NAME || ' AS');
            UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
            UTL_FILE.put_line(fich_salida_pkg,'');
            
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
            open MTDT_SCENARIO (reg_scenario.TABLE_NAME);
            loop
              fetch MTDT_SCENARIO
              into reg_scenario;
              exit when MTDT_SCENARIO%NOTFOUND;
              dbms_output.put_line ('Estoy en el segundo LOOP MTDT_SCENARIO. El escenario es: ' || reg_scenario.SCENARIO);
              if (reg_scenario.SCENARIO = 'N')
              then
                /* ESCENARIO NUEVO */
                dbms_output.put_line ('Estoy en el escenario: N');
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_reg_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION nreg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  IS');
                UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas INTEGER;');
                UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');
                UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
                UTL_FILE.put_line(fich_salida_pkg, '');
                UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
                UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
                /* parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                UTL_FILE.put_line(fich_salida_pkg,'    (');
                open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                primera_col := 1;
                loop
                  fetch MTDT_TC_DETAIL
                  into reg_detail;
                  exit when MTDT_TC_DETAIL%NOTFOUND;
                  dbms_output.put_line ('Estoy en el Tercer Loop. El campo es: ' || reg_detail.TABLE_COLUMN);
                  if primera_col = 1 then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
                    primera_col := 0;
                  else
                    UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_detail.TABLE_COLUMN);
                  end if;
                end loop;
                close MTDT_TC_DETAIL;
                UTL_FILE.put_line(fich_salida_pkg,'    )');
                dbms_output.put_line ('He pasado la parte del INTO');
                /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                /****/
                UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
                open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                primera_col := 1;
                loop
                  fetch MTDT_TC_DETAIL
                  into reg_detail;
                  exit when MTDT_TC_DETAIL%NOTFOUND;
                  dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
                  columna := genera_campo_select (reg_detail);
                  if primera_col = 1 then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || columna);
                    primera_col := 0;
                  else
                    UTL_FILE.put_line(fich_salida_pkg, '    ,' || columna);
                  end if;        
                end loop;
                close MTDT_TC_DETAIL;
                /****/
                /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                /****/ 
                /****/
                /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
                /****/
                dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
                UTL_FILE.put_line(fich_salida_pkg,'    FROM');
                UTL_FILE.put_line(fich_salida_pkg, '    ' || OWNER_SA || '.' || reg_scenario.TABLE_BASE_NAME || ', ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME);
                dbms_output.put_line ('Interface COLUMNS: ' || reg_scenario.INTERFACE_COLUMNS);
                dbms_output.put_line ('Table COLUMNS: ' || reg_scenario.TABLE_COLUMNS);
                where_interface_columns := split_string_coma (reg_scenario.INTERFACE_COLUMNS);
                where_table_columns := split_string_coma(reg_scenario.TABLE_COLUMNS);
                dbms_output.put_line ('El numero de valores del Where interface es: ' || where_interface_columns.count);
                dbms_output.put_line ('El numero de valores del Where interface es: ' || where_table_columns.count);
        
                IF (where_interface_columns.COUNT > 0  and 
                  where_table_columns.COUNT > 0 and 
                  where_interface_columns.COUNT = where_table_columns.COUNT) 
                THEN
                  /****/
                  /* INICIO generacion parte  WHERE */
                  /****/    
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
                  /* Procesamos el campo FILTER . Lo añado a posteriori en la recta final (20141126*/
                  if (reg_scenario.FILTER is null) then
                    FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
                    LOOP
                      UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_detail.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_detail.TABLE_NAME || '.' || where_table_columns(indx) || '(+) AND');
                    END LOOP;
                    UTL_FILE.put_line(fich_salida_pkg, '    '  || reg_detail.TABLE_NAME || '.' || where_table_columns ( where_table_columns.FIRST) || ' IS NULL;' );
                  else
                    FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
                    LOOP
                      UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_detail.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_detail.TABLE_NAME || '.' || where_table_columns(indx) || '(+) AND');
                    END LOOP;
                    UTL_FILE.put_line(fich_salida_pkg, '    '  || reg_detail.TABLE_NAME || '.' || where_table_columns ( where_table_columns.FIRST) || ' IS NULL' );
                    /* Añadimos el campo FILTER */
                    UTL_FILE.put_line(fich_salida_pkg, '    AND');
                    campo_filter := procesa_campo_filter(reg_scenario.FILTER);
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter || ';');
                  end if;
                ELSE /* Puede que no haya un WHERE POR LAS COLUMNAS DE TABLA E INTERFACE PERO SI HAYA FILTER*/
                  if (reg_scenario.FILTER is not null) then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
                    /* Añadimos el campo FILTER */
                    campo_filter := procesa_campo_filter(reg_scenario.FILTER);
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter || ';');
                  end if;
                END IF;
                UTL_FILE.put_line(fich_salida_pkg,'');
                UTL_FILE.put_line(fich_salida_pkg,'    num_filas_insertadas := sql%rowcount;');
                --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
                UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_insertadas;');
                
                UTL_FILE.put_line(fich_salida_pkg,'    exception');
                UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
                UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
                UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros.'');');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
                --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
                UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
                --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
                --UTL_FILE.put_line(fich_salida_pkg,'  END new_reg_' || reg_scenario.TABLE_NAME || ';');
                UTL_FILE.put_line(fich_salida_pkg,'  END nreg_' || nombre_proceso || ';');
                UTL_FILE.put_line(fich_salida_pkg, '');
              end if;
                /** COMIENZO  ESCENARIO EXISTENTE **/
                
              if (reg_scenario.SCENARIO = 'E')
              then
                /* ESCENARIO EXISTENTE */
                dbms_output.put_line ('Estoy en el escenario: E');
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION upt_reg_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ureg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  IS');
                UTL_FILE.put_line(fich_salida_pkg, '  num_filas_upd INTEGER;');
                UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');        
                UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
                UTL_FILE.put_line(fich_salida_pkg, '');
                UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
                UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
                /* parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                UTL_FILE.put_line(fich_salida_pkg,'    (');
                open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                primera_col := 1;
                loop
                  fetch MTDT_TC_DETAIL
                  into reg_detail;
                  exit when MTDT_TC_DETAIL%NOTFOUND;
                  dbms_output.put_line ('Estoy en el Tercer Loop. El campo es: ' || reg_detail.TABLE_COLUMN);
                  if primera_col = 1 then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
                    primera_col := 0;
                  else
                    UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_detail.TABLE_COLUMN);
                  end if;
                end loop;
                close MTDT_TC_DETAIL;
                UTL_FILE.put_line(fich_salida_pkg,'    )');
                dbms_output.put_line ('He pasado la parte del INTO');
                /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                /****/
                UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
                open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                primera_col := 1;
                loop
                  fetch MTDT_TC_DETAIL
                  into reg_detail;
                  exit when MTDT_TC_DETAIL%NOTFOUND;
                  dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
                  columna := genera_campo_select (reg_detail);
                  if primera_col = 1 then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || columna);
                    primera_col := 0;
                  else
                    UTL_FILE.put_line(fich_salida_pkg, '    ,' || columna);
                  end if;        
                end loop;
                close MTDT_TC_DETAIL;
                /****/
                /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                /****/ 
                /****/
                /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
                /****/
                dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
                UTL_FILE.put_line(fich_salida_pkg,'    FROM');
                UTL_FILE.put_line(fich_salida_pkg, '    ' || OWNER_SA || '.' || reg_scenario.TABLE_BASE_NAME || ', ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME);
                dbms_output.put_line ('Interface COLUMNS: ' || reg_scenario.INTERFACE_COLUMNS);
                dbms_output.put_line ('Table COLUMNS: ' || reg_scenario.TABLE_COLUMNS);
                where_interface_columns := split_string_coma (reg_scenario.INTERFACE_COLUMNS);
                where_table_columns := split_string_coma(reg_scenario.TABLE_COLUMNS);
                dbms_output.put_line ('El numero de valores del Where interface es: ' || where_interface_columns.count);
                dbms_output.put_line ('El numero de valores del Where interface es: ' || where_table_columns.count);
        
                IF (where_interface_columns.COUNT > 0  and 
                  where_table_columns.COUNT > 0 and 
                  where_interface_columns.COUNT = where_table_columns.COUNT) 
                THEN
                  /****/
                  /* INICIO generacion parte  WHERE */
                  /****/    
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
                  if (reg_scenario.FILTER is null) then
                    FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
                    LOOP
                      IF indx = where_interface_columns.LAST THEN
                        UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ';');
                      ELSE
                        UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ' and');
                      END IF;
                    END LOOP;
                  else
                    FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
                    LOOP
                      IF indx = where_interface_columns.LAST THEN
                        UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx));
                      ELSE
                        UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' = ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ' and');
                      END IF;
                    END LOOP;
                    /* Añadimos el campo FILTER */
                    UTL_FILE.put_line(fich_salida_pkg, '    and');
                    campo_filter := procesa_campo_filter(reg_scenario.FILTER);
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter || ';');
                  end if;
                ELSE /* Puede que no haya un WHERE POR LAS COLUMNAS DE TABLA E INTERFACE PERO SI HAYA FILTER*/
                  if (reg_scenario.FILTER is not null) then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
                    /* Añadimos el campo FILTER */
                    campo_filter := procesa_campo_filter(reg_scenario.FILTER);
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter || ';');
                  end if;
                END IF;
                UTL_FILE.put_line(fich_salida_pkg,'');
                UTL_FILE.put_line(fich_salida_pkg,'    num_filas_upd := sql%rowcount;');
                --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
                UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_upd;');
                
                UTL_FILE.put_line(fich_salida_pkg,'    exception');
                UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
                UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
                UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al actualizar los registros.'');');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
                --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
                UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
                --UTL_FILE.put_line(fich_salida_pkg,'  END upt_reg_' || reg_scenario.TABLE_NAME || ';');
                UTL_FILE.put_line(fich_salida_pkg,'  END ureg_' || nombre_proceso || ';');
                UTL_FILE.put_line(fich_salida_pkg, '');
              end if;
              /** COMIENZO  ESCENARIO HISTORICO **/
              if (reg_scenario.SCENARIO = 'H')
              then
                /* ESCENARIO HISTORICO */
                dbms_output.put_line ('Estoy en el escenario: H');
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION hst_reg_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION hreg_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  IS');
                UTL_FILE.put_line(fich_salida_pkg, '  num_filas_hst INTEGER:=0;');
                UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');        
                UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
                UTL_FILE.put_line(fich_salida_pkg, '');
                UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
                UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
                /* parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                UTL_FILE.put_line(fich_salida_pkg,'    (');
                open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                primera_col := 1;
                loop
                  fetch MTDT_TC_DETAIL
                  into reg_detail;
                  exit when MTDT_TC_DETAIL%NOTFOUND;
                  dbms_output.put_line ('Estoy en el Tercer Loop. El campo es: ' || reg_detail.TABLE_COLUMN);
                  if primera_col = 1 then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
                    primera_col := 0;
                  else
                    UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_detail.TABLE_COLUMN);
                  end if;
                end loop;
                close MTDT_TC_DETAIL;
                UTL_FILE.put_line(fich_salida_pkg,'    )');
                dbms_output.put_line ('He pasado la parte del INTO');
                /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                /****/
                UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
                open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                primera_col := 1;
                loop
                  fetch MTDT_TC_DETAIL
                  into reg_detail;
                  exit when MTDT_TC_DETAIL%NOTFOUND;
                  dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
                  columna := genera_campo_select (reg_detail);
                  if primera_col = 1 then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || columna);
                    primera_col := 0;
                  else
                    UTL_FILE.put_line(fich_salida_pkg, '    ,' || columna);
                  end if;        
                end loop;
                close MTDT_TC_DETAIL;
                /****/
                /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                /****/ 
                /****/
                /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
                /****/
                dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
                UTL_FILE.put_line(fich_salida_pkg,'    FROM');
                UTL_FILE.put_line(fich_salida_pkg, '    ' || OWNER_SA || '.' || reg_scenario.TABLE_BASE_NAME || ', ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME);
                dbms_output.put_line ('Interface COLUMNS: ' || reg_scenario.INTERFACE_COLUMNS);
                dbms_output.put_line ('Table COLUMNS: ' || reg_scenario.TABLE_COLUMNS);
                where_interface_columns := split_string_coma (reg_scenario.INTERFACE_COLUMNS);
                where_table_columns := split_string_coma(reg_scenario.TABLE_COLUMNS);
                dbms_output.put_line ('El numero de valores del Where interface es: ' || where_interface_columns.count);
                dbms_output.put_line ('El numero de valores del Where interface es: ' || where_table_columns.count);
        
                IF (where_interface_columns.COUNT > 0  and 
                  where_table_columns.COUNT > 0 and 
                  where_interface_columns.COUNT = where_table_columns.COUNT) 
                THEN
                  /****/
                  /* INICIO generacion parte  WHERE */
                  /****/    
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
                  /* Procesamos el campo FILTER . Lo añado a posteriori en la recta final (20141126*/
                  if (reg_scenario.FILTER is null) then
                    FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
                    LOOP
                        UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ' = ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' (+) AND');
                    END LOOP;
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(where_interface_columns.FIRST) || ' IS NULL;' );
                  else
                    FOR indx IN where_interface_columns.FIRST .. where_interface_columns.LAST
                    LOOP
                        UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.TABLE_NAME || '.' || where_table_columns(indx) || ' = ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(indx) || ' (+) AND');
                    END LOOP;
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_scenario.TABLE_BASE_NAME || '.' || where_interface_columns(where_interface_columns.FIRST) || ' IS NULL' );
                    /* Añadimos el campo FILTER */
                    UTL_FILE.put_line(fich_salida_pkg, '    AND');
                    campo_filter := procesa_campo_filter(reg_scenario.FILTER);
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter || ';');
                  end if;
                ELSE /* Puede que no haya un WHERE POR LAS COLUMNAS DE TABLA E INTERFACE PERO SI HAYA FILTER*/
                  if (reg_scenario.FILTER is not null) then
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || 'WHERE ');
                    /* Añadimos el campo FILTER */
                    campo_filter := procesa_campo_filter(reg_scenario.FILTER);
                    UTL_FILE.put_line(fich_salida_pkg, '    ' || campo_filter || ';');
                  end if;
                END IF;
                UTL_FILE.put_line(fich_salida_pkg,'');
                UTL_FILE.put_line(fich_salida_pkg,'    num_filas_hst := sql%rowcount;');

/**************************************************/
                /* (20150114) Angel Ruiz . VIENE LA PARTE RECIENTE PARA PROCESAR SI LA DIMENSION POSEE CARGA MANUAL INICIAL */
                /* Comprobamos que la Dimension no tiene carga inicial manual */
                if (reg_scenario.FILTER_CARGA_INI is not null) then
                  /* Si hay un valor en este campo, es que la dimension posee registros cargados al margen de las cargas por interfaz */
                  /* Con lo que haY que cargarlos en T_* para que no se pierdan */
                  /* al margen de la logica normal de carga de la dimension */
                  UTL_FILE.put_line(fich_salida_pkg, '');
                  UTL_FILE.put_line(fich_salida_pkg,'    INSERT');
                  UTL_FILE.put_line(fich_salida_pkg,'    INTO ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
                  /* parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                  UTL_FILE.put_line(fich_salida_pkg,'    (');
                  open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                  primera_col := 1;
                  loop
                    fetch MTDT_TC_DETAIL
                    into reg_detail;
                    exit when MTDT_TC_DETAIL%NOTFOUND;
                    dbms_output.put_line ('Estoy en el Tercer Loop. El campo es: ' || reg_detail.TABLE_COLUMN);
                    if primera_col = 1 then
                      UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_detail.TABLE_COLUMN);
                      primera_col := 0;
                    else
                      UTL_FILE.put_line(fich_salida_pkg,'    ,' || reg_detail.TABLE_COLUMN);
                    end if;
                  end loop;
                  close MTDT_TC_DETAIL;
                  UTL_FILE.put_line(fich_salida_pkg,'    )');
                  dbms_output.put_line ('He pasado la parte del INTO');
                  /* Fin generacion parte  INTO (CMPO1, CAMPO2, CAMPO3, ...) */
                  /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                  /****/
                  UTL_FILE.put_line(fich_salida_pkg,'    SELECT');
                  open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
                  primera_col := 1;
                  loop
                    fetch MTDT_TC_DETAIL
                    into reg_detail;
                    exit when MTDT_TC_DETAIL%NOTFOUND;
                    dbms_output.put_line ('Antes de la llamada a la funcion con columna: ' || reg_detail.TABLE_COLUMN);
                    columna := genera_campo_select (reg_detail);
                    if primera_col = 1 then
                      UTL_FILE.put_line(fich_salida_pkg, '    ' || columna);
                      primera_col := 0;
                    else
                      UTL_FILE.put_line(fich_salida_pkg, '    ,' || columna);
                    end if;        
                  end loop;
                  close MTDT_TC_DETAIL;
                  /****/
                  /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
                  /****/ 
                  /****/
                  /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
                  /****/
                  dbms_output.put_line ('Antes de pasar a la parte del FROM: ');
                  UTL_FILE.put_line(fich_salida_pkg,'    FROM');
                  UTL_FILE.put_line(fich_salida_pkg,'    ' ||  OWNER_DM || '.' || reg_scenario.TABLE_NAME);
                  UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
                  UTL_FILE.put_line(fich_salida_pkg,'    ' || reg_scenario.FILTER_CARGA_INI || ';');
                  UTL_FILE.put_line(fich_salida_pkg,'');
                  UTL_FILE.put_line(fich_salida_pkg,'    num_filas_hst := num_filas_hst + sql%rowcount;');
                end if;                
/**************************************************/
                
                --UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg,'    num_filas_hst := sql%rowcount;');
                --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
                UTL_FILE.put_line(fich_salida_pkg,'    RETURN num_filas_hst;');
                
                UTL_FILE.put_line(fich_salida_pkg,'    exception');
                UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
                UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
                UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Se ha producido un error al historificar los registros'');');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
                --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
                UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
                --UTL_FILE.put_line(fich_salida_pkg,'  END hreg_' || reg_scenario.TABLE_NAME || ';');
                UTL_FILE.put_line(fich_salida_pkg,'  END hreg_' || nombre_proceso || ';');
                UTL_FILE.put_line(fich_salida_pkg, '');
              end if;
        
           end loop;
           close MTDT_SCENARIO;
          
           --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_ne_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
           UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lne_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
           UTL_FILE.put_line(fich_salida_pkg, '  IS');
           UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_new integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_updt integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_hist integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
           UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
           UTL_FILE.put_line(fich_salida_pkg, '');
           UTL_FILE.put_line(fich_salida_pkg, '    /* INICIAMOS EL BUCLE POR CADA UNA DE LAS INSERCIONES EN LA TABLA DE STAGING */');
           UTL_FILE.put_line(fich_salida_pkg, '    /* EN EL CASO DE LAS DIMENSIONES SOLO DEBE HABER UN REGISTRO YA QUE NO HAY RETRASADOS */');
           UTL_FILE.put_line(fich_salida_pkg, '    dbms_output.put_line (''Inicio del proceso de carga: ''' || ' || ''' || 'load_ne_' || reg_tabla.TABLE_NAME || ''' || ''.'');');
           UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar :=' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_ne_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
           UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
           UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
           UTL_FILE.put_line(fich_salida_pkg, '    end if;');
           UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
           UTL_FILE.put_line(fich_salida_pkg, '');
           UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
           --UTL_FILE.put_line(fich_salida_pkg, '      SET TRANSACTION NAME ''TRAN_' || reg_tabla.TABLE_NAME || ''';');
           UTL_FILE.put_line(fich_salida_pkg, '');
           UTL_FILE.put_line(fich_salida_pkg, '      /* Truncamos la tabla antes de insertar los nuevos registros por si se lanza dos veces*/');
           UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_T || '.T_' || nombre_tabla_reducido || ''';');    
           
           /* Generamos las llamadas a los procedimientos para realizar las cargas */
           /* Generamos la llamada para cargar los registros NUEVOS */
            FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
            LOOP
              if lista_scenarios_presentes (indx) = 'N'
              then
                --UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_new := new_reg_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
                UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_new := nreg_' || nombre_proceso || ' (fch_carga_in, fch_datos_in);');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros insertados es: '' || numero_reg_new || ''.'');');
              end if;
            END LOOP;
           /* Generamos la llamada para cargar los registros EXISTENTES */
            FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
            LOOP
              if lista_scenarios_presentes (indx) = 'E'
              then
                --UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_updt := upt_reg_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
                UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_updt := ureg_' || nombre_proceso || ' (fch_carga_in, fch_datos_in);');
                UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''El numero de registros actualizados es: '' || numero_reg_updt || ''.'');');
              end if;
            END LOOP;
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ne_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_new, numero_reg_updt);');
            UTL_FILE.put_line(fich_salida_pkg, '      COMMIT;');
            UTL_FILE.put_line(fich_salida_pkg, '    end if;');
            
        /***********/
            UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg,'    RETURN 0;');
            
            UTL_FILE.put_line(fich_salida_pkg,'    exception');
            --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
            --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
            UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
            UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
            UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
            UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
            UTL_FILE.put_line(fich_salida_pkg, '     ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ne_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            UTL_FILE.put_line(fich_salida_pkg,'      commit;        /* commit de la insercion del fin fallido*/');
            UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
            UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '  END load_ne_' || reg_tabla.TABLE_NAME || ';');
            UTL_FILE.put_line(fich_salida_pkg, '  END lne_' || nombre_proceso || ';');
            UTL_FILE.put_line(fich_salida_pkg, '');
        /***********/
        
           --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_dh_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
           UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE ldh_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
           UTL_FILE.put_line(fich_salida_pkg, '  IS');
           UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_hist integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
           UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
           UTL_FILE.put_line(fich_salida_pkg, '');
           UTL_FILE.put_line(fich_salida_pkg, '    /* INICIAMOS EL BUCLE POR CADA UNA DE LAS INSERCIONES EN LA TABLA DE STAGING */');
           UTL_FILE.put_line(fich_salida_pkg, '    /* EN EL CASO DE LAS DIMENSIONES SOLO DEBE HABER UN REGISTRO YA QUE NO HAY RETRASADOS */');
           UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_dh_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
           UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
           UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
           UTL_FILE.put_line(fich_salida_pkg, '    end if;');
           UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
           UTL_FILE.put_line(fich_salida_pkg, '');
           UTL_FILE.put_line(fich_salida_pkg, '      dbms_output.put_line (''Inicio del proceso de carga: ''' || ' || ''' || 'load_dh_' || reg_tabla.TABLE_NAME || ''' || ''.'');');
           UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
           --UTL_FILE.put_line(fich_salida_pkg, '      SET TRANSACTION NAME ''TRAN_' || reg_tabla.TABLE_NAME || ''';');
           UTL_FILE.put_line(fich_salida_pkg, '');
        
           /* Generamos la llamada para cargar los registros HISTORICO */
            FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
            LOOP
              if lista_scenarios_presentes (indx) = 'H'
              then
                --UTL_FILE.put_line(fich_salida_pkg,'    numero_reg_hist := hst_reg_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
                UTL_FILE.put_line(fich_salida_pkg,'    numero_reg_hist := hreg_' || nombre_proceso || ' (fch_carga_in, fch_datos_in);');
                UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''El numero de registros historificados es: '' || numero_reg_hist || ''.'');');
              end if;
            END LOOP;
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_dh_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_hist);');
            
            UTL_FILE.put_line(fich_salida_pkg, '      COMMIT;');
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
            UTL_FILE.put_line(fich_salida_pkg, '     ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_dh_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            UTL_FILE.put_line(fich_salida_pkg,'      commit;');
            UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
            UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '  END load_dh_' || reg_tabla.TABLE_NAME || ';');
            UTL_FILE.put_line(fich_salida_pkg, '  END ldh_' || nombre_proceso || ';');
            UTL_FILE.put_line(fich_salida_pkg, '');
        /***********/
            --UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_ex_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
            UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE lex_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
            UTL_FILE.put_line(fich_salida_pkg, '  IS');
            UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer := 0;');
            UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
            UTL_FILE.put_line(fich_salida_pkg, '  num_reg INTEGER;');
            UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '    dbms_output.put_line (''Inicio del proceso de carga: ''' || ' || ''' || 'load_ex_' || reg_tabla.TABLE_NAME || ''' || ''.'');');
            UTL_FILE.put_line(fich_salida_pkg, '    /* Lo primero que se hace es mirar que paso es el primero a ejecutar */');
            UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
            UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
            UTL_FILE.put_line(fich_salida_pkg, '    end if;');
            
            UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos "ex"  tienen cuatro pasos */');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Comienza en el primer paso */');
            UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg,'      SELECT COUNT(*) INTO num_reg FROM ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ';');
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''RENAME ' || reg_tabla.TABLE_NAME || ' TO ' || nombre_tabla_reducido || '_OLD''' || ';');    
            UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''';');    
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), 0, 0, num_reg);');
            UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el segundo paso */');
            UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');    
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''RENAME T_' || nombre_tabla_reducido || ' TO ' || reg_tabla.TABLE_NAME || ''';');    
            UTL_FILE.put_line(fich_salida_pkg, '      INSERT /*+ APPEND */ INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' SELECT * FROM ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ';');
            UTL_FILE.put_line(fich_salida_pkg, '      num_reg := sql%rowcount;');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), num_reg);');
            UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');    
            --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el tercer paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');        
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''RENAME ' || nombre_tabla_reducido || '_OLD TO T_' || nombre_tabla_reducido || ''';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '3, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el cuarto paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');        
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE app_mvnodm.T_' || nombre_tabla_reducido || ' DROP STORAGE'';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '4, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
           -- UTL_FILE.put_line(fich_salida_pkg, '      commit;');
           -- UTL_FILE.put_line(fich_salida_pkg, '      dbms_output.put_line (''EL PROCESO HA ACABADO OK'');');
            UTL_FILE.put_line(fich_salida_pkg, '    end if; ');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 2) then');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos "ex"  tienen cuatro pasos */');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Comienza en el segundo paso */');
            UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''RENAME T_' || nombre_tabla_reducido || ' TO ' || reg_tabla.TABLE_NAME || ''';');    
            UTL_FILE.put_line(fich_salida_pkg, '      INSERT /*+ APPEND */ INTO ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ' SELECT * FROM ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ';');
            UTL_FILE.put_line(fich_salida_pkg, '      num_reg := sql%rowcount;');            
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), num_reg);');
            UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');    
            
            --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el tercer paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');        
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''RENAME ' || nombre_tabla_reducido || '_OLD TO T_' || nombre_tabla_reducido || ''';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '3, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el cuarto paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');        
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE app_mvnodm.T_' || nombre_tabla_reducido || ' DROP STORAGE'';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '4, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            --UTL_FILE.put_line(fich_salida_pkg, '      dbms_output.put_line (''EL PROCESO HA ACABADO OK'');');
            UTL_FILE.put_line(fich_salida_pkg, '    end if; ');
            UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 3) then');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos "ex"  tienen cuatro pasos */');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* Comienza en el tercer paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''RENAME ' || nombre_tabla_reducido || '_OLD TO T_' || nombre_tabla_reducido || ''';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '3, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el cuarto paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := siguiente_paso_a_ejecutar+1;');        
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE app_mvnodm.T_' || nombre_tabla_reducido || ' DROP STORAGE'';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '4, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            --UTL_FILE.put_line(fich_salida_pkg, '      dbms_output.put_line (''EL PROCESO HA ACABADO OK'');');
            --UTL_FILE.put_line(fich_salida_pkg, '    end if; ');
            --UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 4) then');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos "ex"  tienen cuatro pasos */');
            --UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
            --UTL_FILE.put_line(fich_salida_pkg, '      /* comienza el cuarto paso */');
            --UTL_FILE.put_line(fich_salida_pkg, '      EXECUTE IMMEDIATE ''TRUNCATE TABLE app_mvnodm.T_' || nombre_tabla_reducido || ' DROP STORAGE'';');    
            --UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, '      app_mvnomt.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '4, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            --UTL_FILE.put_line(fich_salida_pkg, '      commit;');
            --UTL_FILE.put_line(fich_salida_pkg, '      dbms_output.put_line (''EL PROCESO HA ACABADO OK'');');
            --UTL_FILE.put_line(fich_salida_pkg, '    end if; ');
            --UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg,'    exception');
            UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
            UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
            UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
            UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'', ' || 'siguiente_paso_a_ejecutar, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
            UTL_FILE.put_line(fich_salida_pkg, '');
            
            --UTL_FILE.put_line(fich_salida_pkg,'  END load_ex_' || reg_tabla.TABLE_NAME || ';');
            UTL_FILE.put_line(fich_salida_pkg,'  END lex_' || nombre_proceso || ';');
            UTL_FILE.put_line(fich_salida_pkg, '');
            /****/
            /* FIN de la generacion de la funcion load */
            /****/
            
            --UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || reg_tabla.TABLE_NAME || ';' );
            UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || nombre_proceso || ';' );
            UTL_FILE.put_line(fich_salida_pkg, '/' );
            /******/
            /* FIN DE LA GENERACION DEL PACKAGE */
            /******/    
            UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg, 'grant execute on app_mvnodm.pkg_' || reg_tabla.TABLE_NAME || ' to app_mvnotc;');
            UTL_FILE.put_line(fich_salida_pkg, 'grant execute on ' || OWNER_DM || '.pkg_' || nombre_proceso || ' to ' || OWNER_TC || ';');
            UTL_FILE.put_line(fich_salida_pkg, '/');
            UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');
        
          
            /******/
            /* FIN DEL PACKGE BODY */
            /******/    
        /****************************************************/
        /****************************************************/
        /****************************************************/
        /****************************************************/
        /****************************************************/
            /******/
            /* INICIO DE LA GENERACION DEL sh de NUEVOS Y EXISTENTES */
            /******/
            
        /***********************/
            UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
            UTL_FILE.put_line(fich_salida_load, '#############################################################################');
            UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
            UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_load, '# Archivo    :       load_ne_' ||  reg_tabla.TABLE_NAME || '.sh                            #');
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
            UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ne_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
            UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_ne_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
            --UTL_FILE.put_line(fich_salida_load, 'echo "load_ne_' || reg_tabla.TABLE_NAME || '" > ${MVNO_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FCH_CARGA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
            UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
            UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_load, 'fi');
            UTL_FILE.put_line(fich_salida_load, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            --UTL_FILE.put_line(fich_salida_sh, 'set -x');
            UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
            UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
            UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
            UTL_FILE.put_line(fich_salida_load, '################################################################################');
            UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
            UTL_FILE.put_line(fich_salida_load, '################################################################################');
            UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="' || v_REQ_NUMER || '"');
            --UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="Req96817"');
            UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || v_REQ_NUMER || '_load_ne_' || reg_tabla.TABLE_NAME);
            --UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=Req96817_load_ne_' || reg_tabla.TABLE_NAME);
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
        
        /***********************/
            UTL_FILE.put_line(fich_salida_load, '# Llamada a sql_plus');
            UTL_FILE.put_line(fich_salida_load, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
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
            --UTL_FILE.put_line(fich_salida_load, '  APP_MVNODM.pkg_' || reg_tabla.TABLE_NAME || '.' || 'load_ne_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'',''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
            UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'lne_' || nombre_proceso || '(''${FCH_CARGA}'',''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
            UTL_FILE.put_line(fich_salida_load, 'end;');
            UTL_FILE.put_line(fich_salida_load, '/');
            UTL_FILE.put_line(fich_salida_load, 'exit 0;');
            UTL_FILE.put_line(fich_salida_load, 'EOF');
        
            UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
            UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
            UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_ne_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
            UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
            UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ne' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
            UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ne' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            --UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
            UTL_FILE.put_line(fich_salida_load, '  exit 1');
            UTL_FILE.put_line(fich_salida_load, 'fi');
            UTL_FILE.put_line(fich_salida_load, '');
            UTL_FILE.put_line(fich_salida_load, 'echo "El proceso load_' ||  'ne_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ne_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            UTL_FILE.put_line(fich_salida_load, 'exit 0');
            /******/
            /* FIN DE LA GENERACION DEL sh de NUEVOS Y EXISTENTES */
            /******/
            /******/
            /* INICIO DE LA GENERACION DEL sh de HISTORICOS */
            /******/
            
            /**************/
            UTL_FILE.put_line(fich_salida_hist, '#!/bin/bash');
            UTL_FILE.put_line(fich_salida_hist, '#############################################################################');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Telefonica Moviles Mexico SA DE CV                                        #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Archivo    :       load_dh_' ||  reg_tabla.TABLE_NAME || '.sh                            #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Autor      : <SYNAPSYS>.                                                  #');
            UTL_FILE.put_line(fich_salida_hist, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || 'S.        #');
            UTL_FILE.put_line(fich_salida_hist, '# Parametros :                                                              #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Ejecucion  :                                                              #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Historia : 31-Octubre-2014 -> Creacion                                    #');
            UTL_FILE.put_line(fich_salida_hist, '# Caja de Control - M :                                                     #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
            UTL_FILE.put_line(fich_salida_hist, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Caducidad del Requerimiento :                                             #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Dependencias :                                                            #');
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Usuario:                                                                  #');   
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '# Telefono:                                                                 #');   
            UTL_FILE.put_line(fich_salida_hist, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_hist, '#############################################################################');
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '#Obtiene los password de base de datos                                         #');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, 'InsertaFinFallido()');
            UTL_FILE.put_line(fich_salida_hist, '{');
            UTL_FILE.put_line(fich_salida_hist, '   #Se especifican parametros usuario y la BD');
            UTL_FILE.put_line(fich_salida_hist, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_dh_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
            UTL_FILE.put_line(fich_salida_hist, '   if [ $? -ne 0 ]');
            UTL_FILE.put_line(fich_salida_hist, '   then');
            UTL_FILE.put_line(fich_salida_hist, '      SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
            UTL_FILE.put_line(fich_salida_hist, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
            UTL_FILE.put_line(fich_salida_hist, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
            UTL_FILE.put_line(fich_salida_hist, '      exit 1;');
            UTL_FILE.put_line(fich_salida_hist, '   fi');
            UTL_FILE.put_line(fich_salida_hist, '   return 0');
            UTL_FILE.put_line(fich_salida_hist, '}');
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, 'InsertaFinOK()');
            UTL_FILE.put_line(fich_salida_hist, '{');
            UTL_FILE.put_line(fich_salida_hist, '   #Se especifican parametros usuario y la BD');
            UTL_FILE.put_line(fich_salida_hist, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_dh_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
            UTL_FILE.put_line(fich_salida_hist, '   if [ $? -ne 0 ]');
            UTL_FILE.put_line(fich_salida_hist, '   then');
            UTL_FILE.put_line(fich_salida_hist, '      SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
            UTL_FILE.put_line(fich_salida_hist, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
            UTL_FILE.put_line(fich_salida_hist, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
            UTL_FILE.put_line(fich_salida_hist, '      exit 1;');
            UTL_FILE.put_line(fich_salida_hist, '   fi');
            UTL_FILE.put_line(fich_salida_hist, '   return 0');
            UTL_FILE.put_line(fich_salida_hist, '}');
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
            UTL_FILE.put_line(fich_salida_hist, '# Comprobamos si el numero de parametros es el correcto');
            UTL_FILE.put_line(fich_salida_hist, 'if [ $# -ne 3 ] ; then');
            UTL_FILE.put_line(fich_salida_hist, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
            UTL_FILE.put_line(fich_salida_hist, '  echo ${SUBJECT}');        
            UTL_FILE.put_line(fich_salida_hist, '  exit 1');
            UTL_FILE.put_line(fich_salida_hist, 'fi');
            UTL_FILE.put_line(fich_salida_hist, '# Recogida de parametros');
            UTL_FILE.put_line(fich_salida_hist, 'FCH_CARGA=${1}');
            UTL_FILE.put_line(fich_salida_hist, 'FCH_DATOS=${2}');
            UTL_FILE.put_line(fich_salida_hist, 'BAN_FORZADO=${3}');
            UTL_FILE.put_line(fich_salida_hist, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
            --UTL_FILE.put_line(fich_salida_hist, 'echo "load_dh_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FCH_CARGA}' || '.log ');
            UTL_FILE.put_line(fich_salida_hist, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
            UTL_FILE.put_line(fich_salida_hist, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
            UTL_FILE.put_line(fich_salida_hist, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_hist, 'fi');
            UTL_FILE.put_line(fich_salida_hist, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_hist, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_hist, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_hist, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_hist, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_hist, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            --UTL_FILE.put_line(fich_salida_sh, 'set -x');
            UTL_FILE.put_line(fich_salida_hist, '#Permite los acentos y U');
            UTL_FILE.put_line(fich_salida_hist, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
            UTL_FILE.put_line(fich_salida_hist, 'export NLS_LANG');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, 'REQ_NUM="' || v_REQ_NUMER || '"');
            --UTL_FILE.put_line(fich_salida_hist, 'REQ_NUM="Req96817"');
            UTL_FILE.put_line(fich_salida_hist, 'INTERFAZ=' || v_REQ_NUMER || '_load_dh_' || reg_tabla.TABLE_NAME);
            --UTL_FILE.put_line(fich_salida_hist, 'INTERFAZ=Req96817_load_dh_' || reg_tabla.TABLE_NAME);
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '# LIBRERIAS                                                                    #');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
            UTL_FILE.put_line(fich_salida_hist, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
            UTL_FILE.put_line(fich_salida_hist, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
            UTL_FILE.put_line(fich_salida_hist, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, '# Cuentas  Produccion / Desarrollo                                             #');
            UTL_FILE.put_line(fich_salida_hist, '################################################################################');
            UTL_FILE.put_line(fich_salida_hist, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
            UTL_FILE.put_line(fich_salida_hist, '  ### Cuentas para mantenimiento');
            UTL_FILE.put_line(fich_salida_hist, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
            UTL_FILE.put_line(fich_salida_hist, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
            UTL_FILE.put_line(fich_salida_hist, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
            UTL_FILE.put_line(fich_salida_hist, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
            UTL_FILE.put_line(fich_salida_hist, 'else');
            UTL_FILE.put_line(fich_salida_hist, '  ### Cuentas para mantenimiento');
            UTL_FILE.put_line(fich_salida_hist, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
            UTL_FILE.put_line(fich_salida_hist, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
            UTL_FILE.put_line(fich_salida_hist, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
            UTL_FILE.put_line(fich_salida_hist, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
            UTL_FILE.put_line(fich_salida_hist, 'fi');
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
            UTL_FILE.put_line(fich_salida_hist, 'BD_CLAVE=${PASSWORD}');
            
            /**************/
            UTL_FILE.put_line(fich_salida_hist, '# Llamada a sql_plus');
            UTL_FILE.put_line(fich_salida_hist, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
            UTL_FILE.put_line(fich_salida_hist, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
            UTL_FILE.put_line(fich_salida_hist, 'whenever sqlerror exit 1;');
            UTL_FILE.put_line(fich_salida_hist, 'whenever oserror exit 2;');
            UTL_FILE.put_line(fich_salida_hist, 'set feedback off;');
            UTL_FILE.put_line(fich_salida_hist, 'set serveroutput on;');
            UTL_FILE.put_line(fich_salida_hist, 'set echo on;');
            UTL_FILE.put_line(fich_salida_hist, 'set pagesize 0;');
            UTL_FILE.put_line(fich_salida_hist, 'set verify off;');
            UTL_FILE.put_line(fich_salida_hist, '');
            --UTL_FILE.put_line(fich_salida_hist, 'declare');
            --UTL_FILE.put_line(fich_salida_hist, '  num_filas_insertadas number;');
            UTL_FILE.put_line(fich_salida_hist, 'begin');
            --UTL_FILE.put_line(fich_salida_hist, '  APP_MVNODM.pkg_' || reg_tabla.TABLE_NAME || '.' || 'load_dh_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
            UTL_FILE.put_line(fich_salida_hist, '  ' || OWNER_DM || '.pkg_' || nombre_proceso || '.' || 'ldh_' || nombre_proceso || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
            UTL_FILE.put_line(fich_salida_hist, 'end;');
            UTL_FILE.put_line(fich_salida_hist, '/');
            UTL_FILE.put_line(fich_salida_hist, 'EOF');
            UTL_FILE.put_line(fich_salida_hist, 'err_salida=$?');
            UTL_FILE.put_line(fich_salida_hist, 'if [ ${err_salida} -ne 0 ]; then');
            UTL_FILE.put_line(fich_salida_hist, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_dh_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
            UTL_FILE.put_line(fich_salida_hist, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
            UTL_FILE.put_line(fich_salida_hist, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_dh' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
            UTL_FILE.put_line(fich_salida_hist, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_dh' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            --UTL_FILE.put_line(fich_salida_hist, '  InsertaFinFallido');
            UTL_FILE.put_line(fich_salida_hist, '  exit 1');
            UTL_FILE.put_line(fich_salida_hist, 'fi');
            UTL_FILE.put_line(fich_salida_hist, '');
            UTL_FILE.put_line(fich_salida_hist, 'echo "El proceso  load_' ||  'dh_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_dh_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            UTL_FILE.put_line(fich_salida_hist, 'exit 0');
        
            /******/
            /* FIN DE LA GENERACION DEL sh de HISTORICOS */
            /******/
        
            /******/
            /* INICIO DE LA GENERACION DEL sh de EXCHANGE */
            /******/
            
            /*************/
            UTL_FILE.put_line(fich_salida_exchange, '#!/bin/bash');
            UTL_FILE.put_line(fich_salida_exchange, '#############################################################################');
            UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_exchange, '# Telefonica Moviles Mexico SA DE CV                                        #');
            UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_exchange, '# Archivo    :       load_ex_' ||  reg_tabla.TABLE_NAME || '.sh                            #');
            UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_exchange, '# Autor      : <SYNAPSYS>.                                                  #');
            UTL_FILE.put_line(fich_salida_exchange, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || 'S.        #');
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
            UTL_FILE.put_line(fich_salida_exchange, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
            --UTL_FILE.put_line(fich_salida_exchange, 'echo "load_ex_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FCH_CARGA}' || '.log ');
            UTL_FILE.put_line(fich_salida_exchange, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
            UTL_FILE.put_line(fich_salida_exchange, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
            UTL_FILE.put_line(fich_salida_exchange, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_exchange, 'fi');
            UTL_FILE.put_line(fich_salida_exchange, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
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
            --UTL_FILE.put_line(fich_salida_exchange, 'REQ_NUM="Req96817"');
            UTL_FILE.put_line(fich_salida_exchange, 'INTERFAZ=' || v_REQ_NUMER || '_load_ex_' || reg_tabla.TABLE_NAME);
            --UTL_FILE.put_line(fich_salida_exchange, 'INTERFAZ=Req96817_load_ex_' || reg_tabla.TABLE_NAME);
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
            
            /*************/
            UTL_FILE.put_line(fich_salida_exchange, '# Llamada a sql_plus');
            UTL_FILE.put_line(fich_salida_exchange, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
            UTL_FILE.put_line(fich_salida_exchange, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
            UTL_FILE.put_line(fich_salida_exchange, 'whenever sqlerror exit 1');
            UTL_FILE.put_line(fich_salida_exchange, 'whenever oserror exit 2');
            UTL_FILE.put_line(fich_salida_exchange, 'set feedback off');
            UTL_FILE.put_line(fich_salida_exchange, 'set serveroutput on');
            UTL_FILE.put_line(fich_salida_exchange, 'set pagesize 0;');
            UTL_FILE.put_line(fich_salida_exchange, 'set verify off;');
            UTL_FILE.put_line(fich_salida_exchange, '');
            --UTL_FILE.put_line(fich_salida_exchange, 'declare');
            --UTL_FILE.put_line(fich_salida_exchange, '  num_filas_insertadas number;');
            UTL_FILE.put_line(fich_salida_exchange, 'begin');
            --UTL_FILE.put_line(fich_salida_exchange, '  APP_MVNODM.pkg_' || reg_tabla.TABLE_NAME || '.' || 'load_ex_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
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
            --UTL_FILE.put_line(fich_salida_exchange, '  InsertaFinFallido');
            UTL_FILE.put_line(fich_salida_exchange, '  exit 1');
            UTL_FILE.put_line(fich_salida_exchange, 'fi');
            UTL_FILE.put_line(fich_salida_exchange, '');
            UTL_FILE.put_line(fich_salida_exchange, 'echo "El proceso de exchange load_' ||  'ex_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            UTL_FILE.put_line(fich_salida_exchange, '');
            UTL_FILE.put_line(fich_salida_exchange, 'exit 0');
            /******/
            /* FIN DE LA GENERACION DEL sh de EXCHANGE */
            /******/
        
            
            UTL_FILE.FCLOSE (fich_salida_load);
            UTL_FILE.FCLOSE (fich_salida_pkg);
            UTL_FILE.FCLOSE (fich_salida_hist);
            UTL_FILE.FCLOSE (fich_salida_exchange);
    end if;
    if (reg_tabla.TABLE_TYPE = 'I')
    then
            dbms_output.put_line ('Estoy en el primer LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);
            nombre_fich_carga := 'load_' || reg_tabla.TABLE_NAME || '.sh';
            nombre_fich_pkg := 'pkg_' || reg_tabla.TABLE_NAME || '.sql';
            fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
            fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
            nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 4); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
        
            UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_SA || '.pkg_' || reg_tabla.TABLE_NAME || ' AS');
            UTL_FILE.put_line(fich_salida_pkg,'');

            --lista_scenarios_presentes.delete;
            /******/
            /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
            /******/
            
            /* (20150720) Angel Ruiz. NF: Historico para tablas de Integracion */
            select nvl(HISTORY, 'NULO') into v_history FROM MTDT_INTERFACE_SUMMARY
            where
            TRIM(CONCEPT_NAME) = substr(reg_tabla.TABLE_NAME, 4) and
            SOURCE = 'SA';
            
            if (v_history <> 'NULO') then
              v_concept_name := substr(reg_tabla.TABLE_NAME, 4);
              if (length(v_concept_name) < 24) then
                nombre_proceso := 'SA_' || v_concept_name;
              else
                nombre_proceso := v_concept_name;
              end if;              
              /* Ocurre que hemos de llevar un historico de esta tabla de INTEGRACION */
              UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pre_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'');');
              UTL_FILE.put_line(fich_salida_pkg,'');
            end if;
            /* (20150720) Angel Ruiz.FIN */
            /* Tercero genero los metodos para los escenarios */
            
            open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
            loop
              fetch MTDT_SCENARIO
              into reg_scenario;
              exit when MTDT_SCENARIO%NOTFOUND;
              dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
              nombre_tabla_base_redu := SUBSTR(reg_scenario.TABLE_BASE_NAME, 4);
              nombre_tabla_base_sp_redu:=substr(nombre_tabla_base_redu, 1, 3);
        
              /* Elaboramos los prototipos de la funciones que cargaran los distintos escenarios */
              UTL_FILE.put_line(fich_salida_pkg,'');
              if (reg_scenario.SCENARIO = 'P')
              then
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION i_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu  || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
              end if;
            end loop;   /* Fin del loop MTDT_SCENARIO */
            close MTDT_SCENARIO;
            
            UTL_FILE.put_line(fich_salida_pkg,'');
            UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in in VARCHAR2);');
            UTL_FILE.put_line(fich_salida_pkg, '' ); 
            UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || reg_tabla.TABLE_NAME || ';' );
            UTL_FILE.put_line(fich_salida_pkg, '/' );
            
            /******/
            /* FIN DEL PACKAGE DEFINITION */
            /******/
            /******/
            /* INICIO DEL PACKGE BODY */
            /******/
            UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY ' || OWNER_SA || '.pkg_' || reg_tabla.TABLE_NAME || ' AS');
            UTL_FILE.put_line(fich_salida_pkg,'');
            UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_tabla (table_name_in IN VARCHAR2) return number');
            UTL_FILE.put_line(fich_salida_pkg,'  IS');
            UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
            UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_tabla varchar(30);BEGIN select table_name into nombre_tabla from all_tables where table_name = '''''' || table_name_in || ''''''; END;'';');
            UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
            UTL_FILE.put_line(fich_salida_pkg,'  exception');
            UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
            UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
            UTL_FILE.put_line(fich_salida_pkg,'  END existe_tabla;');
            UTL_FILE.put_line(fich_salida_pkg,'');
            UTL_FILE.put_line(fich_salida_pkg,'  FUNCTION existe_particion (partition_name_in IN VARCHAR2, table_name_in IN VARCHAR2) return number');
            UTL_FILE.put_line(fich_salida_pkg,'  IS');
            UTL_FILE.put_line(fich_salida_pkg,'  BEGIN');
            UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''DECLARE nombre_particion varchar(30);BEGIN select partition_name into nombre_particion from all_tab_partitions where partition_name = '''''' || partition_name_in || '''''' and table_name = '''''' || table_name_in || ''''''; END;'';');
            UTL_FILE.put_line(fich_salida_pkg,'    return 1;');
            UTL_FILE.put_line(fich_salida_pkg,'  exception');
            UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
            UTL_FILE.put_line(fich_salida_pkg,'    return 0;');
            UTL_FILE.put_line(fich_salida_pkg,'  END existe_particion;');
            UTL_FILE.put_line(fich_salida_pkg,'');
            /* (20150720) Angel Ruiz. NF: Historico para tablas de Integracion */
            if (v_history <> 'NULO') then
              /* La tabla de integracion debe tener una tabla de histórico */
              if (length(v_concept_name) <= 18) then
                v_nombre_particion := 'SA_' || v_concept_name;
              else
                v_nombre_particion := v_concept_name;
              end if;
              if (regexp_count(v_history, '^[0-9][Mm]',1,'i') > 0) then
                /* Obtenemos el numero de meses que se deben de guardar en el historico */
                v_num_meses:= substr(v_history,1,1);
              else
                /* No sigue la especificacion requerida el campo donde se guarda el tiempo de historico */
                /* Por defecto ponemos 2 meses */
                v_num_meses := 2;
              end if;
              /* Ocurre que hemos de llevar un historico de esta tabla de INTEGRACION */
              UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE pre_' || nombre_proceso || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2 := ''N'')');
              UTL_FILE.put_line(fich_salida_pkg, '  IS' ); 
              UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
              UTL_FILE.put_line(fich_salida_pkg,'   exis_partition number(1);');
              UTL_FILE.put_line(fich_salida_pkg,'   fch_particion varchar2(8);');
              UTL_FILE.put_line(fich_salida_pkg, '  BEGIN' );
              UTL_FILE.put_line(fich_salida_pkg,'' );
              UTL_FILE.put_line(fich_salida_pkg,'  /* Primero borramos la particion que se ha quedado obsoleta */');
              UTL_FILE.put_line(fich_salida_pkg,'  fch_particion := TO_CHAR(ADD_MONTHS(TO_DATE(fch_carga_in,''YYYYMMDD''), -' || v_num_meses || '), ''YYYYMMDD'');');
              UTL_FILE.put_line(fich_salida_pkg,'  FOR nombre_particion_rec IN (');
              UTL_FILE.put_line(fich_salida_pkg,'    select partition_name' );
              UTL_FILE.put_line(fich_salida_pkg,'    from user_tab_partitions' );
              UTL_FILE.put_line(fich_salida_pkg,'    where table_name = ''SAH_' || v_concept_name || '''');
              UTL_FILE.put_line(fich_salida_pkg,'    and partition_name < ''' || v_nombre_particion || ''' || ''_'' || fch_particion )');
              UTL_FILE.put_line(fich_salida_pkg,'  LOOP' );
              --UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''' || v_nombre_particion || ''' || ''_''' || ' || fch_particion, ''SAH_'' || ''' || v_concept_name || ''');');
              UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (nombre_particion_rec.partition_name, ' || '''SAH_'' || ''' || v_concept_name || ''');');
              UTL_FILE.put_line(fich_salida_pkg,'    if (exis_partition = 1) then' );
              --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || v_concept_name || ''' || '' DROP PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_particion' || ';');
              UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || v_concept_name || ''' || '' DROP PARTITION '' || nombre_particion_rec.partition_name'  || ';');
              UTL_FILE.put_line(fich_salida_pkg,'    end if;' );
              UTL_FILE.put_line(fich_salida_pkg,'  END LOOP;' );
              UTL_FILE.put_line(fich_salida_pkg,'' );
              UTL_FILE.put_line(fich_salida_pkg,'  /* Segundo comrpobamos si hay que crear o truncar la particion sobre la que vamos a salvaguardar la informacion */');
              UTL_FILE.put_line(fich_salida_pkg,'    fch_particion := TO_CHAR(TO_DATE(fch_carga_in,''YYYYMMDD'')+1, ''YYYYMMDD'');'); 
              UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''' || v_nombre_particion || ''' || ''_''' || ' || fch_carga_in, ''SAH_'' || ''' || v_concept_name || ''');');
              UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
              UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SAH_'' || ''' || v_concept_name || ''' || '' TRUNCATE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_carga_in;');
              UTL_FILE.put_line(fich_salida_pkg,'  else' );
              --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''CREATE TABLE ' || 'app_mvnosa.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in  || '' AS SELECT * FROM SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
              UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || v_concept_name || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_carga_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE ' || TABLESPACE_SA || ''';');
              UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
              UTL_FILE.put_line(fich_salida_pkg,'  /* TERCERO LLEVO A CABO LA SALVAGUARDA DE LA INFORMACION */' );
              UTL_FILE.put_line(fich_salida_pkg,'  INSERT /*+ APPEND */ INTO ' || OWNER_SA || '.SAH_' || v_concept_name);
              UTL_FILE.put_line(fich_salida_pkg,'  (');
              OPEN dtd_interfaz_detail (v_concept_name, 'SA');
              primera_col := 1;
              LOOP
                FETCH dtd_interfaz_detail
                INTO reg_interface_detail;
                EXIT WHEN dtd_interfaz_detail%NOTFOUND;
                IF primera_col = 1 THEN /* Si es primera columna */
                  UTL_FILE.put_line(fich_salida_pkg,'  ' || reg_interface_detail.COLUMNA);
                  primera_col := 0;
                ELSE
                  UTL_FILE.put_line(fich_salida_pkg,'  ,' || reg_interface_detail.COLUMNA);
                END IF;
              END LOOP;
              CLOSE dtd_interfaz_detail;
              UTL_FILE.put_line(fich_salida_pkg,'  ,CVE_DIA');
              UTL_FILE.put_line(fich_salida_pkg,'  )');
              UTL_FILE.put_line(fich_salida_pkg,'  SELECT');
              OPEN dtd_interfaz_detail (v_concept_name, 'SA');
              primera_col := 1;
              LOOP
                FETCH dtd_interfaz_detail
                INTO reg_interface_detail;
                EXIT WHEN dtd_interfaz_detail%NOTFOUND;
                IF primera_col = 1 THEN /* Si es primera columna */
                  UTL_FILE.put_line(fich_salida_pkg,'  ' || reg_interface_detail.COLUMNA);
                  primera_col := 0;
                ELSE
                  UTL_FILE.put_line(fich_salida_pkg,'  ,' || reg_interface_detail.COLUMNA);
                END IF;
              END LOOP;
              CLOSE dtd_interfaz_detail;
              UTL_FILE.put_line(fich_salida_pkg, '  ,TO_NUMBER(fch_carga_in)');
              UTL_FILE.put_line(fich_salida_pkg, '  FROM ' || OWNER_SA || '.' || reg_tabla.TABLE_NAME);
              UTL_FILE.put_line(fich_salida_pkg, '  ;');
              UTL_FILE.put_line(fich_salida_pkg, '  commit;');
              UTL_FILE.put_line(fich_salida_pkg, '');
              UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_SA || ''' || ''.'' || ''' || reg_tabla.TABLE_NAME || ''';');
              UTL_FILE.put_line(fich_salida_pkg, '');
              UTL_FILE.put_line(fich_salida_pkg,'  exception');
              UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
              UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Se ha producido un error en el pre-proceso de staging. Tabla: '' || ''' || 'SA_' || v_concept_name || ''');');
              UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
              UTL_FILE.put_line(fich_salida_pkg,'    raise;');
              UTL_FILE.put_line(fich_salida_pkg, '  END pre_' || nombre_proceso || ';'); 
              UTL_FILE.put_line(fich_salida_pkg, '');
              
            end if;
            /* (20150720) Angel Ruiz. NF: FIN */
            /*******************/
            num_sce_integra:=0;
            /* Genero los cuerpos de los metodos que implementan los escenarios */
            open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
            loop
              fetch MTDT_SCENARIO
              into reg_scenario;
              exit when MTDT_SCENARIO%NOTFOUND;
              dbms_output.put_line ('Estoy en el segundo LOOP MTDT_SCENARIO. El escenario es: ' || reg_scenario.SCENARIO);
              /********************/
              /********************/
              nombre_tabla_base_redu := SUBSTR(reg_scenario.TABLE_BASE_NAME, 4);
              nombre_tabla_base_sp_redu:=substr(nombre_tabla_base_redu, 1, 3);
        
              if (reg_scenario.SCENARIO = 'P')
              then
                num_sce_integra := num_sce_integra+1; /* Puede ocurrir que la misma tabla se cree o cargue a partir de varias tablas. Aqui contamos el numero. */
                dbms_output.put_line ('Estoy en el escenario: P');
                UTL_FILE.put_line(fich_salida_pkg,'');
                --UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION i_' || nombre_tabla_reducido || '_' || nombre_tabla_base_redu || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                
                UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION i_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
                UTL_FILE.put_line(fich_salida_pkg, '  IS');
                UTL_FILE.put_line(fich_salida_pkg, '  num_filas_insertadas INTEGER;');
                UTL_FILE.put_line(fich_salida_pkg, '  var_fch_inicio date := sysdate;');
                UTL_FILE.put_line(fich_salida_pkg,'   exis_tabla number(1);');
                
                UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
                UTL_FILE.put_line(fich_salida_pkg, '');
                
                if num_sce_integra > 1 then
                  
                  UTL_FILE.put_line(fich_salida_pkg, '    INSERT INTO ' || OWNER_SA || '.' || reg_scenario.TABLE_NAME);
                  UTL_FILE.put_line(fich_salida_pkg, '    (');
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_scenario.TABLE_COLUMNS);
                  UTL_FILE.put_line(fich_salida_pkg, '    )');
                else
                  UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' || ''' || OWNER_SA || '.'' || ''' || reg_scenario.TABLE_NAME || ''';');
                  UTL_FILE.put_line(fich_salida_pkg, '    INSERT INTO ' || OWNER_SA || '.' || reg_scenario.TABLE_NAME);
                  UTL_FILE.put_line(fich_salida_pkg, '    (');
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_scenario.TABLE_COLUMNS);
                  UTL_FILE.put_line(fich_salida_pkg, '    )');
                end if;
                if (reg_scenario."SELECT" is null) then
                  /* Significa que se crea a partir de todos los campos de la tabla */
                  UTL_FILE.put_line(fich_salida_pkg, '    SELECT');
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || reg_scenario.INTERFACE_COLUMNS);
                  UTL_FILE.put_line(fich_salida_pkg, '    FROM ' || OWNER_SA || '.' || reg_scenario.TABLE_BASE_NAME);
                  if (reg_scenario.FILTER is not null) then
                    UTL_FILE.put_line(fich_salida_pkg, '    WHERE ' || procesa_campo_filter_dinam(reg_scenario.FILTER));
                  end if;
                  if (reg_scenario."GROUP" is not null) then
                    UTL_FILE.put_line(fich_salida_pkg, '    GROUP BY ' || reg_scenario."GROUP");
                  end if;
                  UTL_FILE.put_line(fich_salida_pkg, '    ;');
                else
                  /* Se ha escrito una SELECT */
                  /* Y PEGO LA SELECT QUE SE HA ESCRITO A PARTIR DEL INSERT */
                  UTL_FILE.put_line(fich_salida_pkg, '    ' || procesa_campo_filter(reg_scenario."SELECT"));
                  UTL_FILE.put_line(fich_salida_pkg, '    ;');
                  --UTL_FILE.put_line(fich_salida_pkg, '      ('); 
                end if;
                UTL_FILE.put_line(fich_salida_pkg,'');
                UTL_FILE.put_line(fich_salida_pkg,'  num_filas_insertadas := sql%rowcount;');
                --UTL_FILE.put_line(fich_salida_pkg,'    commit;');
                UTL_FILE.put_line(fich_salida_pkg,'  RETURN num_filas_insertadas;');
                
                UTL_FILE.put_line(fich_salida_pkg,'  exception');
                UTL_FILE.put_line(fich_salida_pkg,'  when NO_DATA_FOUND then');
                UTL_FILE.put_line(fich_salida_pkg,'    return sql%rowcount;');
                UTL_FILE.put_line(fich_salida_pkg,'  when OTHERS then');
                UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Se ha producido un error al insertar los nuevos registros.'');');
                UTL_FILE.put_line(fich_salida_pkg,'    dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
                --UTL_FILE.put_line(fich_salida_pkg,'      rollback;');
                UTL_FILE.put_line(fich_salida_pkg,'    RAISE;');
                --UTL_FILE.put_line(fich_salida_pkg,'      return sqlcode;');
                UTL_FILE.put_line(fich_salida_pkg,'  END i_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu || ';');
                UTL_FILE.put_line(fich_salida_pkg, '');
                
              end if;      
              /********************/
              /********************/
        
           end loop;
           close MTDT_SCENARIO;
          
           UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
           UTL_FILE.put_line(fich_salida_pkg, '  IS');
           /* Declaro las variables donde voy a retornar el numero de filas insertado */
            open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
            loop
              fetch MTDT_SCENARIO
              into reg_scenario;
              exit when MTDT_SCENARIO%NOTFOUND;
              nombre_tabla_base_redu := SUBSTR(reg_scenario.TABLE_BASE_NAME, 4);
              nombre_tabla_base_sp_redu:=substr(nombre_tabla_base_redu, 1, 3);
              UTL_FILE.put_line(fich_salida_pkg, '  v_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu  || ' integer := 0;');
            end loop;
            close MTDT_SCENARIO;
           UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_new integer := 0;');
           --UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_updt integer:=0;');
           --UTL_FILE.put_line(fich_salida_pkg, '  numero_reg_hist integer:=0;');
           UTL_FILE.put_line(fich_salida_pkg, '  siguiente_paso_a_ejecutar PLS_integer;');
           UTL_FILE.put_line(fich_salida_pkg, '  inicio_paso_tmr TIMESTAMP;');
           UTL_FILE.put_line(fich_salida_pkg, '  BEGIN');
           UTL_FILE.put_line(fich_salida_pkg, '');
            /* (20150720) Angel Ruiz. NF: Historico para tablas de Integracion */
            if (v_history <> 'NULO') then
              /* He de incluir la salvaguarda de los datos */
             UTL_FILE.put_line(fich_salida_pkg, '    /* SALVAGUARDAMOS LOS DATOS */');
             UTL_FILE.put_line(fich_salida_pkg, '    pre_' || nombre_proceso || ' (fch_carga_in, fch_datos_in, forzado_in);');
            end if;
           UTL_FILE.put_line(fich_salida_pkg, '    /* INICIAMOS EL BUCLE POR CADA UNA DE LAS INSERCIONES EN LA TABLA DE STAGING */');
           UTL_FILE.put_line(fich_salida_pkg, '    /* EN EL CASO DE LAS DIMENSIONES SOLO DEBE HABER UN REGISTRO YA QUE NO HAY RETRASADOS */');
           UTL_FILE.put_line(fich_salida_pkg, '    dbms_output.put_line (''Inicio del proceso de carga: ''' || ' || ''' || 'load_' || reg_tabla.TABLE_NAME || ''' || ''.'');');
           UTL_FILE.put_line(fich_salida_pkg, '    siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.siguiente_paso (''load_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
           UTL_FILE.put_line(fich_salida_pkg, '    if (forzado_in = ''F'') then');
           UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := 1;');
           UTL_FILE.put_line(fich_salida_pkg, '    end if;');
           UTL_FILE.put_line(fich_salida_pkg, '    if (siguiente_paso_a_ejecutar = 1) then');
           UTL_FILE.put_line(fich_salida_pkg, '');
           UTL_FILE.put_line(fich_salida_pkg, '      inicio_paso_tmr := cast (systimestamp as timestamp);');
           --UTL_FILE.put_line(fich_salida_pkg, '      SET TRANSACTION NAME ''TRAN_' || reg_tabla.TABLE_NAME || ''';');
           UTL_FILE.put_line(fich_salida_pkg, '');
           open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
           loop
             fetch MTDT_SCENARIO
             into reg_scenario;
             exit when MTDT_SCENARIO%NOTFOUND;
             nombre_tabla_base_redu := SUBSTR(reg_scenario.TABLE_BASE_NAME, 4);
             nombre_tabla_base_sp_redu:=substr(nombre_tabla_base_redu, 1, 3);
             UTL_FILE.put_line(fich_salida_pkg,'      v_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu || ' :=  i_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu || ' (fch_carga_in, fch_datos_in);');
             UTL_FILE.put_line(fich_salida_pkg,'      numero_reg_new := numero_reg_new + v_' || nombre_tabla_reducido || '_' || nombre_tabla_base_sp_redu || ';');
           end loop;
           close MTDT_SCENARIO;
        
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
            UTL_FILE.put_line(fich_salida_pkg, '      /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
            UTL_FILE.put_line(fich_salida_pkg, '      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in,''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_new, 0, 0, numero_reg_new, 0);');
            UTL_FILE.put_line(fich_salida_pkg, '      COMMIT;');
            UTL_FILE.put_line(fich_salida_pkg, '    end if;');
            
        /***********/
            UTL_FILE.put_line(fich_salida_pkg, '');
            --UTL_FILE.put_line(fich_salida_pkg,'    RETURN 0;');
            
            UTL_FILE.put_line(fich_salida_pkg,'    exception');
            --UTL_FILE.put_line(fich_salida_pkg,'    when NO_DATA_FOUND then');
            --UTL_FILE.put_line(fich_salida_pkg,'      return sql%rowcount;');
            UTL_FILE.put_line(fich_salida_pkg,'    when OTHERS then');
            UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''EL PROCESO HA ACABADO CON ERRORES.'');');
            UTL_FILE.put_line(fich_salida_pkg,'      dbms_output.put_line (''Error code: '' || sqlcode || ''. Mensaje: '' || sqlerrm);');
            UTL_FILE.put_line(fich_salida_pkg,'      ROLLBACK;');
            UTL_FILE.put_line(fich_salida_pkg,'      ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM || '.inserta_monitoreo (''' || 'load_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 1, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
            UTL_FILE.put_line(fich_salida_pkg,'      commit;    /* Hacemos el commit del inserta_monitoreo*/');
            UTL_FILE.put_line(fich_salida_pkg,'      RAISE;');
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, '  END load_' || reg_tabla.TABLE_NAME || ';');
            UTL_FILE.put_line(fich_salida_pkg, '');
            /****/
            /* FIN de la generacion de la funcion load */
            /****/
            
            UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || reg_tabla.TABLE_NAME || ';' );
            UTL_FILE.put_line(fich_salida_pkg, '/' );
            /******/
            /* FIN DE LA GENERACION DEL PACKAGE */
            /******/    
            UTL_FILE.put_line(fich_salida_pkg, '');
            UTL_FILE.put_line(fich_salida_pkg, 'grant execute on ' || OWNER_SA || '.pkg_' || reg_tabla.TABLE_NAME || ' to ' || OWNER_TC || ';');
            UTL_FILE.put_line(fich_salida_pkg, '/');
            UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');
        
          
            /******/
            /* FIN DEL PACKGE BODY */
            /******/    
        /****************************************************/
        /****************************************************/
        /****************************************************/
        /****************************************************/
        /****************************************************/
            /******/
            /* INICIO DE LA GENERACION DEL sh de INTEGRACION */
            /******/
            
        /***********************/
            UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
            UTL_FILE.put_line(fich_salida_load, '#############################################################################');
            UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
            UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_load, '# Archivo    :       load_ ' ||  reg_tabla.TABLE_NAME || '.sh                            #');
            UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
            UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                                                  #');
            UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || 'S.        #');
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
            UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
            UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
            
            --UTL_FILE.put_line(fich_salida_sh, 'set -x');
            --UTL_FILE.put_line(fich_salida_load, 'echo "load_' || reg_tabla.TABLE_NAME || '" > ${MVNO_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FCH_CARGA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
            UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
            UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_load, 'fi');
            UTL_FILE.put_line(fich_salida_load, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
            UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
            UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');    
            UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');    
            UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');    
            UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');    
            UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
            UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
            UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
            UTL_FILE.put_line(fich_salida_load, '################################################################################');
            UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
            UTL_FILE.put_line(fich_salida_load, '################################################################################');
            UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="' || v_REQ_NUMER || '"');
            --UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="Req96817"');
            UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || v_REQ_NUMER || '_load_' || reg_tabla.TABLE_NAME);
            --UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=Req96817_load_' || reg_tabla.TABLE_NAME);
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
        
        /***********************/
            UTL_FILE.put_line(fich_salida_load, '# Llamada a sql_plus');
            UTL_FILE.put_line(fich_salida_load, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
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
            UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_SA || '.pkg_' || reg_tabla.TABLE_NAME || '.' || 'load_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'',''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
            UTL_FILE.put_line(fich_salida_load, 'end;');
            UTL_FILE.put_line(fich_salida_load, '/');
            UTL_FILE.put_line(fich_salida_load, 'exit 0;');
            UTL_FILE.put_line(fich_salida_load, 'EOF');
        
            UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
            UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
            UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
            UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
            UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
            UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            --UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
            UTL_FILE.put_line(fich_salida_load, '  exit 1');
            UTL_FILE.put_line(fich_salida_load, 'fi');
            UTL_FILE.put_line(fich_salida_load, '');
            UTL_FILE.put_line(fich_salida_load, 'echo "El proceso load_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
            UTL_FILE.put_line(fich_salida_load, 'exit 0');
            /******/
            /* FIN DE LA GENERACION DEL sh de NUEVOS Y EXISTENTES */
            /******/
            /******/
            UTL_FILE.FCLOSE (fich_salida_load);
            UTL_FILE.FCLOSE (fich_salida_pkg);
    
    end if;
  end loop;   
  close MTDT_TABLA;
end;

