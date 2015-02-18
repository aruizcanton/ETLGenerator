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
    trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_TRAFV_CU_MVNO');  

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
      METADATO.MTDT_TC_DETAIL
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
      METADATO.MTDT_TC_DETAIL
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
      METADATO.MTDT_TC_DETAIL
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
  type lista_tablas_from is table of varchar(200);
  type lista_condi_where is table of varchar(150);

  
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
  nombre_tabla_reducido           VARCHAR2(30);
  --nombre_tabla_base_reducido           VARCHAR2(30);
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  
  l_FROM                                      lista_tablas_from := lista_tablas_from();
  l_WHERE                                   lista_condi_where := lista_condi_where();
  v_hay_look_up                           VARCHAR2(1):='N';
  


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
      /* Busco el signo = */
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

  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
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
    cadena_resul  VARCHAR(500);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(500);
    cola                   VARCHAR2(500);
    pos_ant            PLS_integer;
    v_encontrado  VARCHAR2(1);
    v_alias             VARCHAR2(40);
    table_columns_lkup  list_strings := list_strings();
    ie_column_lkup    list_strings := list_strings();
    tipo_columna  VARCHAR2(30);
    mitabla_look_up VARCHAR2(200);
    l_registro          ALL_TAB_COLUMNS%rowtype;
  begin
    /* Seleccionamos el escenario primero */
      case reg_detalle_in.RUL
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno :=  '    ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;      
      when 'LKUP' then
        /* Se trata de hacer el LOOK UP con la tabla dimension */
        /* (20150126) Angel Ruiz. Primero recojo la tabla del modelo con la que se hace LookUp. NO puede ser tablas T_* sino su equivalesnte del modelo */
        dbms_output.put_line('ESTOY EN EL LOOKUP. Al principio');
        --if (regexp_count(reg_detalle_in.TABLE_LKUP,'^DMD_') > 0) then  /* Se trata de una dimension normal y corriente */
        --  mitabla_look_up:=reg_detalle_in.TABLE_LKUP;
        --elsif  (regexp_count(reg_detalle_in.TABLE_LKUP,'^T_') >0) then/* Se trata de una tabla T_ */
        --  mitabla_look_up:='DMD_' || substr(reg_detalle_in.TABLE_LKUP,3);
        --elsif (regexp_count(reg_detalle_in.TABLE_LKUP,'^DMT_') >0) then /* Se trata de una tabla DMT_ */
        --  mitabla_look_up:=reg_detalle_in.TABLE_LKUP;
        --elsif (regexp_count(reg_detalle_in.TABLE_LKUP,'^(') >0) then /* Se trata de una subquery */
        --else
        --  mitabla_look_up:=reg_detalle_in.TABLE_LKUP;
        --end if;
        --if (reg_detalle_in.LKUP_COM_RULE <> "") then
        l_FROM.extend;
        /* (20150130) Angel Ruiz */
        /* Nueva incidencia. */
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          v_alias := 'LKUP_' || l_FROM.count;
          mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          v_encontrado:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              v_encontrado:='Y';
            end if;
          END LOOP;
          if (v_encontrado='Y') then
            v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          else
            v_alias := reg_detalle_in.TABLE_LKUP;
            l_FROM (l_FROM.last) := ', ' || OWNER_DM || '.' || reg_detalle_in.TABLE_LKUP;
          end if;
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
          valor_retorno :=  '    NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2)';
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer el JOIN */
        table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        if (table_columns_lkup.COUNT > 1) then
          /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
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
        valor_retorno :=  '    SYSDATE';
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

/************/


begin
  /* (20141223) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
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
    UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || reg_tabla.TABLE_NAME || ' AS');
    lista_scenarios_presentes.delete;
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    dbms_output.put_line ('Comienzo la generacion del PACKAGE DEFINITION');
    /* Primero de todo miro si hay funciones de LOOKUP para crear */
    --dbms_output.put_line ('Antes de mirar funciones para hacer LOOKUP');
    --open MTDT_TC_LOOKUP (reg_tabla.TABLE_NAME);
    --loop
      --fetch MTDT_TC_LOOKUP
      --into reg_lookup;
      --exit when MTDT_TC_LOOKUP%NOTFOUND;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      --prototipo_fun := genera_encabezado_funcion_pkg (reg_lookup);
      --UTL_FILE.put_line(fich_salida_pkg,'');
      --UTL_FILE.put_line(fich_salida_pkg, prototipo_fun);
      
    --end loop;
    --close MTDT_TC_LOOKUP;
    --dbms_output.put_line ('Despues de mirar funciones para hacer LOOKUP');
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
      
      if (reg_scenario.SCENARIO = 'N')      /*  Procesamos el escenario NUEVO  */
      then
        /* Tenemos el escenario Nuevo */
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
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
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ope_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
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
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION alt_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
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
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION icc_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
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
        UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION num_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER;');
        UTL_FILE.put_line(fich_salida_pkg, '' ); 
        /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
        lista_scenarios_presentes.EXTEND;
        lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'NUM';
      end if;
      
    end loop; /* fin del LOOP MTDT_SCENARIO  */
    close MTDT_SCENARIO;
    
    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_he_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');

    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_ex_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2);');
    
    UTL_FILE.put_line(fich_salida_pkg, '' ); 
    UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || reg_tabla.TABLE_NAME || ';' );
    UTL_FILE.put_line(fich_salida_pkg, '/' );

    /* GENERACION DEL PACKAGE BODY */
    UTL_FILE.put_line(fich_salida_pkg,'CREATE OR REPLACE PACKAGE BODY ' || OWNER_DM || '.pkg_' || reg_tabla.TABLE_NAME || ' AS');
    UTL_FILE.put_line(fich_salida_pkg,'');

    dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION. :-)');
    
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
    UTL_FILE.put_line(fich_salida_pkg,'    exis_partition :=  existe_particion (' || '''PA_' || nombre_tabla_reducido || '_' || ''' || fch_datos_in, ''' || reg_tabla.TABLE_NAME || ''');');
    UTL_FILE.put_line(fich_salida_pkg,'    if (exis_partition = 0) then' );      
    UTL_FILE.put_line(fich_salida_pkg,'    /* Creo la particion */'); 
    --UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') UPDATE INDEXES'';');
    UTL_FILE.put_line(fich_salida_pkg, '    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_DM || '.' || reg_tabla.TABLE_NAME || ''' || '' ADD PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE '' || ''' || reg_tabla.TABLESPACE || ''';');
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
    --dbms_output.put_line ('Antes de generar las funciones de LOOKUP');
    --open MTDT_TC_LOOKUP (reg_tabla.TABLE_NAME);
    --loop
      --fetch MTDT_TC_LOOKUP
      --into reg_lookup;
      --exit when MTDT_TC_LOOKUP%NOTFOUND;
      /* Se trata de hacer el LOOK UP con la tabla dimension */
      --genera_cuerpo_funcion_pkg (reg_lookup);      
    --end loop;
    --close MTDT_TC_LOOKUP;
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
      if (reg_scenario.SCENARIO = 'N')  /* Proceso el escenario NEW */
      then
        /* SCENARIO NUEVO */
          dbms_output.put_line ('Estoy dentro del scenario NUEVO');
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION new_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
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
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
            v_hay_look_up := 'Y';
          END LOOP;
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
          UTL_FILE.put_line(fich_salida_pkg, '  END new_' || reg_scenario.TABLE_NAME || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion new */
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'OPE')  /* Proceso el escenario OPE */
      then
        /* SCENARIO OPE */
          dbms_output.put_line ('Dentro de OPE');
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION ope_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
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
          /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
          l_FROM.delete;
          l_WHERE.delete;
          /* Fin de la inicialización */
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
          /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
          v_hay_look_up:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
            v_hay_look_up := 'Y';
          END LOOP;
          /* FIN */
          dbms_output.put_line ('Despues de generar el FROM');
          /* INICIO generacion parte  WHERE  */

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
          UTL_FILE.put_line(fich_salida_pkg, '  END ope_' || reg_scenario.TABLE_NAME || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion ope */
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'ALT')  /* Proceso el escenario ALT */
      then
        /* SCENARIO ALT */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION alt_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
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
          /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
          l_FROM.delete;
          l_WHERE.delete;
          /* Fin de la inicialización */
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
          /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
          v_hay_look_up:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
            v_hay_look_up := 'Y';
          END LOOP;
          /* FIN */

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
            /* Hay tablas de LookUp. Hay que poner las condiciones de los Where*/
              UTL_FILE.put_line(fich_salida_pkg,'    WHERE');
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
          UTL_FILE.put_line(fich_salida_pkg, '  END alt_' || reg_scenario.TABLE_NAME || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion ope */
      
      /*************/
      /*************/
      if (reg_scenario.SCENARIO = 'ICC')  /* Proceso el escenario ICC */
      then
        /* SCENARIO ICC */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION icc_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
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
          /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
          l_FROM.delete;
          l_WHERE.delete;
          /* Fin de la inicialización */
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
          /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
          v_hay_look_up:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
            v_hay_look_up := 'Y';
          END LOOP;
          /* FIN */
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
          UTL_FILE.put_line(fich_salida_pkg, '  END icc_' || reg_scenario.TABLE_NAME || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion ICC */
      
      /**************/
      /**************/
      if (reg_scenario.SCENARIO = 'NUM')  /* Proceso el escenario NUM */
      then
        /* SCENARIO NUM */
          UTL_FILE.put_line(fich_salida_pkg, '  FUNCTION num_' || reg_scenario.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2) return NUMBER');
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
          /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
          l_FROM.delete;
          l_WHERE.delete;
          /* Fin de la inicialización */
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
          /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
          v_hay_look_up:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx));
            v_hay_look_up := 'Y';
          END LOOP;
          /* FIN */
          
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
          UTL_FILE.put_line(fich_salida_pkg, '  END num_' || reg_scenario.TABLE_NAME || ';');
          UTL_FILE.put_line(fich_salida_pkg, '');
      end if;   /* FIN de la generacion de la funcion num */
      
      /**************/
      /**************/
    
    end loop;
    close MTDT_SCENARIO;

    /* Generamos los procedimientos que llaman a las funciones escenarios */
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_he_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
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
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.siguiente_paso (''load_he_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
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
    UTL_FILE.put_line(fich_salida_pkg, '        pkg_' || reg_tabla.TABLE_NAME || '.' || 'pre_proceso (fch_carga_in, fch_datos_in);');
    --UTL_FILE.put_line(fich_salida_pkg, '        SET TRANSACTION NAME ''TRAN_' || reg_tabla.TABLE_NAME || ''';');
    UTL_FILE.put_line(fich_salida_pkg, '');
    /* Generamos las llamadas a los procedimientos para realizar las cargas */
    /* Generamos la llamada para cargar los registros NUEVOS */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'N'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_new := ' || 'pkg_' || reg_tabla.TABLE_NAME || '.' || 'new_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_new;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros insertados es: '' || numero_reg_new || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros OPE */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'OPE'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_ope := ' || 'pkg_' || reg_tabla.TABLE_NAME || '.' || 'ope_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_ope;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros ope es: '' || numero_reg_ope || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros ALT */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'ALT'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_alt := ' || 'pkg_' || reg_tabla.TABLE_NAME || '.' || 'alt_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_alt;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros ope es: '' || numero_reg_alt || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros ALT */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'ICC'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_icc := ' || 'pkg_' || reg_tabla.TABLE_NAME || '.' || 'icc_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_icc;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros icc es: '' || numero_reg_icc || ''.'');');
      end if;
    END LOOP;
    /* Generamos la llamada para cargar los registros NUM */
    FOR indx IN lista_scenarios_presentes.FIRST .. lista_scenarios_presentes.LAST
    LOOP
      if lista_scenarios_presentes (indx) = 'NUM'
      then
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_num := ' || 'pkg_' || reg_tabla.TABLE_NAME || '.' || 'num_' || reg_tabla.TABLE_NAME || ' (fch_carga_in, fch_datos_in);');
        UTL_FILE.put_line(fich_salida_pkg,'         numero_reg_tot := numero_reg_tot + numero_reg_num;');
        UTL_FILE.put_line(fich_salida_pkg,'         dbms_output.put_line (''El numero de registros num es: '' || numero_reg_num || ''.'');');
      end if;
    END LOOP;
    UTL_FILE.put_line(fich_salida_pkg, '');
    --UTL_FILE.put_line(fich_salida_pkg, '         pkg_' || reg_tabla.TABLE_NAME || '.' || 'pos_proceso (fch_carga_in, to_char(reg_monitoreo.FCH_DATOS, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '         numero_reg_salvaguardados := pkg_' || reg_tabla.TABLE_NAME || '.' || 'pos_proceso (fch_carga_in, fch_datos_in);');
    UTL_FILE.put_line(fich_salida_pkg, '         /* Este tipo de procesos solo tienen un paso, por eso aparece un 1 en el campo de paso */');
    UTL_FILE.put_line(fich_salida_pkg, '         /* Este tipo de procesos solo tienen un paso, y ha terminado OK por eso aparece un 0 en el siguiente campo */');
    UTL_FILE.put_line(fich_salida_pkg, '         ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''), numero_reg_tot, 0, 0, numero_reg_salvaguardados);');
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
    UTL_FILE.put_line(fich_salida_pkg, '  END load_he_' || reg_tabla.TABLE_NAME || ';');
    UTL_FILE.put_line(fich_salida_pkg, '');
  
    /**************/
    
    UTL_FILE.put_line(fich_salida_pkg, '  PROCEDURE load_ex_' || reg_tabla.TABLE_NAME || ' (fch_carga_in IN VARCHAR2, fch_datos_in IN VARCHAR2, forzado_in IN VARCHAR2)');
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
    UTL_FILE.put_line(fich_salida_pkg, '      siguiente_paso_a_ejecutar := ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.siguiente_paso (''load_ex_' || reg_tabla.TABLE_NAME || '.sh'', to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '      if (forzado_in = ''F'') then');
    UTL_FILE.put_line(fich_salida_pkg, '        siguiente_paso_a_ejecutar := 1;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if;');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 1) then');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Este tipo de procesos ex solo tienen dos pasos */');
    UTL_FILE.put_line(fich_salida_pkg, '        /* Comienza en el primer paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_DM || '.' || reg_scenario.TABLE_NAME);    
    UTL_FILE.put_line(fich_salida_pkg, '        EXCHANGE PARTITION PA_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' ');    
    UTL_FILE.put_line(fich_salida_pkg, '        WITH TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' ');    
    UTL_FILE.put_line(fich_salida_pkg, '        WITHOUT VALIDATION'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '1, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '        /* comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' CASCADE CONSTRAINTS PURGE'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
    UTL_FILE.put_line(fich_salida_pkg, '        commit;');
    UTL_FILE.put_line(fich_salida_pkg, '      end if; ');
    UTL_FILE.put_line(fich_salida_pkg, '      if (siguiente_paso_a_ejecutar = 2) then');
    UTL_FILE.put_line(fich_salida_pkg, '        /* comienza el segundo paso */');
    UTL_FILE.put_line(fich_salida_pkg, '        inicio_paso_tmr := cast (systimestamp as timestamp);');
    --UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE APP_MVNODM.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in;');    
    UTL_FILE.put_line(fich_salida_pkg, '        EXECUTE IMMEDIATE ''DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ''' || ''_'' || fch_datos_in || '' CASCADE CONSTRAINTS PURGE'';');    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, '        ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo (''' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh'',' || '2, 0, inicio_paso_tmr, systimestamp, to_date(fch_datos_in, ''yyyymmdd''), to_date(fch_carga_in, ''yyyymmdd''));');
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
    UTL_FILE.put_line(fich_salida_pkg, '  END load_ex_' || reg_tabla.TABLE_NAME || ';');
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, 'END pkg_' || reg_tabla.TABLE_NAME || ';' );
    UTL_FILE.put_line(fich_salida_pkg, '/' );
    /******/
    /* FIN DE LA GENERACION DEL PACKAGE */
    /******/
    /******/    
    UTL_FILE.put_line(fich_salida_pkg, '');
    UTL_FILE.put_line(fich_salida_pkg, 'grant execute on ' || OWNER_DM || '.pkg_' || reg_tabla.TABLE_NAME || ' to app_mvnotc;');
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
    UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos de STAGING para MVNOS.        #');
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
    UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${MVNO_SQL}/insert_monitoreo.sql ' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
    UTL_FILE.put_line(fich_salida_load, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${MVNO_SQL}/insert_monitoreo.sql ' || 'load_he_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
    UTL_FILE.put_line(fich_salida_load, '. ${MVNO_ENTORNO}/entornoMVNO_MEX.sh');
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
    UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${MVNO_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  mkdir ${MVNO_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, 'MVNO_TRAZAS=${MVNO_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
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
    UTL_FILE.put_line(fich_salida_load, '. ${MVNO_UTILIDADES}/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_load, '. ${MVNO_UTILIDADES}/UtilArchivo.sh');
    UTL_FILE.put_line(fich_salida_load, '. ${MVNO_UTILIDADES}/UtilUnix.sh');
    UTL_FILE.put_line(fich_salida_load, '. ${MVNO_UTILIDADES}/UtilMVNO.sh');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=`cat ${MVNO_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=`cat ${MVNO_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_load, 'else');
    UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=`cat ${MVNO_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=`cat ${MVNO_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
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
    UTL_FILE.put_line(fich_salida_load, 'sqlplus -s /nolog <<EOF >> ${MVNO_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
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
    UTL_FILE.put_line(fich_salida_load, '  ' || OWNER_DM || '.pkg_' || reg_tabla.TABLE_NAME || '.' || 'load_he_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
    UTL_FILE.put_line(fich_salida_load, 'end;');
    UTL_FILE.put_line(fich_salida_load, '/');
    UTL_FILE.put_line(fich_salida_load, 'exit 0;');
    UTL_FILE.put_line(fich_salida_load, 'EOF');
    UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_he_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${MVNO_TRAZAS}/' || 'load_he' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${MVNO_TRAZAS}/' || 'load_he' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, 'echo "El proceso load_' ||  'he_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${MVNO_TRAZAS}/' || 'load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
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
    UTL_FILE.put_line(fich_salida_exchange, '# Proposito  : Shell que ejecuta los procesos de STAGING para MVNOS.        #');
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
    UTL_FILE.put_line(fich_salida_exchange, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${MVNO_SQL}/insert_monitoreo.sql ' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
    UTL_FILE.put_line(fich_salida_exchange, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${MVNO_SQL}/insert_monitoreo.sql ' || 'load_ex_' || reg_tabla.TABLE_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
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
    UTL_FILE.put_line(fich_salida_exchange, '. ${MVNO_ENTORNO}/entornoMVNO_MEX.sh');
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
    --UTL_FILE.put_line(fich_salida_exchange, 'echo "load_ex_' || reg_tabla.TABLE_NAME || '" > ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_exchange, 'if ! [ -d ${MVNO_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_exchange, '  mkdir ${MVNO_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_exchange, 'fi');
    UTL_FILE.put_line(fich_salida_exchange, 'MVNO_TRAZAS=${MVNO_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "${0}" > ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "Forzado: ${BAN_FORZADO}"  >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
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
    UTL_FILE.put_line(fich_salida_exchange, '. ${MVNO_UTILIDADES}/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_exchange, '. ${MVNO_UTILIDADES}/UtilArchivo.sh');
    UTL_FILE.put_line(fich_salida_exchange, '. ${MVNO_UTILIDADES}/UtilUnix.sh');
    UTL_FILE.put_line(fich_salida_exchange, '. ${MVNO_UTILIDADES}/UtilMVNO.sh');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
    UTL_FILE.put_line(fich_salida_exchange, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    UTL_FILE.put_line(fich_salida_exchange, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL_USUARIOS=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_DWH=`cat ${MVNO_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_USUARIOS=`cat ${MVNO_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_exchange, 'else');
    UTL_FILE.put_line(fich_salida_exchange, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL_USUARIOS=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  CTA_MAIL=`cat ${MVNO_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_DWH=`cat ${MVNO_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_exchange, '  TELEFONOS_USUARIOS=`cat ${MVNO_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    UTL_FILE.put_line(fich_salida_exchange, 'fi');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
    UTL_FILE.put_line(fich_salida_exchange, 'BD_CLAVE=${PASSWORD}');
    
    /*****************************************************/
    UTL_FILE.put_line(fich_salida_exchange, '# Llamada a sql_plus');
    UTL_FILE.put_line(fich_salida_exchange, 'sqlplus -s /nolog <<EOF >> ${MVNO_TRAZAS}/load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
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
    UTL_FILE.put_line(fich_salida_exchange, '  ' || OWNER_DM || '.pkg_' || reg_tabla.TABLE_NAME || '.' || 'load_ex_' || reg_tabla.TABLE_NAME || '(''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
    UTL_FILE.put_line(fich_salida_exchange, 'end;');
    UTL_FILE.put_line(fich_salida_exchange, '/');
    UTL_FILE.put_line(fich_salida_exchange, 'exit 0;');
    UTL_FILE.put_line(fich_salida_exchange, 'EOF');
    UTL_FILE.put_line(fich_salida_exchange, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_exchange, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_exchange, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a load_ex_' || reg_tabla.TABLE_NAME || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_exchange, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_exchange, '  echo ${SUBJECT} >> ' || '${MVNO_TRAZAS}/' || 'load_ex' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_exchange, '  echo `date` >> ' || '${MVNO_TRAZAS}/' || 'load_ex' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_exchange, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_exchange, '  exit 1');
    UTL_FILE.put_line(fich_salida_exchange, 'fi');
    UTL_FILE.put_line(fich_salida_exchange, '');
    UTL_FILE.put_line(fich_salida_exchange, 'echo "El proceso de exchange load_' ||  'ex_' || reg_tabla.TABLE_NAME || ' se ha realizado correctamente." >> ' || '${MVNO_TRAZAS}/' || 'load_ex_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
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

