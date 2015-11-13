DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      TRIM(INTERFACE_NAME) "INTERFACE_NAME",
      TRIM(COUNTRY) "COUNTRY",
      TRIM(TYPE) "TYPE",
      TRIM(SEPARATOR) "SEPARATOR",
      TRIM(LENGTH) "LENGTH",
      TRIM(FREQUENCY) "FREQUENCY",
      TRIM(DELAYED) "DELAYED",
      TRIM(HISTORY) "HISTORY"
    FROM MTDT_INTERFACE_SUMMARY    
    WHERE SOURCE <> 'SA';  -- Este origen es el que se ha considerado para las dimensiones que son de integracion ya que se cargan a partir de otras dimensiones de SA 
    --and CONCEPT_NAME in ('TRAFE_CU_MVNO', 'TRAFD_CU_MVNO', 'TRAFV_CU_MVNO');
    --AND DELAYED = 'S';
    --WHERE CONCEPT_NAME NOT IN ( 'EMPRESA', 'ESTADO_CEL', 'FINALIZACION_LLAMADA', 'POSICION_TRAZO_LLAMADA', 'TRONCAL', 'TIPO_REGISTRO', 'MSC');
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      TRIM(COLUMNA) "COLUMNA",
      TRIM(KEY) "KEY",
      TRIM(TYPE) "TYPE",
      TRIM(LENGTH) "LENGTH",
      TRIM(NULABLE) "NULABLE",
      POSITION,
      TRIM(FORMAT) "FORMAT"
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      TRIM(CONCEPT_NAME) = concep_name_in and
      TRIM(SOURCE) = source_in
    ORDER BY POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col PLS_INTEGER;
      num_column PLS_INTEGER;
      v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;
      
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      lista_pos                                    list_posiciones := list_posiciones (); 
      
      fich_salida                                 UTL_FILE.file_type;
      fich_salida_sh                          UTL_FILE.file_type;
      nombre_fich                              VARCHAR(40);
      nombre_fich_sh                        VARCHAR(40);  
      tipo_col                                      VARCHAR(1000);
      nombre_interface_a_cargar   VARCHAR(150);
      nombre_flag_a_cargar            VARCHAR(150);
      pos_ini_pais                             PLS_integer;
      pos_fin_pais                             PLS_integer;
      pos_ini_fecha                           PLS_integer;
      pos_fin_fecha                           PLS_integer;
      pos_ini_hora                              PLS_integer;
      pos_fin_hora                              PLS_integer;
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      NAME_DM                                VARCHAR(60);
      nombre_proceso                      VARCHAR(30);
      parte_entera                              VARCHAR2(60);
      parte_decimal                           VARCHAR2(60);
      long_parte_entera                    PLS_integer;
      long_parte_decimal                  PLS_integer;
      mascara                                     VARCHAR2(250);
      nombre_fich_cargado               VARCHAR2(1) := 'N';
      

  function procesa_campo_formateo (cadena_in in varchar2, nombre_campo_in in varchar2) return varchar2
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
    dbms_output.put_line ('Entro en procesa_campo_formateo');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco el nombre del campo = */
      sustituto := ':' || nombre_campo_in;
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_campo_formateo. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, nombre_campo_in, pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (nombre_campo_in));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + (length (':' || nombre_campo_in));
        dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
        pos := pos_ant;
      end loop;
    end if;
    return cadena_resul;
  end;

  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  /* (20141219) FIN*/

  OPEN dtd_interfaz_summary;
  LOOP
    
      FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND; 
      nombre_fich := 'ctl_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '.ctl';
      nombre_fich_sh := 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '.sh';
      fich_salida := UTL_FILE.FOPEN ('SALIDA',nombre_fich,'W');
      fich_salida_sh := UTL_FILE.FOPEN ('SALIDA',nombre_fich_sh,'W');
      /* Angel Ruiz (20141223) Hecho porque hay paquetes que no compilan */
       if (length(reg_summary.CONCEPT_NAME) < 24) then
        nombre_proceso := 'SA_' || reg_summary.CONCEPT_NAME;
      else
        nombre_proceso := reg_summary.CONCEPT_NAME;
      end if;
      
      UTL_FILE.put_line(fich_salida, 'load data');
      --UTL_FILE.put_line(fich_salida, 'infile ' || '_DIR_DATOS/DMDIST_' || reg_summary.COUNTRY || '_' || reg_summary.SOURCE || '_' || reg_summary.CONCEPT_NAME || '_YYYYMMDD' || '.dat');
      UTL_FILE.put_line(fich_salida, 'into table ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
      --if (reg_summary.DELAYED = 'S') then
      --  UTL_FILE.put_line(fich_salida, 'append');
      --else
      --if (reg_summary.delayed = 'S') then
      UTL_FILE.put_line(fich_salida, 'append');
      --else
        --UTL_FILE.put_line(fich_salida, 'truncate');
      --end if;
      --end if;
      IF reg_summary.TYPE = 'S'             /*  El fichero posee un separador de campos */
      THEN
        UTL_FILE.put_line(fich_salida, 'fields terminated by "' || reg_summary.SEPARATOR || '"');
        UTL_FILE.put_line(fich_salida, 'trailing nullcols');
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            /*(20150116) Angel Ruiz. introduzco formateo en la columnas */
            if (reg_datail.format is not null) then
              /* Hay formateo de la columna */
              tipo_col := 'CHAR (' || reg_datail.LENGTH || ') "' || procesa_campo_formateo (reg_datail.format, reg_datail.COLUMNA) || '"';
            else
              /* (20150326) Angel Ruiz. Incidencia */
              if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE is null and reg_datail.LENGTH>2) then
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              elsif (reg_datail.NULABLE is null and (reg_datail.LENGTH>2 and reg_datail.LENGTH<=11)) then
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              elsif (reg_datail.NULABLE is null and reg_datail.LENGTH>11) then 
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NO INFORMADO'')"';
              else
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ')';
              end if;
              --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.NULABLE is null and reg_datail.LENGTH>2) then
              --  tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              --elsif (regexp_count(reg_datail.COLUMNA,'^DES_',1,'i') >0 and reg_datail.NULABLE is null and reg_datail.LENGTH>11) then
              --  tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NO INFORMADO'')"';
              --elseif (regexp_count(reg_datail.COLUMNA,'^DES_',1,'i') >0 and reg_datail.NULABLE is null and (reg_datail.LENGTH>2 and reg_datail.LENGTH<=11)) then
              --  tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              --else
                --tipo_col := 'CHAR (' || reg_datail.LENGTH || ')';
              --end if;
            end if;
            /*(20150715) Angel Ruiz. Nueva Funcionalidad. Columna para almacenar el fichero del que se carga la informacion.*/
            if (reg_datail.COLUMNA = 'FILE_NAME') then
              tipo_col := 'CONSTANT "MY_FILE"';
              nombre_fich_cargado := 'Y';
            end if;
            /*(20150715) Angel Ruiz. Fin. */
          WHEN reg_datail.TYPE = 'NU' THEN
            --tipo_col := 'TO_NUMBER (' || reg_datail.LENGTH || ')';
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE is null) then
              tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            else            
              tipo_col := '';
            end if;
          WHEN reg_datail.TYPE = 'DE' THEN
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.NULABLE is null) then
              tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            else            
              tipo_col := '';
            end if;
          WHEN reg_datail.TYPE = 'FE' THEN
            if (reg_datail.LENGTH = 14) then
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              if (reg_datail.NULABLE is null ) then
                tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101000000'', ''YYYYMMDDHH24MISS''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDDHH24MISS''))"';
              else
                tipo_col := 'DATE "YYYYMMDDHH24MISS"';
              end if;              
            else
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              if (reg_datail.NULABLE is null ) then
                tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101'', ''YYYYMMDD''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDD''))"';
              else
                tipo_col := 'DATE "YYYYMMDD"';
              end if;
            end if;
          WHEN reg_datail.TYPE = 'IM' THEN
            /* Tratamos el tema de los importes para que vengan con separador de miles el . y separador de decimales la , */
            tipo_col:='';
            mascara:='';
            --dbms_output.put_line('Estoy en el caso de IMPORTES');
            parte_entera := substr(reg_datail.LENGTH, 1, instr(reg_datail.LENGTH, ',') -1);
            --dbms_output.put_line('Parte entera:' || parte_entera);
            long_parte_entera := to_number(parte_entera);
            parte_decimal := substr(reg_datail.LENGTH, instr(reg_datail.LENGTH, ',') +1);
            --dbms_output.put_line('Parte decimal:' || parte_decimal);
            long_parte_decimal := to_number(parte_decimal);
            --dbms_output.put_line('La longitud de parte decimal:' || long_parte_decimal);
            --dbms_output.put_line('La longitud de parte entera:' || long_parte_entera);
            for contador in 1 .. long_parte_entera-long_parte_decimal
            loop
              mascara := mascara || '9';
            end loop;
            for contador in 1 .. long_parte_decimal
            loop
              if contador = 1 then
                mascara := mascara || 'D9';
              else
                mascara := mascara || '9';
              end if;
            end loop;
            --dbms_output.put_line('Despues del bucle');
            --dbms_output.put_line('Mascara: ' || mascara);
            --tipo_col := '"TO_NUMBER(:' || reg_datail.COLUMNA || ', ''' || mascara || ''', ''NLS_NUMERIC_CHARACTERS='''',.'''''')"';
            tipo_col := '"TO_NUMBER(NVL(TRIM(:' || reg_datail.COLUMNA || '), ''0''), ''' || mascara || ''', ''NLS_NUMERIC_CHARACTERS='''',.'''''')"';
            dbms_output.put_line('Tipo de columna: ' || tipo_col);
            --tipo_col :='';
          WHEN reg_datail.TYPE = 'TI' THEN
            if (reg_datail.NULABLE is null) then
              tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''000000'')"';
            else            
              tipo_col := 'CHAR (8)';
            end if;
          END CASE;
          IF primera_col = 1
          THEN
            UTL_FILE.put_line(fich_salida, '( ' || reg_datail.COLUMNA || ' ' || tipo_col);
            primera_col := 0;
          ELSE
            UTL_FILE.put_line(fich_salida, ', ' || reg_datail.COLUMNA || ' ' || tipo_col ); 
          END IF;
        END LOOP;
        --if (reg_summary.DELAYED = 'S') then
          --UTL_FILE.put_line(fich_salida, ', FCH_REGISTRO DATE ''YYYYMMDD'' "NVL(:FCH_REGISTRO, ''_YYYYMMDD'')"');
          --UTL_FILE.put_line(fich_salida, ', CVE_DIA  "NVL(:CVE_DIA, _YYYYMMDD)"');
        --end if;
        /* (20150605) Angel Ruiz. AÑADIDO PARA CHEQUEAR LA CALIDAD DEL DATO */
        --UTL_FILE.put_line(fich_salida, ', FILE_NAME CONSTANT "MY_FILE"' ); 
        /* (20150605) Fin */
        UTL_FILE.put_line(fich_salida, ')');
        CLOSE dtd_interfaz_detail;
      ELSE  /* SE TRATA DE QUE EL FICHERO VIENE POR POSICION */
        /* Primero me recorro todo el cursor para ir guardando las longitudes */
        primera_col := 1;
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          lista_pos.EXTEND;
          lista_pos(lista_pos.LAST) :=  reg_datail.POSITION;
        END LOOP;
        close dtd_interfaz_detail;
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        num_column := 0;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          num_column := num_column+1;
          CASE 
          WHEN reg_datail.TYPE = 'AN' THEN
            /*(20150116) Angel Ruiz. introduzco formateo en la columnas */
            if (reg_datail.format is not null) then
              /* Hay formateo de la columna */
              tipo_col := 'CHAR "' || procesa_campo_formateo (reg_datail.format, reg_datail.COLUMNA) || '"';
            else
              /* (20150326) Angel Ruiz. Incidencia */
              if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE is null and reg_datail.LENGTH>2) then
                tipo_col := 'CHAR ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              elsif (reg_datail.NULABLE is null and reg_datail.LENGTH>2 and reg_datail.LENGTH<=11) then
                tipo_col := 'CHAR ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              elsif (reg_datail.NULABLE is null and reg_datail.LENGTH>11) then
                tipo_col := 'CHAR (' || reg_datail.LENGTH || ') ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NO INFORMADO'')"';
              else
                tipo_col := 'CHAR';
              end if;
              --if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE is null and reg_datail.LENGTH>2) then
                --tipo_col := 'CHAR ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NI#'')"';
              --elsif (regexp_count(reg_datail.COLUMNA,'^DES_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE is null and reg_datail.LENGTH>11) then
                --tipo_col := 'CHAR ' || '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''NO INFORMADO'')"';
              --else
                --tipo_col := 'CHAR';
              --end if;
            end if;
            /*(20150715) Angel Ruiz. Nueva Funcionalidad. Columna para almacenar el fichero del que se carga la informacion.*/
            if (reg_datail.COLUMNA = 'FILE_NAME') then
              tipo_col := 'CONSTANT "MY_FILE"';
              nombre_fich_cargado := 'Y';
            end if;
            /*(20150715) Angel Ruiz. Fin. */
          WHEN reg_datail.TYPE = 'NU' THEN
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE is null) then
              tipo_col := 'INTEGER EXTERNAL "NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            else            
              tipo_col := 'INTEGER EXTERNAL';
            end if;
            --tipo_col := '';
          WHEN reg_datail.TYPE = 'DE' THEN
            /* (20160209) Angel Ruiz */
            /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
            if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE is null) then
              tipo_col := 'DECIMAL EXTERNAL "NVL(TRIM(:' || reg_datail.COLUMNA || '), -3)"';
            else            
              tipo_col := 'DECIMAL EXTERNAL';
            end if;
            --tipo_col := '';
          WHEN reg_datail.TYPE = 'FE' THEN
            if (reg_datail.LENGTH = 14) then
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              if (reg_datail.KEY is null and reg_datail.NULABLE is null ) then
                tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101'', ''YYYYMMDDHH24MISS''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDDHH24MISS''))"';
              else
                tipo_col := 'DATE "YYYYMMDDHH24MISS"';
              end if;
            else
              /* (20141217) Angel Ruiz */
              /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
              if (reg_datail.KEY is null and reg_datail.NULABLE is null ) then
                tipo_col := '"DECODE (TRIM(:' || reg_datail.COLUMNA || '),'''',TO_DATE(''19900101'', ''YYYYMMDD''), TO_DATE(:' || reg_datail.COLUMNA || ',''YYYYMMDD''))"';
              else
                tipo_col := 'DATE "YYYYMMDD"';
              end if;
            end if;
          WHEN reg_datail.TYPE = 'IM' THEN
            tipo_col := 'DECIMAL EXTERNAL';
          WHEN reg_datail.TYPE = 'TI' THEN
            if (reg_datail.NULABLE is null) then
              tipo_col := '"NVL(TRIM(:' || reg_datail.COLUMNA || '), ''000000'')"';
            else            
              tipo_col := 'CHAR';
            end if;
          END CASE;
          IF primera_col = 1
          THEN
            UTL_FILE.put_line(fich_salida, '( ' || reg_datail.COLUMNA || '            POSITION(1:' || (lista_pos (num_column+1)-1) || ')     ' || tipo_col);
            primera_col := 0;
          ELSE
            if lista_pos.last = num_column then
              UTL_FILE.put_line (fich_salida, ', ' || reg_datail.COLUMNA || '            POSITION(' || reg_datail.POSITION || ':' || reg_summary.LENGTH || ')     ' || tipo_col); 
            else
              UTL_FILE.put_line (fich_salida, ', ' || reg_datail.COLUMNA || '            POSITION(' || reg_datail.POSITION || ':' || (lista_pos (num_column+1)-1) || ')     ' || tipo_col); 
            end if;
          END IF;
        END LOOP;
        close dtd_interfaz_detail;
        /* (20150605) Angel Ruiz. AÑADIDO PARA CHEQUEAR LA CALIDAD DEL DATO */
        --UTL_FILE.put_line(fich_salida, ', FILE_NAME CONSTANT "MY_FILE"' ); 
        /* (20150605) Fin */
        UTL_FILE.put_line(fich_salida, ')');
      END IF;
      lista_pos.delete;
    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/
    nombre_interface_a_cargar := reg_summary.INTERFACE_NAME;
    pos_ini_pais := instr(reg_summary.INTERFACE_NAME, '_XXX_');
    if (pos_ini_pais > 0) then
      pos_fin_pais := pos_ini_pais + length ('_XXX_');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_pais -1) || '_' || reg_summary.COUNTRY || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
    end if;
    pos_ini_fecha := instr(reg_summary.INTERFACE_NAME, '_YYYYMMDD');
    if (pos_ini_fecha > 0) then
      pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_${FCH_DATOS}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
    end if;
    /* (20160225) Angel Ruiz */
    pos_ini_hora := instr(nombre_interface_a_cargar, 'HH24MISS');
    if (pos_ini_hora > 0) then
      pos_fin_hora := pos_ini_hora + length ('HH24MISS');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
    end if;
    /*****************************/
    nombre_flag_a_cargar := substr (nombre_interface_a_cargar, 1, instr(nombre_interface_a_cargar, '.')) || 'flag';
    UTL_FILE.put_line(fich_salida_sh, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_sh, '#############################################################################');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Telefonica Moviles Mexico SA DE CV                                        #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Archivo    :       load_SA_' ||  reg_summary.CONCEPT_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Autor      : <SYNAPSYS>.                                                  #');
    UTL_FILE.put_line(fich_salida_sh, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || '.        #');
    UTL_FILE.put_line(fich_salida_sh, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Historia : 31-Octubre-2014 -> Creacion                                    #');
    UTL_FILE.put_line(fich_salida_sh, '# Caja de Control - M :                                                     #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
    UTL_FILE.put_line(fich_salida_sh, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Caducidad del Requerimiento :                                             #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Dependencias :                                                            #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Usuario:                                                                  #');   
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Telefono:                                                                 #');   
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '#############################################################################');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '#Obtiene los password de base de datos                                         #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    --UTL_FILE.put_line(fich_salida_sh, 'ObtenContrasena()');
    --UTL_FILE.put_line(fich_salida_sh, '{');
    --UTL_FILE.put_line(fich_salida_sh, '   #Se especifican parametros usuario y la BD');
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    --UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_sh, '   #Obtenemos el password de la base de datos');
    --UTL_FILE.put_line(fich_salida_sh, '   TraePass ${BD_SID} ${USER}');
    --UTL_FILE.put_line(fich_salida_sh, '   if [ $? -ne 0 ] ;');
    --UTL_FILE.put_line(fich_salida_sh, '   then');
    --UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}: No se encontro el PASSWORD en la libreria para la base de datos: ${BD_SID}."');
    --UTL_FILE.put_line(fich_salida_sh, '      echo "No se encontro el PASSWORD en la libreria para la base de datos: ${BD_SID}." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    --UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    --UTL_FILE.put_line(fich_salida_sh, '      exit 1;');
    --UTL_FILE.put_line(fich_salida_sh, '   fi');
    --UTL_FILE.put_line(fich_salida_sh, '   if [ "${PASSWORD}" = "" ] ; then');
    --UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}: Error no se pudo obtener el password para el usuario $2 y BD $1"');
    --UTL_FILE.put_line(fich_salida_sh, '   		echo "${INTERFAZ}: Error no se pudo obtener el password para el usuario $2 y BD $1" | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    --UTL_FILE.put_line(fich_salida_sh, '   		${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    --UTL_FILE.put_line(fich_salida_sh, '   		echo `date`');
    --UTL_FILE.put_line(fich_salida_sh, '   		exit 1;');
    --UTL_FILE.put_line(fich_salida_sh, '   fi');
    --UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_sh, '   #Validamos que se pueda conectar a la base de datos');
    --UTL_FILE.put_line(fich_salida_sh, '   ChkConexion ${BD_SID} ${USER} ${PASSWORD}');
    --UTL_FILE.put_line(fich_salida_sh, '   if [ $? -ne 0 ]');
    --UTL_FILE.put_line(fich_salida_sh, '   then');
    --UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}:No se pudo conectar a la BD, [Conexion a base de datos]"');
    --UTL_FILE.put_line(fich_salida_sh, '      echo "${INTERFAZ}: No se pudo conectar a la BD: ${BD_SID}, USER=${USER}, PASSWORD=${PASSWORD}." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    --UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    --UTL_FILE.put_line(fich_salida_sh, '      exit 1;');
    --UTL_FILE.put_line(fich_salida_sh, '   fi');
    --UTL_FILE.put_line(fich_salida_sh, '   return 0');
    --UTL_FILE.put_line(fich_salida_sh, '}');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_sh, '{');
    UTL_FILE.put_line(fich_salida_sh, '   #Se especifican parametros usuario y la BD');
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    UTL_FILE.put_line(fich_salida_sh, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_SA_' || reg_summary.CONCEPT_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_sh, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_sh, '   then');
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
    UTL_FILE.put_line(fich_salida_sh, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1;');
    UTL_FILE.put_line(fich_salida_sh, '   fi');
    UTL_FILE.put_line(fich_salida_sh, '   return 0');
    UTL_FILE.put_line(fich_salida_sh, '}');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_sh, '{');
    UTL_FILE.put_line(fich_salida_sh, '   #Se especifican parametros usuario y la BD');
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    UTL_FILE.put_line(fich_salida_sh, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_SA_' || reg_summary.CONCEPT_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" systimestamp ${FCH_DATOS} ${FCH_CARGA} ${TOT_INSERTADOS} 0 0 ${TOT_LEIDOS} ${TOT_RECHAZADOS}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_sh, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_sh, '   then');
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
    UTL_FILE.put_line(fich_salida_sh, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1;');
    UTL_FILE.put_line(fich_salida_sh, '   fi');
    UTL_FILE.put_line(fich_salida_sh, '   return 0');
    UTL_FILE.put_line(fich_salida_sh, '}');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos si el numero de parametros es el correcto');
    UTL_FILE.put_line(fich_salida_sh, 'if [ $# -ne 3 ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT}');        
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '# Recogida de parametros');
    UTL_FILE.put_line(fich_salida_sh, 'FCH_CARGA=${1}');
    UTL_FILE.put_line(fich_salida_sh, 'FCH_DATOS=${2}');
    UTL_FILE.put_line(fich_salida_sh, 'BAN_FORZADO=${3}');
    UTL_FILE.put_line(fich_salida_sh, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
    --UTL_FILE.put_line(fich_salida_sh, 'FECHA_HORA = ﻿`date +%d/%m/%Y\ %H:%M:%S`');
    --UTL_FILE.put_line(fich_salida_sh, 'echo "load_SA_' || reg_summary.CONCEPT_NAME || '" > ${MVNO_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ! -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    --UTL_FILE.put_line(fich_salida_sh, 'set -x');
    UTL_FILE.put_line(fich_salida_sh, '#Permite los acentos y U');
    UTL_FILE.put_line(fich_salida_sh, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
    UTL_FILE.put_line(fich_salida_sh, 'export NLS_LANG');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'REQ_NUM="' || v_REQ_NUMER || '"');
    --UTL_FILE.put_line(fich_salida_sh, 'REQ_NUM="Req89208"');
    UTL_FILE.put_line(fich_salida_sh, 'INTERFAZ=' || v_REQ_NUMER || '_load_SA_' || reg_summary.CONCEPT_NAME);
    --UTL_FILE.put_line(fich_salida_sh, 'INTERFAZ=Req89208_load_SA_' || reg_summary.CONCEPT_NAME);
    
    --UTL_FILE.put_line(fich_salida_sh, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_REQ=/reportes/requerimientos/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SHELL=${PATH_REQ}shells/${REQ_NUM}/shell/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SQL=${PATH_REQ}shells/${REQ_NUM}/sql/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_TEMP=${PATH_REQ}salidas/${REQ_NUM}/TEMP/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_ENVIA_SMS=/dbdata24/requerimientos/shells/Utilerias/EnviaSMS/');
    --UTL_FILE.put_line(fich_salida_sh, 'else');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_REQ=/reportes/URC/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SHELL=${PATH_REQ}Shells/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SQL=${PATH_REQ}sql/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_TEMP=${PATH_REQ}TEMP/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_ENVIA_SMS=/dbdata24/requerimientos/shells/Utilerias/EnviaSMS/');
    --UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# LIBRERIAS                                                                    #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
    --UTL_FILE.put_line(fich_salida_sh, '# Se levantan las variables de ORACLE.');
    --UTL_FILE.put_line(fich_salida_sh, 'LdVarOra');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    --UTL_FILE.put_line(fich_salida_sh, '  BD_MVNO=UBITEL');
    --UTL_FILE.put_line(fich_salida_sh, '  USR_MVNO=ubitel_own');
    --UTL_FILE.put_line(fich_salida_sh, '  PWD_MVNO=');
    UTL_FILE.put_line(fich_salida_sh, 'else');
    UTL_FILE.put_line(fich_salida_sh, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    --UTL_FILE.put_line(fich_salida_sh, '  BD_MVNO=BIDESA');
    --UTL_FILE.put_line(fich_salida_sh, '  USR_MVNO=ubitel_own');
    --UTL_FILE.put_line(fich_salida_sh, '  PWD_MVNO=');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
    UTL_FILE.put_line(fich_salida_sh, 'BD_CLAVE=${PASSWORD}');
    UTL_FILE.put_line(fich_salida_sh, 'ULT_PASO_EJECUTADO=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF');
    UTL_FILE.put_line(fich_salida_sh, 'WHENEVER SQLERROR EXIT 1;');
    UTL_FILE.put_line(fich_salida_sh, 'WHENEVER OSERROR EXIT 2;');
    UTL_FILE.put_line(fich_salida_sh, 'SET PAGESIZE 0;');
    UTL_FILE.put_line(fich_salida_sh, 'SET HEADING OFF;');
    UTL_FILE.put_line(fich_salida_sh, '  SELECT nvl(MAX(' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PASO),0)');
    UTL_FILE.put_line(fich_salida_sh, '  FROM');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' || OWNER_MTDT || '.MTDT_PASO');
    UTL_FILE.put_line(fich_salida_sh, '  WHERE');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.FCH_CARGA = to_date(''${FCH_CARGA}'', ''yyyymmdd'') AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.FCH_DATOS = to_date(''${FCH_DATOS}'', ''yyyymmdd'') AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PROCESO.NOMBRE_PROCESO = ''${0}'' AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PROCESO.CVE_PROCESO = ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PROCESO AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PROCESO.CVE_PROCESO = ' || OWNER_MTDT || '.MTDT_PASO.CVE_PROCESO AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_PASO.CVE_PASO = ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_PASO AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO.CVE_RESULTADO = 0;');
    UTL_FILE.put_line(fich_salida_sh, 'QUIT;');
    UTL_FILE.put_line(fich_salida_sh, 'EOF`');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${ULT_PASO_EJECUTADO} -eq 1 ] && [ "${BAN_FORZADO}" = "N" ]');
    UTL_FILE.put_line(fich_salida_sh, 'then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Ya se ejecutaron Ok todos los pasos de este proceso."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  exit 0');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'INICIO_PASO_TMR=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF');
    UTL_FILE.put_line(fich_salida_sh, 'WHENEVER SQLERROR EXIT 1;');
    UTL_FILE.put_line(fich_salida_sh, 'WHENEVER OSERROR EXIT 2;');
    UTL_FILE.put_line(fich_salida_sh, 'SET PAGESIZE 0;');
    UTL_FILE.put_line(fich_salida_sh, 'SET HEADING OFF;');
    UTL_FILE.put_line(fich_salida_sh, 'SELECT cast (systimestamp as timestamp) from dual;');
    UTL_FILE.put_line(fich_salida_sh, 'QUIT;');
    UTL_FILE.put_line(fich_salida_sh, 'EOF`');
    --UTL_FILE.put_line(fich_salida_sh, 'echo "Inicio de la carga de la tabla de staging ' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '."' || ' >> ' || '$MVNO_LOG/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_$FCH_CARGA.log');
    UTL_FILE.put_line(fich_salida_sh, '');
    --if (reg_summary.DELAYED = 'S') then
    /* Significa que pueden venir retrasados */
    /* Hay que gestionar la llegada de retrasados con el particionado */
    /* (20141219) Angel Ruiz. Finalmente todos los procesos van a llamar a un pro-procesado para truncar tablsa o particiones antes de ejecutar el sqlploader*/
    UTL_FILE.put_line(fich_salida_sh, '# Llamada al proceso previo al loader para el truncado de la tabla de STAGIN');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '# Llamada a sql_plus');
    UTL_FILE.put_line(fich_salida_sh, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
    UTL_FILE.put_line(fich_salida_sh, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
    UTL_FILE.put_line(fich_salida_sh, 'whenever sqlerror exit 1;');
    UTL_FILE.put_line(fich_salida_sh, 'whenever oserror exit 2;');
    UTL_FILE.put_line(fich_salida_sh, 'set feedback off;');
    UTL_FILE.put_line(fich_salida_sh, 'set serveroutput on;');
    UTL_FILE.put_line(fich_salida_sh, 'set echo on;');
    UTL_FILE.put_line(fich_salida_sh, 'set pagesize 0;');
    UTL_FILE.put_line(fich_salida_sh, 'set verify off;');
    UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_load, 'declare');
    --UTL_FILE.put_line(fich_salida_load, '  num_filas_insertadas number;');
    UTL_FILE.put_line(fich_salida_sh, 'begin');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_SA || '.pkg_' || nombre_proceso || '.' || 'pre_' || nombre_proceso || ' (''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
    UTL_FILE.put_line(fich_salida_sh, 'end;');
    UTL_FILE.put_line(fich_salida_sh, '/');
    UTL_FILE.put_line(fich_salida_sh, 'exit 0;');
    UTL_FILE.put_line(fich_salida_sh, 'EOF');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a pre_' || nombre_proceso || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_sh, '# Antes de llamar al sqlloader preparamos el fichero de control del loader (.ctl)');
    --UTL_FILE.put_line(fich_salida_sh, '# para que cargue la fecha de datos correspondiente a la pasada por parametro');
    --UTL_FILE.put_line(fich_salida_sh, '# Llamada a sqlldr');
    --UTL_FILE.put_line(fich_salida_sh, 'sed ''s/_YYYYMMDD/${FCH_DATOS/'' ' || '${MVNO_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl > ' || '${MVNO_TMP}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl');
    --UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_sh, 'sqlldr ${BD_USUARIO}/${BD_CLAVE} DATA=${MVNO_FUENTE}/' || nombre_interface_a_cargar || ' \'); 
    --UTL_FILE.put_line(fich_salida_sh, 'CONTROL=${MVNO_TMP}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl \' );
    --UTL_FILE.put_line(fich_salida_sh, 'LOG=${MVNO_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_$FCH_CARGA' || '.log \');
    --UTL_FILE.put_line(fich_salida_sh, 'BAD=${MVNO_DESCARTADOS}/' || 'DMDIST_' || reg_summary.COUNTRY || '_' || reg_summary.SOURCE || '_' || reg_summary.CONCEPT_NAME || '_$FCH_CARGA' || '.bad ' ||  '>> ' || '${MVNO_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_$FCH_CARGA.log ' || '2>&' || '1');
    --UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
    --UTL_FILE.put_line(fich_salida_sh, '');
    --UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
    --UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlloader en la carga de la tabla de staging ' || 'SA_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
    --UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    --UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${MVNO_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_$FCH_CARGA.log');    
    --UTL_FILE.put_line(fich_salida_sh, '  echo `date`');
    --UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
    --UTL_FILE.put_line(fich_salida_sh, '  exit 1');    
    --UTL_FILE.put_line(fich_salida_sh, 'fi');    
    --UTL_FILE.put_line(fich_salida_sh, 'rm ${MVNO_TMP}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl');
    --UTL_FILE.put_line(fich_salida_sh, '');
    --else
    --end if;
    /* (20150225) ANGEL RUIZ. Aparecen HH24MISS como parte del nombre en el DM Distribucion */
    /* (20150827) ANGEL RUIZ. He comentado el IF de despues porque no funcionaba cuando el fichero viene sin HHMMSS*/
    --if (pos_ini_hora > 0) then
      UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=`ls -1 ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar ||'`');
      --UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_FLAG=`ls -1 ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_flag_a_cargar ||'`');
    --end if;    
    /****************************/
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos que los ficheros a cargar existen');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "${NOMBRE_FICH_CARGA:-SIN_VALOR}" = "SIN_VALOR" ] ; then');
    if (reg_summary.FREQUENCY = 'E') then
      /* Se trata de una carga eventual, por lo que a veces el fichero puede no venir y entonces no debe acabar con error */
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen fichero para cargar. El fichero es de carga eventual. No hay error.' || '${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || '."');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinOK');
      UTL_FILE.put_line(fich_salida_sh, '    exit 0');
    else
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen ficheros para cargar. ' || '${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || '."');
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');
    end if;
    UTL_FILE.put_line(fich_salida_sh, 'else');
    UTL_FILE.put_line(fich_salida_sh, '  for FILE in ${NOMBRE_FICH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, '  do');
    UTL_FILE.put_line(fich_salida_sh, '    NAME_FLAG=`echo $FILE | sed -e ''s/\.[Dd][Aa][Tt]/\.flag/''`');
    UTL_FILE.put_line(fich_salida_sh, '    if [ ! -f ${FILE} ] || [ ! -f ${NAME_FLAG} ] ; then');    
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}: No existe fichero o su fichero de flag a cargar. ' || '${FILE}' || '."');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
    UTL_FILE.put_line(fich_salida_sh, '      echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '      InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1');    
    UTL_FILE.put_line(fich_salida_sh, '    fi');
    UTL_FILE.put_line(fich_salida_sh, '  done');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    /*(20160715) Angel Ruiz. Nueva Funcionalidad. Escribir el nombre del fichero cargado en una columna de la tabla de Staging */
    if (nombre_fich_cargado = 'Y') then
    /* (20150605) Angel Ruiz. AÑADIDO PARA CHEQUEAR LA CALIDAD DEL DATO */
      UTL_FILE.put_line(fich_salida_sh, '# Cargamos los ficheros');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'for FILE in ${NOMBRE_FICH_CARGA}');
      UTL_FILE.put_line(fich_salida_sh, 'do');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS=`basename ${FILE}`');
      --UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`basename ${FILE%.*}`.ctl');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/\.ctl/''`');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS_T=`echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/_/''`');
      --UTL_FILE.put_line(fich_salida_sh, '  cat ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl | sed "s/MY_FILE/${NOMBRE_FICH_DATOS}/g" > ' || '${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  sed "s/MY_FILE/${NOMBRE_FICH_DATOS}/" ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl > '  || '${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  # Llamada a sqlldr');
      UTL_FILE.put_line(fich_salida_sh, '  sqlldr ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} DATA=${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/${NOMBRE_FICH_DATOS} \'); 
      UTL_FILE.put_line(fich_salida_sh, '  CONTROL=${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL} \' );
      --UTL_FILE.put_line(fich_salida_sh, '  LOG=${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS%.*}_${FECHA_HORA}' || '.log \');
      UTL_FILE.put_line(fich_salida_sh, '  LOG=${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}_${FECHA_HORA}' || '.log \');
      UTL_FILE.put_line(fich_salida_sh, '  BAD=${' || NAME_DM || '_DESCARTADOS}/ ' ||  '>> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: Surgio un error en el sqlloader en la carga de la tabla de staging ' || 'SA_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '    #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '    rm ${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');    
      UTL_FILE.put_line(fich_salida_sh, '  fi');    
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '  rm ${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  REG_LEIDOS=`grep "^Total logical records read:" ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}_${FECHA_HORA}' || '.log | cut -d":" -f2 | sed ''s/ *//''`');
      UTL_FILE.put_line(fich_salida_sh, '  REG_INSERTADOS=`grep "Rows* successfully loaded." ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}_${FECHA_HORA}' || '.log | sed ''s/^ *//'' | cut -d" " -f1`');
      UTL_FILE.put_line(fich_salida_sh, '  REG_RECHAZADOS=`grep "^Total logical records rejected:" ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}_${FECHA_HORA}' || '.log | cut -d":" -f2 | sed ''s/ *//''`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_LEIDOS=`expr ${TOT_LEIDOS} + ${REG_LEIDOS}`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_INSERTADOS=`expr ${TOT_INSERTADOS} + ${REG_INSERTADOS}`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_RECHAZADOS=`expr ${TOT_RECHAZADOS} + ${REG_RECHAZADOS}`');
      UTL_FILE.put_line(fich_salida_sh, '');
      
      UTL_FILE.put_line(fich_salida_sh, 'done');
      /* (20150605) FIN */
    else
      UTL_FILE.put_line(fich_salida_sh, '# Llamada a sqlldr');
      UTL_FILE.put_line(fich_salida_sh, '  sqlldr ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} DATA=${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || ' \'); 
      --UTL_FILE.put_line(fich_salida_sh, '  sqlldr ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} DATA=${NOMBRE_FICH_CARGA}' || ' \'); 
      UTL_FILE.put_line(fich_salida_sh, '  CONTROL=${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl \' );
      UTL_FILE.put_line(fich_salida_sh, '  LOG=${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log \');
      --UTL_FILE.put_line(fich_salida_sh, '  BAD=${' || NAME_DM || '_DESCARTADOS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.bad ' ||  '>> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');
      UTL_FILE.put_line(fich_salida_sh, '  BAD=${' || NAME_DM || '_DESCARTADOS}/ ' ||  '>> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlloader en la carga de la tabla de staging ' || 'SA_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '  exit 1');    
      UTL_FILE.put_line(fich_salida_sh, 'fi');    
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'REG_LEIDOS=`grep "^Total logical records read:" ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log | cut -d":" -f2 | sed ''s/ *//''`');
      UTL_FILE.put_line(fich_salida_sh, 'REG_INSERTADOS=`grep "Rows* successfully loaded." ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log | sed ''s/^ *//'' | cut -d" " -f1`');
      UTL_FILE.put_line(fich_salida_sh, 'REG_RECHAZADOS=`grep "^Total logical records rejected:" ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log | cut -d":" -f2 | sed ''s/ *//''`');
      UTL_FILE.put_line(fich_salida_sh, '');
    end if;
    /* (20151108) Angel Ruiz. BUG: El paso a historico de las tablas de staging se hace despues de haber llevado a cabo la carga */
    if (reg_summary.HISTORY IS NOT NULL) then
      UTL_FILE.put_line(fich_salida_sh, '# Llevamos a cabo el paso a historico');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '# Llamada a sql_plus');
      UTL_FILE.put_line(fich_salida_sh, 'sqlplus -s /nolog <<EOF >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
      UTL_FILE.put_line(fich_salida_sh, 'connect ${BD_USUARIO}/${BD_CLAVE}@${BD_SID}');
      UTL_FILE.put_line(fich_salida_sh, 'whenever sqlerror exit 1;');
      UTL_FILE.put_line(fich_salida_sh, 'whenever oserror exit 2;');
      UTL_FILE.put_line(fich_salida_sh, 'set feedback off;');
      UTL_FILE.put_line(fich_salida_sh, 'set serveroutput on;');
      UTL_FILE.put_line(fich_salida_sh, 'set echo on;');
      UTL_FILE.put_line(fich_salida_sh, 'set pagesize 0;');
      UTL_FILE.put_line(fich_salida_sh, 'set verify off;');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'begin');
      UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_SA || '.pkg_' || nombre_proceso || '.' || 'pos_' || nombre_proceso || ' (''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
      UTL_FILE.put_line(fich_salida_sh, 'end;');
      UTL_FILE.put_line(fich_salida_sh, '/');
      UTL_FILE.put_line(fich_salida_sh, 'exit 0;');
      UTL_FILE.put_line(fich_salida_sh, 'EOF');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a pos_' || nombre_proceso || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');        
      UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '  exit 1');
      UTL_FILE.put_line(fich_salida_sh, 'fi');
      UTL_FILE.put_line(fich_salida_sh, '');
    end if;
    /* (20151108) Angel Ruiz. Fin BUG: */
    /*(20160715) Angel Ruiz. Nueva Funcionalidad.*/
    UTL_FILE.put_line(fich_salida_sh, '# Insertamos que el proceso y el paso se han Ejecutado Correctamente');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinOK');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_MVNO.inserta_monitoreo en la carga de SA_' || reg_summary.CONCEPT_NAME || '. Error  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'echo "La carga de la tabla ' ||  'SA_' || reg_summary.CONCEPT_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '# Movemos el fichero cargado a /' || NAME_DM || '/MEX/DESTINO');    
    UTL_FILE.put_line(fich_salida_sh, 'if [ ! -d ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  mkdir ${' || NAME_DM || '_DESTINO}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    --UTL_FILE.put_line(fich_salida_sh, 'mv ${NOMBRE_FICH_CARGA}' || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    UTL_FILE.put_line(fich_salida_sh, 'mv ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    --UTL_FILE.put_line(fich_salida_sh, 'mv ${NOMBRE_FICH_FLAG}' || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    UTL_FILE.put_line(fich_salida_sh, 'mv ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_flag_a_cargar || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    UTL_FILE.put_line(fich_salida_sh, 'exit 0');    
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
      
      UTL_FILE.FCLOSE (fich_salida);
      UTL_FILE.FCLOSE (fich_salida_sh);
      
      
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;
