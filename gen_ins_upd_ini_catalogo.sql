DECLARE
  CURSOR  CUR_dtd_permited_values  
  is
    select 
      TRIM(ID_LIST) "ID_LIST",
      TRIM(CVE) "CVE",
      TRIM(ITEM_NAME) "ITEM_NAME",
      TRIM(VALUE) "VALUE",
      TRIM(DESCRIPTION) DESCRIPTION,
      TRIM(AGREGATION) "AGREGATION",
      TRIM(VALUE_AGREGATION) "VALUE_AGREGATION",
      FCH_REGISTRO,
      FCH_MODIFICACION
    FROM  MTDT_PERMITED_VALUES
    order by 
      ID_LIST, 
      CVE;
      
  reg_per_val CUR_dtd_permited_values%rowtype;
  cve_foraneo NUMBER(10);
  pos_guion integer;
  valor_min varchar(20);
  valor_max varchar(20);
  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  PREFIJO_DM                            VARCHAR2(60);
  
BEGIN
  /* (20141220) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  /* (20141220) FIN*/

  DBMS_OUTPUT.put_line('set echo on;');
  DBMS_OUTPUT.put_line('set define off;');    /* (20150120 (Angel Ruiz) Anyadido por si vinieran unpersand para que no saque el prompt */
  DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
  DBMS_OUTPUT.put_line('');
  OPEN CUR_dtd_permited_values;
  LOOP
    
    fetch CUR_dtd_permited_values into reg_per_val;
    EXIT WHEN CUR_dtd_permited_values%NOTFOUND;

    IF (reg_per_val.FCH_REGISTRO = reg_per_val.FCH_MODIFICACION)
    /* ESTAMOS EN LA INSERCION INICIAL EN LAS TABLAS CATALOGOS */
    /* GENERAREMOS LOS INSERTS PARA ESTA CARGA INICIAL Y UNICA */
    THEN
      IF (regexp_count(reg_per_val.AGREGATION,'^CVE_',1,'i') >0)
      THEN
        /* Esta tabla de catalogo tiene una clave foranea */
        /* La tratamos de diferente manera */
        /*
        DBMS_OUTPUT.put_line('insert into DMD_' || reg_per_val.ITEM_NAME || '(' || 'CVE_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_' || reg_per_val.ITEM_NAME || ', ' || 'DES_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_LIST, ID_FUENTE,');  
        DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ',');
        DBMS_OUTPUT.put_line('FCH_REGISTRO, ' ||  'FCH_MODIFICACION)');
        DBMS_OUTPUT.put_line('SELECT ' || reg_per_val.CVE || ', ''' || reg_per_val.VALUE || ''',');  
        DBMS_OUTPUT.put_line('''' || reg_per_val.DESCRIPTION || ''',');
        DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''', ''MAN'', '); 
        DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ', ');         
        DBMS_OUTPUT.put_line('sysdate, sysdate FROM ' || 'DMD_' || substr(reg_per_val. AGREGATION,5));
        DBMS_OUTPUT.put_line( ' WHERE ID_' || substr(reg_per_val. AGREGATION,5) || ' = ''' || TRIM(reg_per_val.VALUE_AGREGATION) || ''';');
        */
        
        /*+++++++++++++++*/

        DBMS_OUTPUT.put_line('insert into ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || '(' || 'CVE_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_' || reg_per_val.ITEM_NAME || ', ' || 'DES_' || reg_per_val.ITEM_NAME || ',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line('ID_LIST, ID_FUENTE,');
          end if;
        else
          DBMS_OUTPUT.put_line('ID_LIST,');
        end if;
        DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ',');
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          DBMS_OUTPUT.put_line('  MIN_' || reg_per_val.ITEM_NAME || ', ' || '  MAX_' || reg_per_val.ITEM_NAME || ',');
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('FCH_REGISTRO, ' ||  'FCH_MODIFICACION)');
        DBMS_OUTPUT.put_line('VALUES (' || reg_per_val.CVE || ', ''' || trim(reg_per_val.VALUE) || ''',');  
        DBMS_OUTPUT.put_line('''' || replace(reg_per_val.DESCRIPTION, '''', '''''') || ''',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''', ''MAN'',');
          end if;
        else
          DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''',');
        end if;
        DBMS_OUTPUT.put_line(reg_per_val.VALUE_AGREGATION || ',');
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          pos_guion := instr (reg_per_val.VALUE, '-');
          IF (pos_guion > 0) THEN
            /* Hemos encontrado el guion */
            valor_min := trim(substr (reg_per_val.VALUE, 1, pos_guion-1));
            valor_max := trim(substr (reg_per_val.VALUE, pos_guion +1));
            DBMS_OUTPUT.put_line( valor_min || ', ' ||  valor_max || ',');
          ELSE
            /* Vemos si encontramos el simbolo ">"  */
            pos_guion := instr (reg_per_val.VALUE, '>');
            IF (pos_guion > 0) THEN
              /* Hemos encontrado el simbolo ">"  */
              valor_min := trim(substr (reg_per_val.VALUE, pos_guion+1));
              DBMS_OUTPUT.put_line( valor_min || ', ' ||  'NULL' || ',');
            ELSE
              /* si no hemos encontrado ningun simbolo reconocible es que no se   */
              /* se ha introducido un valor correcto en el metamodelo y dejamos los campos a NULL */
              DBMS_OUTPUT.put_line( 'NULL' || ', ' ||  'NULL' || ',');
            END IF;
          END IF;
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('sysdate, sysdate);');
        DBMS_OUTPUT.put_line('commit;');
        DBMS_OUTPUT.put_line('');
        
        /*+++++++++++++++*/
        
      ELSE
        DBMS_OUTPUT.put_line('insert into ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || '(' || 'CVE_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_' || reg_per_val.ITEM_NAME || ', ' || 'DES_' || reg_per_val.ITEM_NAME || ',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line('ID_LIST, ID_FUENTE,');
          end if;
        else
          DBMS_OUTPUT.put_line('ID_LIST,');  
        end if;
         /* HAY VALOR EN LA COLUMNA AGREGATION  */
        IF (regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0)
        THEN
          DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ',');
        END IF;
        /*FIN TRATAMIENTO VALOR COLUMNA AGREGATION*/
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          DBMS_OUTPUT.put_line('  MIN_' || reg_per_val.ITEM_NAME || ', ' || '  MAX_' || reg_per_val.ITEM_NAME || ',');
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('FCH_REGISTRO, ' ||  'FCH_MODIFICACION)');
        DBMS_OUTPUT.put_line('VALUES (' || reg_per_val.CVE || ', ''' || trim(reg_per_val.VALUE) || ''',');  
        DBMS_OUTPUT.put_line('''' || replace(reg_per_val.DESCRIPTION, '''', '''''') || ''',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''', ''MAN'',');
          end if;
        else
          DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''','); 
        end if;
        /* HAY VALOR EN LA COLUMNA AGREGATION  */
        IF (regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0)
        THEN
          DBMS_OUTPUT.put_line('''' || reg_per_val.VALUE_AGREGATION || ''',');
        END IF;
         
        /*FIN TRATAMIENTO VALOR COLUMNA AGREGATION*/
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          pos_guion := instr (reg_per_val.VALUE, '-');
          IF (pos_guion > 0) THEN
            /* Hemos encontrado el guion */
            valor_min := trim(substr (reg_per_val.VALUE, 1, pos_guion-1));
            valor_max := trim(substr (reg_per_val.VALUE, pos_guion +1));
            DBMS_OUTPUT.put_line( valor_min || ', ' ||  valor_max || ',');
          ELSE
            /* Vemos si encontramos el simbolo ">"  */
            pos_guion := instr (reg_per_val.VALUE, '>');
            IF (pos_guion > 0) THEN
              /* Hemos encontrado el simbolo ">"  */
              valor_min := trim(substr (reg_per_val.VALUE, pos_guion+1));
              DBMS_OUTPUT.put_line( valor_min || ', ' ||  'NULL' || ',');
            ELSE
              /* si no hemos encontrado ningun simbolo reconocible es que no se   */
              /* se ha introducido un valor correcto en el metamodelo y dejamos los campos a NULL */
              DBMS_OUTPUT.put_line( 'NULL' || ', ' ||  'NULL' || ',');
            END IF;
          END IF;
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('sysdate, sysdate);');
        DBMS_OUTPUT.put_line('commit;');
        DBMS_OUTPUT.put_line('');
      END IF;
    ELSE
      /* ESTAMOS EN LA ACTUALIZACION POSTERIORES DE LOS CATALOGOS */
      /* GENERAREMOS LOS UPDATES  */
      /* Los updates los vamos a implementar con un DELETE seguido de un INSERT */
      DBMS_OUTPUT.put_line('delete ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ' WHERE ' || 'CVE_' || reg_per_val.ITEM_NAME || ' = ' || reg_per_val.CVE || ';');
      /* Generamos el insert que casi una copia identica de la insercion que hacemos la primera vez */
      /* Solo cambia el campo FCH_MODIFICACION */
      IF (regexp_count(reg_per_val.AGREGATION,'^CVE_',1,'i') >0)
      THEN
        /* Esta tabla de catalogo tiene una clave foranea */
        /* La tratamos de diferente manera */
        /*
        DBMS_OUTPUT.put_line('insert into DMD_' || reg_per_val.ITEM_NAME || '(' || 'CVE_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_' || reg_per_val.ITEM_NAME || ', ' || 'DES_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_LIST, ID_FUENTE,');  
        DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ',');
        DBMS_OUTPUT.put_line('FCH_REGISTRO, ' ||  'FCH_MODIFICACION)');
        DBMS_OUTPUT.put_line('SELECT ' || reg_per_val.CVE || ', ''' || reg_per_val.VALUE || ''',');  
        DBMS_OUTPUT.put_line('''' || reg_per_val.DESCRIPTION || ''',');
        DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''', ''MAN'', '); 
        DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ', '); 
        DBMS_OUTPUT.put_line('''' || reg_per_val.FCH_REGISTRO || ''', sysdate FROM ' || 'DMD_' || substr(reg_per_val. AGREGATION,5));
        DBMS_OUTPUT.put_line( ' WHERE ID_' || substr(reg_per_val. AGREGATION,5) || ' = ''' || TRIM(reg_per_val.VALUE_AGREGATION) || ''';');
        */
        
        DBMS_OUTPUT.put_line('insert into ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || '(' || 'CVE_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_' || reg_per_val.ITEM_NAME || ', ' || 'DES_' || reg_per_val.ITEM_NAME || ',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line('ID_LIST, ID_FUENTE,');
          end if;
        else
          DBMS_OUTPUT.put_line('ID_LIST,');
        end if;
        DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ',');
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          DBMS_OUTPUT.put_line('  MIN_' || reg_per_val.ITEM_NAME || ', ' || '  MAX_' || reg_per_val.ITEM_NAME || ',');
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('FCH_REGISTRO, ' ||  'FCH_MODIFICACION)');
        DBMS_OUTPUT.put_line('VALUES (' || reg_per_val.CVE || ', ''' || trim(reg_per_val.VALUE) || ''',');  
        DBMS_OUTPUT.put_line('''' || replace(reg_per_val.DESCRIPTION, '''', '''''') || ''',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''', ''MAN'',');
          end if;
        else
          DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''',');
        end if;
        DBMS_OUTPUT.put_line(reg_per_val.VALUE_AGREGATION || ',');
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          pos_guion := instr (reg_per_val.VALUE, '-');
          IF (pos_guion > 0) THEN
            /* Hemos encontrado el guion */
            valor_min := trim(substr (reg_per_val.VALUE, 1, pos_guion-1));
            valor_max := trim(substr (reg_per_val.VALUE, pos_guion +1));
            DBMS_OUTPUT.put_line( valor_min || ', ' ||  valor_max || ',');
          ELSE
            /* Vemos si encontramos el simbolo ">"  */
            pos_guion := instr (reg_per_val.VALUE, '>');
            IF (pos_guion > 0) THEN
              /* Hemos encontrado el simbolo ">"  */
              valor_min := trim(substr (reg_per_val.VALUE, pos_guion+1));
              DBMS_OUTPUT.put_line( valor_min || ', ' ||  'NULL' || ',');
            ELSE
              /* si no hemos encontrado ningun simbolo reconocible es que no se   */
              /* se ha introducido un valor correcto en el metamodelo y dejamos los campos a NULL */
              DBMS_OUTPUT.put_line( 'NULL' || ', ' ||  'NULL' || ',');
            END IF;
          END IF;
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('sysdate, sysdate);');
        DBMS_OUTPUT.put_line('commit;');
        DBMS_OUTPUT.put_line('');
        
        /*****************************/
      ELSE
        DBMS_OUTPUT.put_line('insert into ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || '(' || 'CVE_' || reg_per_val.ITEM_NAME || ',');
        DBMS_OUTPUT.put_line('ID_' || reg_per_val.ITEM_NAME || ', ' || 'DES_' || reg_per_val.ITEM_NAME || ',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line('ID_LIST, ID_FUENTE,');
          end if;
        else
          DBMS_OUTPUT.put_line('ID_LIST,');  
        end if;
         /* HAY VALOR EN LA COLUMNA AGREGATION  */
        IF (regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0)
        THEN
          DBMS_OUTPUT.put_line(reg_per_val.AGREGATION || ',');
        END IF;
        /*FIN TRATAMIENTO VALOR COLUMNA AGREGATION*/
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          DBMS_OUTPUT.put_line('  MIN_' || reg_per_val.ITEM_NAME || ', ' || '  MAX_' || reg_per_val.ITEM_NAME || ',');
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('FCH_REGISTRO, ' ||  'FCH_MODIFICACION)');
        DBMS_OUTPUT.put_line('VALUES (' || reg_per_val.CVE || ', ''' || trim(reg_per_val.VALUE) || ''',');  
        DBMS_OUTPUT.put_line('''' || replace(reg_per_val.DESCRIPTION, '''', '''''') || ''',');
        if (reg_per_val.ITEM_NAME <> 'FUENTE') then
          /* Introducido por si el ITEM se llama FUENTE, para que no haya campo duplicados */
          if (reg_per_val.ITEM_NAME <> 'ALMACEN') then /* Metido a posteriori   20150227*/
            DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''', ''MAN'',');
          end if;
        else
          DBMS_OUTPUT.put_line(''''|| reg_per_val.ID_LIST || ''','); 
        end if;
        /* HAY VALOR EN LA COLUMNA AGREGATION  */
        IF (regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0)
        THEN
          DBMS_OUTPUT.put_line('''' || reg_per_val.VALUE_AGREGATION || ''',');
        END IF;
        /*FIN TRATAMIENTO VALOR COLUMNA AGREGATION*/
        /* Cambio efectuado el 21/10/2014 */
        IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
        /* Se trata de una tabla del tipo DMD_RANGO_ */
        /* por lo que ha de tener dos campos mas  */
          pos_guion := instr (reg_per_val.VALUE, '-');
          IF (pos_guion > 0) THEN
            /* Hemos encontrado el guion */
            valor_min := trim(substr (reg_per_val.VALUE, 1, pos_guion-1));
            valor_max := trim(substr (reg_per_val.VALUE, pos_guion +1));
            DBMS_OUTPUT.put_line( valor_min || ', ' ||  valor_max || ',');
          ELSE
            /* Vemos si encontramos el simbolo ">"  */
            pos_guion := instr (reg_per_val.VALUE, '>');
            IF (pos_guion > 0) THEN
              /* Hemos encontrado el simbolo ">"  */
              valor_min := trim(substr (reg_per_val.VALUE, pos_guion+1));
              DBMS_OUTPUT.put_line( valor_min || ', ' ||  'NULL' || ',');
            ELSE
              /* si no hemos encontrado ningun simbolo reconocible es que no se   */
              /* se ha introducido un valor correcto en el metamodelo y dejamos los campos a NULL */
              DBMS_OUTPUT.put_line( 'NULL' || ', ' ||  'NULL' || ',');
            END IF;
          END IF;
        END IF;
        /* Fin cambio */
        DBMS_OUTPUT.put_line('sysdate, sysdate);');
        DBMS_OUTPUT.put_line('commit;');
        DBMS_OUTPUT.put_line('');
      END IF;
    END IF;

  END LOOP;
  CLOSE CUR_dtd_permited_values;
  DBMS_OUTPUT.put_line('set echo off;');
  DBMS_OUTPUT.put_line('exit SUCCESS;');  
END;


    