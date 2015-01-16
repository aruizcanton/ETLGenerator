declare

cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE"
    FROM
      MTDT_TC_SCENARIO
    WHERE TABLE_TYPE in ('D', 'I')
    order by
    TABLE_TYPE;

  
  reg_tabla MTDT_TABLA%rowtype;
      

  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_base_redu        VARCHAR2(30);
  nombre_tabla_base_sp_redu  VARCHAR2(30);
  nombre_tabla_reducido           VARCHAR2(30);
  num_sce_integra number(2) := 0;
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);

begin
  /* (20141222) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  /* (20141222) FIN*/
  dbms_output.put_line('set echo on;');
  dbms_output.put_line('whenever sqlerror exit 1;');
  dbms_output.put_line(''); 
  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    if (reg_tabla.TABLE_TYPE = 'D') 
    then
            nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
            /* Angel Ruiz (20141201) Hecho porque hay paquetes que no compilan */
             if (length(reg_tabla.TABLE_NAME) < 25) then
              nombre_proceso := reg_tabla.TABLE_NAME;
            else
              nombre_proceso := nombre_tabla_reducido;
            end if;
        
            dbms_output.put_line('DROP PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ';');
        
    end if;
    if (reg_tabla.TABLE_TYPE = 'I')
    then
            nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 4); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
        
            dbms_output.put_line ('DROP PACKAGE ' || OWNER_SA || '.pkg_' || reg_tabla.TABLE_NAME || ';');
    end if;
  end loop;   
  close MTDT_TABLA;
  dbms_output.put_line(''); 
  dbms_output.put_line('set echo off;');
  dbms_output.put_line('exit SUCCESS;');
end;

