declare


nombre_proceso                        VARCHAR2(30);
nombre_tabla_base_redu        VARCHAR2(30);
nombre_tabla_base_sp_redu  VARCHAR2(30);
nombre_tabla_reducido           VARCHAR2(30);
  
cursor MTDT_TABLA
  is
    SELECT
      DISTINCT TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(mtdt_modelo_logico.TABLESPACE) "TABLESPACE"
    FROM
      MTDT_TC_SCENARIO, mtdt_modelo_logico
    WHERE MTDT_TC_SCENARIO.TABLE_TYPE = 'H' and
    trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_logico.TABLE_NAME);
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_TRAFD_CU_MVNO', 'DMF_TRAFE_CU_MVNO', 'DMF_TRAFV_CU_MVNO');
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('DMF_MOVIMIENTOS_MVNO', 'DMF_RECARGAS_MVNO', 'DMF_PARQUE_MVNO');  




  
  reg_tabla MTDT_TABLA%rowtype;     

  
  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(30);

  

  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);




begin
  /* (20141223) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  /* (20141223) FIN*/
  dbms_output.put_line('set echo on;');
  dbms_output.put_line('whenever sqlerror exit 1;');
  dbms_output.put_line(''); 
  
  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    /* Angel Ruiz (20141201) Hecho porque hay paquetes que no compilan */
     if (length(reg_tabla.TABLE_NAME) < 25) then
      nombre_proceso := reg_tabla.TABLE_NAME;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    dbms_output.put_line ('DROP PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ';');
  end loop;
  close MTDT_TABLA;
  dbms_output.put_line(''); 
  dbms_output.put_line('set echo off;');
  dbms_output.put_line('exit SUCCESS;');
end;

