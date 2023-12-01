#!/bin/bash
#############################################################################
#                                                                           #
#                                                                           #
# Archivo    : Lanza_cadena.sh                                              #
# Autor      :  <SYNAPSYS>.                                                 #
# Proposito  : Shell que ejecuta la cadena De Medallia.                     #
# Parametros : <FCH_CARGA> <FCH_DATOS> <BAN_FORZADO>                        #
#                                                                           #
# Ejecucion  :                                                              #
#                                                                           #
# Historia : 25-03-2023 -> Creacion                                         #
#                                                                           #
# Observaciones:                                                            #
#                                                                           #
#                                                                           #
# Caducidad del Requerimiento :                                             #
#                                                                           #
# Dependencias :                                                            #
#                                                                           #
# Usuario:                                                                  #
#                                                                           #
# Telefono:                                                                 #
#                                                                           #
#############################################################################

################################################################################
# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #
################################################################################
# Comprobamos si hay parametros de Ejecucion, de no ser asi toma la fecha del dia de ayer y BAN_FORZADO "N"
# Uso: ${0} <fch_carga> <fch_datos> <forzado>"
if [ $# -ne 3 ] ; then
# Recogida de parametros
        FCH_CARGA=`date '+%Y%m%d' --date '-1 day'`
        FCH_DATOS=`date '+%Y%m%d' --date '-1 day'`
        BAN_FORZADO="N"
        DATAMART="MDLL"
        echo "FCH_CARGA:" ${FCH_CARGA}
        echo "FCH_DATOS:" ${FCH_DATOS}
        echo "BAN_FORZADO:" ${BAN_FORZADO}
        echo "DATAMART:" ${DATAMART}
else
# Recogida de parametros
        FCH_CARGA=${1}
        FCH_DATOS=${2}
        BAN_FORZADO=${3}
        DATAMART="MDLL"
        echo "FCH_CARGA:" ${FCH_CARGA}
        echo "FCH_DATOS:" ${FCH_DATOS}
        echo "BAN_FORZADO:" ${BAN_FORZADO}
        echo "DATAMART:" ${DATAMART}
fi

################################################################################
# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #
################################################################################
#TOKEN="6195946698:AAFHavArurc8wxU6rFLTiDxiUMex6ClBOnw"
#CHAT_ID="378171580"

export MDLL_ENTORNO=/app/encuesta/Produccion/GACMedallia/MDLL/COMUN/Shell/Entorno

PROCESO="Lanza_cadena_${DATAMART}"
cadena_entorno='. ${MDLL_ENTORNO}/entornoMDLL_PAN.sh'
cadena_utilBD='. ${MDLL_UTILIDADES}/UtilBD.sh'
cadena_utilA='. ${MDLL_UTILIDADES}/UtilArchivo.sh'
cadena_utilU='. ${MDLL_UTILIDADES}/UtilUnix.sh'
cadena_utilDM='. ${MDLL_UTILIDADES}/UtilMDLL.sh'
cadena_trazas='${MDLL_TRAZAS}'
cadena_carga='${MDLL_CARGA}'
cadena_sql='${MDLL_SQL}'
cadena_tmp='${MDLL_TMP}'
#cadena_host='${HOST_VAR}'
cadena_configuracion='${MDLL_CONFIGURACION}'
buscar='VAR'
reemplazar=${DATAMART}
eval  ${cadena_entorno//$buscar/$reemplazar}
eval  ${cadena_utilBD//$buscar/$reemplazar}
eval  ${cadena_utilA//$buscar/$reemplazar}
eval  ${cadena_utilU//$buscar/$reemplazar}
eval  ${cadena_utilDM//$buscar/$reemplazar}
TRAZAS=`eval echo ${cadena_trazas//$buscar/$reemplazar}`
echo ${TRAZAS}
CARGA=`eval echo ${cadena_carga//$buscar/$reemplazar}`
echo ${CARGA}
SQL=`eval echo ${cadena_sql//$buscar/$reemplazar}`
echo ${SQL}
TMP=`eval echo ${cadena_tmp//$buscar/$reemplazar}`
echo ${TMP}
CONFIGURACION=`eval echo ${cadena_configuracion//$buscar/$reemplazar}`
echo ${CONFIGURACION}
NOMBRE_HOST=`hostname`
echo ${NOMBRE_HOST}

FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`
FCH_CARGA_MES=`echo ${FCH_CARGA} | cut -c 1-6`

# Comprobamos si existe el directorio de Trazas para fecha de carga
if ! [ -d ${TRAZAS}/${FCH_CARGA} ] ; then
  mkdir ${TRAZAS}/${FCH_CARGA}
fi
TRAZAS=${TRAZAS}/${FCH_CARGA}
echo "${0}" > ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
echo "Fecha de Carga: ${FCH_CARGA}"  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
echo "Fecha de Datos: ${FCH_DATOS}"  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
echo "Forzado: ${BAN_FORZADO}"  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log

################################################################################
# Cuentas  Produccion / Desarrollo                                             #
################################################################################
if [ ${NOMBRE_HOST} == `hostname` ]; then
  ### Cuentas para mantenimiento
  CTA_MAIL_USUARIOS=`cat ${CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`
  CTA_MAIL=`cat ${CONFIGURACION}/Correos_Mtto_ReportesBI.txt`
  TELEFONOS_DWH=`cat ${CONFIGURACION}/TelefonosMantto.txt`
  TELEFONOS_USUARIOS=`cat ${CONFIGURACION}/TELEFONOS_USUARIOS.txt`
else
  ### Cuentas para mantenimiento
  CTA_MAIL_USUARIOS=`cat ${CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`
  CTA_MAIL=`cat ${CONFIGURACION}/Correos_Mtto_ReportesBI.txt`
  TELEFONOS_DWH=`cat ${CONFIGURACION}/TelefonosMantto.txt`
  TELEFONOS_USUARIOS=`cat ${CONFIGURACION}/TELEFONOS_USUARIOS.txt`
fi

################################################################################
#Funcion  InsertaMonitoreo                                                     #
################################################################################


InsertaMonitoreo()
{
NOMBRE_PROCESO=${1}

sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} 2> /dev/null << EOF
WHENEVER SQLERROR EXIT 1;
set PAGESIZE 0;
set HEADING OFF;
INSERT
    INTO MTDT_MONITOREO
    SELECT
      cve_proceso
      ,1
      ,-5
      ,CURRENT_DATE
      ,CURRENT_DATE
      ,TO_DATE(${FCH_CARGA},'YYYYMMDD')
      ,TO_DATE(${FCH_DATOS},'YYYYMMDD')
      ,0
      ,0
      ,0
      ,0
      ,0
     , CURRENT_DATE
    FROM
      MTDT_PROCESO
    WHERE
      NOMBRE_PROCESO = '${NOMBRE_PROCESO}';
COMMIT;
quit;
EOF

EV_SQL=$?
  if [ ${EV_SQL} -ne 0 ]; then
    SUBJECT="Surgio un error en InsertaMonitoreo.(ERROR al ejecutar sqlplus)"
    echo "Surgio un error en InsertaMonitoreo." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"
    echo `date`
    MENSAJE="Surgio un error en InsertaMonitoreo.(ERROR al ejecutar sqlplus)."
    /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
    exit 1;
  fi
}

ValidaEjecucionProceso()
{

while [ ${current_retry} -lt ${max_retries} ]; do

ObtenContrasena ${BD_SID} ${BD_USUARIO}
BD_CLAVE=${PASSWORD}

VALIDA_PROCESO=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<EOF
WHENEVER SQLERROR EXIT 1;
WHENEVER OSERROR EXIT 2;
SET PAGESIZE 0;
SET HEADING OFF;
SELECT COUNT(1)
FROM BDDWH.DWM_EJECUCION
WHERE CVE_JOB=111
AND CVE_ERROR=0
AND FECHA_DATOS=${FCH_CARGA}
AND CVE_PAIS_TM=5;
QUIT;
EOF`
if [ ${VALIDA_PROCESO} -gt 0  ]
then
  echo "Se ha ejecutado proceso DWA_PARQUE_ABO_MES_DN."
  echo `date`
  current_retry=$((max_retries + 1))
else
  # La verificación no es exitosa, espera 10 minutos
  echo "En espera 10 minutos"
  sleep 600  # 600 segundos = 10 minutos

  current_retry=$((current_retry + 1))  # Incrementa el contador de intentos
  echo "current_retry= ${current_retry}"

fi
done

MAX_FECHA=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
whenever sqlerror exit 1
set pagesize 0
set heading off
SELECT MAX(FECHA_DATOS) CVE_DIA FROM BDDWH.DWM_EJECUCION
WHERE CVE_JOB=114
AND CVE_ERROR=0 AND CVE_PAIS_TM=5;
quit
!eof`

sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
UPDATE TC_FSM_CONFIGURACION SET VALOR = TRIM(${MAX_FECHA})
WHERE GRUPO='CARGA' AND CLAVE='FECHA_CARGA_PARQUE_ABONADO';
COMMIT;
quit
!eof
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: No se pudo actualizar VALOR de TC_FSM_CONFIGURACION en la CLAVE FECHA_CARGA_PARQUE_ABONADO"
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
  MENSAJE="No se pudo actualizar el valor del campo VALOR de la tabla TC_FSM_CONFIGURACION . (ERROR al ejecutar sqlplus)."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
  echo `date`
fi

#Actualizacion de la FECHA que contiene la tabla MDX_ENVIOS_MEDALLIA que contiene los DN en cuarentena de esa fecha
sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
UPDATE TC_FSM_CONFIGURACION SET VALOR =(SELECT MAX(TO_CHAR(FECHA -1,'YYYYMMDD')) CVE_DIA FROM MDX_ENVIOS_MEDALLIA)
WHERE GRUPO='CARGA' and CLAVE='FECHA_MAX_INSTANCIA_MDL';
COMMIT;
quit
!eof
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: No se pudo actualizar VALOR de TC_FSM_CONFIGURACION en la CLAVE FECHA_CARGA_PARQUE_ABONADO"
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
  MENSAJE="No se pudo actualizar el valor del campo VALOR de la tabla TC_FSM_CONFIGURACION . (ERROR al ejecutar sqlplus)."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
  echo `date`
fi

}

# Inicia cadena
echo "INICIA CADENA `date` "

ObtenContrasena ${BD_SID} ${BD_USUARIO}
BD_CLAVE=${PASSWORD}

# Recuperamos el CHAT_ID y el TOKEN para enviar mensajes al Bot de Telegram
CHAT_ID=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
whenever sqlerror exit 1
set pagesize 0
set heading off
SELECT VALOR FROM TC_FSM_CONFIGURACION WHERE GRUPO = 'CARGA' AND CLAVE='CHAT_ID_TLGRM_MEDALLIA';
quit
!eof`
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: WARNING: no se pudo obtener el CHAT_ID de Telegram."
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
  echo `date`
fi
TOKEN=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
whenever sqlerror exit 1
set pagesize 0
set heading off
SELECT VALOR FROM TC_FSM_CONFIGURACION WHERE GRUPO = 'CARGA' AND CLAVE='TOKEN_TLGRM_MEDALLIA';
quit
!eof`
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: WARNING: no se pudo obtener el TOKEN de Telegram."
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
  echo `date`
fi


#Funcion que valida ejecucion del  proceso cve_job=111 [DWA_PARQUE_ABO_MES_DN]
ValidaEjecucionProceso

#Actualizacion de la tabla TC_FSM_CONFIGURACION para la MAX(CVE_DIA) DE APP_VALIDADWH_PAN.TMP_PARQUE_CANAL_RS
CVE_DIA=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
whenever sqlerror exit 1
set pagesize 0
set heading off
SELECT /*+PARALLEL(A, 4) */ max(cve_dia) CVE_DIA FROM APP_VALIDADWH_PAN.TMP_PARQUE_CANAL_RS A;
quit
!eof`
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: No se pudo obtener el CVE_DIA de la tabla TMP_PARQUE_CANAL_RS."
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
   MENSAJE="No se pudo obtener el valor maximo del campo CVE_DIA de la tabla TMP_PARQUE_CANAL_RS . (ERROR al ejecutar sqlplus)."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
  echo `date`
fi

#Actualizamos la tabla de Configuracion con el valor para los cruces con la tabla TMP_PARQUE_CANAL_RS
sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
UPDATE TC_FSM_CONFIGURACION SET VALOR = TRIM(${CVE_DIA})
WHERE GRUPO='CARGA' AND CLAVE='TMP_PARQUE_CANAL_RS';
COMMIT;
quit
!eof
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: No se pudo actualizar VALOR de TC_FSM_CONFIGURACION"
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
  MENSAJE="No se pudo actualizar el valor del campo VALOR de la tabla TC_FSM_CONFIGURACION . (ERROR al ejecutar sqlplus)."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
  echo `date`
fi


#Lanza ID_BLOQUE
sqlplus ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} @${SQL}/Lanza_ID_BLOQUE.sql ${TMP}/Lanza_ID_BLOQUE.txt
err_salida=$?
if [ ${err_salida} -ne 0 ]; then
  SUBJECT="Surgio un error en el sql Lanza_ID_BLOQUE.sql. (ERROR al ejecutar sqlplus)"
  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"
  echo ${SUBJECT} >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
  echo `date`  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
  MENSAJE="Surgio un error en el sql Lanza_ID_BLOQUE.sql. (ERROR al ejecutar sqlplus)."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
  exit 1;
fi

lista_bloque=`cat ${TMP}/Lanza_ID_BLOQUE.txt | awk  '{print $1}'`

for b in ${lista_bloque}
do

#Lanza Procesos
sed -e "s/#VAR_BLOQUE#/${b}/g"   ${SQL}/Lanza_PROCESO.sql > ${SQL}/Lanza_PROCESO_tmp.sql
sqlplus ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} @${SQL}/Lanza_PROCESO_tmp.sql ${TMP}/Lanza_PROCESO.txt
err_salida=$?
if [ ${err_salida} -ne 0 ]; then
  SUBJECT="${PROCESO}: Surgio un error en el sql de Lanza_PROCESO.sql. Error:  ${err_salida}."
  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"
  echo ${SUBJECT} >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
  echo `date` >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
  MENSAJE="${PROCESO}: Surgio un error en el sql de Lanza_PROCESO.sql. Error:  ${err_salida}."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"

  exit 1
fi

lista_procesos=`cat ${TMP}/Lanza_PROCESO.txt | awk -F'|' '{print $1}'`

for i in ${lista_procesos}
do


V_TEMP=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} << EOF
WHENEVER SQLERROR EXIT 1;
set PAGESIZE 0;
set HEADING OFF;
select  sum(precedencia.cve_proceso) as cve_proceso
FROM
(
select A.cve_proceso
 FROM
(select CVE_PROCESO,NOMBRE_PROCESO
,CASE WHEN nvl(PRECEDENCIA,'0')='0' THEN '-1' ELSE regexp_substr(PRECEDENCIA, '[^,]+', 1, 1) end as uno
FROM
 MTDT_PROCESO  WHERE NOMBRE_PROCESO='$i') A
LEFT OUTER JOIN (SELECT -1 CVE_PROCESO FROM DUAL
UNION
SELECT CVE_PROCESO
FROM MTDT_MONITOREO
WHERE TO_CHAR(FCH_CARGA,'YYYYMMDD') = '${FCH_CARGA}'  AND TO_CHAR(FCH_DATOS,'YYYYMMDD')= '${FCH_DATOS}'  AND CVE_RESULTADO=0
GROUP BY CVE_PROCESO) uno
ON (A.uno=uno.CVE_PROCESO)
WHERE uno.CVE_PROCESO IS NULL
union
select 0 as cve_proceso  FROM DUAL
) precedencia;
QUIT;
EOF`

V_CVE_PROCESO=`echo ${V_TEMP} | cut -d"|" -f1`

V_NOM_PROCESO=`echo $i`
echo ${V_NOM_PROCESO}

if [ "${V_CVE_PROCESO}"  == "0" ] ; then
nombre_proceso=`echo ${V_NOM_PROCESO} | awk -F'.' '{print $1}'`
sh -x ${CARGA}/${nombre_proceso}.sh ${FCH_CARGA} ${FCH_CARGA} ${BAN_FORZADO} > ${TRAZAS}/${nombre_proceso}_${FCH_CARGA}_${BAN_FORZADO}_${FECHA_HORA}.log 2>&1
ARCHIVO_LOG=${TRAZAS}/${nombre_proceso}_${FCH_CARGA}_${BAN_FORZADO}_${FECHA_HORA}.log
V_ERROR=`tail -1 ${ARCHIVO_LOG} | grep -ic 'exit 1'`
if [ ${V_ERROR} -ne 0 ] ; then
  echo "Surgio un error en el proceso ${ARCHIVO_LOG}" >> ${TRAZAS}/${nombre_proceso}_${FCH_CARGA}_${BAN_FORZADO}_${FECHA_HORA}.log
  echo `date`
  MENSAJE="Surgio un error en el proceso ${ARCHIVO_LOG}"
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"

  exit 1
fi


else
echo "El proceso ${V_NOM_PROCESO} no se ha cargado,su dependencia no ha sido ejecutada." >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
MENSAJE="El proceso ${V_NOM_PROCESO} no se ha cargado,su dependencia no ha sido ejecutada."
/opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
InsertaMonitoreo ${V_NOM_PROCESO}
fi


done

done


DURACICION_CADENA_EJE=`sqlplus -s ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} <<!eof
whenever sqlerror exit 1
set pagesize 0
set heading off
select to_char(max(fch_inicio) - min(fch_inicio), 'HH24:MI:SS') DURACION from MTDT_MONITOREO
WHERE FCH_CARGA=TO_DATE('${FCH_CARGA}', 'YYYYMMDD');
quit
!eof`
if [ $? -ne 0 ]; then
  SUBJECT="${REQ_NUM}: WARNING: no se pudo la duración de la cadena."
  echo "Surgio un error al obtener la fecha y hora del sistema." | mailx -s "${SUBJECT}" "${CTA_MAIL}"
  echo `date`
fi
echo "La duración de la cadena ha sido de: ${DURACICION_CADENA_EJE}"

echo "Se lleva a cabo el purgado de la tabla MTDT_MONITOREO..."
sqlplus ${BD_USUARIO}/${BD_CLAVE}@${BD_SID} @${SQL}/salvaguarda_MTDT_MONITOREO.sql
err_salida=$?
if [ ${err_salida} -ne 0 ]; then
  SUBJECT="Surgio un error en el sql salvaguarda_MTDT_MONITOREO.sql. (ERROR al ejecutar sqlplus)"
  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"
  echo ${SUBJECT} >> ${TRAZAS}/salvaguarda_MTDT_MONITOREO_${FECHA_HORA}.log
  echo `date`  >> ${TRAZAS}/salvaguarda_MTDT_MONITOREO_${FECHA_HORA}.log
  MENSAJE="Warning. Surgio un error en el sql salvaguarda_MTDT_MONITOREO.sql. (ERROR al ejecutar sqlplus)."
  /opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"
fi

echo "Se purgan los ficheros temporales..."
rm ${TMP}/Lanza_ID_BLOQUE.txt   ${TMP}/Lanza_PROCESO.txt ${SQL}/Lanza_PROCESO_tmp.sql


echo "El proceso ${PROCESO} se ha realizado correctamente."  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log
echo "Finaliza Proceso: `date +%d/%m/%Y\ %H:%M:%S` "  >> ${TRAZAS}/${PROCESO}_${FECHA_HORA}.log


MENSAJE="La cadena de ejecución para la fecha de carga ${FCH_CARGA} ha terminado correctamente. Duración: ${DURACICION_CADENA_EJE}"
/opt/jdk1.8.0_202/bin/java -classpath ${MDLL_UTILIDADES}/sendMsgByTelegram.jar SendMsgByTelegram "${TOKEN}" "${CHAT_ID}" "${MENSAJE}"

#Depuracion de archivos Historicos y mantiene los ultimos 30 dias, primero lista los directorios que borrara y despues los borra
#Listado y borrado de la carpeta Trazas
find ${MDLL_TRAZAS} -type d -mtime +30 -exec ls -d  {} \;
find ${MDLL_TRAZAS} -type d -mtime +30 -exec rm -rf {} \;
#Listado y borrado de la carpeta Fuente
find ${MDLL_FUENTE} -type d -mtime +30 -exec ls -d  {} \;
find ${MDLL_FUENTE} -type d -mtime +30 -exec rm -rf {} \;

exit 0


