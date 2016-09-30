#!/bin/bash
<<COMMENT
Params: 1 --> fecha (DD.MM.YYYY)
	2 --> accion (Create/Drop)
	3 --> tipo_tabla (e.j. Hourly)
	4 --> DB-USER
	5 --> DB-PASSWORD
	6 --> DB-ALIAS (e.j. DSMART2)
COMMENT
# Set path, descomentar la linea que corresponde a CORTADO
RUTA='/home/oracle/Particiones'
# RUTA='/home/calidad/Particiones' # para CORTADO

sqlplus -S $4/$5@$6 @$RUTA/createPartitions.sql $1 $2 $3 $RUTA/q_create_partitions.sql > /dev/null
# Limpio todas las filas que no corresponden a las sentencias alter 
# echo "--Limpieza -----------"
#sed -i -n '/--INICIO/,$!d' $RUTA/q_create_partitions.sql
#sed -i -n '/--FIN/,$!d' $RUTA/q_create_partitions.sql
#sed -i -n '/--INICIO/,$!d' $RUTA/q_create_partitions.sql
#sed -i -n '/--FIN/q' $RUTA/q_create_partitions.sql
