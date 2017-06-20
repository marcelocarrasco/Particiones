#!/bin/bash
<<COMMENT
Params: --> FECHA_PROC (DD.MM.YYYY)
	--> CANT-DIAS = 3
COMMENT
# Set path, descomentar la linea que corresponde a CORTADO
FECHA_PROC=$(date  +%d.%m.%Y)
#
# Borra particiones de tipo RAW
sh $HOME/Pentaho61/data-integration/kitchen.sh -file=$HOME/Particiones/PentahoSourceFiles/DropPartRawEndToEnd.kjb -param:CANT-DIAS=3 -param:FECHA-PROC=$FECHA_PROC
# Borra particiones de tipo HOUR
sh $HOME/Pentaho61/data-integration/kitchen.sh -file=$HOME/Particiones/PentahoSourceFiles/DropPartHourEndToEnd.kjb -param:CANT-MESES=2 -param:FECHA-PROC=$FECHA_PROC
exit
