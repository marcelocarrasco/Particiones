set trimspool on
set line 500
set escape on
set feedback off
set verify off
set serveroutput on

spool &4;

DECLARE
  VV                VARCHAR2(200 CHAR);
  V2                VARCHAR2(200 CHAR);
  A                 NUMBER;
  V_FECHA_DESDE     DATE;
  V_FECHA_HASTA     DATE;
  V_NEXT_HOUR       DATE;
  V_FECHA_WHILE     DATE;
  V_FECHA           VARCHAR2(10 CHAR);
  
  V_FECHA_MASCARA   VARCHAR2(20 CHAR);
  V_MASCARA         VARCHAR2(20 CHAR);
BEGIN

  SELECT DECODE(UPPER('&2'), 'DROP', DECODE(UPPER('&3'), 'HOURLY', 1, 'DAILY', 2, 'WEEKLY', 3, 'MONTHLY', 4),
                           'CREATE', DECODE(UPPER('&3'), 'HOURLY', 5, 'DAILY', 6, 'WEEKLY', 7, 'MONTHLY', 8)) A
  INTO A FROM DUAL;
  --DBMS_OUTPUT.PUT_LINE('--INICIO');
  FOR SIN IN (SELECT NOMBRE_TABLA,
                     NOMBRE_TABLESPACE,
                     PARTICION_ESQUEMA,
                     PARTICION_ESQUEMA_MSC_FECHA,
                     PARTICION_FORMATO_MSC_FECHA,
                     '100K' PARTICION_EXTENT_INITIAL
                FROM (SELECT  TABLE_NAME                                                    NOMBRE_TABLA
                              ,case
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'HOUR' 
                                        THEN 'TBS_HOUR'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) IN ('BH','DAY') 
                                        THEN 'TBS_DAY'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'IBHW' 
                                        THEN 'TBS_SUMMARY'
                              end NOMBRE_TABLESPACE
                              ,REPLACE(SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',1)+1),'_','') PARTICION_ESQUEMA
                              ,case
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'HOUR' 
                                        THEN 'YYYYMMDDHH24'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'BH'
                                        THEN 'YYYYMMDD'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'DAY'
                                        THEN 'YYYYMMDD'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'IBHW'
                                        THEN 'YYYYMM'
                              end PARTICION_ESQUEMA_MSC_FECHA
                              ,case
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'HOUR'
                                        THEN 'YYYY.MM.DD HH24'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) IN ('BH','DAY','IBHW')
                                        THEN 'DD.MM.YYYY'
                              end PARTICION_FORMATO_MSC_FECHA
                              ,case
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'HOUR'
                                        THEN 'Hourly'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) IN ('BH','DAY')
                                        THEN 'Daily'
                                  WHEN SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',-1,1)+1,LENGTH(TABLE_NAME)) = 'IBHW'
                                        THEN 'Weekly'
                              end PARTICION_TIPO_TABLA
                              ,REPLACE(TABLE_NAME,'_','')                                   ID_TABLA
                              ,'ENABLED'                                                    OBSERVACIONES
                              ,NULL                                                         DESCRIPCION_TABLA
                              ,'ENABLED'                                                    PARTICION_PERMISO_CREATE
                              ,'ENABLED'                                                    PARTICION_PERMISO_DROP
                      FROM USER_TABLES
                      WHERE TABLE_NAME NOT LIKE '%_AUX'
                      ) CALIDAD_PARAMETROS_TABLAS
               WHERE PARTICION_TIPO_TABLA = '&3'
               AND DECODE('&2', 'Drop', PARTICION_PERMISO_DROP, 'Create', PARTICION_PERMISO_CREATE) = 'ENABLED'
               ORDER BY NOMBRE_TABLESPACE, NOMBRE_TABLA)LOOP
               --
      SELECT FECHA_DESDE,
             FECHA_HASTA,
             TO_CHAR(FECHA_DESDE, SIN.PARTICION_ESQUEMA_MSC_FECHA) FECHA,
             DECODE (A, 3, FECHA_DESDE + 7,
                        4, ADD_MONTHS(FECHA_DESDE, 1),
                        6, FECHA_DESDE + 1,
                        7, FECHA_DESDE + 7,
                        8, ADD_MONTHS(FECHA_DESDE, 1),
                           FECHA_DESDE + 1/24 ) MANANA,
             DECODE (A, 3, TO_CHAR(FECHA_DESDE + 7, SIN.PARTICION_FORMATO_MSC_FECHA),
                        4, TO_CHAR(ADD_MONTHS(FECHA_DESDE, 1), SIN.PARTICION_FORMATO_MSC_FECHA),
                        6, TO_CHAR(FECHA_DESDE + 1, SIN.PARTICION_FORMATO_MSC_FECHA),
                        7, TO_CHAR(FECHA_DESDE + 7, SIN.PARTICION_FORMATO_MSC_FECHA),
                        8, TO_CHAR(ADD_MONTHS(FECHA_DESDE, 1), SIN.PARTICION_FORMATO_MSC_FECHA),
                           TO_CHAR(FECHA_DESDE + 1/24, SIN.PARTICION_FORMATO_MSC_FECHA) ) FECHA_MASCARA,
             SIN.PARTICION_FORMATO_MSC_FECHA MASCARA
        INTO V_FECHA_DESDE,
             V_FECHA_HASTA,
             V_FECHA,
             V_NEXT_HOUR,
             V_FECHA_MASCARA,
             V_MASCARA
        FROM (SELECT DECODE (A, 1, TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'DAY') - (16 * 7),
                                3, TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'DAY') + (1 * 7),
                                4, ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'MONTH'), 1),
                                5, TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'DAY') + (1 * 7),
                                6, ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'Q'), 3),
                                7, TRUNC(ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'Q'), 3), 'DAY'),
                                8, ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'Q'), 3)
                            ) FECHA_DESDE,
                     DECODE (A, 1, TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'DAY') - (7 * 7) + (7 - 1/24),
                                3, TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'DAY') + (16 * 7) + (7 - 1/24),
                                4, ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'MONTH'), 4),
                                5, TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'DAY') + (1 * 7) + (7 - 1/24),
                                6, LAST_DAY(ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'Q'), 5)),
                                7, TRUNC(LAST_DAY(ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'Q'), 5)), 'DAY'),
                                8, ADD_MONTHS(TRUNC(TO_DATE('&1','DD.MM.YYYY'), 'Q'), 5)
                            ) FECHA_HASTA
                FROM DUAL
             );
        /*  DEPENDIENDO DEL CASO, EL BUCLE ES HOUR, DAY, WEEK O MONTH  */
        SELECT DECODE (A, 3, V_FECHA_HASTA + 7,
                          4, ADD_MONTHS(V_FECHA_HASTA, 1),
                          6, V_FECHA_HASTA + 1,
                          7, V_FECHA_HASTA + 7,
                          8, ADD_MONTHS(V_FECHA_HASTA, 1), V_FECHA_HASTA + 1/24)
        INTO V_FECHA_WHILE
        FROM DUAL;
        --
        WHILE V_FECHA_DESDE < V_FECHA_WHILE LOOP
          V_FECHA_DESDE := V_NEXT_HOUR;
          IF A IN (1, 2, 3, 4) THEN
             VV:= 'ALTER TABLE SMART.'||SIN.NOMBRE_TABLA||' DROP PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||';';
             DBMS_OUTPUT.PUT_LINE(VV);
          ELSE
             VV:= 'ALTER TABLE '||SIN.NOMBRE_TABLA||' ADD PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||
                  ' VALUES LESS THAN (TO_DATE('''||V_FECHA_MASCARA|| ''','''||V_MASCARA||'''))'||
                  ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;';
             DBMS_OUTPUT.PUT_LINE(VV);
          END IF;
          --
          SELECT TO_CHAR(V_FECHA_DESDE, SIN.PARTICION_ESQUEMA_MSC_FECHA)                                FECHA,
                 DECODE (A, 3, V_FECHA_DESDE + 7,
                            4, ADD_MONTHS(V_FECHA_DESDE, 1),
                            6, V_FECHA_DESDE + 1,
                            7, V_FECHA_DESDE + 7,
                            8, ADD_MONTHS(V_FECHA_DESDE, 1),
                               V_FECHA_DESDE + 1/24 )                                                   MANANA,
                 DECODE (A, 3, TO_CHAR(V_FECHA_DESDE + 7, SIN.PARTICION_FORMATO_MSC_FECHA),
                            4, TO_CHAR(ADD_MONTHS(V_FECHA_DESDE, 1), SIN.PARTICION_FORMATO_MSC_FECHA),
                            6, TO_CHAR(V_FECHA_DESDE + 1, SIN.PARTICION_FORMATO_MSC_FECHA),
                            7, TO_CHAR(V_FECHA_DESDE + 7, SIN.PARTICION_FORMATO_MSC_FECHA),
                            8, TO_CHAR(ADD_MONTHS(V_FECHA_DESDE, 1), SIN.PARTICION_FORMATO_MSC_FECHA),
                               TO_CHAR(V_FECHA_DESDE + 1/24, SIN.PARTICION_FORMATO_MSC_FECHA) )         FECHA_MASCARA,
                 SIN.PARTICION_FORMATO_MSC_FECHA                                                        MASCARA
          INTO V_FECHA,
               V_NEXT_HOUR,
               V_FECHA_MASCARA,
               V_MASCARA
          FROM DUAL;
          V_FECHA_DESDE := V_NEXT_HOUR;
        END LOOP;
        /*  IF A CONSECUENCIA DE EFECTO RESIDUAL DE WHILE ANTERIOR, DEJANDO FUERA ULTIMA LINEA.  */
        IF A IN (1, 2, 3, 4) THEN
           VV:= 'ALTER TABLE SMART.'||SIN.NOMBRE_TABLA||' DROP PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||';';
           DBMS_OUTPUT.PUT_LINE(VV);
        ELSE
           VV:= 'ALTER TABLE '||SIN.NOMBRE_TABLA||' ADD PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||
                ' VALUES LESS THAN (TO_DATE('''||V_FECHA_MASCARA|| ''','''||V_MASCARA||'''))'||
                ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;';
           DBMS_OUTPUT.PUT_LINE(VV);
        END IF; 
  END LOOP;
  --DBMS_OUTPUT.PUT_LINE('--FIN');
END;
/

spool off
set trimspool off
set escape off
set feedback on
set verify on
set serveroutput off 

exit;