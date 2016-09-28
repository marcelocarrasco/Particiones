


CREATE TABLE CALIDAD_PARAMETROS_TABLAS 
(
  NOMBRE_TABLA                VARCHAR2(30 CHAR) 
, NOMBRE_TABLESPACE           VARCHAR2(30 CHAR) 
, PARTICION_ESQUEMA           VARCHAR2(30 CHAR) 
, PARTICION_ESQUEMA_MSC_FECHA VARCHAR2(15 CHAR) 
, PARTICION_FORMATO_MSC_FECHA VARCHAR2(15 CHAR) 
--, PARTICION_EXTENT_INITIAL    VARCHAR2(10 CHAR) 
--, PARTICION_EXTENT_NEXT       VARCHAR2(10 CHAR) 
, PARTICION_PERMISO_CREATE    VARCHAR2(20 CHAR) 
, PARTICION_PERMISO_DROP      VARCHAR2(20 CHAR) 
, PARTICION_TIPO_TABLA        VARCHAR2(20 CHAR) 
--, REPORTE_CAMPO_CANTIDAD_FORMULA VARCHAR2(100 CHAR) 
--, REPORTE_CAMPO_MEDICION_FORMULA VARCHAR2(200 CHAR) 
--, REPORTE_CAMPO_MEDICION_UNIDAD VARCHAR2(2 CHAR) 
--, REPORTE_CAMPO_FECHA VARCHAR2(40 CHAR) 
--, REPORTE_CLAUSULA_ORDER_BY NUMBER(*, 0) 
--, REPORTE_CLAUSULA_WHERE_BY VARCHAR2(100 CHAR) 
--, REPORTE_TIPO_STAT VARCHAR2(20 CHAR) 
--, REPORTE_PLATAFORMA VARCHAR2(20 CHAR) 
--, REPORTE_PLATAFORMA_DESCRIPCION VARCHAR2(100 CHAR) 
--, REPORTE_STATUS VARCHAR2(20 CHAR) 
--, REPORTE_TIPO_TABLA VARCHAR2(20 CHAR) 
, NOMBRE_TABLA_OBJETO VARCHAR2(30 CHAR) 
, DESCRIPCION_TABLA VARCHAR2(100 CHAR) 
, OBSERVACIONES VARCHAR2(20 CHAR) 
, N_PERIODO VARCHAR2(20 CHAR) 
, N_ELEMENTO VARCHAR2(20 CHAR) 
, ID_TABLA VARCHAR2(30 CHAR) NOT NULL 
, STAT_STATUS VARCHAR2(50 CHAR) 
--, SPARE002_CHR VARCHAR2(50 CHAR) 
--, SPARE003_CHR VARCHAR2(50 CHAR) 
--, SPARE004_CHR VARCHAR2(50 CHAR) 
--, SPARE005_CHR VARCHAR2(50 CHAR) 
--, SPARE006_CHR VARCHAR2(50 CHAR) 
--, SPARE007_CHR VARCHAR2(50 CHAR) 
--, SPARE008_CHR VARCHAR2(50 CHAR) 
--, SPARE009_CHR VARCHAR2(50 CHAR) 
--, SPARE010_CHR VARCHAR2(50 CHAR) 
) 
LOGGING 
TABLESPACE TBS_AUXILIAR 
PCTFREE 30 
PCTUSED 70 
INITRANS 1;

ALTER TABLE CALIDAD_PARAMETROS_TABLAS ADD CONSTRAINT CALIDAD_PARAMETROS_TABLAS_PK PRIMARY KEY (ID_TABLA);

CREATE UNIQUE INDEX CALIDAD_PARAMETROS_TABLAS_PK ON CALIDAD_PARAMETROS_TABLAS (ID_TABLA ASC);




SELECT  TABLE_NAME                                                    NOMBRE_TABLA
        ,'TBS_HOUR'                                                   NOMBRE_TABLESPACE
        ,REPLACE(SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',1)+1),'_','') PARTICION_ESQUEMA
        ,'YYYYMMDDHH24'                                               PARTICION_ESQUEMA_MSC_FECHA
        ,'DD.MM.YYYY HH24'                                            PARTICION_FORMATO_MSC_FECHA
        ,'Hourly'                                                     PARTICION_TIPO_TABLA
        ,REPLACE(TABLE_NAME,'_','')                                   ID_TABLA
        ,'ENABLED'                                                    OBSERVACIONES
        ,NULL                                                         DESCRIPCION_TABLA
        ,'ENABLED'                                                    PARTICION_PERMISO_CREATE
        ,'ENABLED'                                                    PARTICION_PERMISO_DROP
        ,LENGTH(REPLACE(SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',1)+1),'_','')||'2016092700') LON
FROM USER_TABLES
WHERE TABLE_NAME LIKE '%_HOUR'
ORDER BY ID_TABLA;



--
-- Particiones
--
SET SERVEROUTPUT on
SET VERIFY OFF
set feedback off
set lines 500

define accion = 'Create' -- AMP2
define tipo_tabla = 'Hourly' -- AMP3
define fecha = '27.09.2016' --AMP1

DECLARE
  VV                VARCHAR2(200 CHAR);
  V2                VARCHAR2(200 CHAR);
  A                 NUMBER;
  V_FECHA_DESDE     DATE;
  V_FECHA_HASTA     DATE;
  V_NEXT_HOUR       DATE;
  V_FECHA_WHILE     DATE;
  V_FECHA           VARCHAR2(10 CHAR);
  
--  C_PATH            CONSTANT CALIDAD_PARAMETROS.PRM_ID%TYPE := 1;
--  C_NAME_FILE_SQL   CONSTANT CALIDAD_PARAMETROS.PRM_ID%TYPE := 143;
  
--  W_PATH_ARCHIVO    CALIDAD_PARAMETROS.PRM_VALUE%TYPE;
--  W_NAME_FILE_SQL   CALIDAD_PARAMETROS.PRM_VALUE%TYPE;
  
--  W_ARCHIVO_CTL     UTL_FILE.FILE_TYPE;
  
--  V_INI_EXTENTION   CALIDAD_PARAMETROS.PRM_VALUE%TYPE;
--  V_NEXT_EXTENTION  CALIDAD_PARAMETROS.PRM_VALUE%TYPE;
  
  V_FECHA_MASCARA   VARCHAR2(20 CHAR);
  V_MASCARA         VARCHAR2(20 CHAR);
BEGIN

  SELECT DECODE(UPPER('&accion'), 'DROP', DECODE(UPPER('&tipo_tabla'), 'HOURLY', 1, 'DAILY', 2, 'WEEKLY', 3, 'MONTHLY', 4),
                           'CREATE', DECODE(UPPER('&tipo_tabla'), 'HOURLY', 5, 'DAILY', 6, 'WEEKLY', 7, 'MONTHLY', 8)) A
  INTO A FROM DUAL;
  FOR SIN IN (SELECT NOMBRE_TABLA,
                     NOMBRE_TABLESPACE,
                     PARTICION_ESQUEMA,
                     PARTICION_ESQUEMA_MSC_FECHA,
                     PARTICION_FORMATO_MSC_FECHA,
                     '100K' PARTICION_EXTENT_INITIAL
                FROM (SELECT  TABLE_NAME                                                    NOMBRE_TABLA
                              ,'TBS_HOUR'                                                   NOMBRE_TABLESPACE
                              ,REPLACE(SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',1)+1),'_','') PARTICION_ESQUEMA
                              ,'YYYYMMDDHH24'                                               PARTICION_ESQUEMA_MSC_FECHA
                              ,'DD.MM.YYYY HH24'                                            PARTICION_FORMATO_MSC_FECHA
                              ,'Hourly'                                                     PARTICION_TIPO_TABLA
                              ,REPLACE(TABLE_NAME,'_','')                                   ID_TABLA
                              ,'ENABLED'                                                    OBSERVACIONES
                              ,NULL                                                         DESCRIPCION_TABLA
                              ,'ENABLED'                                                    PARTICION_PERMISO_CREATE
                              ,'ENABLED'                                                    PARTICION_PERMISO_DROP
                              --,LENGTH(REPLACE(SUBSTR(TABLE_NAME,INSTR(TABLE_NAME,'_',1)+1),'_','')||'2016092700') LON
                      FROM USER_TABLES
                      WHERE TABLE_NAME LIKE '%_HOUR'
                      --AND TABLE_NAME = 'CSCO_CGN_STATS_HOUR'
                      ) CALIDAD_PARAMETROS_TABLAS
               WHERE PARTICION_TIPO_TABLA = '&tipo_tabla'
               AND DECODE('&accion', 'Drop', PARTICION_PERMISO_DROP, 'Create', PARTICION_PERMISO_CREATE) = 'ENABLED'
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
        FROM (SELECT DECODE (A, 1, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') - (16 * 7),
                                3, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') + (1 * 7),
                                4, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'MONTH'), 1),
                                5, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') + (1 * 7),
                                6, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'Q'), 3),
                                --7, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') + (1 * 7),
                                7, TRUNC(ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'Q'), 3), 'DAY'),
                                --8, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'MONTH'), 1)
                                8, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'Q'), 3)
                            ) FECHA_DESDE,
                     DECODE (A, 1, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') - (7 * 7) + (7 - 1/24),
                                3, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') + (16 * 7) + (7 - 1/24),
                                4, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'MONTH'), 4),
                                5, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') + (1 * 7) + (7 - 1/24),
                                6, LAST_DAY(ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'Q'), 5)),
                                --7, TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'DAY') + (16 * 7) + (7 - 1/24),
                                7, TRUNC(LAST_DAY(ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'Q'), 5)), 'DAY'),
                                --8, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'MONTH'), 4)
                                8, ADD_MONTHS(TRUNC(TO_DATE('&fecha','DD.MM.YYYY'), 'Q'), 5)
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
  
        WHILE V_FECHA_DESDE < V_FECHA_WHILE LOOP
          V_FECHA_DESDE := V_NEXT_HOUR;
          IF A IN (1, 2, 3, 4) THEN
             VV:= 'ALTER TABLE SMART.'||SIN.NOMBRE_TABLA||' DROP PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||';';
             --UTL_FILE.PUT_LINE(W_ARCHIVO_CTL,VV);
             DBMS_OUTPUT.PUT_LINE(VV);
          ELSE

             VV:= 'ALTER TABLE '||SIN.NOMBRE_TABLA||' ADD PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||
                  ' VALUES LESS THAN (TO_DATE('''||V_FECHA_MASCARA|| ''','''||V_MASCARA||'''))'||chr(13)||
                  ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;';
             --UTL_FILE.PUT_LINE(W_ARCHIVO_CTL,VV);
             DBMS_OUTPUT.PUT_LINE(VV);
             
--             VV:= ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;';
--             --UTL_FILE.PUT_LINE(W_ARCHIVO_CTL,VV);
--             DBMS_OUTPUT.PUT_LINE(VV);
          END IF;
          --DBMS_OUTPUT.PUT_LINE(VV);
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
           --UTL_FILE.PUT_LINE(W_ARCHIVO_CTL,VV);
           DBMS_OUTPUT.PUT_LINE(VV);
        ELSE
  
           VV:= 'ALTER TABLE '||SIN.NOMBRE_TABLA||' ADD PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||
                ' VALUES LESS THAN (TO_DATE('''||V_FECHA_MASCARA|| ''','''||V_MASCARA||'''))'||chr(13)||
                ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;';
           --UTL_FILE.PUT_LINE(W_ARCHIVO_CTL,VV);
           DBMS_OUTPUT.PUT_LINE(VV);
  
--           VV:= ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;';
--           --UTL_FILE.PUT_LINE(W_ARCHIVO_CTL,VV);
--           DBMS_OUTPUT.PUT_LINE(VV);
        END IF; 
  END LOOP;
END;

