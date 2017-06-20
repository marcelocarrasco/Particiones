/*
Params: 1 --> fecha (DD.MM.YYYY)
	2 --> accion (Create/Drop)
	3 --> tipo_tabla (e.j. Hourly)
	4 --> DB-USER
	5 --> DB-PASSWORD
	6 --> DB-ALIAS (e.j. DSMART2)
*/

CREATE OR REPLACE FUNCTION GET_CREATE_PARTITIONS(P_FECHA      IN VARCHAR2,
                                                 P_ACCION     IN VARCHAR2 DEFAULT 'Create',
                                                 P_TIPO_TABLA IN VARCHAR2 DEFAULT 'Hourly'
                                                 ) RETURN SENTENCIAS_TAB AS
  SENTENCIAS SENTENCIAS_TAB := SENTENCIAS_TAB();
  --
  VV                VARCHAR2(200 CHAR);
  V2                VARCHAR2(200 CHAR);
  A                 NUMBER;
  V_FECHA_DESDE     DATE;
  V_FECHA_HASTA     DATE;
  V_NEXT_HOUR       DATE;
  V_FECHA_WHILE     DATE;
  V_FECHA           VARCHAR2(10 CHAR);
  --
  V_FECHA_MASCARA   VARCHAR2(20 CHAR);
  V_MASCARA         VARCHAR2(20 CHAR);
BEGIN

  SELECT DECODE(UPPER(P_ACCION), 'DROP', DECODE(UPPER(P_TIPO_TABLA), 'HOURLY', 1, 'DAILY', 2, 'WEEKLY', 3, 'MONTHLY', 4),
                           'CREATE', DECODE(UPPER(P_TIPO_TABLA), 'HOURLY', 5, 'DAILY', 6, 'WEEKLY', 7, 'MONTHLY', 8)) A
  INTO A FROM DUAL;
  --
  FOR SIN IN (SELECT NOMBRE_TABLA,
                     NOMBRE_TABLESPACE,
                     PARTICION_ESQUEMA,
                     PARTICION_ESQUEMA_MSC_FECHA,
                     PARTICION_FORMATO_MSC_FECHA,
                     '100K' PARTICION_EXTENT_INITIAL
               FROM  CALIDAD_PARAMETROS_TABLAS
               WHERE PARTICION_TIPO_TABLA = P_TIPO_TABLA
               AND OBSERVACIONES = 'ENABLED'
               AND DECODE(P_ACCION, 'Drop', PARTICION_PERMISO_DROP, 'Create', PARTICION_PERMISO_CREATE) = 'ENABLED'
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
        FROM (SELECT DECODE (A, 1, TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'DAY') - (16 * 7),
                                3, TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'DAY') + (1 * 7),
                                4, ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'MONTH'), 1),
                                5, TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'DAY') + (1 * 7),
                                6, ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'Q'), 3),
                                7, TRUNC(ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'Q'), 3), 'DAY'),
                                8, ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'Q'), 3)
                            ) FECHA_DESDE,
                     DECODE (A, 1, TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'DAY') - (7 * 7) + (7 - 1/24),
                                3, TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'DAY') + (16 * 7) + (7 - 1/24),
                                4, ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'MONTH'), 4),
                                5, TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'DAY') + (1 * 7) + (7 - 1/24),
                                6, LAST_DAY(ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'Q'), 5)),
                                7, TRUNC(LAST_DAY(ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'Q'), 5)), 'DAY'),
                                8, ADD_MONTHS(TRUNC(TO_DATE(P_FECHA,'DD.MM.YYYY'), 'Q'), 5)
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
          --
          SENTENCIAS.EXTEND();
          --
          IF A IN (1, 2, 3, 4) THEN
            SENTENCIAS(SENTENCIAS.LAST) :=SENTENCIA_REC('ALTER TABLE SMART.'||SIN.NOMBRE_TABLA||' DROP PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||';','');
          ELSE
            SENTENCIAS(SENTENCIAS.LAST) :=SENTENCIA_REC('ALTER TABLE '||SIN.NOMBRE_TABLA||' ADD PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||
                  ' VALUES LESS THAN (TO_DATE('''||V_FECHA_MASCARA|| ''','''||V_MASCARA||'''))'||
                  ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;','');
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
          SENTENCIAS.EXTEND();
          SENTENCIAS(SENTENCIAS.LAST) :=SENTENCIA_REC('ALTER TABLE SMART.'||SIN.NOMBRE_TABLA||' DROP PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||';','');
        ELSE
          SENTENCIAS(SENTENCIAS.LAST) :=SENTENCIA_REC('ALTER TABLE '||SIN.NOMBRE_TABLA||' ADD PARTITION '||SIN.PARTICION_ESQUEMA||V_FECHA||
               ' VALUES LESS THAN (TO_DATE('''||V_FECHA_MASCARA|| ''','''||V_MASCARA||'''))'||
               ' TABLESPACE '||SIN.NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;','');
        END IF; 
  END LOOP;
  RETURN SENTENCIAS;
END;