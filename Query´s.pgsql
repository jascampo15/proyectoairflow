--Contar registros en cada capa
SELECT 'usos_raw' AS tabla, COUNT(*) AS total_registros FROM usos_raw
UNION ALL
SELECT 'usos_silver' AS tabla, COUNT(*) AS total_registros FROM usos_silver
UNION ALL
SELECT 'usos_gold' AS tabla, sum(total_trx) AS total_registros FROM usos_gold
UNION ALL
SELECT 'lines_raw' AS tabla, count(*) AS total_registros FROM lines_raw
UNION ALL
SELECT 'lines_silver' AS tabla, count(*) AS total_registros FROM lines_silver
;

--DELETE from usos_silver;
--DELETE from lines_raw;

SELECT * 
FROM usos_raw a
where a.id_txn >= '99990001'
;



SELECT 
    TO_CHAR(a.date_time, 'YYYY-MM-DD') AS fecha,
    TO_CHAR(a.date_time, 'HH24') AS hora,
    a.id_line AS id_linea,
    b.ld_descshort AS name_linea,
    a.type_intg AS integracion,
    a.veh_id AS veh_id,
    a.terminal_id AS sn_terminal,
    a.amount As valor,
    COUNT(*) AS total_trx
FROM usos_silver a
INNER JOIN lines_silver b ON CAST(b.ld_id AS BIGINT) = CAST(a.id_line AS BIGINT)
WHERE a.date_time IS NOT NULL
GROUP BY TO_CHAR(a.date_time, 'YYYY-MM-DD'),
TO_CHAR(a.date_time, 'HH24'), 
a.id_line,
b.ld_descshort,
a.type_intg,
a.veh_id ,
a.terminal_id, 
a.amount
ORDER BY MIN(a.date_time) DESC;

SELECT 
    TO_CHAR(a.fecha, 'YYYY-MM-DD') AS fecha,
    a.integracion AS integracion,
    sum(a.valor) As valor,
    sum(a.total_trx) AS total_trx
FROM usos_gold a
WHERE a.fecha IS NOT NULL
GROUP BY TO_CHAR(a.fecha, 'YYYY-MM-DD'),
         a.integracion
ORDER BY MIN(a.fecha) ASC;
