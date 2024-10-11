MODEL (
    name siops.homologacoes,
    
    depends_on ["raw_siops.th_homologacao"],
    kind FULL
);


SELECT * EXCLUDE(NU_PERIODO),
        NU_ANO || CASE  
            WHEN NU_PERIODO = '12' THEN '-1'
            WHEN NU_PERIODO = '14' THEN '-2'
            WHEN NU_PERIODO = '1'  THEN '-3'
            WHEN NU_PERIODO = '18' THEN '-4'
            WHEN NU_PERIODO = '20' THEN '-5'
            WHEN NU_PERIODO = '2'  THEN '-6' 
        END AS "ANOBIMESTRE" 
    FROM
        RAW_SIOPS.TH_HOMOLOGACAO
    ORDER BY DT_HOMOLOGACAO
