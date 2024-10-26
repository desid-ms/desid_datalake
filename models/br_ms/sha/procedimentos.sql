MODEL (
    name sha.procedimentos
);

SELECT 
    competencia,
    codigo_procedimento::int as codigo_procedimento,
    procedimento,
    codigo_cid,
    doenca_tratada,
    codigo_tipo_financiamento,
    hc.codigo_sha     
FROM sigtap.procedimentos s 

LEFT JOIN 
     raw.sha__hc_sigtap hc on hc.codigo_procedimento_sigtap::int = codigo_procedimento::int  
