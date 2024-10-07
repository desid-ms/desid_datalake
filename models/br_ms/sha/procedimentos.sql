MODEL (
    name sha.procedimentos,
    kind INCREMENTAL_BY_UNIQUE_KEY  (
        unique_key codigo_procedimento_sigtap
    )
    
);

SELECT 
     f.codigo_procedimento_sigtap, 
     p."Código TUSS" AS codigo_tuss, 
     f.codigo_sha, 
     p."Procedimento SUS" AS ds_procedimento_sus, 
     p."Termo TUSS" AS ds_procedimento_tuss,  
     f.ds_sha_pt, 
FROM raw_sha.funcao_procedimento f 
 JOIN 
     raw_sha.procedimentos p 
 ON 
    f.codigo_procedimento_sigtap = p."Código SUS";

