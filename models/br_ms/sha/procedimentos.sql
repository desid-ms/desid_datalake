MODEL (
    name sha.procedimentos,
    kind INCREMENTAL_BY_UNIQUE_KEY  (
        unique_key codigo_procedimento_sigtap
    )
    
);

SELECT 
     f.codigo_procedimento_sigtap as id_procedimento_sigtap, 
     p.codigo_tuss,
     f.codigo_sha, 
     p.procedimento_sus, 
     p.termo_tuss AS ds_procedimento_tuss,  
     f.ds_sha_pt, 
FROM raw.sha__funcao_procedimento f 
 JOIN 
     raw.sha__procedimentos p 
 ON 
    f.codigo_procedimento_sigtap::text = p.CODIGO_SUS;

