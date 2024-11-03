MODEL (
    name raw.sisab__tipos,
    kind FULL
);
    
select #1 as tabela, #2 as chave, #3 as valor   FROM (
        VALUES
        -- LOCAL DE ATENDIMENTO
        ('LOCAL_DE_ATENDIMENTO', 1, 'UBS'),
        ('LOCAL_DE_ATENDIMENTO', 2, 'Unidade móvel'),
        ('LOCAL_DE_ATENDIMENTO', 3, 'Rua'),
        ('LOCAL_DE_ATENDIMENTO', 4, 'Domicílio'),
        ('LOCAL_DE_ATENDIMENTO', 5, 'Escola / Creche'),
        ('LOCAL_DE_ATENDIMENTO', 6, 'Outros'),
        ('LOCAL_DE_ATENDIMENTO', 7, 'Polo (academia da saúde)'),
        ('LOCAL_DE_ATENDIMENTO', 8, 'Instituição / Abrigo'),
        ('LOCAL_DE_ATENDIMENTO', 9, 'Unidade prisional ou congêneres'),
        ('LOCAL_DE_ATENDIMENTO', 10, 'Unidade socioeducativa'),
        ('LOCAL_DE_ATENDIMENTO', 11, 'Hospital'),
        ('LOCAL_DE_ATENDIMENTO', 12, 'Unidade de pronto atendimento'),
        ('LOCAL_DE_ATENDIMENTO', 13, 'CACON / UNACON'),
        ('LOCAL_DE_ATENDIMENTO', 14, 'Hospital SOS Urgência / Emergência'),
        ('LOCAL_DE_ATENDIMENTO', 15, 'Hospital SOS demais setores'),

        -- PRATICAS INTEGRATIVAS COMPLEMENTARES
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 1, 'Medicina tradicional chinesa'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 2, 'Antroposofia aplicado a saúde'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 3, 'Homeopatia'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 4, 'Fitoterapia'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 5, 'Termalismo / Crenoterapia'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 6, 'Práticas corporais e mentais em PICS'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 7, 'Técnicas manuais em PICS'),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 8, 'Outros'),

        -- PRATICAS TEMAS PARA SAUDE
        ('PRATICAS_TEMAS_PARA_SAUDE', 1, 'Alimentação saudável'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 2, 'Aplicação tópica de fluor'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 3, 'Saúde ocular'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 4, 'Autocuidado de pessoas com doenças crônicas'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 5, 'Cidadania e direitos homanos'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 6, 'Saúde do trabalhador'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 7, 'Dependência química / tabaco / álcool / outras drogas'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 8, 'Envelhecimento / Climatério / Andropausa / Etc'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 9, 'Escovação dentária supervisionada'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 10, 'Plantas medicinais / Fitoterapia'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 11, 'Práticas corporais / Atividade física'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 12, 'Práticas corporais e mentais em PIC'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 13, 'Prevenção da violência e promoção da cultura da paz'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 14, 'Saúde ambiental'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 15, 'Saúde bucal'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 16, 'Saúde mental'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 17, 'Saúde sexual e reprodutiva'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 18, 'Semana saúde na escola'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 19, 'Agravos negligenciados'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 20, 'Antropometria'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 21, 'Outros'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 22, 'Saúde auditiva'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 23, 'Desenvolvimento da linguagem'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 24, 'Verificação da situação vacinal'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 25, 'PNCT 1'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 26, 'PNCT 2'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 27, 'PNCT 3'),
        ('PRATICAS_TEMAS_PARA_SAUDE', 28, 'PNCT 4'),

        -- TIPO ATIVIDADE COLETIVA
        ('TIPO_ATIVIDADE_COLETIVA', 1, 'Reunião de equipe'),
        ('TIPO_ATIVIDADE_COLETIVA', 2, 'Reunião com outras equipes de saúde'),
        ('TIPO_ATIVIDADE_COLETIVA', 3, 'Reunião intersetorial / Conselho local de saúde / Controle social'),
        ('TIPO_ATIVIDADE_COLETIVA', 4, 'Educação em saúde'),
        ('TIPO_ATIVIDADE_COLETIVA', 5, 'Atendimento em grupo'),
        ('TIPO_ATIVIDADE_COLETIVA', 6, 'Avaliação / Procedimento coletivo'),
        ('TIPO_ATIVIDADE_COLETIVA', 7, 'Mobilização social'));