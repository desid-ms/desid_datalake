MODEL (
    name raw.sisab__procedimentos,
    kind FULL
);
    
WITH TEMA_SAUDE AS (
    SELECT TIPO, VALOR, PROCEDIMENTO FROM (
    VALUES

        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde bucal', 101020104),
        ('PRATICAS_TEMAS_PARA_SAUDE','Alimentação saudável', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Aplicação tópica de fluor', 101020015),
        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde ocular', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Autocuidado de pessoas com doenças crônicas', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Cidadania e direitos homanos', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde do trabalhador', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Dependência química / tabaco / álcool / outras drogas', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Envelhecimento / Climatério / Andropausa / Etc', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Escovação dentária supervisionada', 101020031),
        ('PRATICAS_TEMAS_PARA_SAUDE','Plantas medicinais / Fitoterapia', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Práticas corporais / Atividade física', 101010036),
        ('PRATICAS_TEMAS_PARA_SAUDE','Práticas corporais e mentais em PIC', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Prevenção da violência e promoção da cultura da paz', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde ambiental', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde mental', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde sexual e reprodutiva', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Semana saúde na escola', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Agravos negligenciados', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Antropometria', 101040024),
        ('PRATICAS_TEMAS_PARA_SAUDE','Outros', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Saúde auditiva', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Desenvolvimento da linguagem', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','Verificação da situação vacinal', 301040087),
        ('PRATICAS_TEMAS_PARA_SAUDE','PNCT 1', 301080011),
        ('PRATICAS_TEMAS_PARA_SAUDE','PNCT 2', 301080011),
        ('PRATICAS_TEMAS_PARA_SAUDE','PNCT 3', 301080011),
        ('PRATICAS_TEMAS_PARA_SAUDE','PNCT 4', 301080011),

        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 'Medicina tradicional chinesa', 309050235),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 'Antroposofia aplicado a saúde', 101050097),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 'Homeopatia', 309050197),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 'Fitoterapia', 309050200),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 'Ayurveda', 309050227),
        ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', 'Outros', 101050020)
        
        ('TIPO_ATENDIMENTO', 'Escuta inicial / Orientação', 301060079),
        ('TIPO_ATENDIMENTO', 'Atendimento de urgência', 301060037)) AS v(TIPO, VALOR, PROCEDIMENTO)),
    
    
