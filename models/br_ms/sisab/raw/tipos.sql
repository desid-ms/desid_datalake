MODEL (
    name raw.sisab__tipos,
    kind FULL
);
    
SELECT tabela, chave, valor FROM (
    VALUES
    ('TIPO_FICHA_ATENDIMENTO', '2' , 'Ficha de Cadastro Individual'),
    ('TIPO_FICHA_ATENDIMENTO', '3' , 'Ficha de Cadastro Domiciliar'),
    ('TIPO_FICHA_ATENDIMENTO', '4' , 'Ficha de Atendimento Individual'),
    ('TIPO_FICHA_ATENDIMENTO', '5' , 'Ficha de Atendimento Odontológico'),
    ('TIPO_FICHA_ATENDIMENTO', '6' , 'Ficha de Atividade Coletiva'),
    ('TIPO_FICHA_ATENDIMENTO', '7' , 'Ficha de Procedimentos'),
    ('TIPO_FICHA_ATENDIMENTO', '8' , 'Ficha de Visita Domiciliar'),
    ('TIPO_FICHA_ATENDIMENTO', '10' , 'Ficha de Atendimento Domiciliar'),
    ('TIPO_FICHA_ATENDIMENTO', '11' , 'Ficha de Avaliação de Elegibilidade'),
    ('TIPO_FICHA_ATENDIMENTO', '12' , 'Marcadores de Consumo Alimentar'),
    
    ('LOCAL_DE_ATENDIMENTO', '1' , 'UBS'),
    ('LOCAL_DE_ATENDIMENTO', '2' , 'Unidade móvel'),
    ('LOCAL_DE_ATENDIMENTO', '3' , 'Rua'),
    ('LOCAL_DE_ATENDIMENTO', '4' , 'Domicílio'),
    ('LOCAL_DE_ATENDIMENTO', '5' , 'Escola / Creche'),
    ('LOCAL_DE_ATENDIMENTO', '6' , 'Outros'),
    ('LOCAL_DE_ATENDIMENTO', '7' , 'Polo (academia da saúde)'),
    ('LOCAL_DE_ATENDIMENTO', '8' , 'Instituição / Abrigo'),
    ('LOCAL_DE_ATENDIMENTO', '9' , 'Unidade prisional ou congêneres'),
    ('LOCAL_DE_ATENDIMENTO', '10' , 'Unidade socioeducativa'),
    ('LOCAL_DE_ATENDIMENTO', '11' , 'Hospital'),
    ('LOCAL_DE_ATENDIMENTO', '12' , 'Unidade de pronto atendimento'),
    ('LOCAL_DE_ATENDIMENTO', '13' , 'CACON / UNACON'),
    ('LOCAL_DE_ATENDIMENTO', '14' , 'Hospital SOS Urgência / Emergência'),
    ('LOCAL_DE_ATENDIMENTO', '15' , 'Hospital SOS demais setores'),
    
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '1' , 'Medicina tradicional chinesa'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '2' , 'Antroposofia aplicado a saúde'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '3' , 'Homeopatia'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '4' , 'Fitoterapia'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '5' , 'Termalismo / Crenoterapia'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '6' , 'Práticas corporais e mentais em PICS'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '7' , 'Técnicas manuais em PICS'),
    ('PRATICAS_INTEGRATIVAS_COMPLEMENTARES', '8' , 'Outros'),

    
    ('PRATICAS_TEMAS_PARA_SAUDE', '1', 'Alimentação saudável'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '2', 'Aplicação tópica de fluor'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '3', 'Saúde ocular'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '4', 'Autocuidado de pessoas com doenças crônicas'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '5', 'Cidadania e direitos homanos'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '6', 'Saúde do trabalhador'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '7', 'Dependência química / tabaco / álcool / outras drogas'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '8', 'Envelhecimento / Climatério / Andropausa / Etc'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '9', 'Escovação dentária supervisionada'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '10', 'Plantas medicinais / Fitoterapia'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '11', 'Práticas corporais / Atividade física'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '12', 'Práticas corporais e mentais em PIC'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '13', 'Prevenção da violência e promoção da cultura da paz'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '14', 'Saúde ambiental'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '15', 'Saúde bucal'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '16', 'Saúde mental'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '17', 'Saúde sexual e reprodutiva'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '18', 'Semana saúde na escola'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '19', 'Agravos negligenciados'),
    ('PRATICAS_TEMAS_PARA_SAUDE', '20', 'Antropometria'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '21', 'Outros'), --TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '22', 'Saúde auditiva'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '23', 'Desenvolvimento da linguagem'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '24', 'Verificação da situação vacinal'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '25', 'PNCT 1'), -- Programa Nacional de Controle do Tabagismo  -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '26', 'PNCT 2'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '27', 'PNCT 3'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '28', 'PNCT 4'), -- PRÁTICA
    ('PRATICAS_TEMAS_PARA_SAUDE', '29', 'Ações de combate ao Aedes aegypti'), -- TEMA
    ('PRATICAS_TEMAS_PARA_SAUDE', '30', 'Outro procedimento coletivo'), -- TEMA

    ('TIPO_ATIVIDADE_COLETIVA', '1', 'Reunião de equipe'),
    ('TIPO_ATIVIDADE_COLETIVA', '2', 'Reunião com outras equipes de saúde'),
    ('TIPO_ATIVIDADE_COLETIVA', '3', 'Reunião intersetorial / Conselho local de saúde / Controle social'),
    ('TIPO_ATIVIDADE_COLETIVA', '4', 'Educação em saúde'),
    ('TIPO_ATIVIDADE_COLETIVA', '5', 'Atendimento em grupo'),
    ('TIPO_ATIVIDADE_COLETIVA', '6', 'Avaliação / Procedimento coletivo'),
    ('TIPO_ATIVIDADE_COLETIVA', '7', 'Mobilização social'),

    ('TIPO_ATENDIMENTO', '1', 'Consulta agendada programada / Cuidado continuado'),
    ('TIPO_ATENDIMENTO', '2', 'Consulta agendada'),
    ('TIPO_ATENDIMENTO', '4', 'Escuta inicial / Orientação'),
    ('TIPO_ATENDIMENTO', '5', 'Consulta no dia'),
    ('TIPO_ATENDIMENTO', '6', 'Atendimento de urgência'),
    ('TIPO_ATENDIMENTO', '7', 'Atendimento programado'),
    ('TIPO_ATENDIMENTO', '8', 'Atendimento não programado'),

    ('TIPO_DE_CONSULTA_ODONTO', '1', 'Primeira consulta odontológica programática'),
    ('TIPO_DE_CONSULTA_ODONTO', '2', 'Consulta de retorno'),
    ('TIPO_DE_CONSULTA_ODONTO', '4', 'Consulta de manutenção'),

    ('VIGILANCIA_EM_SAUDE_BUCAL', '1', 'Abscesso dento alveolar'),
    ('VIGILANCIA_EM_SAUDE_BUCAL', '2', 'Alteração em tecidos moles'),
    ('VIGILANCIA_EM_SAUDE_BUCAL', '3', 'Dor de dente'),
    ('VIGILANCIA_EM_SAUDE_BUCAL', '4', 'Fendas / Fissuras lábio palatais'),
    ('VIGILANCIA_EM_SAUDE_BUCAL', '5', 'Fluorose dentária moderada / severa'),
    ('VIGILANCIA_EM_SAUDE_BUCAL', '6', 'Traumatismo dento alveolar'),
    ('VIGILANCIA_EM_SAUDE_BUCAL', '99', 'Não identificado'),

    ('TEMAS_PARA_REUNIAO', '1', 'Questões administrativas / funcionamento'),
    ('TEMAS_PARA_REUNIAO', '2', 'Processo de trabalho'),
    ('TEMAS_PARA_REUNIAO', '3', 'Diagnóstico / Monitoramento do território'),
    ('TEMAS_PARA_REUNIAO', '4', 'Planejamento / Monitoramento das ações da equipe'),
    ('TEMAS_PARA_REUNIAO', '5', 'Discussão de caso ou projeto terapêutico singular'),
    ('TEMAS_PARA_REUNIAO', '6', 'Educação permanente'),
    ('TEMAS_PARA_REUNIAO', '7', 'Outros'),

    ('MOTIVO_VISITA', '1', 'Cadastramento / Atualização'),
    ('MOTIVO_VISITA', '2', 'Consulta'),
    ('MOTIVO_VISITA', '3', 'Exame'),
    ('MOTIVO_VISITA', '4', 'Vacina'),
    ('MOTIVO_VISITA', '5', 'Gestante'),
    ('MOTIVO_VISITA', '6', 'Puérpera'),
    ('MOTIVO_VISITA', '7', 'Recém-nascido'),
    ('MOTIVO_VISITA', '8', 'Criança'),
    ('MOTIVO_VISITA', '9', 'Pessoa com desnutrição'),
    ('MOTIVO_VISITA', '10', 'Pessoa em reabilitação ou com deficiência'),
    ('MOTIVO_VISITA', '11', 'Pessoa com hipertensão'),
    ('MOTIVO_VISITA', '12', 'Pessoa com diabetes'),
    ('MOTIVO_VISITA', '13', 'Pessoa com asma'),
    ('MOTIVO_VISITA', '14', 'Pessoa com DPOC / enfisema'),
    ('MOTIVO_VISITA', '15', 'Pessoa com câncer'),
    ('MOTIVO_VISITA', '16', 'Pessoa com outras doenças crônicas'),
    ('MOTIVO_VISITA', '17', 'Pessoa com hanseníase'),
    ('MOTIVO_VISITA', '18', 'Pessoa com tuberculose'),
    ('MOTIVO_VISITA', '19', 'Domiciliados / Acamados'),
    ('MOTIVO_VISITA', '20', 'Condições de vulnerabilidade social'),
    ('MOTIVO_VISITA', '21', 'Condicionalidades do bolsa família'),
    ('MOTIVO_VISITA', '22', 'Saúde mental'),
    ('MOTIVO_VISITA', '23', 'Usuário de álcool'),
    ('MOTIVO_VISITA', '24', 'Usuário de outras drogas'),
    ('MOTIVO_VISITA', '25', 'Egresso de internação'),
    ('MOTIVO_VISITA', '26', 'Controle de ambientes / vetores'),
    ('MOTIVO_VISITA', '27', 'Convite para atividades coletivas / campanha de saúde'),
    ('MOTIVO_VISITA', '28', 'Outros'),
    ('MOTIVO_VISITA', '29', 'Visita periódica'),
    ('MOTIVO_VISITA', '30', 'Condicionalidades do bolsa família'),
    ('MOTIVO_VISITA', '31', 'Orientação / Prevenção'),
    ('MOTIVO_VISITA', '32', 'Sintomáticos respiratórios'),
    ('MOTIVO_VISITA', '33', 'Tabagista'),
    ('MOTIVO_VISITA', '34', 'Ação educativa'),
    ('MOTIVO_VISITA', '35', 'Imóvel com foco'),
    ('MOTIVO_VISITA', '36', 'Ação mecânica'),
    ('MOTIVO_VISITA', '37', 'Tratamento focal'),
    
    ('PROCEDIMENTOS_FICHA', 'ABPG001', 'Acupuntura com inserção de agulhas'),
    ('PROCEDIMENTOS_FICHA', 'ABPG002', 'Administração de vitamina A'),
    ('PROCEDIMENTOS_FICHA', 'ABPG003', 'Cateterismo vesical de alívio'),
    ('PROCEDIMENTOS_FICHA', 'ABPG004', 'Cauterização química de pequenas lesões'),
    ('PROCEDIMENTOS_FICHA', 'ABPG005', 'Cirurgia de unha (cantoplastia)'),
    ('PROCEDIMENTOS_FICHA', 'ABPG006', 'Cuidado de estomas'),
    ('PROCEDIMENTOS_FICHA', 'ABPG007', 'Curativo especial'),
    ('PROCEDIMENTOS_FICHA', 'ABPG008', 'Drenagem de abscesso'),
    ('PROCEDIMENTOS_FICHA', 'ABEX004', 'Eletrocardiograma'),
    ('PROCEDIMENTOS_FICHA', 'ABPG010', 'Coleta de citopatológico de colo uterino'),
    ('PROCEDIMENTOS_FICHA', 'ABPG011', 'Exame do pé diabético'),
    ('PROCEDIMENTOS_FICHA', 'ABPG012', 'Exérese / Biópsia / Punção de tumores superficiais de pele'),
    ('PROCEDIMENTOS_FICHA', 'ABPG013', 'Fundoscopia (exame de fundo de olho)'),
    ('PROCEDIMENTOS_FICHA', 'ABPG014', 'Infiltração em cavidade sinovial'),
    ('PROCEDIMENTOS_FICHA', 'ABPG015', 'Remoção de corpo estranho da cavidade auditiva e nasal'),
    ('PROCEDIMENTOS_FICHA', 'ABPG016', 'Remoção de corpo estranho subcutâneo'),
    ('PROCEDIMENTOS_FICHA', 'ABPG017', 'Retirada de cerume'),
    ('PROCEDIMENTOS_FICHA', 'ABPG018', 'Retirada de pontos de cirurgias'),
    ('PROCEDIMENTOS_FICHA', 'ABPG019', 'Sutura simples'),
    ('PROCEDIMENTOS_FICHA', 'ABPG020', 'Triagem oftalmológica'),
    ('PROCEDIMENTOS_FICHA', 'ABPG021', 'Tamponamento de epistaxe'),
    ('PROCEDIMENTOS_FICHA', 'ABPG022', 'Teste rápido de gravidez'),
    ('PROCEDIMENTOS_FICHA', 'ABPG040', 'Teste rápido para dosagem de proteinúria'),
    ('PROCEDIMENTOS_FICHA', 'ABPG024', 'Teste rápido para HIV'),
    ('PROCEDIMENTOS_FICHA', 'ABPG025', 'Teste rápido para hepatite C'),
    ('PROCEDIMENTOS_FICHA', 'ABPG026', 'Teste rápido para sífilis'),
    ('PROCEDIMENTOS_FICHA', 'ABPG027', 'Administração de medicamentos via oral'),
    ('PROCEDIMENTOS_FICHA', 'ABPG028', 'Administração de medicamentos via intramuscular'),
    ('PROCEDIMENTOS_FICHA', 'ABPG029', 'Administração de medicamentos via endovenosa'),
    ('PROCEDIMENTOS_FICHA', 'ABPG030', 'Administração de medicamentos via inalação / nebulização'),
    ('PROCEDIMENTOS_FICHA', 'ABPG031', 'Administração de medicamentos via tópica'),
    ('PROCEDIMENTOS_FICHA', 'ABPG032', 'Administração de penincilina para tratamento de sífilis'),

    ('ATENDIMENTO_DOMICILIAR', '1', 'Acamado'),
    ('ATENDIMENTO_DOMICILIAR', '2', 'Domiciliado'),
    ('ATENDIMENTO_DOMICILIAR', '3', 'Úlceras / Feridas (grau III ou IV)'),
    ('ATENDIMENTO_DOMICILIAR', '4', 'Acompanhamento nutricional'),
    ('ATENDIMENTO_DOMICILIAR', '5', 'Uso de sonda naso-gástrica - SNG'),
    ('ATENDIMENTO_DOMICILIAR', '6', 'Uso de sonda naso-enteral - SNE'),
    ('ATENDIMENTO_DOMICILIAR', '7', 'Uso de gastrostomia'),
    ('ATENDIMENTO_DOMICILIAR', '8', 'Uso de colostomia'),
    ('ATENDIMENTO_DOMICILIAR', '9', 'Uso de cistostomia'),
    ('ATENDIMENTO_DOMICILIAR', '10', 'Uso de sonda vesical de demora - SVD'),
    ('ATENDIMENTO_DOMICILIAR', '11', 'Acompanhamento pré-operatório'),
    ('ATENDIMENTO_DOMICILIAR', '12', 'Acompanhamento pós-operatório'),
    ('ATENDIMENTO_DOMICILIAR', '13', 'Adaptação ao uso de órtese / prótese'),
    ('ATENDIMENTO_DOMICILIAR', '14', 'Reabilitação domiciliar'),
    ('ATENDIMENTO_DOMICILIAR', '15', 'Cuidados paliativos oncológico'),
    ('ATENDIMENTO_DOMICILIAR', '16', 'Cuidados paliativos não-oncológico'),
    ('ATENDIMENTO_DOMICILIAR', '17', 'Oxigenoterapia domiciliar'),
    ('ATENDIMENTO_DOMICILIAR', '18', 'Uso de traqueostomia'),
    ('ATENDIMENTO_DOMICILIAR', '19', 'Uso de aspirador de vias aéreas para higiene brônquica'),
    ('ATENDIMENTO_DOMICILIAR', '20', 'Suporte ventilatório não invasivo - CPAP'),
    ('ATENDIMENTO_DOMICILIAR', '21', 'Suporte ventilatório não invasivo - BiPAP'),
    ('ATENDIMENTO_DOMICILIAR', '22', 'Diálise peritonial'),
    ('ATENDIMENTO_DOMICILIAR', '23', 'Paracentese'),
    ('ATENDIMENTO_DOMICILIAR', '24', 'Medicação parenteral'),

    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301070024', 'Acompanhamento de paciente em reabilitação em comunicação alternativa'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301050082', 'Antibioticoterapia parenteral'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301070075', 'Atendimento / acompanhamento de paciente em reabilitação do desenvolvimento neuropsicomotor'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0302040021', 'Atendimento fisioterapêutico paciente com transtorno respiratório sem complicações sistêmicas'),
    
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301050090', 'Atendimento médico com finalidade de atestar óbito'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301070067', 'Atendimento / acompanhamento em reabilitação nas múltiplas deficiências'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100047', 'Cateterismo vesical de alívio'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100055', 'Cateterismo vesical de demora'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0201020041', 'Coleta de material para exame laboratorial'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100063', 'Cuidados com estomas'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100071', 'Cuidados com traqueostomia'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100098', 'Enema'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100144', 'Oxigenoterapia'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100152', 'Retirada de pontos de cirurgias básicas (por paciente)'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100179', 'Sondagem gástrica'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301100187', 'Terapia de reidratação oral'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301050120', 'Terapia de reidratação parenteral'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301070113', 'Terapia fonoaudiológica individual'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0308010019', 'Tratamento de traumatismos de localização especificada / não especificada'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0303190019', 'Tratamento em reabilitação'),
    ('PROCEDIMENTOS_ATENCAO_DOMICILIAR', '0301050104', 'Visita domiciliar pós-óbito'),

    ('PROBLEMAS_CONDICOES', 'ABP009', 'Asma'),
    ('PROBLEMAS_CONDICOES', 'ABP019', 'Dengue'),
    ('PROBLEMAS_CONDICOES', 'ABP008', 'Desnutrição'),
    ('PROBLEMAS_CONDICOES', 'ABP006', 'Diabetes'),
    ('PROBLEMAS_CONDICOES', 'ABP010', 'DPOC'),
    ('PROBLEMAS_CONDICOES', 'ABP020', 'DST'),
    ('PROBLEMAS_CONDICOES', 'ABP018', 'Hanseníase'),
    ('PROBLEMAS_CONDICOES', 'ABP005', 'Hipertensão Arterial'),
    ('PROBLEMAS_CONDICOES', 'ABP007', 'Obesidade'),
    ('PROBLEMAS_CONDICOES', 'ABP001', 'Pré-natal'),
    ('PROBLEMAS_CONDICOES', 'ABP004', 'Puericultura'),
    ('PROBLEMAS_CONDICOES', 'ABP002', 'Puerpério (até 42 dias)'),
    ('PROBLEMAS_CONDICOES', 'ABP023', 'Rastreamento de Câncer de mama'),
    ('PROBLEMAS_CONDICOES', 'ABP022', 'Rastreamento de Câncer do colo do útero'),
    ('PROBLEMAS_CONDICOES', 'ABP024', 'Rastreamento de Risco cardiovascular'),
    ('PROBLEMAS_CONDICOES', 'ABP015', 'Reabilitação'),
    ('PROBLEMAS_CONDICOES', 'ABP014', 'Saúde Mental'),
    ('PROBLEMAS_CONDICOES', 'ABP003', 'Saúde Sexual e Reprodutiva'),
    ('PROBLEMAS_CONDICOES', 'ABP011', 'Tabagismo'),
    ('PROBLEMAS_CONDICOES', 'ABP017', 'Tuberculose'),
    ('PROBLEMAS_CONDICOES', 'ABP012', 'Usuário de álcool'),
    ('PROBLEMAS_CONDICOES', 'ABP013', 'Usuário de outras drogas'),
    
    ('EXAMES_SOLICITADOS', 'ABEX002', 'Colesterol total'),
    ('EXAMES_SOLICITADOS', 'ABEX003', 'Creatinina'),
    ('EXAMES_SOLICITADOS', 'ABEX027', 'EAS / EQU'),
    ('EXAMES_SOLICITADOS', 'ABEX004', 'Eletrocardiograma'),
    ('EXAMES_SOLICITADOS', 'ABEX030', 'Eletroforese de Hemoglobina'),
    ('EXAMES_SOLICITADOS', 'ABEX005', 'Espirometria'),
    ('EXAMES_SOLICITADOS', 'ABEX006', 'Exame de escarro'),
    ('EXAMES_SOLICITADOS', 'ABEX026', 'Glicemia'),
    ('EXAMES_SOLICITADOS', 'ABEX007', 'HDL'),
    ('EXAMES_SOLICITADOS', 'ABEX008', 'Hemoglobina glicada'),
    ('EXAMES_SOLICITADOS', 'ABEX028', 'Hemograma'),
    ('EXAMES_SOLICITADOS', 'ABEX009', 'LDL'),
    ('EXAMES_SOLICITADOS', 'ABEX013', 'Retinografia/Fundo de olho com oftamologista'),
    ('EXAMES_SOLICITADOS', 'ABEX019', 'Sorologia de Sifilis (VDRL)'),
    ('EXAMES_SOLICITADOS', 'ABEX016', 'Sorologia para Dengue'),
    ('EXAMES_SOLICITADOS', 'ABEX018', 'Sorologia para HIV'),
    ('EXAMES_SOLICITADOS', 'ABEX031', 'Teste indireto de antiglobulina humana (TIA)'),
    ('EXAMES_SOLICITADOS', 'ABEX020', 'Teste da orelhinha'),
    ('EXAMES_SOLICITADOS', 'ABEX023', 'Teste de gravidez'),
    ('EXAMES_SOLICITADOS', 'ABEX022', 'Teste do olhinho'),
    ('EXAMES_SOLICITADOS', 'ABEX021', 'Teste do pezinho'),
    ('EXAMES_SOLICITADOS', 'ABEX024', 'Ultrassonografia obstétrica'),
    ('EXAMES_SOLICITADOS', 'ABEX029', 'Urocultura'),

    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO001', 'Acesso a polpa dentária e medicação (por dente)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO002', 'Adaptação de Prótese Dentária'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO003', 'Aplicação de cariostático (por dente)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO004', 'Aplicação de selante (por dente)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO005', 'Aplicação tópica de flúor (individual por sessão)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO006', 'Capeamento pulpar'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO007', 'Cimentação de prótese'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO008', 'Curativo de demora c/ ou s/ preparo biomecânico'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPG008', 'Drenagem de abscesso'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO010', 'Evidenciação de placa bacteriana'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO011', 'Exodontia de dente decíduo'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO012', 'Exodontia de dente permanente'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO013', 'Instalação de prótese dentária'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO014', 'Moldagem dento-gengival p/ construção de prótese dentária'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO015', 'Orientação de Higiene Bucal'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO016', 'Profilaxia / Remoção de placa bacteriana'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO017', 'Pulpotomia dentária'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO018', 'Radiografia Periapical / Interproximal'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO019', 'Raspagem alisamento e polimento supragengivais (por sextante)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO020', 'Raspagem alisamento subgengivais (por sextante)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO021', 'Restauração de dente decíduo'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO022', 'Restauração de dente permanente anterior'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO023', 'Restauração de dente permanente posterior'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPG018', 'Retirada de pontos de cirurgias básicas (por paciente)'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO025', 'Selamento provisório de cavidade dentária'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO026', 'Tratamento de alveolite'),
    ('PROCEDIMENTOS_ODONTOLOGICOS', 'ABPO027', 'Ulotomia / Ulectomia')
) AS v(tabela, chave, valor);
    
    
