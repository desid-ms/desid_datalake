MODEL (
  name raw.sha__procedimentos_ab,
  kind FULL
);

with ab_sigtap AS (
    SELECT 
        co_procedimento_ab, 
        procedimento_ab, 
        co_procedimento_sigtap, 
        procedimento_sigtap 
    FROM (
        VALUES
        ('ABPG007', 'Curativo especial', '0301100276', 'Curativo especial'),
        ('ABPG027', 'Administração de medicamentos via Oral', '0301100217', 'Administração de medicamentos por via oral'),
        ('ABPG028', 'Administração de medicamentos via Intramuscular', '0301100209', 'Administração de medicamentos por via intramuscular'),
        ('ABPG029', 'Administração de medicamentos via Endovenosa', '0301100195', 'Administração de medicamentos por via endovenosa'),
        ('ABPG031', 'Administração de medicamentos via Tópica', '0301100233', 'Administração tópica de medicamento(s)'),
        ('ABPG032', 'Administração de Penicilina para tratamento de sífilis', '0301100241', 'Administração de penicilina para tratamento de sífilis'),
        ('ABPG041', 'Administração de medicamentos via Subcutânea (SC)', '0301100225', 'Administração de medicamentos por via subcutânea (sc)'),
        ('ABEX013', 'Retinografia/Fundo de olho com oftalmologista', '0211060020', 'Biomicroscopia de fundo de olho *'),
        ('ABEX014', 'Retinografia coloria', '0211060178', 'Retinografia colorida binocular*'),
        ('ABEX015', 'Retinografia fluorescente', '0211060186', 'Retinografia fluorescente binocular*'),
        ('ABEX022', 'Teste do olhinho (TRV)', '0211060000', 'Teste Diagnóstico em oftalmologia*'),
        ('ABPG015', 'Remoção de corpo estranho da cavidade auditiva e nasal', '0404010300', 'Retirada de corpo estranho da cavidade auditiva e nasal'),
        ('ABPG017', 'Retirada de cerume', '0404010270', 'Remoção de cerúmen de conduto auditivo externo uni / bilateral'),
        ('ABPG034', 'Aferição de temperatura', '0301100250', 'Aferição de temperatura'),
        ('ABPG035', 'Curativo simples', '0301100284', 'Curativo simples'),
        ('ABPG038', 'Medição de altura', '0101040075', 'Medição de altura'),
        ('ABPG039', 'Medição de peso', '0101040083', 'Medição de peso'),
        ('ABPG040', 'Teste rápido para dosagem de proteinúria', '0214010155', 'Teste rápido de proteinúria'),
        ('ABPO015', 'Orientação de Higiene Bucal', '0101020104', 'Orientação de higiene bucal')
    ) AS v(co_procedimento_ab, procedimento_ab, co_procedimento_sigtap, procedimento_sigtap)
)

select 
    ab.co_procedimento_ab as codigo_ab,
    p.codigo_procedimento,
    p.procedimento,
    'SISAB' as fonte,
    p.codigo_sha,
    p.codigo_tipo_financiamento,
    p.valor
from ab_sigtap ab
join sha.procedimentos p on ab.co_procedimento_sigtap = p.codigo_procedimento
