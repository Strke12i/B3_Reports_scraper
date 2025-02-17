CREATE MATERIALIZED VIEW IF NOT EXISTS Indicadores AS
SELECT
    e.id_empresa,
    e.nome_empresa,
    r.data_inicio,
    r.data_fim,

    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND
    dr.descricao = 'Atribuído a Sócios da Empresa Controladora' THEN dr.valor ELSE 0 END) AS lucro_liquido,
    
    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Resultado Líquido das Operações Continuadas' THEN dr.valor ELSE 0 END) / NULLIF(SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Receita de Venda de Bens e/ou Serviços' THEN dr.valor ELSE 0 END), 0) AS margem_liquida,

    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Resultado Antes do Resultado Financeiro e dos Tributos' THEN dr.valor ELSE 0 END) AS ebit,

    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Resultado Antes do Resultado Financeiro e dos Tributos' THEN dr.valor ELSE 0 END) / NULLIF(SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Receita de Venda de Bens e/ou Serviços' THEN dr.valor ELSE 0 END), 0) AS margem_ebit,

     SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Resultado Antes do Resultado Financeiro e dos Tributos' THEN dr.valor ELSE 0 END) + SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Depreciação, Amortização e Exaustão' THEN dr.valor ELSE 0 END) AS ebitda,

    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Resultado Antes do Resultado Financeiro e dos Tributos' THEN dr.valor ELSE 0 END) + SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Depreciação, Amortização e Exaustão' THEN dr.valor ELSE 0 END) / NULLIF(SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Receita de Venda de Bens e/ou Serviços' THEN dr.valor ELSE 0 END), 0) AS margem_ebitda,

    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Ativo Total' THEN dr.valor ELSE 0 END) as ativo_total,

    -- Passivo Circulante
    -- onde Passivo Circulante é tipo_relatorio['Balanço Patrimonial Ativo'] AND descricao['Passivo Circulante']
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Passivo Circulante' THEN dr.valor ELSE 0 END) as passivo_circulante,

    -- Total emprestismos e financiamentos
    -- onde Total emprestismos e financiamentos é tipo_relatorio['Balanço Patrimonial Ativo'] AND descricao['Empréstimos e Financiamentos']
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Empréstimos e Financiamentos' THEN dr.valor ELSE 0 END) as total_emprestimos_e_financiamentos,

    -- Caixa e Equivalentes de Caixa
    -- onde Caixa e Equivalentes de Caixa é tipo_relatorio['Balanço Patrimonial Ativo'] AND descricao['Caixa e Equivalentes de Caixa']
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Caixa e Equivalentes de Caixa' THEN dr.valor ELSE 0 END) as caixa_e_equivalentes_de_caixa,

    -- Caixa e Equivalentes de Caixa
    -- onde Caixa e Equivalentes de Caixa é tipo_relatorio['Balanço Patrimonial Ativo'] AND descricao['Aplicações Financeiras1']
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Aplicações Financeiras1' THEN dr.valor ELSE 0 END) as aplicacoes_financeiras,

    -- Capital Investido
    -- Ativo total - Passivo Circulante + Total emprestismos e financiamento - caixa e equivalentes - aplicações financeiras
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Ativo Total' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Passivo Circulante' THEN dr.valor ELSE 0 END) +
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Empréstimos e Financiamentos' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Caixa e Equivalentes de Caixa' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Aplicações Financeiras1' THEN dr.valor ELSE 0 END) as capital_investido,

    -- Patrimonio Liquido Consolidado
    -- onde Patrimonio Liquido Consolidado é tipo_relatorio['Balanço Patrimonial Passivo'] AND descricao['Patrimônio Líquido Consolidado']
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Patrimônio Líquido Consolidado' THEN dr.valor ELSE 0 END) as patrimonio_liquido_consolidado,

    -- Patrimonio Liquido
    -- onde Patrimonio Liquido é Patriciao Liquido Consolidado - Participação dos Acionistas Não Controladores
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Patrimônio Líquido Consolidado' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Participação dos Acionistas Não Controladores' THEN dr.valor ELSE 0 END) as patrimonio_liquido,

    -- Divida Bruta
    -- onde Divida Bruta é tipo_relatorio['Balanço Patrimonial Passivo'] AND descricao['Empréstimos e Financiamentos'] + tipo_relatorio['Balanço Patrimonial Passivo'] AND descricao['Empréstimos e Financiamentos3']
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Empréstimos e Financiamentos' THEN dr.valor ELSE 0 END) +
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Empréstimos e Financiamentos3' THEN dr.valor ELSE 0 END) as divida_bruta,

    -- Divida Liquida
    -- onde Divida Liquida é Divida Bruta - Caixa e Equivalentes de Caixa - Aplicações Financeiras
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Empréstimos e Financiamentos' THEN dr.valor ELSE 0 END) +
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Empréstimos e Financiamentos3' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Caixa e Equivalentes de Caixa' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Aplicações Financeiras1' THEN dr.valor ELSE 0 END) as divida_liquida,

    -- ROIC
    -- onde ROIC é (EBIT * 0.66) / Capital Investido, caso Capital Investido seja 0, então ROIC é 0
    (SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Resultado Antes do Resultado Financeiro e dos Tributos' THEN dr.valor ELSE 0 END) * 0.66) /
    NULLIF(SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Ativo Total' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Passivo Circulante' THEN dr.valor ELSE 0 END) +
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Empréstimos e Financiamentos' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Caixa e Equivalentes de Caixa' THEN dr.valor ELSE 0 END) -
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Ativo' AND dr.descricao = 'Aplicações Financeiras1' THEN dr.valor ELSE 0 END), 0) as roic,

    -- ROE
    -- onde ROE é (Lucro Liquido + Atribuído a Sócios Não Controladores) / (Patrimonio Liquido + Participação dos Acionistas Não Controladores)
    (SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Atribuído a Sócios da Empresa Controladora' THEN dr.valor ELSE 0 END) +
    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração do Resultado' AND dr.descricao = 'Atribuído a Sócios Não Controladores' THEN dr.valor ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Patrimônio Líquido Consolidado' THEN dr.valor ELSE 0 END) +
    SUM(CASE WHEN r.tipo_relatorio = 'Balanço Patrimonial Passivo' AND dr.descricao = 'Participação dos Acionistas Não Controladores' THEN dr.valor ELSE 0 END), 0) as roe

FROM relatorio r JOIN dados_relatorio dr ON r.id_relatorio = dr.id_relatorio JOIN empresa e ON r.id_empresa = e.id_empresa GROUP BY e.id_empresa, r.data_inicio, r.data_fim;