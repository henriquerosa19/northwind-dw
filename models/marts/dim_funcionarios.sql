with
    stg_funcionarios as (
        select 
            id_funcionario
            , id_gerente
            , nome_funcionario
            , cargo_funcionario
            , dt_nascimento_funcionario
            , dt_contratacao
            , cidade_funcionario
            , regiao_funcionario
            , pais_funcionario
            , informacoes_funcionario   
        from {{ ref('stg_erp__funcionarios') }}
    )

    , self_join_funcioniarios as (
        select
            funcionarios.id_funcionario
            , funcionarios.id_gerente
            , funcionarios.nome_funcionario

            , gerente.nome_funcionario as nome_gerente

            , funcionarios.cargo_funcionario
            , funcionarios.dt_nascimento_funcionario
            , funcionarios.dt_contratacao
            , funcionarios.cidade_funcionario
            , funcionarios.regiao_funcionario
            , funcionarios.pais_funcionario
            , funcionarios.informacoes_funcionario

           
        from stg_funcionarios as funcionarios
        left join stg_funcionarios as gerente on 
            funcionarios.id_gerente = gerente.id_funcionario
    )

    select * 
    from self_join_funcioniarios