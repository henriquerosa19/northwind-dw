with
    funcionarios as (
        select *
        from {{ ref("dim_funcionarios")}}
    )

    ,produtos as (
        select *
        from {{ ref("dim_produtos")}}
    )

    ,int_vendas as (
        select *
        from {{ ref("int_vendas__pedido_itens")}}
    )

    , joined_tabelas as (
        select
        int_vendas.sk_pedido_item
        , int_vendas.id_pedido
        , int_vendas.id_produto
        , int_vendas.id_funcionario
        , int_vendas.id_cliente
        , int_vendas.id_transportadora
        , int_vendas.data_do_pedido
        , int_vendas.data_requerida_entrega
        , int_vendas.data_do_envio
        , int_vendas.frete
        , int_vendas.destinatario
        , int_vendas.endereco_destinatario
        , int_vendas.cidade_destinatario
        , int_vendas.regiao_destinatario
        , int_vendas.cep_destinatario
        , int_vendas.pais_destinatario
        , int_vendas.preco_da_unidade
        , int_vendas.quantidade
        , int_vendas.desconto_perc

        , produtos.nome_produto
        , produtos.eh_discontinuado
        , produtos.nome_categoria
        , produtos.nome_fornecedor
        , produtos.pais_fornecedor
        
        , funcionarios.nome_funcionario
        , funcionarios.nome_gerente
        , funcionarios.cargo_funcionario
        , funcionarios.dt_nascimento_funcionario
        , funcionarios.dt_contratacao

        from int_vendas
        left join produtos on
            int_vendas.id_produto = produtos.id_produto
        left join funcionarios on
            int_vendas.id_funcionario = funcionarios.id_funcionario
    )

select *
from joined_tabelas
