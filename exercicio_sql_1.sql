-- a) Quais são os 50 maiores produtores em faturamento ($) de 2021?

-- Optei por utilizar o filtro de purchase_status como 'APROVADA'
-- visto que estamos atuando com dados de faturamento. Limit 50 para trazermos
-- os 50 primeiros.

SELECT
    purchase.producer_id
    ,SUM(product_item.purchase_value) as faturamento_total
FROM purchase 
INNER JOIN product_item ON purchase.prod_item_id = product_item.prod_item_id
WHERE YEAR(purchase.release_date) = 2021 AND purchase.purchase_status = 'APROVADA'
GROUP BY purchase.producer_id
ORDER BY faturamento_total DESC
LIMIT 50

-- b) Quais são os 2 produtos que mais faturaram ($) de cada produtor?

-- Para melhor organizacao optei por criar uma CTE calculando
-- o faturamento de cada produto para cada produtor. Depois disso, utilizei
-- de um artificio de em que faco um row_number para rankear os produtos e 
-- um partition by por produtor. Obtendo assim o ranking de faturamento dos
-- produtos para cada produtor. Depois apenas realizei a consulta principal
-- trazendo esses dados e filtrando os top 2 de cada produtor.

WITH faturamento_produto AS (
    SELECT
        purchase.producer_id
        ,product_item.product_id
        ,SUM(product_item.purchase_value) AS faturamento_total
    FROM purchase 
    INNER JOIN product_item ON purchase.prod_item_id = product_item.prod_item_id
    WHERE purchase.purchase_status = 'APROVADA'
    GROUP BY 
        purchase.producer_id
        ,product_item.product_id
),

ranking_produto AS (
    SELECT
        faturamento_produto.*
        ,ROW_NUMBER() OVER (PARTITION BY faturamento_produto.producer_id ORDER BY faturamento_produto.faturamento_total DESC) AS rank
    FROM faturamento_produto
)

SELECT
    producer_id
    ,product_id
    ,faturamento_total
FROM ranking_produto
WHERE rank <= 2