CREATE TABLE ecommerce.tb_seller_buybox_pedido_ecomm (
	cod_pedido                    STRING    OPTIONS(description="Código do pedido"),
	cod_carrinho_pedido           STRING    OPTIONS(description="Código do carrinho de compras do pedido"),
	cod_cliente_marketplace       STRING    OPTIONS(description="Código do cliente no marketplace"),
	cod_organizacao               STRING    OPTIONS(description="Código da organização do marketplace"),
	dt_hr_criacao_pedido          TIMESTAMP OPTIONS(description="Data e hora de criação do pedido"),
	st_pedidos
		ARRAY<
			STRUCT<
				st_pedido                     STRING    OPTIONS(description="Situação do pedido"),
				dt_hr_atualizacao_pedido      TIMESTAMP OPTIONS(description="Data e hora de atualização do pedido")
	>>,
	cod_ip_pedido                 STRING    OPTIONS(description="IP do dispositivo de compra"),
	des_app_pedido                STRING    OPTIONS(description="Descrição do aplicativo de compra"),
	des_canal_venda_pedido        STRING    OPTIONS(description="Descrição do canal de venda"),
	vlr_taxa_pedido               FLOAT64   OPTIONS(description="Valor da taxa aplicada ao pedido"),
	vlr_subtotal_pedido           FLOAT64   OPTIONS(description="Valor subtotal do pedido"),
	vlr_desconto_pedido           FLOAT64   OPTIONS(description="Valor de desconto do pedido"),
	vlr_frete_pedido              FLOAT64   OPTIONS(description="Valor do frete do pedido"),
	vlr_total_pedido              FLOAT64   OPTIONS(description="Valor total do pedido"),
	vlr_desconto_fidelidade       FLOAT64   OPTIONS(description="Valor do desconto fidelidade"),
	tp_integracao                 STRING    OPTIONS(description="Descrição do tipo de integração"),
	cod_pedido_item               STRING    OPTIONS(description="Código do pedido no item de compra"),
	cod_carrinho_item             STRING    OPTIONS(description="Código do carrinho no item de compra"),
	cod_produto                   STRING    OPTIONS(description="Código do produto"),
	qt_item                       INT64     OPTIONS(description="Quantidade do item"),
	vlr_preco_max_item            FLOAT64   OPTIONS(description="Valor do preço máximo do item"),
	vlr_preco_item                FLOAT64   OPTIONS(description="Valor do preço do item"),
	vlr_desconto_item             FLOAT64   OPTIONS(description="Valor do desconto do item"),
	flg_presente_item             BOOLEAN   OPTIONS(description="Flag se item for presente (Sim/Não)"),
	cod_armazem_item              STRING    OPTIONS(description="Código do armazém do item"),
	dt_hr_criacao_item            TIMESTAMP OPTIONS(description="Data e hora de criação do item"),
	dt_hr_atualizacao_item        TIMESTAMP OPTIONS(description="Data e hora de atualização do item"),
	flg_entrega_lojafisica_item   BOOLEAN   OPTIONS(description="Flag se item entregue por loja física (Sim/Não)"),
	flg_oculto_item               BOOLEAN   OPTIONS(description="Flag se item de compra oculto  (Sim/Não)"),
	flg_excluido_item             BOOLEAN   OPTIONS(description="Flag se item de compra excluído  (Sim/Não)"),
	des_categoria                 STRING    OPTIONS(description="Descrição da categoria do item"),
	tp_armazem_item               STRING    OPTIONS(description="Descrição do tipo de armazém do item"),
	cod_pacote_entrega_item       STRING    OPTIONS(description="Código do pacote de entrega do item"),
	url_imagem_item               STRING    OPTIONS(description="URL da imagem do item"),
	cod_pedido_seller             STRING    OPTIONS(description="Código do pedido no seller"),
	cod_carrinho_item_seller      STRING    OPTIONS(description="Código do carrinho no seller"),
	cod_seller                    STRING    OPTIONS(description="Código do seller"),
	cod_organizacao_seller        STRING    OPTIONS(description="Código da organização do seller"),
	st_pedidos_seller
		ARRAY<
			STRUCT<
				st_pedido_seller              STRING    OPTIONS(description="Situação do pedido no seller"),
				dt_hr_atualizacao_item_seller TIMESTAMP OPTIONS(description="Data e Hora de atualização do item")
	>>,
	dt_hr_criacao_item_seller     TIMESTAMP OPTIONS(description="Data e hora de criação do item"),
	cep_endereco_entrega          STRING    OPTIONS(description="CEP do endereço de entrega do pedido"),
	uf_endereco_entrega           STRING    OPTIONS(description="UF do endereço de entrega do pedido"),
	cidade_endereco_entrega       STRING    OPTIONS(description="Cidade do endereço de entrega do pedido"),
	dt_hr_carga                   TIMESTAMP OPTIONS(description="Data e hora da carga na trusted"),
	dt_hr_referencia              TIMESTAMP OPTIONS(description="Data e hora em que o evento chegou no GCP")
)
PARTITION BY DATE(dt_hr_criacao_pedido)
CLUSTER BY cod_pedido
OPTIONS(description="Dados de pedidos de sellers do buybox do marketplace da plataforma BLZ")
;