CREATE TABLE ecommerce.tb_seller_buybox_ecomm (
	cod_seller                                STRING    OPTIONS(description="Código do Seller"),
	des_tag                                   STRING    OPTIONS(description="Tag de identificação do seller"),
	nome_seller                               STRING    OPTIONS(description="Nome do seller"),
	cod_organizacao_seller                    STRING    OPTIONS(description="Organização do Seller (UN = 1, Beleza na Web)"),
	flg_seller_org                            BOOLEAN   OPTIONS(description="Flag se o seller é também a organização, ou seja, quando a própria beleza vende os produtos(true/false)"),
	qt_produto_estoque                        INTEGER   OPTIONS(description="Quantidade total do produto no estoque "),
	lista_estoque 
		ARRAY<
			STRUCT< 
    			cod_inventario_produto        STRING    OPTIONS(description="Código de identificação do registro de inventário"),
				qt_estoque_localidade         INTEGER   OPTIONS(description="Quantidade do produto na localidade"),
				logradouro_localidade         STRING    OPTIONS(description="Descrição do logradouro da localidade"),
				tp_localidade_estoque         STRING    OPTIONS(description="Tipo da localidade do estoque, por exemplo- Marketplace, loja física, entre outros."),
				des_localidade_prioridade     STRING    OPTIONS(description="Descrição da prioridade da localidade na venda"),
				qt_dias_entrega_localidade    INTEGER   OPTIONS(description="Tempo para a entrega, em dias, a partir da localidade")
  	>>,
  	lista_espec_preco
		ARRAY<
			STRUCT<
				cod_produto                   STRING    OPTIONS(description="Código do produto no marketplace"),
				tp_produto_buybox             STRING    OPTIONS(description="Tipo do produto (por exemplo, se é BuyBoxWinner)"),
				des_produto_buybox            STRING    OPTIONS(description="Lista com a descrição adicional sobre o preço"),
				vlr_venda_original            FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Valor de venda original do produtorice"),
				vlr_venda_produto             FLOAT64   OPTIONS(description="Preço do produto"),
				vlr_venda_maximo              FLOAT64   OPTIONS(description="Valor do preço máximo do produto"),
				pct_desconto                  FLOAT64   OPTIONS(description="Percentual de desconto"),
				vlr_desconto                  FLOAT64   OPTIONS(description="Valor do desconto"),
				qt_parcelas                   INTEGER   OPTIONS(description="Numero de parcelas"),
				vlr_parcela                   FLOAT64   OPTIONS(description="Valor de cada parcela "),
				tp_seller                     STRING    OPTIONS(description="Tipo do Seller (por exemplo Third Party - mktp-3p)"),
				cod_seller_preco_geral        STRING    OPTIONS(description="Código do seller na especificação geral de preços"),
				nome_seller_preco_geral       STRING    OPTIONS(description="Nome do seller na especificação geral de preços")
	>>,
	lista_preco 
		ARRAY<
			STRUCT<
    			cod_produto                   STRING    OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) -  Código do produto no marketplace"),
				tp_produto_buybox             STRING    OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Tipo do produto (por exemplo, se é BuyBoxWinner)"),
				des_produto_buybox            STRING    OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Lista com a descrição adicional sobre o preço"),
    			vlr_venda_original            FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Valor de venda original do produtorice"),
				vlr_venda_produto             FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Preço do produto"),
				vlr_venda_maximo              FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Valor do preço máximo do produto"),
				pct_desconto                  FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Percentual de desconto"),
				vlr_desconto                  FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Valor do desconto"),
				qt_parcelas                   INTEGER   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Numero de parcelas"),
				vlr_parcela                   FLOAT64   OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Valor de cada parcela "),
				tp_seller                     STRING    OPTIONS(description="Lista de preços (por exemplo APP ou Desktop) - Tipo do Seller (por exemplo Third Party - mktp-3p)"),
				cod_seller_precos			  STRING    OPTIONS(description="Código do seller na lista de preços"),
				nome_seller_precos			  STRING    OPTIONS(description="Nome do seller na lista de preços")
  	>>,
	flg_presente                              BOOLEAN   OPTIONS(description="Flag se é presente (True/false)"),
	flg_vendavel                              BOOLEAN   OPTIONS(description="Flag se é vendável (True/false)"),
	dt_hr_referencia                          TIMESTAMP OPTIONS(description="Data e hora de referência dos dados"),
	dt_hr_carga                               TIMESTAMP OPTIONS(description="Data e hora de carga dos dados na camada Trusted"),
	tp_inventario                             STRING    OPTIONS(description="Tipo do Inventário"),
	nome_empresa_envio                        STRING    OPTIONS(description="Nome da empresa que enviará o produto - por exemplo Beleza na Web se o seller utilizar o serviço beleza envios."),
	flg_empresa_envio_habilitada              BOOLEAN   OPTIONS(description="Flag se a empresa de envio está ahabilitada para entrega (true/false)"),
	qt_dias_entrega_estoque_geral			  INTEGER   OPTIONS(description="Tempo para a entrega, em dias, a partir do estoque geral"),
	dt_hr_atualizacao_produto				  TIMESTAMP OPTIONS(description="Data e hora de atualização do produto"),
	dt_hr_release_produto					  TIMESTAMP OPTIONS(description="Data e hora de lançamento do produto")
)PARTITION BY TIMESTAMP_TRUNC(dt_hr_carga, DAY)
OPTIONS(description="Dados de advertisement de sellers no buyBox do marketplace da plataforma BLZ");