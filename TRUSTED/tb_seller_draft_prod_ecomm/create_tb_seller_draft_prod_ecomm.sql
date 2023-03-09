CREATE TABLE ecommerce.tb_seller_draft_prod_ecomm (
	nome_classe                      STRING    OPTIONS(description="Nome da classe"),
	cod_objeto                       STRING    OPTIONS(description="Código do objeto na mensagem"),
	des_mensagem_alteracao           STRING    OPTIONS(description="Descrição da mensagem de alteração do produto"),
	email_solicitante_alteracao      STRING    OPTIONS(description="Email do solicitante da alteração do produto"),
	cod_solicitante_alteracao        STRING    OPTIONS(description="Id do solicitante da alteração do produto"),
	dt_hr_solicitacao_ajuste         TIMESTAMP OPTIONS(description="Data de solicitação de alteração do produto"),
	cod_aprovador                    STRING    OPTIONS(description="Código do aprovador"),
	email_aprovador                  STRING    OPTIONS(description="Email do aprovador"),
	dt_hr_aprovacao                  TIMESTAMP OPTIONS(description="Data de aprovação do draft do produto"),
	des_linha_resumo                 STRING    OPTIONS(description="Resumo da descrição da linha para o produto"),
	des_marca_produto                STRING    OPTIONS(description="Descrição da marca para o produto"),
	des_marca_resumo                 STRING    OPTIONS(description="Resumo da descrição da marca para o produto"),
	cod_departamento                 STRING    OPTIONS(description="Código do Departamento"),
	des_categoria                    STRING    OPTIONS(description="Lista das categorias do produto"),
	des_departamento                 STRING    OPTIONS(description="Descrição do departamento para o produto"),
	des_departamento_resumo          STRING    OPTIONS(description="Resumo da descrição do departamento para o produto"),
	des_acao                         STRING    OPTIONS(description="Descrição da ação de uso do produto"),
	des_comousar                     STRING    OPTIONS(description="Descrição de como usar o produto"),
	des_beneficio                    STRING    OPTIONS(description="Descrição dos benefícios de uso do produto"),
	des_produto                      STRING    OPTIONS(description="Descrição completa do produto"),
	des_paraquem                     STRING    OPTIONS(description="Descrição de público alvo do produto"),
	des_ocasiao                      STRING    OPTIONS(description="Descrição de ocasiões que combinam com o produto"),
	des_resultado                    STRING    OPTIONS(description="Descrição do resultado de uso do produto"),
	des_produto_resumo               STRING    OPTIONS(description="Resumo da descrição do produto"),
	des_comofunciona                 STRING    OPTIONS(description="Descrição de como funciona o produto"),
	des_oque                         STRING    OPTIONS(description="Descrição o que é o produto"),
	des_fundo_piramideolfativa       STRING    OPTIONS(description="Notas de fundo da pirâmide olfativa do produto"),
	des_corpo_piramideolfativa       STRING    OPTIONS(description="Notas de corpo da pirâmide olfativa do produto"),
	des_topo_piramideolfativa        STRING    OPTIONS(description="Notas de topo da pirâmide olfativa do produto"),
	flg_habilitado                   BOOLEAN   OPTIONS(description="Status produto habilitado para venda"),
	lista_gtins
		ARRAY<
			STRUCT<
				cod_ean_lista        STRING    OPTIONS(description="Lista com código do EAN- European Article Number ou Numeração Européia de Artigos")
	>>,
	lista_imagem_objeto
		ARRAY<
			STRUCT<
				flg_destaque         BOOLEAN   OPTIONS(description="Status produto em destaque"),
				arquivo_img_produto  STRING    OPTIONS(description="Arquivo da imagem do produto"),
				url_img_produto      STRING    OPTIONS(description="URL da imagem do produto")
	>>,
	lista_invalidos
		ARRAY<
			STRUCT<
				des_campos_invalidos   STRING    OPTIONS(description="Lista com a descrição de campos não válidos"),
				nome_campos_invalidos  STRING    OPTIONS(description="Lista de nome de campos não válidos")
	>>,
	dt_hr_alteracao                  TIMESTAMP OPTIONS(description="Data de alteração"),
	email_autor_alteracao            STRING    OPTIONS(description="Email do autor da alteração"),
	url_midia_produto                STRING    OPTIONS(description="URL da mídia do produto"),
	nome_produto                     STRING    OPTIONS(description="Nome do produto"),
	cod_organizacao                  STRING    OPTIONS(description="Código da organização"),
	des_unid_volume                  STRING    OPTIONS(description="Unidade do volume do produto, indicado na embalagem"),
	vlr_volume                       STRING    OPTIONS(description="Valor do volume do produto, indicado na embalagem"),
	lista_propriedade
		ARRAY<
			STRUCT<
				des_propriedades       STRING    OPTIONS(description="Lista de propriedades do produto"),
				des_valor_propriedades STRING    OPTIONS(description="Lista de valores de propriedades do produto")
	>>,
	dt_hr_lancamento                 TIMESTAMP OPTIONS(description="Data de liberação de venda do produto"),
	vlr_peso                         STRING    OPTIONS(description="Valor do peso do produto, indicado na embalagem"),
	des_unid_peso                    STRING    OPTIONS(description="Unidade do peso do produto, indicado na embalagem"),
	cod_seller                       STRING    OPTIONS(description="Código do seller"),
	cod_referencia                   STRING    OPTIONS(description="Código de referencia do seller"),
	cod_produto_seller               STRING    OPTIONS(description="Código identificador do produto no seller"),
	des_seo                          STRING    OPTIONS(description="A descrição do site de SEO representa sua página inicial. Os mecanismos de pesquisa mostram essa descrição nos resultados de pesquisa de sua página inicial se não encontrarem conteúdo mais relevante para os termos de pesquisa de um visitante."),
	des_palavras_chaves              STRING    OPTIONS(description="Descrição de palavras-chaves do produto"),
	des_titulo_seo                   STRING    OPTIONS(description="Descrição do título que aparece no código HTML de uma página. É ele que vai ser lido pelo Google (na hora que ele examina todos os sites da internet) e exibido na página de resultados."),
	cod_produto                      STRING    OPTIONS(description="Código identificador do produto. As marcas do GB estão cadastradas no E-commerce iniciando com a letra da marca no E-commerce, conforme- <B/Q/E><código 5 dígitos SAP>"),
	st_produto                       STRING    OPTIONS(description="Status do cadastro do produto (Recusado, Aprovado, Cadastro Incompleto, Aprovado com Pendencia)"),
	flg_visivel                      BOOLEAN   OPTIONS(description="Status se produto visível no marktplace"),
	dt_hr_carga                      TIMESTAMP OPTIONS(description="Data e hora de carga na trusted"),
	cod_produto_match                STRING    OPTIONS(description="Código de produto  no marketplace (matchProductSku)"),
	dt_hr_referencia                 TIMESTAMP OPTIONS(description="Horário de publicação de uma mensagem"),
	qt_comprimento_produto           NUMERIC   OPTIONS(description="Informa o comprimento do produto"),
	qt_largura_produto               NUMERIC   OPTIONS(description="Informa a largura do produto"),
	qt_altura_produto                NUMERIC   OPTIONS(description="Informa a altura do produto"),
	qt_peso_produto                  NUMERIC   OPTIONS(description="Informa o peso do produto"),
	cod_marca_produto                STRING    OPTIONS(description="Código da marca para o produto"),
	cod_autor_alteracao              STRING    OPTIONS(description="Código do autor da alteração do produto"),
	nome_loja_seller                 STRING    OPTIONS(description="Nome da loja do seller"),
	tag_loja_seller                  STRING    OPTIONS(description="Tag da loja do seller"),
	cod_organizacao_seller           STRING    OPTIONS(description="Código da organização do seller"),
	flg_ativacao_seller              BOOLEAN   OPTIONS(description="Flag de ativação do seller"),
	nome_empresa_envio               STRING    OPTIONS(description="Nome da empresa de envio"),
	flg_empresa_envio_habilitada     BOOLEAN   OPTIONS(description="Flag de empresa de envio habilitada"),
	cod_empresa_integradora          STRING    OPTIONS(description="Código da empresa integradora"),
	prefixo_empresa_integradora      STRING    OPTIONS(description="Prefixo da empresa integradora"),
	nome_empresa_integradora         STRING    OPTIONS(description="Nome da empresa integradora"),
	token_empresa_integradora        STRING    OPTIONS(description="Token da empresa integradora"),
	nome_habilitado_por              STRING    OPTIONS(description="Nome de quem habilitou o produto"),
	des_razao_recusa                 STRING    OPTIONS(description="Descrição para a razão de recusa do produto"),
	nome_solicitante_ajustes         STRING    OPTIONS(description="Nome do solicitante de ajustes após aprovação"),
	dt_hr_solicitacao_ajustes        TIMESTAMP OPTIONS(description="Data da solicitação de ajustes após aprovação"),
	msg_produto_negado               STRING    OPTIONS(description="Descrição da mensagem de produto negado"),
	cod_solicitante_produto_negado   STRING    OPTIONS(description="Código do solicitante de produto negado"),
	email_solicitante_produto_negado STRING    OPTIONS(description="Email do solicitante de produto negado"),
	dt_hr_produto_negado             TIMESTAMP OPTIONS(description="Data e hora de produto negado"),
	cod_ean                          STRING    OPTIONS(description="Código do EAN- European Article Number ou Numeração Européia de Artigo")
)
PARTITION BY DATETIME_TRUNC(dt_hr_carga, DAY)
OPTIONS(description="Dados de Draft dos Produtos dos Sellers na plataforma BLZ")
;