CREATE TABLE ecommerce.tb_seller_ecomm (
	nom_classe                      STRING    OPTIONS(description="Nome da classe"),
	cod_seller                      STRING    OPTIONS(description="Código do seller"),
	des_loja                        STRING    OPTIONS(description="Parecer descritivo da loja"),
	flg_ativo                       BOOLEAN   OPTIONS(description="Status de seller ativo/desativado"),
	des_login                       STRING    OPTIONS(description="Login de acesso"),
	cidade_comercial                STRING    OPTIONS(description="Nome da cidade"),
	compl_comercial                 STRING    OPTIONS(description="Complemento do logradouro"),
	distrito_comercial              STRING    OPTIONS(description="Distrito do logradouro"),
	nr_logradouro                   STRING    OPTIONS(description="Número do logradouro"),
	cep_comercial                   STRING    OPTIONS(description="CEP do logradouro"),
	estado_comercial                STRING    OPTIONS(description="Sigla do estado do logradouro"),
	logradouro_comercial            STRING    OPTIONS(description="Descrição do endereço"),
	cod_conta                       STRING    OPTIONS(description="Conta da conta bancária"),
	cod_verificacao                 INT64     OPTIONS(description="Digito verificador da conta bancária"),
	cod_agencia                     STRING    OPTIONS(description="Código da agência bancária"),
	cod_banco                       INT64     OPTIONS(description="Código do banco"),
	banco_documento                 STRING    OPTIONS(description="Documento da conta bancária"),
	nome_conta                      STRING    OPTIONS(description="Nome da conta bancária"),
	cod_marca                       STRING    OPTIONS(description="Código da marca"),
	des_marca_resumo                STRING    OPTIONS(description="Resumo da descrição da marca"),
	des_categoria                   STRING    OPTIONS(description="Descrição da categoria dos produtos do seller"),
	nom_companhia                   STRING    OPTIONS(description="Nome da companhia (razão social do seller)"),
	cod_empresa_envio               STRING    OPTIONS(description="Código da empresa para envio"),
	nom_empresa_envio               STRING    OPTIONS(description="Nome da empresa para envio"),
	nom_empresa_envio_resumo        STRING    OPTIONS(description="Resumo do nome da empresa para envio"),
	tp_envio                        STRING    OPTIONS(description="Tipo de envio"),
	flg_empresa_envio_habiltada     BOOLEAN   OPTIONS(description="Status se a empresa de envio está habiltada"),
	des_empresa_envio_invalida      STRING    OPTIONS(description="Lista com a descrição de dados não válidos sobre a empresa de envio"),
	st_empresa_envio                STRING    OPTIONS(description="Status do cadastro, por exemplo- sucesso, entre outros"),
	dt_hr_empresa_envio_atualizacao TIMESTAMP OPTIONS(description="Data de atualização dos dados sobre a empresa de envio"),
	tp_transferencia_monetaria      STRING    OPTIONS(description="Descrição do tipo de transferencia monetária"),
	tx_comissao                     STRING    OPTIONS(description="Valor da taxa de comissão de venda"),
	nr_dia_comissao                 INT64     OPTIONS(description="Número do dia do mês em que é repassada a comissão de venda"),
	lista_contatos
		ARRAY<
			STRUCT<
				tel_celular_comercial         STRING    OPTIONS(description="Número do telefone celular comercial"),
				nom_departamento_comercial    STRING    OPTIONS(description="Nome do departamento comercial"),
				email_comercial               STRING    OPTIONS(description="Email do departamento comercial"),
				nom_contato_comercial         STRING    OPTIONS(description="Nome do contato comercial"),
				tel_comercial                 STRING    OPTIONS(description="Número do telefone")
	>>,
	dt_hr_criacao                   TIMESTAMP OPTIONS(description="Data de criação"),
	prz_entrega                     INT64     OPTIONS(description="Prazo de entrega"),
	des_politica_entrega            STRING    OPTIONS(description="Descrição com os detalhes da política de entrega"),
	dt_hr_desativado                TIMESTAMP OPTIONS(description="Data de desativação"),
	flg_habilitado                  BOOLEAN   OPTIONS(description="Status de seller habiltado/desabilitado"),
	dt_hr_ativado                   TIMESTAMP OPTIONS(description="Data de ativação"),
	cod_integrador                  STRING    OPTIONS(description="Código do integrador"),
	nom_integrador                  STRING    OPTIONS(description="Nome do integrador. Por exemplo, PluggTo."),
	sgl_integrador                  STRING    OPTIONS(description="Sigla do integrador (Por exemplo prefixo)"),
	dt_hr_atualizacao               TIMESTAMP OPTIONS(description="Data de atualização"),
	url_img_logo_seller             STRING    OPTIONS(description="URL da imagem do logotipo do seller"),
	cod_organizacao                 INT64     OPTIONS(description="Código da organização"),
	nom_gateway_pagamento           STRING    OPTIONS(description="Nome do gateway de pagamento"),
	cod_gateway_pagamento           STRING    OPTIONS(description="Código do gateway de pagamento"),
	st_gateway_pagamento            STRING    OPTIONS(description="Status do gateway de pagamento"),
	cnpj_seller                     STRING    OPTIONS(description="CNPJ do seller"),
	des_politica_devolucao          STRING    OPTIONS(description="Descrição com os detalhes da política de devolução"),
	insc_estadual                   STRING    OPTIONS(description="Inscrição estadual"),
	nom_seller                      STRING    OPTIONS(description="Nome do seller"),
	tag_seller                      STRING    OPTIONS(description="Nome do seller onde cada espaco entre as palavras se torna um underscore."),
	des_erro_atualizacao            STRING    OPTIONS(description="Lista com a descrição dos erros de atualização"),
	st_atualizacao                  STRING    OPTIONS(description="Status de atualizacao"),
	ver_seller                      INT64     OPTIONS(description="Versão do seller"),
	url_seller                      STRING    OPTIONS(description="URL da página web do seller (fora do marketplace da beleza)"),
	dt_hr_carga                     TIMESTAMP OPTIONS(description="Data e hora da carga na trusted"),
	dt_hr_referencia                TIMESTAMP OPTIONS(description="Horário de publicação de uma mensagem"),
	des_empresa_transportadora      STRING    OPTIONS(description="Descrição da empresa transportadora"),
	des_token_integracao_seller     STRING    OPTIONS(description="Descrição do Token de integração do seller"),
	des_codigo_seller               STRING    OPTIONS(description="Descrição do código do seller")
)
PARTITION BY TIMESTAMP_TRUNC(dt_hr_carga, DAY)
OPTIONS(description="Dados de Sellers na plataforma BLZ")
;