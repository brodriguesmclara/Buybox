CREATE OR REPLACE PROCEDURE sp.prc_load_tb_seller_buybox_ecomm(
	VAR_PRJ_RAW 				        STRING,
	VAR_PRJ_RAW_CUSTOM 	        STRING,
	VAR_PRJ_TRUSTED 		        STRING,
	VAR_PRJ_REFINED 		        STRING,
  VAR_PRJ_SENSITIVE_RAW       STRING,
  VAR_PRJ_SENSITIVE_TRUSTED   STRING,
  VAR_PRJ_SENSITIVE_REFINED   STRING
)
BEGIN

    -- Parametros usados para tabela de controle e log
    DECLARE VAR_PROCEDURE  DEFAULT 'prc_load_tb_seller_buybox_ecomm';
    DECLARE VAR_DELTA_INI  DATETIME;
    DECLARE VAR_DELTA_FIM  DATE;
    DECLARE VAR_TABELA     STRING;
    DECLARE VAR_DTH_INICIO TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

    -- Inicio do bloco de TRY/CATCH (tratamento de erros)
    BEGIN

	  	-- Recupera parametros da TB_AUX_CONFIG_CARGA_BUS
	  	CALL sp.prc_get_params_sensitive(VAR_PROCEDURE,VAR_PRJ_TRUSTED,VAR_DELTA_INI,VAR_DELTA_FIM,VAR_TABELA);

      -- Cria tabela temporaria retirando o nó de advertisemente do array
      EXECUTE IMMEDIATE """
      CREATE TEMP TABLE ad_full AS 
      SELECT
        subscription_name
        ,message_id
        ,publish_time
        ,JSON_EXTRACT(data,'$.advertisements') AS  data
        ,JSON_EXTRACT(data,'$') AS  root
      FROM `""" || VAR_PRJ_SENSITIVE_RAW || """.raw_amazon_blz.ad_buybox_list`
      WHERE publish_time BETWEEN '""" || VAR_DELTA_INI || """' AND '""" || VAR_DELTA_FIM || """'
      """;

      --Cricacao da tabela flat que será inserida na tabela final
      EXECUTE IMMEDIATE """
      CREATE TEMP TABLE tb_seller_tmp AS
      SELECT
         REPLACE(JSON_EXTRACT(data,'$.seller.id'), '"', '')  as cod_seller
        ,REPLACE(JSON_EXTRACT(data,'$.seller.tag'), '"', '') AS  des_tag
        ,REPLACE(JSON_EXTRACT(data,'$.seller.name'), '"', '') AS  nome_seller
        ,REPLACE(JSON_EXTRACT(data,'$.seller.organization_id'), '"', '') AS  cod_organizacao_seller
        ,CAST(JSON_EXTRACT(data,'$.seller.isSellerOrg') AS BOOL) AS  flg_seller_org
        ,CAST(JSON_EXTRACT(data,'$.inventory.quantity') AS INT64) AS  qt_produto_estoque
        ,ARRAY(
          SELECT STRUCT(
             JSON_EXTRACT(inventories, '$.id') AS  cod_inventario_produto
            ,CAST(JSON_EXTRACT(inventories,'$.quantity') AS INT64) AS  qt_estoque_localidade
            ,REPLACE(JSON_EXTRACT(inventories,'$.addressLocality'), '"', '') AS  logradouro_localidade
            ,REPLACE(JSON_EXTRACT(inventories,'$.type'), '"', '') AS  tp_localidade_estoque
            ,JSON_EXTRACT(inventories,'$.priority') AS  des_localidade_prioridade
            ,CAST(JSON_EXTRACT(inventories,'$.leadTimeBusinessDays') AS INT64) AS  qt_dias_entrega_localidade) 
          FROM UNNEST(JSON_EXTRACT_ARRAY(data,'$.inventory.inventories')) inventories
        ) AS  lista_estoque
        ,ARRAY(
          SELECT STRUCT(
             REPLACE(JSON_EXTRACT(ps,'$.sku'), '"', '') AS   cod_produto
            ,REPLACE(JSON_EXTRACT(ps,'$.type'), '"', '') AS  tp_produto_buybox
            ,REPLACE(REPLACE(JSON_EXTRACT(ps,'$.values'), '["', ''),'"]','') AS  des_produto_buybox
            ,CAST(JSON_EXTRACT(ps,'$.originalPrice') AS FLOAT64) AS vlr_venda_original
            ,CAST(JSON_EXTRACT(ps,'$.price') AS FLOAT64) AS  vlr_venda_produto
            ,CAST(JSON_EXTRACT(ps,'$.maxPrice') AS FLOAT64) AS  vlr_venda_maximo
            ,CAST(JSON_EXTRACT(ps,'$.percent') AS FLOAT64) AS pct_desconto
            ,CAST(JSON_EXTRACT(ps,'$.discount') AS FLOAT64) AS vlr_desconto
            ,CAST(JSON_EXTRACT(ps,'$.installments.numberOfPayments') AS INT64) AS  qt_parcelas
            ,CAST(JSON_EXTRACT(ps,'$.installments.monthlyPayment') AS FLOAT64) AS  vlr_parcela
            ,JSON_EXTRACT(ps,'$.installments.SellerType') AS  tp_seller
            ,JSON_EXTRACT(ps,'$.seller.id')AS cod_seller_preco_geral
            ,JSON_EXTRACT(ps,'$.seller.name')AS nom_seller_preco_geral)
          FROM UNNEST(JSON_EXTRACT_ARRAY(data,'$.priceSpecification')) ps
        ) AS lista_espec_preco
        ,ARRAY(
          SELECT STRUCT(
             REPLACE(JSON_EXTRACT(price,'$.sku'), '"', '') AS  cod_produto
            ,REPLACE(JSON_EXTRACT(price,'$.type'), '"', '') AS  tp_produto_buybox
            ,REPLACE(REPLACE(JSON_EXTRACT(price,'$.values'), '["', ''),'"]','') AS  des_produto_buybox
            ,CAST(JSON_EXTRACT(price,'$.originalPrice') AS FLOAT64) AS vlr_venda_original
            ,CAST(JSON_EXTRACT(price,'$.price') AS FLOAT64) AS vlr_venda_produto
            ,CAST(JSON_EXTRACT(price,'$.maxPrice') AS FLOAT64) AS vlr_venda_maximo
            ,CAST(JSON_EXTRACT(price,'$.percent') AS FLOAT64) AS pct_desconto
            ,CAST(JSON_EXTRACT(price,'$.discount') AS FLOAT64) AS vlr_desconto
            ,CAST(JSON_EXTRACT(price,'$.installments.numberOfPayments') AS INT64) AS qt_parcelas
            ,CAST(JSON_EXTRACT(price,'$.installments.monthlyPayment') AS FLOAT64) AS vlr_parcela
            ,JSON_EXTRACT(price,'$.installments.sellerType') AS  tp_seller 
            ,JSON_VALUE(price,'$.seller.id') AS cod_seller_precos
            ,JSON_VALUE(price,'$.seller.name') AS nome_seller_precos)
          FROM UNNEST(JSON_EXTRACT_ARRAY(data,'$.prices')) price
        ) AS  lista_preco
        ,CAST(JSON_EXTRACT(data,'$.gift') AS BOOL) AS  flg_presente
        ,CAST(JSON_EXTRACT(data,'$.marketable') AS BOOL) AS flg_vendavel
        ,publish_time as dt_hr_referencia
        ,CURRENT_TIMESTAMP() AS  dt_hr_carga
        ,REPLACE(JSON_EXTRACT(data,'$.inventory.type'),'"', '') AS  tp_inventario
        ,REPLACE(JSON_EXTRACT(data,'$.seller.deliveryProvider.displayName'),'"','') AS nome_empresa_envio
        ,CAST(JSON_EXTRACT(data,'$.seller.deliveryProvider.enabled') AS BOOL) AS flg_empresa_envio_habilitada
        ,CAST(JSON_EXTRACT(data,'$.inventory.leadTimeBusinessDays') AS INT64) AS qt_dias_entrega_estoque_geral
        ,CAST(JSON_VALUE(root,'$.lastUpdate.updateAt') AS TIMESTAMP) AS dt_hr_atualizacao_produto
        ,CAST(JSON_VALUE(root,'$.releaseDate') AS TIMESTAMP) AS dt_hr_release_produto
      FROM ad_full INNER JOIN UNNEST(JSON_EXTRACT_ARRAY(data,'$')) AS  data;
      """;

      --Delecao dos dados dentro do range D-1 para garantir a nao duplicidade dos dados
      EXECUTE IMMEDIATE """
      DELETE `""" || VAR_PRJ_SENSITIVE_TRUSTED || """.""" || VAR_TABELA || """`
      WHERE DATE(dt_hr_referencia) BETWEEN '""" || VAR_DELTA_INI || """' AND '""" || VAR_DELTA_FIM || """'
      """;

      EXECUTE IMMEDIATE """
      INSERT INTO `""" || VAR_PRJ_SENSITIVE_TRUSTED || """.""" || VAR_TABELA || """` 
      SELECT  
            *
      FROM tb_seller_tmp
       """; 

    -- Grava log com final de execucao com sucesso
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);

	EXCEPTION WHEN ERROR THEN
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);
		RAISE USING MESSAGE = @@error.message;
	END;
END;
