CREATE OR REPLACE PROCEDURE sp.prc_load_tb_seller_buybox_pedido_ecomm(
	VAR_PRJ_RAW 				STRING,
	VAR_PRJ_RAW_CUSTOM 			STRING,
	VAR_PRJ_TRUSTED 			STRING,
	VAR_PRJ_REFINED 			STRING,
    VAR_PRJ_SENSITIVE_RAW   	STRING,
    VAR_PRJ_SENSITIVE_TRUSTED   STRING,
    VAR_PRJ_SENSITIVE_REFINED   STRING   
)
BEGIN

    -- Parametros usados para tabela de controle e log
    DECLARE VAR_PROCEDURE  DEFAULT 'prc_load_tb_seller_buybox_pedido_ecomm';
    DECLARE VAR_DELTA_INI  DATE;
    DECLARE VAR_DELTA_FIM  DATE;
    DECLARE VAR_TABELA     STRING;
    DECLARE VAR_DTH_INICIO TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

    -- Inicio do bloco de TRY/CATCH (tratamento de erros)
    BEGIN

		-- Recupera parametros da TB_AUX_CONFIG_CARGA_BUS
		CALL sp.prc_get_params_sensitive(VAR_PROCEDURE,VAR_PRJ_TRUSTED,VAR_DELTA_INI,VAR_DELTA_FIM,VAR_TABELA);

		-- Insere os dados na tabela final   
		EXECUTE IMMEDIATE """
		CREATE TEMP TABLE pedidos_temp AS
		WITH pedidos AS (
		SELECT DISTINCT
			JSON_VALUE(orders.data, '$.id') AS cod_pedido,
			JSON_VALUE (orders.data, '$.cart_id') AS cod_carrinho_pedido,
			JSON_VALUE (orders.data, '$.customer_id') AS cod_cliente_marketplace,
			JSON_VALUE (orders.data, '$.organization_id') AS cod_organizacao,
			JSON_VALUE (orders.data, '$.status') AS st_pedido,
			CAST(JSON_VALUE (orders.data, '$.created_at') AS TIMESTAMP) AS dt_hr_criacao_pedido,
			CAST(JSON_VALUE (orders.data, '$.updated_at') AS TIMESTAMP) AS dt_hr_atualizacao_pedido,
			JSON_VALUE (orders.data, '$.ip') AS cod_ip_pedido,
			JSON_VALUE (orders.data, '$.app') AS des_app_pedido,
			JSON_VALUE (orders.data, '$.sales_channel') AS des_canal_venda_pedido,
			CAST(JSON_VALUE (orders.data, '$.difal_tax') AS FLOAT64) AS vlr_taxa_pedido,
			CAST(JSON_VALUE (orders.data, '$.sub_total') AS FLOAT64) AS vlr_subtotal_pedido,
			CAST(JSON_VALUE (orders.data, '$.discount') AS FLOAT64) AS vlr_desconto_pedido,
			CAST(JSON_VALUE (orders.data, '$.freight') AS FLOAT64) AS vlr_frete_pedido,
			CAST(JSON_VALUE (orders.data, '$.total') AS FLOAT64) AS vlr_total_pedido,
			CAST(JSON_VALUE (orders.data, '$.loyalty_discount') AS FLOAT64) AS vlr_desconto_fidelidade,
			--JSON_VALUE (orders.data, '$.integration_type') AS des_tipo_integracao,
			JSON_VALUE (items.data, '$.order_id') AS cod_pedido_item,
			JSON_VALUE (items.data, '$.id') AS cod_carrinho_item,
			JSON_VALUE (items.data, '$.sku') AS cod_produto,
			CAST(JSON_VALUE (items.data, '$.quantity') AS INT64) AS qt_item,
			CAST(JSON_VALUE (items.data, '$.max_price') AS FLOAT64) AS vlr_preco_max_item,
			CAST(JSON_VALUE (items.data, '$.price') AS FLOAT64) AS vlr_preco_item,
			CAST(JSON_VALUE (items.data, '$.discount') AS FLOAT64) AS vlr_desconto_item,
			CAST(JSON_VALUE (items.data, '$.gift') AS BOOLEAN) AS flg_presente_item,
			JSON_VALUE (items.data, '$.warehouse_id') AS cod_armazem_item,
			CAST(JSON_VALUE (items.data, '$.created_at') AS TIMESTAMP) AS dt_hr_criacao_item,
			CAST(JSON_VALUE (items.data, '$.updated_at') AS TIMESTAMP) AS dt_hr_atualizacao_item,
			CAST(JSON_VALUE (items.data, '$.delivered_physical_store') AS BOOLEAN) AS flg_entrega_lojafisica_item,
			CAST(JSON_VALUE (items.data, '$.hidden') AS BOOLEAN) AS flg_oculto_item,
			CAST(JSON_VALUE (items.data, '$.excluded') AS BOOLEAN) AS flg_excluido_item,
			JSON_VALUE (items.data, '$.category') AS des_categoria,
			JSON_VALUE (items.data, '$.warehouse_type') AS tp_armazem_item,
			JSON_VALUE (items.data, '$.shipping_package_id') AS cod_pacote_entrega_item,
			JSON_VALUE (items.data, '$.product_image_url') AS url_imagem_item,
			JSON_VALUE (seller.data, '$.order_id') AS cod_pedido_seller,
			JSON_VALUE (seller.data, '$.item_id') AS cod_carrinho_item_seller,
			JSON_VALUE (seller.data, '$.seller_id') AS cod_seller,
			JSON_VALUE (seller.data, '$.organization_id') AS cod_organizacao_seller,
			JSON_VALUE (seller.data, '$.status') AS st_pedido_seller,
			CAST(JSON_VALUE (seller.data, '$.created_at') AS TIMESTAMP) AS dt_hr_criacao_item_seller,  
			CAST(JSON_VALUE (seller.data, '$.updated_at') AS TIMESTAMP) AS dt_hr_atualizacao_item_seller, 
			JSON_VALUE (address.data, '$.postal_code') AS cep_endereco_entrega,
			JSON_VALUE (address.data, '$.address_region') AS uf_endereco_entrega,
			JSON_VALUE (address.data, '$.address_locality') AS cidade_endereco_entrega,
    		CURRENT_TIMESTAMP() AS dt_hr_carga,
    		orders.publish_time AS dt_hr_referencia
		FROM `""" || VAR_PRJ_SENSITIVE_RAW || """.raw_amazon_blz.orders_streaming` as orders 
		
		JOIN `""" || VAR_PRJ_RAW || """.raw_amazon_blz.item_streaming` as items
		ON JSON_VALUE (orders.data, '$.id') = JSON_VALUE (items.data, '$.order_id')
			
		JOIN `""" || VAR_PRJ_SENSITIVE_RAW || """.raw_amazon_blz.seller_item_streaming` as seller    
		ON JSON_VALUE (items.data, '$.order_id') = JSON_VALUE (seller.data, '$.order_id')
		AND JSON_VALUE (items.data, '$.id') = JSON_VALUE (seller.data, '$.item_id')
		
		JOIN `""" || VAR_PRJ_SENSITIVE_RAW || """.raw_amazon_blz.shipping_address_streaming` as address
		ON JSON_VALUE (items.data, '$.order_id') = JSON_VALUE (address.data, '$.order_id')
    	WHERE DATE(address.publish_time) BETWEEN '""" || VAR_DELTA_INI || """' AND '""" || VAR_DELTA_FIM || """'
    	ORDER BY 1,6,7,41,42 
		)
		SELECT
		   cod_pedido
		  ,cod_carrinho_pedido
		  ,cod_cliente_marketplace
		  ,cod_organizacao
		  ,CAST(MAX(dt_hr_criacao_pedido) AS TIMESTAMP) AS dt_hr_criacao_pedido
		  ,ARRAY(
		        SELECT STRUCT (
		          st_pedido
		          ,CAST(MAX(dt_hr_atualizacao_pedido) AS TIMESTAMP) AS dt_hr_atualizacao_pedido
		        )
		        FROM pedidos as pedido_sta 
		        WHERE pedido_sta.cod_pedido = pedidos.cod_pedido
		        GROUP BY st_pedido
		  ) AS st_pedidos
		  ,cod_ip_pedido
		  ,des_app_pedido
		  ,des_canal_venda_pedido
		  ,vlr_taxa_pedido
		  ,vlr_subtotal_pedido
		  ,vlr_desconto_pedido
		  ,vlr_frete_pedido
		  ,vlr_total_pedido
		  ,vlr_desconto_fidelidade
		  --,des_tipo_integracao
		  ,cod_pedido_item                
		  ,cod_produto
		  ,qt_item
		  ,vlr_preco_max_item
		  ,vlr_preco_item
		  ,vlr_desconto_item
		  ,flg_presente_item
		  ,cod_armazem_item
		  ,CAST(dt_hr_criacao_item AS TIMESTAMP) AS dt_hr_criacao_item
		  ,CAST(dt_hr_atualizacao_item AS TIMESTAMP) AS dt_hr_atualizacao_item
		  ,flg_entrega_lojafisica_item
		  ,flg_oculto_item
		  ,flg_excluido_item
		  ,des_categoria
		  ,tp_armazem_item
		  ,cod_pacote_entrega_item
		  ,url_imagem_item
		  ,cod_pedido_seller
		  ,cod_carrinho_item_seller
		  ,cod_seller
		  ,cod_organizacao_seller
		  ,ARRAY(
		        SELECT STRUCT (
		          st_pedido_seller
		          ,CAST(MAX(dt_hr_atualizacao_item_seller) AS TIMESTAMP) AS dt_hr_atualizacao_item_seller
		        )
		        FROM pedidos as pedido_sta
		        WHERE pedido_sta.cod_pedido = pedidos.cod_pedido 
		        AND pedido_sta.cod_carrinho_item_seller = pedidos.cod_carrinho_item_seller
		        GROUP BY st_pedido_seller
		  ) AS st_pedidos_seller
		  ,cep_endereco_entrega
		  ,uf_endereco_entrega
		  ,cidade_endereco_entrega
      	  ,dt_hr_carga
      	  ,dt_hr_referencia
		FROM pedidos
		GROUP BY 1,2,3,4,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,38,39,40,41,42
		""";

		-- Deleta intervalo de datas a ser inserido
		EXECUTE IMMEDIATE """
		DELETE `""" || VAR_PRJ_SENSITIVE_TRUSTED || """.""" || VAR_TABELA || """`
		WHERE DATE(dt_hr_referencia) BETWEEN '""" || VAR_DELTA_INI || """' AND '""" || VAR_DELTA_FIM || """'
		""";

		-- Insere os dados na tabela final
		EXECUTE IMMEDIATE"""
		INSERT INTO `""" || VAR_PRJ_SENSITIVE_TRUSTED || """.""" || VAR_TABELA || """`(
	 	   cod_pedido
		  ,cod_carrinho_pedido
		  ,cod_cliente_marketplace
		  ,cod_organizacao
		  ,dt_hr_criacao_pedido
		  ,st_pedidos
		  ,cod_ip_pedido
		  ,des_app_pedido
		  ,des_canal_venda_pedido
		  ,vlr_taxa_pedido
		  ,vlr_subtotal_pedido
		  ,vlr_desconto_pedido
		  ,vlr_frete_pedido
		  ,vlr_total_pedido
		  ,vlr_desconto_fidelidade
		  --,des_tipo_integracao
		  ,cod_pedido_item                
		  ,cod_produto
		  ,qt_item
		  ,vlr_preco_max_item
		  ,vlr_preco_item
		  ,vlr_desconto_item
		  ,flg_presente_item
		  ,cod_armazem_item
		  ,dt_hr_criacao_item
		  ,dt_hr_atualizacao_item
		  ,flg_entrega_lojafisica_item
		  ,flg_oculto_item
		  ,flg_excluido_item
		  ,des_categoria
		  ,tp_armazem_item
		  ,cod_pacote_entrega_item
		  ,url_imagem_item
		  ,cod_pedido_seller
		  ,cod_carrinho_item_seller
		  ,cod_seller
		  ,cod_organizacao_seller
		  ,st_pedidos_seller
		  ,cep_endereco_entrega
		  ,uf_endereco_entrega
		  ,cidade_endereco_entrega
          ,dt_hr_carga
          ,dt_hr_referencia
	)
		  SELECT
		   cod_pedido
		  ,cod_carrinho_pedido
		  ,cod_cliente_marketplace
		  ,cod_organizacao
		  ,dt_hr_criacao_pedido
		  ,st_pedidos
		  ,cod_ip_pedido
		  ,des_app_pedido
		  ,des_canal_venda_pedido
		  ,vlr_taxa_pedido
		  ,vlr_subtotal_pedido
		  ,vlr_desconto_pedido
		  ,vlr_frete_pedido
		  ,vlr_total_pedido
		  ,vlr_desconto_fidelidade
		  --,des_tipo_integracao
		  ,cod_pedido_item                
		  ,cod_produto
		  ,qt_item
		  ,vlr_preco_max_item
		  ,vlr_preco_item
		  ,vlr_desconto_item
		  ,flg_presente_item
		  ,cod_armazem_item
		  ,dt_hr_criacao_item
		  ,dt_hr_atualizacao_item
		  ,flg_entrega_lojafisica_item
		  ,flg_oculto_item
		  ,flg_excluido_item
		  ,des_categoria
		  ,tp_armazem_item
		  ,cod_pacote_entrega_item
		  ,url_imagem_item
		  ,cod_pedido_seller
		  ,cod_carrinho_item_seller
		  ,cod_seller
		  ,cod_organizacao_seller
		  ,st_pedidos_seller
		  ,cep_endereco_entrega
		  ,uf_endereco_entrega
		  ,cidade_endereco_entrega
      	  ,dt_hr_carga
      	  ,dt_hr_referencia
		FROM pedidos_temp 
		""";

		-- Grava log com final de execucao com sucesso
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);

	EXCEPTION WHEN ERROR THEN
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);
		RAISE USING MESSAGE = @@error.message;
	END;
END;
