CREATE OR REPLACE PROCEDURE sp.prc_load_tb_seller_draft_prod_ecomm(
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
    DECLARE VAR_PROCEDURE  DEFAULT 'prc_load_tb_seller_draft_prod_ecomm';
    DECLARE VAR_DELTA_INI  DATE;
    DECLARE VAR_DELTA_FIM  DATE;
    DECLARE VAR_TABELA     STRING;
    DECLARE VAR_DTH_INICIO TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

    BEGIN

		-- Recupera parametros da TB_AUX_CONFIG_CARGA_BUS
	  	CALL sp.prc_get_params_sensitive(VAR_PROCEDURE,VAR_PRJ_TRUSTED,VAR_DELTA_INI,VAR_DELTA_FIM,VAR_TABELA);

		-- Insere os dados na tabela final
		EXECUTE IMMEDIATE"""
		CREATE TEMP TABLE draft_temp AS 
		SELECT  
		  	JSON_VALUE(data, '$._class') AS nome_classe
		  	,JSON_VALUE(data, '$._id."$oid"') AS cod_objeto
		  	,JSON_VALUE(data, '$.adjustmentRequest.message') AS des_mensagem_alteracao               
		  	,JSON_VALUE(data, '$.adjustmentRequest.requestedByEmail') AS email_solicitante_alteracao  
		  	,JSON_VALUE(data, '$.adjustmentRequest.requestedById') AS cod_solicitante_alteracao  
		  	,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.adjustmentRequest.requestedAt._date'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_solicitacao_ajuste  
		  	,JSON_VALUE(data, '$.approval.approvedById') AS cod_aprovador  
      		,JSON_VALUE(data, '$.approval.approvedByEmail') AS email_aprovador            
      		,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.approval.approvedAt._date'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_aprovacao            
      		,REPLACE(JSON_EXTRACT(data, '$.brand.line.slugName'),'"','') AS des_linha_resumo          
      		,JSON_VALUE(data, '$.brand.name') AS des_marca_produto         
      		,JSON_VALUE(data, '$.brand.slugName') AS des_marca_resumo          
      		,REPLACE(JSON_EXTRACT(data, '$.department._id'),'"','') AS cod_departamento          
      		,JSON_EXTRACT(data, '$.department.categories') AS des_categoria   
      		,JSON_VALUE(data, '$.department.name') AS des_departamento          
      		,JSON_VALUE(data, '$.department.slugName') AS des_departamento_resumo   
      		,REPLACE(JSON_EXTRACT(data, '$.details.action'),'"','') AS des_acao                  
      		,REPLACE(JSON_EXTRACT(data, '$.details.advice'),'"','') AS des_comousar              
      		,REPLACE(JSON_EXTRACT(data, '$.details.benefits'),'"','') AS des_beneficio             
      		,REPLACE(JSON_EXTRACT(data, '$.details.description'),'"','') AS des_produto               
      		,REPLACE(JSON_EXTRACT(data, '$.details.forWho'),'"','') AS des_paraquem              
      		,REPLACE(JSON_EXTRACT(data, '$.details.occasion'),'"','') AS des_ocasiao               
      		,REPLACE(JSON_EXTRACT(data, '$.details.result'),'"','') AS des_resultado             
      		,REPLACE(JSON_EXTRACT(data, '$.details.shortDescription'),'"','') AS des_produto_resumo        
      		,REPLACE(JSON_EXTRACT(data, '$.details.whatItDoes'),'"','') AS des_comofunciona          
      		,REPLACE(JSON_EXTRACT(data, '$.details.WhatItIs'),'"','') AS des_oque                  
      		,REPLACE(JSON_EXTRACT(data, '$.details.olfactivePyramid.baseNodes'),'"','') AS des_fundo_piramideolfativa
      		,REPLACE(JSON_EXTRACT(data, '$.details.olfactivePyramid.middleNodes'),'"','') AS des_corpo_piramideolfativa
      		,REPLACE(JSON_EXTRACT(data, '$.details.olfactivePyramid.topNodes'),'"','') AS des_topo_piramideolfativa 
      		,CAST(JSON_VALUE(data, '$.enabled') AS BOOLEAN) AS flg_habilitado              
      		,ARRAY(
				SELECT STRUCT(
					JSON_VALUE(data, '$') AS cod_ean_lista)
				FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.gtins'))  AS gtins
			) AS lista_gtins                   
      		,ARRAY(
				SELECT STRUCT(
					CAST(JSON_VALUE(imagens, '$.featured') AS BOOLEAN)  AS flg_destaque
					,JSON_VALUE(imagens, '$.fileName') AS arquivo_img_produto
					,JSON_VALUE(imagens, '$.imageUrl') AS url_img_produto
				)
				FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.imageObjects'))  AS imagens
			) AS lista_imagem_objeto           
      		,ARRAY(
				SELECT STRUCT(
						JSON_VALUE(invalidos, '$.description') AS des_campos_invalidos
						,JSON_VALUE(invalidos, '$.field') AS nome_campos_invalidos
				)
				FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.invalidFields')) AS invalidos
			) AS lista_invalidos   
      		,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.lastUpdate.updateAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_alteracao      
      		,JSON_VALUE(data, '$.lastUpdate.updatedByEmail') AS email_autor_alteracao     
      		,JSON_VALUE(data, '$.mediaObject.updatedByEmail') AS url_midia_produto         
      		,JSON_VALUE(data, '$.name') AS nome_produto              
      		,JSON_VALUE(data, '$.organization._id') AS cod_organizacao           
      		,JSON_VALUE(data, '$.presentation.unit') AS des_unid_volume           
      		,JSON_VALUE(data, '$.presentation.value') AS vlr_volume                
      		,ARRAY(
				SELECT STRUCT(
						JSON_VALUE(propriedade, '$.name') AS des_propriedades
						,JSON_VALUE(propriedade, '$.value') AS des_valor_propriedades
				)
				FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.properties')) AS propriedade
			) AS lista_propriedade    
      		,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.releaseDate."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_lancamento          
      		,JSON_VALUE(data, '$.scaleValue.unit') AS vlr_peso                  
      		,JSON_VALUE(data, '$.scaleValue.value') AS des_unid_peso             
      		,JSON_VALUE(data, '$.seller._id."$oid"') AS cod_seller                
      		,JSON_VALUE(data, '$.seller.referenceId') AS cod_referencia            
      		,JSON_VALUE(data, '$.seller.sku') AS cod_produto_seller        
      		,JSON_VALUE(data, '$.seo.description') AS des_seo                   
      		,JSON_VALUE(data, '$.seo.keywords') AS des_palavras_chaves       
      		,JSON_VALUE(data, '$.seo.title') AS des_titulo_seo            
      		,JSON_VALUE(data, '$.sku') AS cod_produto               
      		,JSON_VALUE(data, '$.status') AS st_produto                
      		,CAST(JSON_VALUE(data, '$.visible') AS BOOLEAN) AS flg_visivel                
      		,CURRENT_TIMESTAMP AS dt_hr_carga               
      		,JSON_VALUE(data, '$.matchProductSku') AS cod_produto_match
			,publish_time AS  dt_hr_referencia 
			,CAST(JSON_VALUE(data,'$.measures.length') AS NUMERIC) AS qt_comprimento_produto
			,CAST(JSON_VALUE(data,'$.measures.width') AS NUMERIC) AS qt_largura_produto
			,CAST(JSON_VALUE(data,'$.measures.height') AS NUMERIC) AS qt_altura_produto
			,CAST(JSON_VALUE(data,'$.measures.weight') AS NUMERIC) AS qt_peso_produto
			,JSON_VALUE(data,'$.brand._id') AS cod_marca_produto
			,JSON_VALUE(data,'$.lastUpdate.updatedById') AS cod_autor_alteracao
			,JSON_VALUE(data,'$.seller.storeName') AS nome_loja_seller
			,JSON_VALUE(data,'$.seller.tag') AS tag_loja_seller
			,JSON_VALUE(data,'$.seller.organizationId') AS cod_organizacao_seller
			,CAST(JSON_VALUE(data,'$.seller.enabled') AS BOOLEAN) AS flg_ativacao_seller
			,JSON_VALUE(data,'$.seller.deliveryProvider.displayName') AS nome_empresa_envio
			,CAST(JSON_VALUE(data,'$.seller.deliveryProvider.enabled') AS BOOLEAN) AS flg_empresa_envio_habilitada
			,JSON_VALUE(data,'$.seller.integrator._id') AS cod_empresa_integradora
			,JSON_VALUE(data,'$.seller.integrator.prefix') AS prefixo_empresa_integradora
			,JSON_VALUE(data,'$.seller.integrator.name') AS nome_empresa_integradora
			,JSON_VALUE(data,'$.seller.integrator.sellerToken') AS token_empresa_integradora
			,JSON_VALUE(data,'$.enabledBy') AS nome_habilitado_por
			,JSON_VALUE(data,'$.reasonForRefusal') AS des_razao_recusa
			,JSON_VALUE(data,'$.approveAwaitingAdjustment.requestedBy') AS nome_solicitante_ajustes
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data,'$.approveAwaitingAdjustment.requestedAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_solicitacao_ajustes
			,JSON_VALUE(data,'$.deny.message') AS msg_produto_negado
			,JSON_VALUE(data,'$.deny.deniedById') AS cod_solicitante_produto_negado
			,JSON_VALUE(data,'$.deny.deniedByEmail') AS email_solicitante_produto_negado
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data,'$.deny.deniedAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_produto_negado
			,JSON_VALUE(data,'$.gtin') AS cod_ean
		FROM `""" || VAR_PRJ_SENSITIVE_RAW || """.raw_amazon_blz.draft_product_list` 
		WHERE DATE(publish_time) BETWEEN '""" || VAR_DELTA_INI || """' AND '""" || VAR_DELTA_FIM || """';
		""";

		-- Deleta intervalo de datas a ser inserido
		EXECUTE IMMEDIATE """
		DELETE `""" || VAR_PRJ_SENSITIVE_TRUSTED || """.""" || VAR_TABELA || """`
		WHERE DATE(dt_hr_referencia) BETWEEN '""" || VAR_DELTA_INI || """' AND '""" || VAR_DELTA_FIM || """'  
		""";

		-- Insere os dados na tabela final
		EXECUTE IMMEDIATE"""
		INSERT INTO `""" || VAR_PRJ_SENSITIVE_TRUSTED || """.""" || VAR_TABELA || """` 
      	SELECT  
      	      *
      	FROM draft_temp
		""";

		 -- Grava log com final de execucao com sucesso
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);

	EXCEPTION WHEN ERROR THEN
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);
		RAISE USING MESSAGE = @@error.message;
	END;
END;
