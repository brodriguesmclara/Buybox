CREATE OR REPLACE PROCEDURE sp.prc_load_tb_seller_ecomm(
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
    DECLARE VAR_PROCEDURE  DEFAULT 'prc_load_tb_seller_ecomm';
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
		CREATE TEMP TABLE seller_temp AS
		SELECT  
			JSON_VALUE(data, '$._class') AS nom_classe                     
			,JSON_VALUE(data, '$._id."$oid"') AS cod_seller                     
			,JSON_VALUE(data, '$.about') AS des_loja                       
			,CAST(JSON_VALUE(data, '$.accessData.active') AS BOOLEAN) AS flg_ativo                      
			,JSON_VALUE(data, '$.accessData.login') AS des_login                      
			,JSON_VALUE(data, '$.address.city') AS cidade_comercial               
			,JSON_VALUE(data, '$.address.complement') AS compl_comercial                
			,JSON_VALUE(data, '$.address.district') AS distrito_comercial             
			,JSON_VALUE(data, '$.address.number') AS nr_logradouro                  
			,JSON_VALUE(data, '$.address.postalCode') AS cep_comercial                  
			,JSON_VALUE(data, '$.address.state') AS estado_comercial               
			,JSON_VALUE(data, '$.address.streetAddress') AS logradouro_comercial           
			,JSON_VALUE(data, '$.address.bankAccount.account')AS cod_conta                      
			,CAST(JSON_VALUE(data, '$.address.bankAccount.accountVV') AS INTEGER) AS cod_verificacao                
			,JSON_VALUE(data, '$.address.bankAccount.agency')AS cod_agencia                    
			,CAST(JSON_VALUE(data, '$.address.bankAccount.code') AS INTEGER) AS cod_banco                      
			,JSON_VALUE(data, '$.address.bankAccount.document')AS banco_documento                 
			,JSON_VALUE(data, '$.address.bankAccount.bank')AS nome_conta                 
			,JSON_VALUE(data, '$.brand._id') AS cod_marca                        
			,JSON_VALUE(data, '$.brand.slugName') AS des_marca_resumo                
			,JSON_VALUE(data, '$.category') AS des_categoria                  
			,JSON_VALUE(data, '$.companyName') AS nom_companhia                  
			,JSON_VALUE(data, '$.configDeliveryProvider.deliveryProvider._id."$oid"') AS cod_empresa_envio          
			,JSON_VALUE(data, '$.configDeliveryProvider.deliveryProvider.displayName') AS nom_empresa_envio          
			,JSON_VALUE(data, '$.configDeliveryProvider.deliveryProvider.name') AS nom_empresa_envio_resumo   
			,JSON_VALUE(data, '$.configDeliveryProvider.deliveryProvider.type') AS tp_envio                 
			,CAST(JSON_VALUE(data, '$.configDeliveryProvider.enabled') AS BOOLEAN) AS flg_empresa_envio_habiltada    
			,JSON_VALUE(data, '$.configDeliveryProvider.reindexStatus.error') AS des_empresa_envio_invalida     
			,JSON_VALUE(data, '$.configDeliveryProvider.reindexStatus.status') AS st_empresa_envio               
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.configDeliveryProvider.updateAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_empresa_envio_atualizacao
			,JSON_VALUE(data, '$.configuration.moneyTransfer') AS tp_transferencia_monetaria     
			,JSON_VALUE(data, '$.configuration.salesCommission') AS tx_comissao                    
			,CAST(JSON_VALUE(data, '$.configuration.salesCommissionDay') AS INTEGER) AS nr_dia_comissao                
			,JSON_VALUE(data, '$.contactPerson[0].cellPhoneNumber') AS tel_celular_comercial         -- ***** VERIFICAR POSSIBILIDADE DE VIRAR ARRAY 
			,JSON_VALUE(data, '$.contactPerson[0].department') AS nom_departamento_comercial         -- ***** VERIFICAR POSSIBILIDADE DE VIRAR ARRAY
			,JSON_VALUE(data, '$.contactPerson[0].email') AS email_comercial                         -- ***** VERIFICAR POSSIBILIDADE DE VIRAR ARRAY
			,JSON_VALUE(data, '$.contactPerson[0].name') AS nom_contato_comercial                    -- ***** VERIFICAR POSSIBILIDADE DE VIRAR ARRAY
			,JSON_VALUE(data, '$.contactPerson[0].phoneNumber') AS tel_comercial                     -- ***** VERIFICAR POSSIBILIDADE DE VIRAR ARRAY
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.createdAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_criacao                  
			,CAST(JSON_VALUE(data, '$.deliveryLeadTime') AS INTEGER) AS prz_entrega                    
			,JSON_VALUE(data, '$.deliveryPolicy') AS des_politica_entrega         --**** CAMPO NAO EXISTENTE  
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.disabledAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_desativado               
			,CAST(JSON_VALUE(data, '$.enabled') AS BOOLEAN) AS flg_habilitado                 
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.enabledAt."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_ativado                  
			,JSON_VALUE(data, '$.integrator._id') AS cod_integrador                 
			,JSON_VALUE(data, '$.integrator.name') AS nom_integrador                 
			,JSON_VALUE(data, '$.integrator.prefix') AS sgl_integrador                 
			,CAST(TIMESTAMP_MILLIS(CAST(REPLACE(JSON_VALUE(data, '$.lastModified."$date"'), '}','') AS INT64)) AS TIMESTAMP) AS dt_hr_atualizacao              
			,JSON_VALUE(data, '$.logo') AS url_img_logo_seller            
			,CAST(JSON_VALUE(data, '$.organizationId') AS INTEGER) AS cod_organizacao              
			,JSON_VALUE(data, '$.paymentGateway.name') AS nom_gateway_pagamento          
			,JSON_VALUE(data, '$.paymentGateway.gatewayInfo.externalId') AS cod_gateway_pagamento          
			,JSON_VALUE(data, '$.paymentGateway.gatewayInfo.status') AS st_gateway_pagamento           
			,JSON_VALUE(data, '$.registeredNumber') AS cnpj_seller                    
			,JSON_VALUE(data, '$.returnPolicy') AS des_politica_devolucao         
			,JSON_VALUE(data, '$.stateInscription') AS insc_estadual                  
			,JSON_VALUE(data, '$.storeName') AS nom_seller                     
			,JSON_VALUE(data, '$.tag') AS tag_seller                     
			,JSON_VALUE(data, '$.updateStatus.error') AS des_erro_atualizacao           
			,JSON_VALUE(data, '$.updateStatus.status') AS st_atualizacao                 
			,CAST(JSON_VALUE(data, '$.version') AS INTEGER) AS ver_seller                    
			,JSON_VALUE(data, '$.websiteUrl') AS url_seller                     
			,CURRENT_TIMESTAMP() AS dt_hr_carga                    
			,publish_time AS dt_hr_referencia  
			,JSON_VALUE(data, '$.data.configDeliveryProvider.defaultCarrier') AS des_empresa_transportadora
      		,JSON_VALUE(data, '$.data.integrator.sellerToken') AS des_token_integracao_seller
      		,JSON_VALUE(data, '$.data.code') AS des_codigo_seller             
		FROM `""" || VAR_PRJ_SENSITIVE_RAW || """.raw_amazon_blz.seller_list`
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
			nom_classe                     
			,cod_seller                     
			,des_loja                       
			,flg_ativo                      
			,des_login                      
			,cidade_comercial               
			,compl_comercial                
			,distrito_comercial             
			,nr_logradouro                  
			,cep_comercial                  
			,estado_comercial               
			,logradouro_comercial           
			,cod_conta                      
			,cod_verificacao                
			,cod_agencia                    
			,cod_banco                      
			,banco_documento                
			,nome_conta                     
			,cod_marca                      
			,des_marca_resumo               
			,des_categoria                  
			,nom_companhia                  
			,cod_empresa_envio              
			,nom_empresa_envio              
			,nom_empresa_envio_resumo       
			,tp_envio                       
			,flg_empresa_envio_habiltada    
			,des_empresa_envio_invalida     
			,st_empresa_envio               
			,dt_hr_empresa_envio_atualizacao
			,tp_transferencia_monetaria     
			,tx_comissao                    
			,nr_dia_comissao                
			,tel_celular_comercial          
			,nom_departamento_comercial     
			,email_comercial                
			,nom_contato_comercial          
			,tel_comercial                  
			,dt_hr_criacao                  
			,prz_entrega                    
			,des_politica_entrega           
			,dt_hr_desativado               
			,flg_habilitado                 
			,dt_hr_ativado                  
			,cod_integrador                 
			,nom_integrador                 
			,sgl_integrador                 
			,dt_hr_atualizacao              
			,url_img_logo_seller            
			,cod_organizacao                
			,nom_gateway_pagamento          
			,cod_gateway_pagamento          
			,st_gateway_pagamento           
			,cnpj_seller                    
			,des_politica_devolucao         
			,insc_estadual                  
			,nom_seller                     
			,tag_seller                     
			,des_erro_atualizacao           
			,st_atualizacao                 
			,ver_seller                     
			,url_seller                     
			,dt_hr_carga                    
			,dt_hr_referencia               
		FROM seller_temp
		""";

		-- Grava log com final de execucao com sucesso
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);

	EXCEPTION WHEN ERROR THEN
		CALL sp.prc_log_exec_sensitive(VAR_TABELA, VAR_DTH_INICIO, @@row_count, VAR_PROCEDURE, @@error.message, VAR_PRJ_TRUSTED);
		RAISE USING MESSAGE = @@error.message;
	END;
END;
