if not exists ( select 1 from auxiliar.tb_aux_config_carga_trusted where des_processo = 'prc_load_tb_seller_draft_prod_ecomm')
then
    insert into auxiliar.tb_aux_config_carga_trusted
        (   des_processo
          , des_dataset
          , des_tabela_destino
          , des_frequencia
          , des_tipo_delta
          , des_campo_delta
          , vlr_faixa_delta_inicio
          , vlr_faixa_delta_fim
          , des_observacao
		      , freq_temporal
        )
        VALUES
        (  'prc_load_tb_seller_draft_prod_ecomm'
          ,'ecommerce'
          ,'tb_seller_draft_prod_ecomm'
          ,'DIARIO'
          ,'INCREMENTAL'
          ,'dt_hr_referencia'
          ,-1
          ,0
          ,NULL
		      ,NULL
        );
    end if;