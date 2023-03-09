CREATE TABLE raw_amazon_blz.ad_buybox_list (
	subscription_name STRING    OPTIONS(description='Nome de uma assinatura'),
	message_id        STRING    OPTIONS(description='ID de uma mensagem'),
	publish_time      TIMESTAMP OPTIONS(description='O horário de publicação de uma mensagem'),
	data              STRING    OPTIONS(description='O corpo da mensagem. O campo data é obrigatório para todas as tabelas do BigQuery de destino.'),
	attributes        STRING    OPTIONS(description='Um objeto JSON com todos os atributos de mensagem. Ele também contém outros campos que fazem parte da mensagem do pub/sub, incluindo a chave de ordem se presente.')
)
PARTITION BY TIMESTAMP_TRUNC(publish_time, DAY)
OPTIONS(description="Dados de advertisement de buybox na plataforma BLZ")
;