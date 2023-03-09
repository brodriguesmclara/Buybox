CREATE TABLE raw_amazon_blz.shipping_address_streaming (
	subscription_name STRING    OPTIONS(description='Nome de uma assinatura'),
	message_id        STRING    OPTIONS(description='ID da uma mensagem'),
	publish_time      TIMESTAMP OPTIONS(description='Horário de publicação de uma mensagem'),
	data              STRING    OPTIONS(description='O corpo da mensagem'),
	attributes        STRING    OPTIONS(description='Um objeto JSON com todos os atributos de mensagem. Ele também contém outros campos que fazem parte da mensagem do Pub/Sub. incluindo a chave de ordem, se presente')
)
PARTITION BY TIMESTAMP_TRUNC(publish_time, DAY)
OPTIONS(description="Dados de endereço de entrega de um seller na order do marketplace beleza na web")
;