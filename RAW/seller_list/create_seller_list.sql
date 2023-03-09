CREATE TABLE raw_amazon_blz.seller_list (
	subscription_name STRING OPTIONS(description='subscription_name'),
	message_id        STRING OPTIONS(description='message_id'),
	publish_time      TIMESTAMP   OPTIONS(description='publish_time'),
	data              STRING OPTIONS(description='data'),
	attributes        STRING OPTIONS(description='attributes')
)PARTITION BY TIMESTAMP_TRUNC(publish_time, DAY)
OPTIONS(description="Dados de Sellers na plataforma BLZ")
;