CREATE TABLE raw_amazon_blz.draft_product_list (
	subscription_name STRING OPTIONS(description='subscription_name'),
	message_id        STRING OPTIONS(description='message_id'),
	publish_time      TIMESTAMP   OPTIONS(description='publish_time'),
	data              STRING OPTIONS(description='data'),
	attributes        STRING OPTIONS(description='attributes')
)PARTITION BY TIMESTAMP_TRUNC(publish_time, DAY)
OPTIONS(description="Dados de Draft dos Produtos dos Sellers na plataforma BLZ")
