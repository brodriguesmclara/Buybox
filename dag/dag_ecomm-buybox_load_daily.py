# ===========================================================================================
# Objeto........: dag_ecomm-buybox_load_daily
# Data Criacao..: 08/02/2023
# Projeto.......: BuyBox 
# Descricao.....: Atualizar as tabelas que serão a base para o modelo de BuyBox
# Departamento..: Arquitetura e Engenharia de Dados
# Autor.........: brunoro@grupoboticario.com.br
# ===========================================================================================


import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.models import Variable
from libs.airflow import log_and_slack, log
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Coleta variaveis do Airflow
env_var = Variable.get("dag_ecomm-buybox_load_daily", deserialize_json=True)

# Variaveis de Projeto
SCHEDULE_INTERVAL = env_var["schedule_interval"]
RETRIES = env_var["retries"]
RETRY_DELAY = env_var["retry_delay"]
DAG_TIMEOUT = env_var["dag_timeout"]
SLACK_WEBHOOK = env_var["slack_webhook"]
VAR_PRJ_RAW = env_var["var_prj_raw"]
VAR_PRJ_RAW_CUSTOM = env_var["var_prj_raw_custom"]
VAR_PRJ_TRUSTED = env_var["var_prj_trusted"]
VAR_PRJ_REFINED = env_var["var_prj_refined"]
VAR_PRJ_SENSITIVE_RAW = env_var["var_prj_sensitive_raw"]
VAR_PRJ_SENSITIVE_TRUSTED = env_var["var_prj_sensitive_trusted"]
VAR_PRJ_SENSITIVE_REFINED = env_var["var_prj_sensitive_refined"]

default_args = {
	"owner": "Gerencia: DS Consumidor, Coord: Consumidor e Marcas | Ecomm",
	"depends_on_past": False,
    "start_date": datetime(2021, 7, 20),
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=5),
	"on_failure_callback": log,
    "on_retry_callback": log,
    "on_success_callback": log,
}

dag = DAG(
    "dag_ecomm-buybox_load_daily",
    default_args=default_args,
    description="Executa pipeline via procuredes diariamente - BuyBox E-Commerce",
	schedule_interval=SCHEDULE_INTERVAL,
    tags=["Chapter/VS: E-commerce & Marketplace", "Projeto: BuyBox","Trusted", "Refined"],
    catchup=False
)

"""
    DATA QUALITY - FUNCTION
"""

def dataquality(TASK_ID, VAR_PRJ, VAR_DATASET, VAR_TABLE, VAR_TOLERANCE_DAYS, VAR_CONFIG_LOG_CARGA):
  """
  Validacao das origens usando Dataquality
  """
  task_dataquality = BigQueryExecuteQueryOperator(
      task_id=TASK_ID,
      sql=f"CALL `data-quality-gb.sp.prc_dataquality_log_carga`('{VAR_PRJ}', '{VAR_DATASET}', '{VAR_TABLE}', '{VAR_TOLERANCE_DAYS}','{VAR_CONFIG_LOG_CARGA}','Dado não foi atualizado no dia atual');",
      use_legacy_sql=False,
      priority="BATCH",
      dag=dag,
      depends_on_past=False
  )

  return task_dataquality

"""
    PROCEDURES - FUNCTION
"""
def exec_proc(VAR_PRJ_SENSITIVE, TASK_ID, PROCEDURE):
    """
    Funcao criada para parametrizar as procedures
    """
    prc_load_task = BigQueryExecuteQueryOperator(
            task_id=TASK_ID,
            sql="CALL `{}.sp.{}`('{}','{}','{}','{}','{}','{}','{}');".format(
                VAR_PRJ_SENSITIVE,
                PROCEDURE,
                VAR_PRJ_RAW,
                VAR_PRJ_RAW_CUSTOM,
                VAR_PRJ_TRUSTED,
                VAR_PRJ_REFINED,
                VAR_PRJ_SENSITIVE_RAW,
                VAR_PRJ_SENSITIVE_TRUSTED,
                VAR_PRJ_SENSITIVE_REFINED
            ),
            use_legacy_sql=False,
            priority="BATCH",
            dag=dag,
            depends_on_past=False
    )

    return prc_load_task

"""
    CONTROLE
"""
## Begin
begin = DummyOperator(task_id='begin', dag=dag)

## Mean
mean = DummyOperator(task_id='mean', dag=dag)

## End
end = DummyOperator(task_id='end', dag=dag)

"""
    ----- DATAQUALITYS -----
"""
## RAW
dataquality_draft_product_list = dataquality("dataquality_draft_product_list", f"{VAR_PRJ_SENSITIVE_RAW}", "raw_amazon_blz", "draft_product_list", "0", "1")
dataquality_ad_buybox_list = dataquality("dataquality_ad_buybox_list", f"{VAR_PRJ_SENSITIVE_RAW}", "raw_amazon_blz", "ad_buybox_list", "0", "1")

## TRUSTED
#dataquality_tb_pedido_total_item = dataquality("dataquality_tb_pedido_total_item", f"{VAR_PRJ_SENSITIVE_TRUSTED}", "plataforma_blz", "tb_pedido_total_item", "0", "1")

"""
   ----- PROCEDURES ------
"""
##TRUSTED
prc_load_tb_seller_draft_prod_ecomm_validacao = exec_proc(f"{VAR_PRJ_SENSITIVE_TRUSTED}","prc_load_tb_seller_draft_prod_ecomm_validacao","prc_load_tb_seller_draft_prod_ecomm_validacao")
prc_load_tb_seller_ecomm_validacao = exec_proc(f"{VAR_PRJ_SENSITIVE_TRUSTED}","prc_load_tb_seller_ecomm_validacao","prc_load_tb_seller_ecomm_validacao")
prc_load_tb_seller_buybox_ecomm_validacao = exec_proc(f"{VAR_PRJ_SENSITIVE_TRUSTED}","prc_load_tb_seller_buybox_ecomm_validacao","prc_load_tb_seller_buybox_ecomm_validacao")


##REFINED
#prc_load_tb_pedido_item_total_unificado = exec_proc(f"{VAR_PRJ_SENSITIVE_REFINED}","prc_load_tb_pedido_item_total_unificado","prc_load_tb_pedido_item_total_unificado")

"""
   ----- EXCECUCAO -----
"""

##TRUSTED
begin >> [dataquality_draft_product_list,dataquality_ad_buybox_list] >> mean  >> [prc_load_tb_seller_draft_prod_ecomm_validacao\
     , prc_load_tb_seller_ecomm_validacao, prc_load_tb_seller_buybox_ecomm_validacao] >> end



   
