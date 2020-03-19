from dataclasses import dataclass


@dataclass
class ConfigJson(object):
    operational_path: str
    tags_admin_path: str
    tags_operating_path: str
    customer_path: str
    end_customer_path: str
    fast_min_results: int
    fast_postgresql: str
    fast_user: str
    fast_password: str
    fast_query: str
    fast_main_query: str
    fast_sc_query: str
    fast_agg_query: str
    fast_nni_query: str
    fast_crm_query: str
    fast_supplier_query: str
    fast_ispwire_query: str
    fast_parquet_path: str
    fast_main_parquet_path: str
    fast_sc_parquet_path: str
    fast_agg_parquet_path: str
    fast_nni_parquet_path: str
    fast_crm_parquet_path: str
    fast_supplier_parquet_path: str
    fast_ispwire_parquet_path: str
    fast_network_parquet_path: str
    rod_agent_smc_parquet_path: str
    rod_relations_parquet_path: str
    elastic_env_index_prefix: str
    elastic_nodes: str
    elastic_port: str
    elastic_user: str
    elastic_password: str

# TODO elastic: spray.json.JsValue
