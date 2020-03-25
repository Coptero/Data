class ConfigJson():
    def __init__(self, operational_path, tags_admin_path, tags_operating_path, customer_path, end_customer_path,
                 fast_min_results, fast_postgresql, fast_user, fast_password, fast_query, fast_main_query,
                 fast_sc_query, fast_agg_query, fast_nni_query, fast_crm_query, fast_supplier_query, fast_ispwire_query,
                 fast_parquet_path, fast_main_parquet_path, fast_sc_parquet_path, fast_agg_parquet_path,
                 fast_nni_parquet_path, fast_crm_parquet_path, fast_supplier_parquet_path, fast_ispwire_parquet_path,
                 fast_network_parquet_path, rod_agent_smc_parquet_path, rod_relations_parquet_path,
                 elastic_env_index_prefix, elastic_nodes, elastic_port, elastic_user, elastic_password):

        self.operational_path = operational_path
        self.tags_admin_path = tags_admin_path
        self.tags_operating_path = tags_operating_path
        self.customer_path = customer_path
        self.end_customer_path = end_customer_path
        self.fast_min_results = fast_min_results
        self.fast_postgresql = fast_postgresql
        self.fast_user = fast_user
        self.fast_password = fast_password
        self.fast_query = fast_query
        self.fast_main_query = fast_main_query
        self.fast_sc_query = fast_sc_query
        self.fast_agg_query = fast_agg_query
        self.fast_nni_query = fast_nni_query
        self.fast_crm_query = fast_crm_query
        self.fast_supplier_query = fast_supplier_query
        self.fast_ispwire_query = fast_ispwire_query
        self.fast_parquet_path = fast_parquet_path
        self.fast_main_parquet_path = fast_main_parquet_path
        self.fast_sc_parquet_path = fast_sc_parquet_path
        self.fast_agg_parquet_path = fast_agg_parquet_path
        self.fast_nni_parquet_path = fast_nni_parquet_path
        self.fast_crm_parquet_path = fast_crm_parquet_path
        self.fast_supplier_parquet_path = fast_supplier_parquet_path
        self.fast_ispwire_parquet_path = fast_ispwire_parquet_path
        self.fast_network_parquet_path = fast_network_parquet_path
        self.rod_agent_smc_parquet_path = rod_agent_smc_parquet_path
        self.rod_relations_parquet_path = rod_relations_parquet_path
        self.elastic_env_index_prefix = elastic_env_index_prefix
        self.elastic_nodes = elastic_nodes
        self.elastic_port = elastic_port
        self.elastic_user = elastic_user
        self.elastic_password = elastic_password
