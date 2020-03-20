from pyspark.sql import *


class FastByAdmin: 
    # TODO r.getAs("FAST_MRC") ????
    def fastColumns(file):
        FastByAdmin = ['fast_customer', 'fast_end_customer', 'fast_country', 'fast_address', 'mrc', 'fast_currency',
                    'fast_name', 'admin_number', 'fast_service_id', 'fast_service_id', 'fast_product_code',
                    'fast_service_type', 'fast_activated_when', 'fast_access_diversity', 'fast_max_resolution_time',
                    'fast_service_availability', 'fast_provision_time', 'fast_top_object_id', 'fast_prod_spec']
        for c, n in zip(file.columns, FastByAdmin):
            file = file.withColumnRenamed(c, n)

        return file