from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getIncidSchema(self, file):
    validationSchema = file.schema
    corrupt_file_schema = [StructField("_corrupt_record", StringType(), True)]
    validationSchema = StructType(validationSchema.fields + corrupt_file_schema)
    return validationSchema


def detailHPDColumns(file, spark):
    AdminNumberTags = ['ticket_id', 'portal_ticket_id', 'sigma_ticket_id', 'service_type', 'submitter',
                       'submit_date', 'last_modification_date', 'status_id', 'substatus_id', 'assigned_support_company',
                       'assigned_support_organization', 'assigned_support_group', 'reported_date', 'reported_source_id',
                       'impact_id', 'urgency_id', 'priority_id', 'operating_company_name', 'operating_le',
                       'operating_bu',
                       'contact_company_name', 'contact_le', 'contact_bu', 'summary', 'notes', 'end_user_country',
                       'end_user_site', 'end_user_address', 'end_user_city', 'resolution_text',
                       'required_resolution_date',
                       'assigned_agent', 'massive_flag', 'ticket_father_id', 'last_resolved_date', 'closed_date',
                       'rfs_date', 'product_categorization_tier_1', 'product_categorization_tier_2',
                       'product_categorization_tier_3',
                       'operational_categorization_tier_1', 'operational_categorization_tier_2',
                       'operational_categorization_tier_3',
                       'closure_categorization_tier_1', 'closure_categorization_tier_2',
                       'closure_categorization_tier_3',
                       'vendor_group', 'bmc_ticket_pending_date', 'inf2_hpd_outage_duration', 'reason_outage',
                       'rfo_available',
                       'ci_name', 'ci_country', 'ci_city', 'ci_id_fast', 'admin_number', 'instanceid']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
