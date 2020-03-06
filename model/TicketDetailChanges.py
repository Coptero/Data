from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getCRQSchema(self, file):
    validationSchema = file.schema
    corrupt_file_schema = [StructField("_corrupt_record", StringType(), True)]
    validationSchema = StructType(validationSchema.fields + corrupt_file_schema)
    return validationSchema


def detailHPDColumns(file, spark):
    AdminNumberTags = ['ticket_id', 'portal_ticket_id', 'ticket_submitter', 'ticket_submit_date', 'last_modified_by',
                       'last_modification_date', 'status_id', 'substatus_id', 'coordinator_company',
                       'coordinator_support_group',
                       'coordinator_name', 'manager_company', 'manager_support_group', 'ticket_reported_date',
                       'reported_source_id',
                       'impact_id', 'urgency_id', 'priority_id', 'operating_company_name',
                       'operating_organization_name',
                       'ci_country', 'company_country', 'summary', 'ticket_notes', 'contact_first_name',
                       'contact_last_name', 'portal_end_user_site_location', 'last_resolved_date', 'closed_date',
                       'vendor_group',
                       'product_categorization_tier_1', 'product_categorization_tier_2',
                       'product_categorization_tier_3', 'operational_categorization_tier_1',
                       'operational_categorization_tier_2', 'operational_categorization_tier_3', 'ticket_summary',
                       'portal_submit_date',
                       'ci_id_fast', 'admin_number', 'instanceid', 'class_crq', 'risl_level', 'change_reason']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
    return file