from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getCRQSchema(file):
    validationSchema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("portal_ticket_id", StringType(), True),
        StructField("ticket_submitter", StringType(), True),
        StructField("ticket_submit_date", StringType(), True),
        StructField("last_modified_by", StringType(), True),
        StructField("last_modification_date", StringType(), True),
        StructField("status_id", StringType(), True),
        StructField("substatus_id", StringType(), True),
        StructField("coordinator_company", StringType(), True),
        StructField("coordinator_support_group", StringType(), True),
        StructField("coordinator_name", StringType(), True),
        StructField("manager_company", StringType(), True),
        StructField("manager_support_group", StringType(), True),
        StructField("ticket_reported_date", StringType(), True),
        StructField("reported_source_id", StringType(), True),
        StructField("impact_id", StringType(), True),
        StructField("urgency_id", StringType(), True),
        StructField("priority_id", StringType(), True),
        StructField("operating_company_name", StringType(), True),
        StructField("operating_organization_name", StringType(), True),
        StructField("ci_country", StringType(), True),
        StructField("company_country", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("ticket_notes", StringType(), True),
        StructField("contact_first_name", StringType(), True),
        StructField("contact_last_name", StringType(), True),
        StructField("portal_end_user_site_location", StringType(), True),
        StructField("last_resolved_date", StringType(), True),
        StructField("closed_date", StringType(), True),
        StructField("vendor_group", StringType(), True),
        StructField("product_categorization_tier_1", StringType(), True),
        StructField("product_categorization_tier_2", StringType(), True),
        StructField("product_categorization_tier_3", StringType(), True),
        StructField("operational_categorization_tier_1", StringType(), True),
        StructField("operational_categorization_tier_2", StringType(), True),
        StructField("operational_categorization_tier_3", StringType(), True),
        StructField("ticket_summary", StringType(), True),
        StructField("portal_submit_date", StringType(), True),
        StructField("ci_id_fast", StringType(), True),
        StructField("admin_number", StringType(), True),
        StructField("instanceid", StringType(), True)
    ])
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
                       'ci_id_fast', 'admin_number', 'instanceid']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
    return file