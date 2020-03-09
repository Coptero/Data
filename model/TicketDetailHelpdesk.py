import sys
sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/") 
from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getIncidSchema(file):
    validationSchema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("portal_ticket_id", StringType(), True),
        StructField("sigma_ticket_id", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("submitter", StringType(), True),
        StructField("submit_date", StringType(), True),
        StructField("last_modification_date", StringType(), True),
        StructField("status_id", StringType(), True),
        StructField("substatus_id", StringType(), True),
        StructField("assigned_support_company", StringType(), True),
        StructField("assigned_support_organization", StringType(), True),
        StructField("assigned_support_group", StringType(), True),
        StructField("reported_date", StringType(), True),
        StructField("reported_source_id", StringType(), True),
        StructField("impact_id", StringType(), True),
        StructField("urgency_id", StringType(), True),
        StructField("priority_id", StringType(), True),
        StructField("operating_company_name", StringType(), True),
        StructField("operating_le", StringType(), True),
        StructField("operating_bu", StringType(), True),
        StructField("contact_company_name", StringType(), True),
        StructField("contact_le", StringType(), True),
        StructField("contact_bu", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("end_user_country", StringType(), True),
        StructField("end_user_site", StringType(), True),
        StructField("end_user_address", StringType(), True),
        StructField("end_user_city", StringType(), True),
        StructField("resolution_text", StringType(), True),
        StructField("required_resolution_date", StringType(), True),
        StructField("assigned_agent", StringType(), True),
        StructField("massive_flag", StringType(), True),
        StructField("ticket_father_id", StringType(), True),
        StructField("last_resolved_date", StringType(), True),
        StructField("closed_date", StringType(), True),
        StructField("rfs_date", StringType(), True),
        StructField("product_categorization_tier_1", StringType(), True),
        StructField("product_categorization_tier_2", StringType(), True),
        StructField("product_categorization_tier_3", StringType(), True),
        StructField("operational_categorization_tier_1", StringType(), True),
        StructField("operational_categorization_tier_2", StringType(), True),
        StructField("operational_categorization_tier_3", StringType(), True),
        StructField("closure_categorization_tier_1", StringType(), True),
        StructField("closure_categorization_tier_2", StringType(), True),
        StructField("closure_categorization_tier_3", StringType(), True),
        StructField("vendor_group", StringType(), True),
        StructField("bmc_ticket_pending_date", StringType(), True),
        StructField("inf2_hpd_outage_duration", StringType(), True),
        StructField("reason_outage", StringType(), True),
        StructField("rfo_available", StringType(), True),
        StructField("ci_name", StringType(), True),
        StructField("ci_country", StringType(), True),
        StructField("ci_city", StringType(), True),
        StructField("ci_id_fast", StringType(), True),
        StructField("admin_number", StringType(), True),
        StructField("instanceid", StringType(), True)
    ])
    #validationSchema = file.schema
    corrupt_file_schema = [StructField("_corrupt_record", StringType(), True)]
    validationSchema = StructType(validationSchema.fields + corrupt_file_schema)
    return validationSchema


def detailHPDColumns(file):
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
    
    return file
