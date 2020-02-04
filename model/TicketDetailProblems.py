from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getPBISchema(file, spark):
    validationSchema = file.schema
    corrupt_file_schema = [StructField("_corrupt_record", StringType(), True)]
    StructType(corrupt_file_schema + validationSchema.fields)
    return validationSchema


def detailPBMColumns(file, spark):
    AdminNumberTags = ['ticket_id', 'portal_ticket_id', 'sigma_ticket_id', 'bmc_ticket_submitter',
                       'bmc_ticket_submit_date',
                       'bmc_ticket_last_modified_by', 'bmc_ticket_last_modification_date', 'status_id', 'substatus_id',
                       'bmc_ticket_assigned_support_group_id', 'impact_id', 'urgency_id', 'priority_id',
                       'portal_ticket_summary',
                       'portal_ticket_notes', 'bmc_ticket_contact_first_name', 'bmc_ticket_contact_email',
                       'portal_end_user_site_location', 'bmc_ticket_required_resolution_date', 'bmc_ticket_father_id',
                       'bmc_ticket_last_resolved_date', 'bmc_ticket_closed_date', 'bmc_ticket_responded_date',
                       'bmc_model_version_id',
                       'bmc_product_id', 'bmc_manufacturer_id', 'portal_ticket_planned_start_date', 'admin_number',
                       'sigma_additional_information', 'sigma_ticket_description', 'product_categorization_tier_1',
                       'product_categorization_tier_2', 'product_categorization_tier_3',
                       'operational_categorization_tier_1',
                       'operational_categorization_tier_2', 'operational_categorization_tier_3',
                       'resolution_category_tier_1',
                       'resolution_category_tier_2', 'resolution_category_tier_3', 'bmc_ticket_summary',
                       'bmc_ticket_owner_support_group_id',
                       'csp_lite_request_description', 'csp_lite_element_summary', 'csp_lite_additional_comments',
                       'csp_express_ci_id',
                       'ci_name', 'admin_number_1', 'ci_country', 'city', 'customer', 'ci_id_fast', 'admin_number_2',
                       'instanceid', 'coordinator_group', 'problem_coordinator', 'assigned_group', 'assignee']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
