from dataclasses import dataclass
from pyspark.sql import *
from pyspark.sql.types import *


@dataclass
class TicketDetailChanges(object):
    ticket_id: str
    portal_ticket_id: str
    ticket_submitter: str
    ticket_submit_date: str
    last_modified_by: str
    last_modification_date: str
    status_id: str
    substatus_id: str
    coordinator_company: str
    coordinator_support_group: str
    coordinator_name: str
    manager_company: str
    manager_support_group: str
    ticket_reported_date: str
    reported_source_id: str
    impact_id: str
    urgency_id: str
    priority_id: str
    operating_company_name: str
    operating_organization_name: str
    ci_country: str
    company_country: str
    summary: str
    ticket_notes: str
    contact_first_name: str
    contact_last_name: str
    portal_end_user_site_location: str
    last_resolved_date: str
    closed_date: str
    vendor_group: str
    product_categorization_tier_1: str
    product_categorization_tier_2: str
    product_categorization_tier_3: str
    operational_categorization_tier_1: str
    operational_categorization_tier_2: str
    operational_categorization_tier_3: str
    ticket_summary: str
    portal_submit_date: str
    ci_id_fast: str
    admin_number: str
    instanceid: str
    class_crq: str
    risl_level: str
    change_reason: str


def getCRQSchema(self):
    validationSchema = TicketDetailChanges.StructType.printSchema()
    validationSchema = validationSchema.StructType.add("_corrupt_record", StringType(), True)
    return validationSchema


def detailCHGColumns(file):
    file.map(lambda r: TicketDetailChanges(
        r.getString(0),
        r.getString(1),
        r.getString(2),
        r.getString(3),
        r.getString(4),
        r.getString(5),
        r.getString(6),
        r.getString(7),
        r.getString(8),
        r.getString(9),
        r.getString(10),
        r.getString(11),
        r.getString(12),
        r.getString(13),
        r.getString(14),
        r.getString(15),
        r.getString(16),
        r.getString(17),
        r.getString(18),
        r.getString(19),
        r.getString(20),
        r.getString(21),
        r.getString(22),
        r.getString(23),
        r.getString(24),
        r.getString(25),
        r.getString(26),
        r.getString(27),
        r.getString(28),
        r.getString(29),
        r.getString(30),
        r.getString(31),
        r.getString(32),
        r.getString(33),
        r.getString(34),
        r.getString(35),
        r.getString(36),
        r.getString(37),
        r.getString(38),
        r.getString(39),
        r.getString(40),
        r.getString(41),
        r.getString(42),
        r.getString(43))
             )


'''
ticket_id','portal_ticket_id,sigma_ticket_id','bmc_ticket_submitter','bmc_ticket_submit_date','bmc_ticket_last_modified_by','bmc_ticket_last_modification_date','status_id','substatus_id','bmc_ticket_assigned_support_group_id','impact_id','urgency_id','priority_id','portal_ticket_summary','portal_ticket_notes','bmc_ticket_contact_first_name','bmc_ticket_contact_email','portal_end_user_site_location','bmc_ticket_required_resolution_date','bmc_ticket_father_id','bmc_ticket_last_resolved_date','bmc_ticket_closed_date','bmc_ticket_responded_date','bmc_model_version_id','bmc_product_id','bmc_manufacturer_id','portal_ticket_planned_start_date','admin_number','sigma_additional_information','sigma_ticket_description','product_categorization_tier_1','product_categorization_tier_2','product_categorization_tier_3','operational_categorization_tier_1','operational_categorization_tier_2','operational_categorization_tier_3','resolution_category_tier_1','resolution_category_tier_2','resolution_category_tier_3','bmc_ticket_summary','bmc_ticket_owner_support_group_id','csp_lite_request_description','csp_lite_element_summary','csp_lite_additional_comments','csp_express_ci_id','ci_name','admin_number_1','ci_country','city','customer','ci_id_fast','admin_number_2','instanceid','coordinator_group','problem_coordinator','assigned_group','assignee'
'''