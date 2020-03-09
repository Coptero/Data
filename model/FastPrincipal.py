from pyspark import *


class FastPrincipal:
    def principalColumns(file, spark):
        AdminNumberTags = ['candno', 'vpnsite', 'vpnsite_admin_number', 'vpnsite_service_id', 'vpnsite_service_type',
                        'vpnsite_multivrf', 'vpnsite_managing_vpn', 'vpnsite_site_topology', 'phlnk',
                        'phlnk_routing_role', 'cpert', 'tbs_pe_access_mode', 'l3_acc_cfs_type', 'service_circuit',
                        'service_circuit_vpn',
                        'service_circuit_vpn_name', 'servsupp_if', 'servsupp_id', 'servsupp_name', 'servsupp_suppname',
                        'servsupp_availability', 'servsupp_mtr', 'servsupp_standard_availability',
                        'servsupp_standard_mtr', 'servsupp_name_txt', 'servsupp_name_acronym']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)

        return file