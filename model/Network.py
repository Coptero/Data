class Network:
    def __init__(self, pop, router, interface, sub_interface, customer, end_customer, vendor, access_type):
        self.pop = pop
        self.router =  router
        self.interface = interface  # interface is a reserved keyword and cannot be used as field name FOR SPARK-EXPLODE
        self.sub_interface = sub_interface
        self.customer = customer
        self.end_customer = end_customer
        self.vendor = vendor
        self.access_type = access_type