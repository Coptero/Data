from dataclasses import dataclass


@dataclass
class Network(object):
    pop: str
    router: str
    interface: str  # interface is a reserved keyword and cannot be used as field name FOR SPARK-EXPLODE
    sub_interface: str
    customer: str
    end_customer: str
    vendor: str
    access_type: str
