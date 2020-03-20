from dataclasses import dataclass


@dataclass
class TicketAgentSmc(object):
    ticket_id: str
    assigned_agent: str
    smc_cluster: str
    reported_source_id: str
