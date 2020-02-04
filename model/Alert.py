from dataclasses import dataclass

# Desconocemos si es necesaria la conversion de esta clase en Python
@dataclass
class Alert(object):
  file: str
  expected_count: int
  result_count: int
  date: str
