{
  "type": "expression",
  "name": "client_host",
  "expression": "regexp_extract(\"clienthostport\", '^(.+?)(?:\\\\([0-9]+\\\\))?$', 1)"
},
{
  "type": "expression",
  "name": "client_port",
  "expression": "regexp_extract(\"clienthostport\", '\\\\(([0-9]+)\\\\)$', 1)"
}
