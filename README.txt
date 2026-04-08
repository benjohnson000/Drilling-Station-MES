MES Prototype Overview

Files:
- app.py               Flask dashboard entry point
- model.py             SQLite database + helper queries
- opcua_interface.py   All PLC / OPC UA communication in one place
- mes_controller.py    Background MES loop, order execution, KPI tracking
- templates/index.html Dashboard page
- static/style.css     Basic styling

Run:
1. pip install flask opcua
2. python app.py

Notes:
- The OPC UA layer defaults to SIMULATE_PLC = True so the prototype works without the lab PLC.
- When you have the PLC, set SIMULATE_PLC = False in opcua_interface.py and update ENDPOINT.
