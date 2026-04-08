MES Prototype Overview

Files:
- app.py               Flask dashboard entry point
- model.py             SQLite database + helper queries
- opcua_interface.py   All PLC / OPC UA communication in one place
- MES_controller.py    Background MES loop, order execution, KPI tracking
- templates/index.html Dashboard page
- static/style.css     Basic styling