import os
import sys
from pathlib import Path

os.environ['DB_PATH'] = '/data/form5500.db'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app
import scheduler

app.DB = Path('/data/form5500.db')
scheduler.DB        = Path('/data/form5500.db')
scheduler.DOWNLOADS = Path('/data/downloads')
scheduler.CACHE     = Path('/data/cache_meta.json')

Path('/data/downloads').mkdir(parents=True, exist_ok=True)

scheduler.start_scheduler()

app.app.run(debug=False, port=int(os.environ.get("PORT", 10000)), host="0.0.0.0")
