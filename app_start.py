import os
import sys
os.environ['DB_PATH'] = '/data/form5500.db'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import app
app.DB = __import__('pathlib').Path('/data/form5500.db')
app.app.run(debug=False, port=int(os.environ.get("PORT", 10000)), host="0.0.0.0")
