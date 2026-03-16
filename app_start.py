import os
os.environ.setdefault("DB_PATH", str(__import__('pathlib').Path.home() / "Form5500/output/form5500.db"))
import app
app.app.run(debug=False, port=int(os.environ.get("PORT", 8080)), host="0.0.0.0")
