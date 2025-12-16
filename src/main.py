import os
from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "Hello World! Rodando no Cloud Run."

if __name__ == "__main__":
    # Pega a porta do ambiente ou usa 8080 como padrão
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)