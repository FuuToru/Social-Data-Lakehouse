from sqlalchemy.engine import create_engine

# Enable CSRF protection (recommended)
ENABLE_CSRF_PROTECTION = True

# Define your PostgreSQL database URI for Superset
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:superset@postgres-superset/superset"
)

# Optional: Superset-specific custom configurations
SECRET_KEY = "your_secret_key"

# Web server port for Superset
SUPERSET_WEBSERVER_PORT = 8088
