from airflow.models.connection import Connection

conn = Connection(
    conn_id="my_aws_connection",
    conn_type="aws",
    login="enter_login_here",  # Reference to AWS Access Key ID
    password="enter_password_here",  # Reference to AWS Secret Access Key
    extra={
        "aws_session_token": "enter_session_here", # Reference to AWS Session Token
    },
)

# Generate Environment Variable Name and Connection URI
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
print(f"{env_key}={conn_uri}")
