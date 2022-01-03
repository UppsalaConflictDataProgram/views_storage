from typing import Optional
from environs import Env
import paramiko

env = Env()
env.read_env()

DB_USER: Optional[str] = env.str("VIEWS_SFTP_DB_USER", None)
DB_HOST = env.str("VIEWS_SFTP_DB_HOST", "0.0.0.0")
DB_PORT = env.int("VIEWS_SFTP_DB_PORT", "5432")
DB_NAME = env.str("VIEWS_SFTP_DB_NAME", "postgres")
DB_SSLMODE = env.str("VIEWS_SFTP_DB_SSLMODE", "require")

PARAMIKO_LOG_FILE = env.str("VIEWS_SFTP_PARAMIKO_LOG_FILE", "/tmp/paramiko.log")

paramiko.util.log_to_file(PARAMIKO_LOG_FILE)
