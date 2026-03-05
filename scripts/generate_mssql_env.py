import urllib.parse
from medallion.silver.silver import DB_CONFIG


def build_sqlalchemy_conn(cfg: dict) -> str:
    driver = cfg.get('driver', 'ODBC Driver 17 for SQL Server')
    server = cfg.get('server', 'localhost')
    port = cfg.get('port', 1433)
    database = cfg.get('database', '')
    trusted = str(cfg.get('trusted_connection', '')).lower() in ('yes', 'true', '1')
    uid = cfg.get('uid')
    pwd = cfg.get('pwd')

    # Build ODBC connection string then URL-encode it for SQLAlchemy
    if trusted:
        odbc = f"DRIVER={{{driver}}};SERVER={server},{port};DATABASE={database};Trusted_Connection=yes;"
        params = {'odbc_connect': odbc}
        conn = 'mssql+pyodbc:///?' + urllib.parse.urlencode(params)
    else:
        # safe simple form: mssql+pyodbc://user:pass@host:port/db?driver=... 
        user = urllib.parse.quote_plus(uid or '')
        password = urllib.parse.quote_plus(pwd or '')
        driver_q = urllib.parse.quote_plus(driver)
        conn = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver={driver_q}"
    return conn


def write_env_file(path='.env'):
    conn = build_sqlalchemy_conn(DB_CONFIG)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"MSSQL_ALCHEMY_CONN={conn}\n")
    print(f'Wrote {path} (MSSQL_ALCHEMY_CONN)')


if __name__ == '__main__':
    write_env_file()
