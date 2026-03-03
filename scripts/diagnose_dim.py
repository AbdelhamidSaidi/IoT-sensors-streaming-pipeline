from medallion.silver.silver import get_connection


def fetch(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def main():
    conn = get_connection()
    cur = conn.cursor()
    try:
        print('gold_central total:', fetch(cur, 'SELECT COUNT(*) FROM [data_warehouse].[dbo].gold_central')[0][0])
        metrics = [r[0] for r in fetch(cur, 'SELECT DISTINCT metric FROM [data_warehouse].[dbo].gold_central WHERE metric IS NOT NULL')]
        print('distinct metrics:', metrics)
        print('distinct sensors per metric:')
        for m, c in fetch(cur, 'SELECT metric, COUNT(DISTINCT sensor_id) FROM [data_warehouse].[dbo].gold_central GROUP BY metric'):
            print(' ', m, c)

        try:
            wm = fetch(cur, 'SELECT TOP 1 last_ts FROM [data_warehouse].[dbo].dim_gold_watermark ORDER BY id DESC')[0][0]
        except Exception as e:
            wm = None
            print('watermark query error:', e)
        print('watermark:', wm)

        for m in metrics:
            dim = '[data_warehouse].[dbo].dim_' + str(m).lower()
            try:
                cnt = fetch(cur, f'SELECT COUNT(*) FROM {dim}')[0][0]
                print(dim, 'rows:', cnt)
                # append-style tables: sensor_id, ts, value, unit
                for r in fetch(cur, f'SELECT TOP 5 sensor_id, ts, value, unit FROM {dim} ORDER BY ts DESC'):
                    print('   ', r)
            except Exception as e:
                print(dim, 'ERROR:', e)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
