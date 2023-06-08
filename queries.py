def count(table_name):
    sql = f'select count(*) as count from {table_name}'
    return sql

def join(table_a, table_b):
    sql = f'''select count(*) from {table_a} a join {table_b} b on a.l_orderkey = b.l_orderkey
        where a.l_shipdate between '1993-01-01' and '1996-12-31\''''
    return sql