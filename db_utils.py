from typing import List
from data_model import ST_Schema
from psycopg2 import sql
import pandas as pd

"""
api to join two aggregated datasets
"""
def join_two_agg_tables(
    cur,
    tbl1: str,
    st_schema1: ST_Schema,
    var1: List[str],
    tbl2: str,
    st_schema2: ST_Schema,
    var2: List[str],
    outer=False,
):
    agg_tbl1 = st_schema1.get_agg_tbl_name(tbl1)
    agg_tbl2 = st_schema2.get_agg_tbl_name(tbl2)

    agg_join_sql = """
        SELECT a1.val, {agg_vars} FROM
        {agg_tbl1} a1 JOIN {agg_tbl2} a2
        ON a1.val = a2.val
        """
    if outer:
        agg_join_sql = """
        SELECT a1.val as key1, a2.val as key2, {agg_vars} FROM
        {agg_tbl1} a1 FULL JOIN {agg_tbl2} a2
        ON a1.val = a2.val
        """
    query = sql.SQL(agg_join_sql).format(
        agg_vars=sql.SQL(",").join(
            [
                sql.SQL("{} AS {}").format(
                    sql.Identifier("a1", var1),
                    sql.Identifier(var1),
                )
            ]
            + [
                sql.SQL("{} AS {}").format(
                    sql.Identifier("a2", var2),
                    sql.Identifier(var2),
                )
            ]
        ),
        agg_tbl1=sql.Identifier(agg_tbl1),
        agg_tbl2=sql.Identifier(agg_tbl2),
    )

    cur.execute(query)

    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df