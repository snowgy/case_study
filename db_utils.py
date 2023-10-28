from typing import List
from data_model import Unit, Variable, AggFunc, ST_Schema, SchemaType
from psycopg2 import sql
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from collections import Counter

db_path = "postgresql://yuegong@localhost/chicago_1m"
db = create_engine(db_path)
# conn = db.connect()
conn_copg2 = psycopg2.connect(db_path)
cur = conn_copg2.cursor()

def join_two_agg_tables(
    cur,
    tbl1: str,
    st_schema1: ST_Schema,
    vars1: List[Variable],
    tbl2: str,
    st_schema2: ST_Schema,
    vars2: List[Variable],
    outer,
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
                    sql.Identifier("a1", var.var_name[:-3]),
                    sql.Identifier(var.var_name),
                )
                for var in vars1
            ]
            + [
                sql.SQL("{} AS {}").format(
                    sql.Identifier("a2", var.var_name[:-3]),
                    sql.Identifier(var.var_name),
                )
                for var in vars2
            ]
        ),
        agg_tbl1=sql.Identifier(agg_tbl1),
        agg_tbl2=sql.Identifier(agg_tbl2),
    )

    cur.execute(query)

    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df