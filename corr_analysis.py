import pandas as pd
from data_model import Unit, ST_Schema
from time_point import T_GRANU
from coordinate import S_GRANU
import ast
from dataclasses import dataclass
from typing import List
from db_utils import join_two_agg_tables
import psycopg2
from sqlalchemy import create_engine
from scipy.stats import pearsonr


@dataclass
class Corr:
    tbl_id1: str
    align_attrs1: List[str]
    agg_attr1: str
    tbl_id2: str
    align_attrs2: List[str]
    agg_attr2: str
    type: str

"""
parse a row that stores the information of a correlation
"""
def parse_row(row):
    return Corr(tbl_id1=row['tbl_id1'], 
                align_attrs1=ast.literal_eval(row['align_attrs1']), 
                agg_attr1 = row['agg_attr1'],
                tbl_id2 = row['tbl_id2'],
                align_attrs2 = ast.literal_eval(row['align_attrs2']),
                agg_attr2 = row['agg_attr2'],
                type = row['align_type'],
            )

"""
Get the join result where the correlation is calculated
"""
def get_aligned_results(cur, row, t_granu: T_GRANU, s_granu: S_GRANU):
    corr = parse_row(row)
    # assemble the spatial temporal attributes with granularities used in a join
    if corr.type == 'spatial':
        st_schema1 = ST_Schema(s_unit=Unit(corr.align_attrs1[0], s_granu))
        st_schema2 = ST_Schema(s_unit=Unit(corr.align_attrs2[0], s_granu))
    elif corr.type == 'temporal':
        st_schema1 = ST_Schema(t_unit=Unit(corr.align_attrs1[0], t_granu))
        st_schema2 = ST_Schema(t_unit=Unit(corr.align_attrs2[0], t_granu))
    else:
        st_schema1 = ST_Schema(t_unit=Unit(corr.align_attrs1[0], t_granu), 
                               s_unit=Unit(corr.align_attrs1[1], s_granu))
        
        st_schema2 = ST_Schema(t_unit=Unit(corr.align_attrs2[0], t_granu),
                               s_unit=Unit(corr.align_attrs2[1], s_granu))
    
    df = join_two_agg_tables(cur, 
                        corr.tbl_id1, st_schema1, corr.agg_attr1, 
                        corr.tbl_id2, st_schema2, corr.agg_attr2)
    df[corr.agg_attr1] = df[corr.agg_attr1].astype(float)
    df[corr.agg_attr2] = df[corr.agg_attr2].astype(float)
    corr, p_value = pearsonr(df[corr.agg_attr1], df[corr.agg_attr2])
    return df, corr, p_value

if __name__ == "__main__":
    db_path = "postgresql://yuegong@localhost/chicago_1m"
    db = create_engine(db_path)
    conn_copg2 = psycopg2.connect(db_path)
    cur = conn_copg2.cursor()

    corrs = pd.read_csv('./chicago_month_census_tract.csv')
    corr_idx = 25
    row = corrs.loc[corr_idx]
    df, corr, p_val = get_aligned_results(cur, row, T_GRANU.MONTH, S_GRANU.TRACT)
    print(df.head())
    print(corr, p_val)