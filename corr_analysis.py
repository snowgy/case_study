import pandas as pd
from data_model import Unit, Variable, AggFunc, ST_Schema, SchemaType
from time_point import T_GRANU
from coordinate import S_GRANU
import ast
from dataclasses import dataclass
from typing import List

"""
Get the row id and retrive the information of a correlation.
Output the aligned results of this correlation
"""
@dataclass
class Corr:
    tbl_id1: str
    align_attrs1: List[str]
    agg_attr1: str
    tbl_id2: str
    align_attrs2: List[str]
    agg_attr2: str
    type: str
    
def parse_row(row):
    return Corr(tbl_id1=row['tbl_id1'], 
                align_attrs1=ast.literal_eval(row['align_attrs1']), 
                agg_attr1 = row['agg_attr1'],
                tbl_id2 = row['tbl_id2'],
                align_attrs2 = ast.literal_eval(row['align_attrs2']),
                agg_attr2 = row['agg_attr2'],
                type = row['type'],
            )

def get_aligned_results(row, t_granu: T_GRANU, s_granu: S_GRANU):
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
    
    
corrs = pd.read_csv('./chicago_month_census_tract.csv')
row = corrs.loc[0]
parse_row(row)