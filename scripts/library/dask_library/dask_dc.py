"""
Author  : David-Alexandre Guenette <da.guenette@icloud.com>
Date    : 2021-01-20
Purpose : Dask library for Data Cleaning. 
"""

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from matplotlib import pyplot as pyplot



class Cleaning():

    def __init__(self, dataframe):
        self.dataframe = dataframe



    def calc_miss_val_percent(self):
        """
        - Counting missing values in the DataFrame
        - Calculating the percent of missing values in the DataFrame
        - Computing the DAG
        """

        missing_values = self.dataframe.isnull().sum()
        missing_count = ((missing_values / dataframe.index.size) * 100)

        with ProgressBar():
            missing_count_pct = missing_count.compute()

        return missing_count_pct




    def filter_sparse_columns(self, missing_count_pct, treshold):
        """Filtering sparse columns"""

        columns_to_drop = missing_count_pct[missing_count_pct > treshold].index
        with ProgressBar():
            df_dropped = self.dataframe.drop(columns_to_drop, axis=1).persist()

        return df_dropped    
