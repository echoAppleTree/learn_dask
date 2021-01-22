"""
Author  : David-Alexandre Guenette <da.guenette@icloud.com>
Date    : 2021-01-20
Purpose : Dask library for Data Cleaning. 
"""

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from matplotlib import pyplot as pyplot


class Cleaner():
    
    def __init__(self, dataframe):
        self.dataframe = dataframe
        
    
    # --- Columns' Methods --- #
    
    def select_columns(self, columns):
        """ Select specific column(s). """

        filtered_df = self.dataframe[columns]
        return filtered_df
    
    def drop_columns(self, columns):
        """ Drop specific column(s). """

        dropped_df = self.dataframe.drop(columns, axis=1)
        return dropped_df
    
    def rename_columns(self, columns):
        """ Rename specific column(s) """

        renamed_df = self.dataframe.rename(columns=columns)
        return renamed_df
    
    # --- Rows' Methods --- #
    
    def select_rows(self, r0, r1):
        """ Select a specific amount of rows. """

        filtered_df = self.dataframe.loc[r0:r1]
        return filtered_df
    
    
    # --- Missing Values' Methods --- #
    
    # Cleaning - Stage 0
    def calc_pct_missing_val(self):
        """ Calculate the percentage of missing values for each columns. """
        
        missing_values = self.dataframe.isnull().sum()
        percent_missing = ((missing_values / self.dataframe.index.size) * 100)
        return percent_missing
    
    # Cleaning - Stage 1
    def drop_columns_treshold(self, treshold, ls):
        """ Drop columns that have a certain treshold of missing values. """

        columns_to_drop = list(ls[ls >= treshold].index)
        df_clean_stage01 = self.dataframe.drop(columns_to_drop, axis=1)
        return df_clean_stage01

    # Cleaning - Stage 2    
    def drop_rows_treshold(self, treshold, ls):
        """ Drop rows that have a certain treshold of missing values"""

        rows_to_drop = list(ls[(ls > 0) & (ls < treshold)].index)
        df_clean_stage02 = self.dataframe.dropna(subset=rows_to_drop)
        return df_clean_stage02
    
    # Cleaning - Stage 3
    def imputing_columns_missing_val(self, treshold ,ls, col_name):
        """ Impute default values in remaining low missing values columns"""

        remaining_columns_to_clean = list(ls[(ls > 0) & (ls < treshold)].index)
        unknown_default_dict = dict(map(lambda columnName: (columnName, col_name), remaining_columns_to_clean))
        df_clean_stage03 = self.dataframe.fillna(unknown_default_dict)
        return df_clean_stage03
