# import libraries
import io
import pandas as pd
import pandas.io.formats.excel
import numpy as np
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
from datetime import date
#import pysurveycto
import csv

today = date.today().strftime("%Y%m%d")
# Read in the excel file of the data contained in this repo
from azure.storage.blob import BlockBlobService

block_blob_service = BlockBlobService(account_name='ctogen2store', account_key='1H4OGV4IhzJ/NFGKdvvNGPIAP0oLD3KRaZ5j5YE+srIuIWpD0I4VX7sSzGPpfHDuet31ElmdV7kR+AStoea43g==')

# Download file from blob 
block_blob_service.get_blob_to_path('data', 'raw/2022/09', '05.csv')

df = pd.read_csv('./data/raw_data/raw_data.csv', na_values = "NaN")
# Identify cells with NA
df.isna().sum()
#Re-arrange so index start from 1, not 0
#df = df.loc[~df.index.isin(to_be_dropped)]
df.index = np.arange(1,len(df)+1)

# print a list of column names
col_list = list(df)

# Add survey metadata to a different df (without review_quality and review_status)
df_meta_df = df[['SubmissionDate','starttime','endtime', 
                 'deviceid','subscriberid','simid','devicephonenum', 
                 'location-Latitude','location-Longitude','location-Altitude', 
                 'location-Accuracy','duration','date','enumerator','a2f', 
                 'rtid_key','rt_replacement', 
                 'rt_position','rt_position97','consent',
                 'businessname_pl','instanceID']].copy()
# Drop unneccessary columns from main df 
df.drop(['SubmissionDate', 'starttime', 'endtime', 'deviceid', 
         'subscriberid', 'simid', 'devicephonenum', 'location-Latitude',
         'location-Longitude', 'location-Altitude', 'location-Accuracy',
         'instanceID', 'formdef_version', 'KEY'], 
        axis = 1, inplace = True)
# Convert duration variable from seconds to minutes
df['duration_min'] = df['duration']/60
df.drop(['duration'], axis = 1, inplace = True)
# ## Answer Scores & Weighted Question Scores
# ### Customer Service `cs_`
cs_cols = ['cs_shops', 'cs_employee', 'cs_emplmanager', 'cs_womenempl', 
           'cs_womenmanager', 'cs_cust', 'cs_wucust', 'cs_loyal', 'cs_credit', 
           'cs_credit_type', 'cs_credit_type_1', 'cs_credit_type_2', 'cs_credit_type_97', 
           'cs_credit97']


# #### Answer Scores (unweighted)
# number of shops: 100 if more than 1
df['cs_shops_sc'] = np.where(df['cs_shops'] > 1, 100, 0)
# loyal customers percentage who buy 50%+ of their product from the retailer's shop 
df['cs_cust_loyal_perc'] = ((df['cs_loyal']/df['cs_cust'])*100).round(1)
df['cs_cust_loyal_sc'] = df['cs_cust_loyal_perc'].round(0)
# Does the Retail Business offer credit to customers?
df['cs_credit_sc'] = np.where(df['cs_credit'] == 1, 100, 0)
# #### Weighted Question/Variables Score
# Apply the question weights to each score answer variable
df['cs_shops_scw'] = df['cs_shops_sc'] * 0.10
df['cs_cust_loyal_scw'] = df['cs_cust_loyal_sc'] * 0.60
df['cs_credit_scw'] = df['cs_credit_sc'] * 0.30

cs_scw_vars = ['cs_shops_scw', 'cs_cust_loyal_scw', 'cs_credit_scw']
# ### Performance Tracking Systems and Technology/Digital Integrations `pts_` and 'tdiapps_'
pts_cols = ['pts_records', 'pts_records1', 'pts_records2', 'pts_records3', 
            'pts_records4', 'pts_records5',  
            'pts_records_image_proof', 'pts_bk', 'pts_bk_how', 'pts_ledger', 
            'pts_ledger_cash', 'pts_ledger_sales', 'pts_ledger_expense', 'pts_ledger_asset', 
            'pts_ledger_inv', 'pts_ledger_credit', 'pts_payable', 'pts_ledger_other', 'pts_ledgerother', 
            'pts_fs', 'pts_fs_cash', 'pts_fs_pl', 'pts_fs_bs', 'pts_fs_other', 
            'pts_fsother', 'pts_fs_prep', 'pts_fs_prep97', 'pts_fsreview', 'fsreview_question', 
            'review_yes', 'review_97', 'pts_fsreview97', 'tdiapps', 'app_as', 
            'app_its', 'app_pos', 'app_ccpayment', 'app_ict_s', 'app_ict_c', 'app_97', 
            'tdiapps97', 'pts_ledger_update', 'pts_ledger_update97', 'pts_inventory','pts_inventory97']


# Which of the following <b>records</b> does the Retail Business have?
df['pts_records_sc'] = (df['pts_records1']*20 
                        + df['pts_records2']*20 
                        + df['pts_records3']*10 
                        + df['pts_records4']*25 
                        + df['pts_records5']*25) 
# Does the Retail Business do bookkeeping?
df['pts_bk_sc'] = np.where(df['pts_bk'] == 1, 100, 0)
# How does the Retail Business do bookkeeping?
df['pts_bk_how_sc'] = np.where(df['pts_bk_how'] == 1, 50, 
                               np.where(df['pts_bk_how'] == 2, 100, 0))
#fake variables (to be changed)
#df['pts_ledger_payables'] = df['pts_ledger_credit']

# What business activities does the Retail Business document?
df['pts_ledger_sc'] = (df['pts_ledger_cash']*10 
                       + df['pts_ledger_sales']*15 
                       + df['pts_ledger_expense']*15 
                       + df['pts_ledger_asset']*15 
                       + df['pts_ledger_inv']*15
                       + df['pts_payable']*15 
                       + df['pts_ledger_credit']*15  
                       + df['pts_ledger_other']*0)
# How frequently does the Retail Business update the cash record?
df['pts_ledger_update_sc'] = np.where(df['pts_ledger_update'] == 1, 100, 
                                      np.where(df['pts_ledger_update'] == 2, 25,0))
# How frequently does the Retail Business reconcile inventory?
df['pts_inventory_sc'] = np.where(df['pts_inventory'] == 1, 100,
                                   np.where(['pts_inventory'] == 2, 75,
                                            np.where(['pts_inventory'] == 3, 25, 0)))
# Which of the following financial statements does the Retail Business prepare?
df['pts_fs_sc'] = (df['pts_fs_cash']*35 
                       + df['pts_fs_pl']*35 
                       + df['pts_fs_bs']*30)
# Frequency of updates to financial statements not scored
# Are financial statements audited or reviewed by the following:?
df['pts_fsreview_sc'] = np.where((df['review_yes'] == 1) & (df['review_97'] == 1), 100,
                                   np.where((df['review_yes'] == 1) & (df['review_97'] == 0), 65,
                                     np.where((df['review_yes'] == 0) & (df['review_97'] == 1), 35, 0)))

# Does the Retail Business use any of the following technology applications?
df['pts_tdiapps_sc'] = (df['app_as']*20 
                        + df['app_its']*20 
                        + df['app_pos']*15 
                        + df['app_ccpayment']*15
                        + df['app_ict_s']*15
                        + df['app_ict_c']*15)
# #### Weighted Question/Variables Score
# Apply the question weights to each scored answer variable
df['pts_records_scw'] = df['pts_records_sc']*0.15
df['pts_bk_scw'] = df['pts_bk_sc']*0.10
df['pts_bk_how_scw'] = df['pts_bk_how_sc']*0
df['pts_ledger_scw'] = df['pts_ledger_sc']*0.20
df['pts_ledger_update_scw'] = df['pts_ledger_update_sc']*0.15
df['pts_inventory_scw'] = df['pts_inventory_sc']*0.15
df['pts_fs_scw'] = df['pts_fs_sc']*0.10
df['pts_fsreview_scw'] = df['pts_fsreview_sc']*0.10
df['pts_tdiapps_scw'] = df['pts_tdiapps_sc']*0.05

pts_scw_vars = ['pts_records_scw', 'pts_bk_scw', 'pts_bk_how_scw', 
                'pts_ledger_scw', 'pts_ledger_update_scw', 'pts_inventory_scw', 
                'pts_fs_scw', 'pts_fsreview_scw', 'pts_tdiapps_scw']
# ### Planning Pratices `pp_`
pp_cols = ['pp_goals', 'pp_ap', 'pp_written', 'pp_ap_written_image', 'pp_ap_budget']

# Does the Retailer have goals for the business?
df['pp_goals_sc'] = np.where(df['pp_goals'] == 1, 100, 0)
# Does the Retailer have a plan for achieving those goals?
df['pp_ap_sc'] = np.where(df['pp_ap'] == 1, 100, 0)
# Is the plan a written plan?
df['pp_written_sc'] = np.where(df['pp_written'] == 1, 100, 0)
# Does the plan include a budget?
df['pp_ap_budget_sc'] = np.where(df['pp_ap_budget'] == 1, 100, 0)
# #### Weighted Question/Variables Score
# Apply the question weights to each scored answer variable
df['pp_goals_scw'] = df['pp_goals_sc']*0.25
df['pp_ap_scw'] = df['pp_ap_sc']*0.25
df['pp_written_scw'] = df['pp_written_sc']*0.25
df['pp_ap_budget_scw'] = df['pp_ap_budget_sc']*0.25

pp_scw_vars = ['pp_goals_scw', 'pp_ap_scw', 'pp_written_scw', 'pp_ap_budget_scw']
# ### Risk Management & External Engagement `rmee_`
rmee_cols = ['rm_insurance', 'rm_question', 'rm_p_insurance', 'rm_v_insurance', 
           'rm_l_insurance', 'rm_h_insurance', 'rm_storage', 'rm_97_insurance', 
           'rm_writtencash', 'rm_writteninvent', 'rm_locked', 'rm_security', 
           'rm_safe', 'rm_budget', 'rm_inventory', 'rm_cash', 'rm_reserves', 
           'rm_succession', 'rm_insurance97', 'ee_reg', 'ee_training', 
           'ee_trainers', 'ee_trainers_1', 'ee_trainers_2', 'ee_trainers_3', 
           'ee_trainers_4', 'ee_trainers_5', 'ee_trainers_97', 'ee_trainers97', 
           'ee_groups', 'ee_otherbusiness']


# Does the Retail Business have insurance?
df['rmee_insurance_sc'] = np.where(df['rm_insurance'] == 1, 100, 0)
# Which of the following risk mgmt practices does the Retailer use?
df['rmee_sc'] = (df['rm_p_insurance']*0
                 + df['rm_v_insurance']*0 
                 + df['rm_l_insurance']*0
                 + df['rm_h_insurance']*0
                 + df['rm_storage']*5 
                 + df['rm_97_insurance']*0
                 + df['rm_writtencash']*5 
                 + df['rm_writteninvent']*5 
                 + df['rm_locked']*5 
                 + df['rm_security']*5 
                 + df['rm_safe']*5 
                 + df['rm_budget']*10 
                 + df['rm_inventory']*15 
                 + df['rm_cash']*15
                 + df['rm_reserves']*15 
                 + df['rm_succession']*15)
# Is the Retail Business officially registered?
df['rmee_reg_sc'] = np.where(df['ee_reg'] == 1, 100, 0)
# Has the Respondent participated in training programs related to the Retail Business 
#  in the past 3 years?
df['rmee_training_sc'] = np.where(df['ee_training'] == 1, 100, 0)
# Does the Respondent belong to any professional organizations or groups related to 
#  his/her farming activity and/or to the Retail Business?
df['rmee_groups_sc'] = np.where(df['ee_groups'] == 1, 100, 0)
# Does the Retailer own or operate another enterprise or business in addition to the Retail Business?
df['rmee_otherbusiness_sc'] = np.where(df['ee_otherbusiness'] == 1, 100, 0)
# #### Weighted Question/Variable Score
# Apply the question weights to each scored answer variable
df['rmee_insurance_scw'] = df['rmee_insurance_sc']*0.1
df['rmee_scw'] = df['rmee_sc']*0.50
df['rmee_reg_scw'] = df['rmee_reg_sc']*0.15
df['rmee_training_scw'] = df['rmee_training_sc']*0.05
df['rmee_groups_scw'] = df['rmee_groups_sc']*0.05
df['rmee_otherbusiness_scw'] = df['rmee_otherbusiness_sc']*0.15

rmee_scw_vars = ['rmee_insurance_scw', 'rmee_scw', 'rmee_reg_scw', 
                 'rmee_training_scw', 'rmee_groups_scw', 'rmee_otherbusiness_scw']
# ### Operational & Financial Performance `ofp_`
ofp_cols = ['ofp_lasset', 'ofp_lasset_bldng', 'ofp_lasset_bldng_num', 'ofp_lasset_shed', 
            'ofp_lasset_shed_num', 'lstorage_num_count', 'ofp_lasset_truck', 
            'ofp_lasset_truck_num', 'ltrucksize_s', 'cal_ltrucksize_s', 
            'ltrucksize_m', 'cal_ltrucksize_m', 'ltrucksize_l', 'cal_ltrucksize_l', 
            'ltrucksize_vl', 'cal_ltrucksize_vl', 'ltrucksize_97', 'cal_ltrucksize_97',
            'ltrucksize_idk', 'cal_ltrucksize_idk', 'ltrucksum', 'trucksize97', 
            'ofp_lasset_motorbike', 'ofp_lasset_motorbike_num', 'ofp_lasset_computer', 
            'ofp_lasset_computer_num', 'ofp_lasset97', 'ofp_lasset97_item', 
            'ofp_lasset97_num', 'ofp_asset_bldng', 'ofp_asset_bldng_num', 'ofp_asset_shed', 
            'ofp_asset_shed_num', 'ostorage_num_count', 'osize_know_1', 'Size_own_1', 
            'Measuring_unit_own_1', 'Other_unit_own_1', 'ofp_asset_truck', 
            'ofp_asset_truck_num', 'otrucksize_s', 'cal_otrucksize_s', 'otrucksize_m', 
            'cal_otrucksize_m', 'otrucksize_l', 'cal_otrucksize_l', 'otrucksize_vl', 
            'cal_otrucksize_vl', 'otrucksize_97', 'cal_otrucksize_97', 'otrucksize_idk', 
            'cal_otrucksize_idk', 'otrucksum', 'otrucksize97', 'ofp_asset_motorbike', 
            'ofp_asset_motorbike_num', 'ofp_asset_computer', 'ofp_asset_computer_num', 
            'ofp_asset97', 'ofp_asset97_item', 'ofp_asset97_num', 'shopsize_question', 
            'ofp_shopsize', 'ofp_shopsizeunit', 'ofp_shopsizeunit97', 'nearestyear_cal', 
            'middleyear_cal', 'furtherstyear_cal', 'yeardif', 'valuesale_question', 
            'vs_nearestyear', 'vs_middleyear', 'vs_furtherstyear', 'ofp_valuenearestyear', 
            'ofp_valuemiddleyear', 'ofp_valuefurthestyear', 'profit_question', 
            'p_nearestyear', 'p_middleyear', 'p_furtherstyear', 'ofp_profitnearestyear', 
            'ofp_profitmiddleyear', 'ofp_profitfurthestyear', 'monthlyexp_question', 
            'ofp_monthlyexp', 'cash_amnt_question', 'ofp_cash_amnt', 'ofp_acct', 
            'ofp_bankacct', 'ofp_borrowed']

# Does the Retailer keep business accounts separate from other accounts?
df['ofp_acct_sc'] = np.where(df['ofp_acct'] == 1, 100, 0)
# Does the Retailer earn a margin or a commission on business activities?
#df['ofp_margin_sc'] = np.where(df['ofp_margin'] == 1, 100, 0)
# Does the Retailer have a bank account?
df['ofp_bankacct_sc'] = np.where(df['ofp_bankacct'] == 1, 100, 0)
# Has the Retailer obtained a loan with the last 3 years from a financial institution?
df['ofp_borrowed_sc'] = np.where(df['ofp_borrowed'] == 1, 100, 0)
# #### Weighted Question/Variable Score
# Apply the question weights to each score answer variable
df['ofp_acct_scw'] = df['ofp_acct_sc']*0.25
#df['ofp_margin_scw'] = df['ofp_margin_sc']*0.10
df['ofp_bankacct_scw'] = df['ofp_bankacct_sc']*0.25
df['ofp_borrowed_scw'] = df['ofp_borrowed_sc']*0.50

ofp_scw_vars = ['ofp_acct_scw', 'ofp_margin_scw', 
                'ofp_bankacct_scw', 'ofp_borrowed_scw']

# ## Category Scores, Benchmarks, Total Score
# ### New (non-scored) Variables
# Sales per Customer (recent year only)
df['sales_per_cust2020'] = (df['ofp_valuenearestyear']/df['cs_cust']).round(0)
# Sales trends
# convert 0 values to NaN
cols = ['ofp_valuenearestyear', 'ofp_valuemiddleyear', 'ofp_valuefurthestyear']
df[cols] = df[cols].replace({0: np.nan})

# Calculate average sales values
df['sales_avg'] = df[cols].mean(axis=1).round(0)

# Count number of years of available sales data
df['sales_data_years'] = df[cols].count(axis=1)

# Calculate percentage change trends across all possible combinations of available data
df['sales_trend_mid_near'] = ((df['ofp_valuenearestyear'] - df['ofp_valuemiddleyear'])
                           /(df['ofp_valuemiddleyear'])).round(3)
df['sales_trend_mid_near'] = df['sales_trend_mid_near'].replace([np.inf, -np.inf], np.nan)
df['sales_trend_far_mid'] = ((df['ofp_valuemiddleyear'] - df['ofp_valuefurthestyear'])
                           /(df['ofp_valuefurthestyear'])).round(3)
df['sales_trend_far_mid'] = df['sales_trend_far_mid'].replace([np.inf, -np.inf], np.nan)
df['sales_trend_far_near'] = ((df['ofp_valuenearestyear'] - df['ofp_valuefurthestyear'])
                           /(df['ofp_valuefurthestyear'])).round(3)
df['sales_trend_far_near'] = df['sales_trend_far_near'].replace([np.inf, -np.inf], np.nan)

# Calculate the average percentage change trend
df['sales_trend_avg'] = df[['sales_trend_far_near', 'sales_trend_far_mid', 
                            'sales_trend_mid_near']].mean(axis=1).round(3)

# Add description for available trend
df['sales_trend_desc'] = np.where(df['sales_trend_avg'] > 0.0, 'Increase',                                   np.where(df['sales_trend_avg'] == 0.0, 'No Change', 
                                           np.where(df['sales_trend_avg'] < 0.0, 
                                                    'Decrease', 
                                                    'Not enough sales data available')))
# Months of cash reserves
# fill NaNs in cash amount to 0 because if retail doesn't keep cash reserve, 
#  then their amount is 0
df['ofp_cash_amnt'] = df['ofp_cash_amnt'].fillna(0)
df['monthscashreserve'] = (df['ofp_cash_amnt']/df['ofp_monthlyexp']).round(1)
# ### Weighted Category & Total ALP Scores
print(cs_scw_vars)
print(pts_scw_vars)
print(pp_scw_vars)
print(rmee_scw_vars)
print(ofp_scw_vars)
# The weighted category score (sum of weighted question scores times categ weight
df['cs_categ_scw'] = ((df['cs_shops_scw'].fillna(0) 
                       + df['cs_cust_loyal_scw'].fillna(0) 
                       + df['cs_credit_scw'].fillna(0))*0.20).round(0)

df['pts_categ_scw'] = ((df['pts_records_scw'].fillna(0) 
                        + df['pts_bk_scw'].fillna(0) 
                        + df['pts_bk_how_scw'].fillna(0) 
                        + df['pts_ledger_scw'].fillna(0) 
                        + df['pts_ledger_update_scw'].fillna(0) 
                        + df['pts_inventory_scw'].fillna(0) 
                        + df['pts_fs_scw'].fillna(0)
                        + df['pts_fsreview_scw'].fillna(0) 
                        + df['pts_tdiapps_scw'].fillna(0))*0.20).round(0)

df['pp_categ_scw'] = ((df['pp_goals_scw'].fillna(0) 
                       + df['pp_ap_scw'].fillna(0) 
                       + df['pp_written_scw'].fillna(0) 
                       + df['pp_ap_budget_scw'].fillna(0))*0.20).round(0)

df['rmee_categ_scw'] = ((df['rmee_insurance_scw'].fillna(0)
                         + df['rmee_scw'].fillna(0) 
                         + df['rmee_reg_scw'].fillna(0) 
                         + df['rmee_training_scw'].fillna(0) 
                         + df['rmee_groups_scw'].fillna(0) 
                         + df['rmee_otherbusiness_scw'].fillna(0))*0.20).round(0)

df['ofp_categ_scw'] = ((df['ofp_acct_scw'].fillna(0) 
                        #+ df['ofp_margin_scw'].fillna(0) 
                        + df['ofp_bankacct_scw'].fillna(0) 
                        + df['ofp_borrowed_scw'].fillna(0))*0.20).round(0)
# ### ALP Total Scores
df['total_sc'] = (df['cs_categ_scw'] 
                  + df['pts_categ_scw'] 
                  + df['pp_categ_scw'] 
                  + df['rmee_categ_scw'] 
                  + df['ofp_categ_scw']).round(0)
df['total_sc']
# ### Conditionality Checks - Adjustment to Total Score

# #### Conditionality: Cannot be in green category if:
# - Does not do bookkeeping.
# - Is not officially registered.

# conditionality check - no bookkeeping 
conditions_bk = [(df['total_sc'] <= 66.0),                
                 ((df['total_sc'] >  66.0) & (df['pts_bk'] == 1)),                
                 ((df['total_sc'] >  66.0) & (df['pts_bk'] == 0))]

values_conditions_bk = ['Conditionality check not required', 
                        'Passes conditionality check',                        
                        'FAILS conditionality check - cannot score above 66 because does not do bookkeeping']

df['cc_bk'] = np.select(conditions_bk, values_conditions_bk)

# conditionality check - not legally registered
conditions_reg = [(df['total_sc'] <= 66.0),                 
                  ((df['total_sc'] >  66.0) & (df['ee_reg'] == 1)),                 
                  ((df['total_sc'] >  66.0) & (df['ee_reg'] == 0))]

values_conditions_reg = ['Conditionality check not required', 
                         'Passes conditionality check',                         
                         'FAILS conditionality check - cannot score above 66 because not legally registred']

df['cc_reg'] = np.select(conditions_reg, values_conditions_reg)
# Generate final score based on adjustments from conditionality check - 
#  if failed conditionalities, drop to 66 (yellow category)
df['total_sc_final'] = np.where((df['cc_bk'] == 'FAILS conditionality check - cannot score above 66 because does not do bookkeeping')|
                                (df['cc_reg'] == 'FAILS conditionality check - cannot score above 66 because not legally registred'), 
                                66.0,                                 
                                df['total_sc'])

total_sc_categ_conditions = [(df['total_sc_final'] <= 33.0),
                             ((df['total_sc_final'] >  33.0) & 
                              (df['total_sc_final'] <= 66.0)),
                             (df['total_sc_final'] >  66.0)]

total_sc_categ_values = ['Red', 'Yellow', 'Green']

df['total_sc_categ'] = np.select(total_sc_categ_conditions, total_sc_categ_values)

df['total_sc_desc'] = np.where(df['total_sc_categ'] == 'Red', 
                               'Very immature, needs basic systems and mgmt practices',
                                 np.where(df['total_sc_categ'] == 'Yellow', 
                                          'Average application of mgmt systems and practices, can improve operational and financial performance',                                          
                                          np.where(df['total_sc_categ'] == 'Green', 
                                                   'Top performer, areas for improvement',0)))
# ## Benchmarks - Average & Top Quartiles
# calculate average score per category
df['cs_categ_avg'] = np.round(df['cs_categ_scw'].mean(),0)
df['pts_categ_avg'] = np.round(df['pts_categ_scw'].mean(),0)
df['pp_categ_avg'] = np.round(df['pp_categ_scw'].mean(),0)
df['rmee_categ_avg'] = np.round(df['rmee_categ_scw'].mean(),0)
df['ofp_categ_avg'] = np.round(df['ofp_categ_scw'].mean(),0)
df['total_sc_final_avg'] = np.round(df['total_sc_final'].mean(),0)
# average benchmark for new vars
df['cs_cust_loyal_perc_avg'] = np.round(df['cs_cust_loyal_perc'].mean(),2)
df['sales_per_cust2020_avg'] = np.round(df['sales_per_cust2020'].mean(),0)
df['monthscashreserve_avg'] = np.round(df['monthscashreserve'].mean(),1)
# calculate top quartile per category
df['cs_categ_topq'] = df['cs_categ_scw'].quantile(0.75).round(0)
df['pts_categ_topq'] = df['pts_categ_scw'].quantile(0.75).round(0)
df['pp_categ_topq'] = df['pp_categ_scw'].quantile(0.75).round(0)
df['rmee_categ_topq'] = df['rmee_categ_scw'].quantile(0.75).round(0)
df['ofp_categ_topq'] = df['ofp_categ_scw'].quantile(0.75).round(0)
df['total_sc_final_topq'] = df['total_sc_final'].quantile(0.75).round(0)
# top quantile benchmarks for new vars
df['cs_cust_loyal_perc_topq'] = df['cs_cust_loyal_perc'].quantile(0.75).round(2)
df['sales_per_cust2020_topq'] = df['sales_per_cust2020'].quantile(0.75).round(0)
df['monthscashreserve_topq'] = df['monthscashreserve'].quantile(0.75).round(1)
# ## Reporting
df['total_sc_categ'].value_counts()
final_score_cols = ['cs_categ_scw', 'pts_categ_scw', 'pp_categ_scw', 'rmee_categ_scw', 
                    'ofp_categ_scw','total_sc_final']
df.columns[:35]
# Create dataframe of scores and export
filename = "ALP_Chemtex_FinalScores.xlsx"

df_categ_scores_df = df[final_score_cols]
df_categ_scores_df.describe()
df_categ_scores_df.index = np.arange(1,len(df_categ_scores_df)+1)
df_categ_scores_df.to_excel('./data/processed_data/{}'.format(filename), header = True)
# ### Apply Label Columns
# Changes columns' dtypes from int to float
df[['date']].dtypes
columns_int = df.select_dtypes(include=[np.int64, np.int32]).columns
df[columns_int] = df[columns_int].astype(float)
df.info()
# Adding new columns with new formulation - revised with Not applicable
import math
df['location'] = df['admin1_pl'].astype(str) + ', ' + df['admin2_pl']
df['pts_inventory_yesno'] = np.where(df['pts_inventory'].isnull(), 'Not applicable', 'YES')


df['cs_cust_rank'] = df['cs_cust'].rank(method = 'first')
df['cs_cust_band'] = pd.qcut(df['cs_cust_rank'], 3, labels = False) + 1

df.fillna({'ofp_lasset_bldng_num':10000,'ofp_lasset_shed_num':10000,'ofp_lasset_truck_num':10000,'ofp_lasset_motorbike_num':10000,\
           'ofp_lasset_computer_num':10000,'ofp_lasset97_num':10000,'ofp_bankacct':10000,\
          }, inplace = True)

df.loc[(df['app_as']+df['app_its']+df['app_pos']+df['app_ccpayment']+df['app_ict_s']+\
        df['app_ict_c']+df['app_97']) >= 1, 'tdiapps_yesno'] = 1
df.loc[df['tdiapps_yesno'].isnull(), 'tdiapps_yesno'] = 0
df.loc[df['tdiapps_yesno'] == 0, 'tdiapps_yesno_label'] = 'NO'
df.loc[df['tdiapps_yesno'] == 1, 'tdiapps_yesno_label'] = 'YES'

df.loc[(df['pts_ledger_cash']+df['pts_ledger_sales']+df['pts_ledger_expense']+df['pts_ledger_asset']+df['pts_ledger_inv']+\
        df['pts_ledger_credit']+df['pts_payable']+df['pts_ledger_other']) >= 1, 'pts_ledger_yesno'] = 1
df.loc[df['pts_ledger_yesno'].isnull(), 'pts_ledger_yesno'] = 0
df.loc[df['pts_ledger_yesno'] == 0, 'pts_ledger_yesno_label'] = 'NO'
df.loc[df['pts_ledger_yesno'] == 1, 'pts_ledger_yesno_label'] = 'YES'

df['pts_ledger_update_yesno'] = np.where((df['pts_ledger_update'] >= 1) & (df['pts_ledger_update'] <= 3), 'YES', 
                                         (np.where(df['pts_ledger_update'] == 0,'NO', 'Not applicable')))
df.to_csv('./data/processed_data/data_result.csv')
# Adding new columns using "ALP_LabelsForPython"
ALP_labels = pd.read_excel("./data/infor_data/5. ALP_LabelsForPython.xlsx", 
                              sheet_name = "details", dtype = {'variable': float}, 
                              index_col = None)
ALP_labels = ALP_labels[ALP_labels['filter'] == 'label']
list_old_columns = ALP_labels['old_column'].unique()
print(len(list_old_columns))
## looping adding new columns
for i in list_old_columns:
    new_values = i
    i = ALP_labels[ALP_labels['old_column'] == i]
    df = df.merge(i[['variable', 'label']], left_on = new_values, 
                  right_on = 'variable', how = 'left')
    df[new_values] = df[new_values].fillna(10000)    
    df.drop(columns = {'variable'}, inplace = True)
    df.rename(columns = {'label': new_values + '_label'}, inplace = True)
    
    df.index = np.arange(1,len(df)+1)
    
#df.loc[df['literacy_label'].isnull(), 'literacy_label'] = '--*'
filename = "ALP_Chemtex_FullProcessedDataWithLabels.xlsx"
df.to_excel('./data/processed_data/{}'.format(filename), header = True, index = False)
