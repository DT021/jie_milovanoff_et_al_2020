# -*- coding: utf-8 -*-
"""
Functions to calculate material flow matrices of bauxite, alumina and aluminum

@author: Alexandre
"""
import requests
import logging
import pandas as pd
from scipy import sparse
import numpy as np
from scipy.sparse import linalg

def get_commodity_data(reporting_area="all",
                       partner_area="all",
                       trade_type=1,
                       commodity_code="TOTAL", 
                       year="recent", 
                       classification="HS"):
    """
    get_commodity_data returns the comtrade data in pandas.DataFrame format for the given parameters
    Args:
        reporting_area (int): code number as defined by comtrade. all
        partner_area (int): code number as defined by comtrade. all
        trade_type: 1 is for imports. 2 is for exports. all
        commodity_code (str): commodity code as defined by comtrade
        year (int): 4 digit e.g. 2003
        classification: 
    ps, r and p are limited to 5 codes each. Only one of the above codes may use the special ALL value in a given API call.
    Returns:
    """
    #Website: https://comtrade.un.org/data/doc/api/
    # global get_request
    un_comtrade_token="TO_OBTAIN" #Token to use for downloading data
    parameters = {"max": 250000,
                  "type": "C",
                  "freq": "A",
                  "px": classification,
                  "ps": year,
                  "r": reporting_area,
                  "p": partner_area,
                  "rg": trade_type,
                  "cc": commodity_code,
                  "fmt": "json",
                  "head":"M",
                  "token": un_comtrade_token
                  }
    #
    TIMEOUT =60
    MAX_TRY = 3
    trial = 0
    failed = True

    while failed:
        try:
            get_request = requests.get("http://comtrade.un.org/api/get?", params=parameters, timeout=TIMEOUT)
            failed = False
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as err:
            logging.warning("request timed out at country {country} at trial: {trial} for error: {err}".format(
                country=reporting_area,
                trial=trial,
                err=err))
            trial += 1
            if trial > MAX_TRY:
                logging.warning("! Giving up on request, country: {country}, year: {year} !".format(
                    country=reporting_area,
                    year=year)
                )
                return []
            else:
                continue

    if get_request.status_code == 200:
        try:
            # payload = json.loads(get_request.content.decode("utf-8"))["dataset"]
            payload = get_request.json()["dataset"]
            if payload == []:
                logging.warning("Empty dataset. Message is: {message}".format(message=get_request.json()["validation"]['message']))
                return []
            else:
                return pd.DataFrame(payload)
        except ValueError:
            logging.warning("country_code: {country}, skipped because of ValueError in json".format(
                            country=reporting_area))
            return "JSON Error"
    else:
        return get_request.status_code


def get_mat_trad_raw(commodity_dict={"name":["Aluminium; unwrought"],"level":4,'classification':'H0'},year=2000,trade_type=1):
    '''
    Construct the trade matrix from UN Comtrade without reconciliation
    Attributes:
        commodity_dict is the dictionnary of the commodity to consider from UN Comtrade database
            {"name":[NAMES_OF_COMODITY],
            "level":LEVEL_OF_COMMODITY,
            'classification':'H0'}
        year
        trade_type: Types of data to consider.
    Output:
        mat_trad is the trade matrix. The rows are the importer. The columns are the exporter.
        aij is the quantity imported by i from j
        Unit:
    
    '''
    #Inputs
    commodity_class = pd.read_excel("inputs/UN Comtrade Commodity Classifications.xlsx",sheet_name="2017-06-13")
    uncomtrade_area_list=pd.read_excel("inputs/Comtrade Country Code and ISO list.xlsx",sheet_name="Sheet1")
    comm_number_list=commodity_class.loc[commodity_class["Description"].isin(commodity_dict["name"]) & (commodity_class["Level"]==commodity_dict["level"]) & (commodity_class["Classification"]==commodity_dict["classification"]),"Code"].tolist() 
    mat_trad = sparse.csc_matrix((len(uncomtrade_area_list),len(uncomtrade_area_list)))
    #Create the dataset from the the databased associated with the specific year and the considered commodities
    for comm_number in comm_number_list:
        #Get data
        dataset = get_commodity_data(reporting_area="all",partner_area="all",trade_type=trade_type,commodity_code=comm_number,year=year,classification=commodity_dict["classification"])
        #World partner is a summary of a country with world. Delete rows
        dataset = dataset.loc[dataset["ptCode"]!=0].reset_index(drop=True)
        #Some missing quantity data. qtCode == 8 represents values in kilograms
        dataset = dataset.loc[dataset["qtCode"]==8].reset_index(drop=True)
        #Some errors in data (nan). 
        dataset = dataset.loc[dataset["NetWeight"].notnull()].reset_index(drop=True)
        #Delete re-import (when imports are from the same region)
        dataset = dataset.loc[dataset["rtCode"]!=dataset["ptCode"]].reset_index(drop=True)
        #Update dataset with importer and exporter indexes. The rows are the importer. The columns are the exporter.
        #Attention: If trade_type is 1, then imports. Reporter is the importer. Otherwise, reporter is the exporter.
        if trade_type==1:
            dataset["index_importer"] = [uncomtrade_area_list[uncomtrade_area_list["ctyCode"]==dataset.loc[i,"rtCode"]].index[0] for i in dataset.index]
            dataset["index_exporter"] = [uncomtrade_area_list[uncomtrade_area_list["ctyCode"]==dataset.loc[i,"ptCode"]].index[0] for i in dataset.index]
        elif trade_type==2:
            dataset["index_exporter"] = [uncomtrade_area_list[uncomtrade_area_list["ctyCode"]==dataset.loc[i,"rtCode"]].index[0] for i in dataset.index]
            dataset["index_importer"] = [uncomtrade_area_list[uncomtrade_area_list["ctyCode"]==dataset.loc[i,"ptCode"]].index[0] for i in dataset.index]
        #Create sparse matrix from previous dataset
        temp_mat_trad = sparse.csc_matrix((dataset["NetWeight"],(dataset["index_importer"],dataset["index_exporter"])),shape=(len(uncomtrade_area_list),len(uncomtrade_area_list)))
    #Create total matrix of bilateral trade data from previous matrix per commodity.
    mat_trad = mat_trad+temp_mat_trad
    return mat_trad

def get_mat_trad_reconciliation(commodity_dict={"name":["Aluminium; unwrought"],"level":4,'classification':'H0'},year=2000):
    '''
    Construct the trade matrix from UN Comtrade with reconciliation method from Gehlhar
    Attributes:
        commodity_dict is the dictionnary of the commodity to consider from UN Comtrade database
            {"name":[NAMES_OF_COMODITY],
            "level":LEVEL_OF_COMMODITY,
            'classification':'H0'}
        year
    Output:
        mat_trad is the trade matrix. The rows are the importer. The columns are the exporter.
        aij is the quantity imported by i from j
        Unit: kg
    
    '''
    #Inputs
    commodity_class = pd.read_excel("inputs/UN Comtrade Commodity Classifications.xlsx",sheet_name="2017-06-13")
    uncomtrade_area_list = pd.read_excel("inputs/Comtrade Country Code and ISO list.xlsx",sheet_name="Sheet1")
    comm_number_list = commodity_class.loc[commodity_class["Description"].isin(commodity_dict["name"]) & (commodity_class["Level"]==commodity_dict["level"]) & (commodity_class["Classification"]==commodity_dict["classification"]),"Code"].tolist() 
    mat_trad = sparse.csc_matrix((len(uncomtrade_area_list),len(uncomtrade_area_list)))
    #Create the dataset from the the databased associated with the specific year and the considered commodities
    for comm_number in comm_number_list:
        #1) Extract data from UN COMTRADE. Process them.
        #Get data (for imports and exports)
        dataset = get_commodity_data(reporting_area="all",partner_area="all",trade_type="all",commodity_code=comm_number,year=year,classification=commodity_dict["classification"])
        #Eliminate re-imports or re-exports. Only consider imports and exports. Eliminate imports and exports from same partner
        dataset = dataset.loc[dataset["rtCode"]!=dataset["ptCode"]].reset_index(drop=True)
        #Only consider trade reported as imports or exports
        dataset = dataset.loc[dataset["rgCode"].isin([1,2])].reset_index(drop=True)
        #World partner is a summary of a country with world. Delete rows
        dataset = dataset.loc[dataset["ptCode"]!=0].reset_index(drop=True)
        #Some errors in data (nan). 
        dataset = dataset.loc[dataset["NetWeight"].notnull()].reset_index(drop=True)
        
        #2) Create a dataset of transactions. Each row is a transaction, with one variable for importer value, and one for exporter value
        #First for imports
        imports_dt = dataset.loc[dataset["rgCode"].isin([1])].reset_index(drop=True)[['rtCode','ptCode','NetWeight']]
        imports_dt = imports_dt.rename({"rtCode":"Importer","ptCode":"Exporter","NetWeight":"Reported_imports"},axis="columns") 
        #Second for exports
        exports_dt = dataset.loc[dataset["rgCode"].isin([2])].reset_index(drop=True)[['rtCode','ptCode','NetWeight']]
        exports_dt = exports_dt.rename({"rtCode":"Exporter","ptCode":"Importer","NetWeight":"Reported_exports"},axis="columns") 
        #Merge the two dataset
        trade_dt = imports_dt
        trade_dt["Reported_exports"] = 0
        for i in exports_dt.index:
            #If existing data, update dataframe
            if any(x==True for x in (trade_dt["Importer"]==exports_dt.loc[i,"Importer"]) & (trade_dt["Exporter"]==exports_dt.loc[i,"Exporter"])):
                trade_dt.loc[(trade_dt["Importer"]==exports_dt.loc[i,"Importer"]) & (trade_dt["Exporter"]==exports_dt.loc[i,"Exporter"]),"Reported_exports"] = exports_dt.loc[i,"Reported_exports"]
            #If not existing, create new row
            else:
                trade_dt = trade_dt.append(exports_dt.loc[i,],ignore_index=True)
        #Convert NaN values in 0. We asssume no imports
        trade_dt.loc[trade_dt["Reported_imports"].isnull(),"Reported_imports"] = 0
        
        #3) Calculate Accuracy level of the transactions
        trade_dt["Accuracy_level"] = 1
        trade_dt.loc[trade_dt["Reported_imports"]!=0,"Accuracy_level"] = abs(trade_dt.loc[trade_dt["Reported_imports"]!=0,"Reported_imports"].values-trade_dt.loc[trade_dt["Reported_imports"]!=0,"Reported_exports"].values)/trade_dt.loc[trade_dt["Reported_imports"]!=0,"Reported_imports"].values
        accuracy_threshold = 0.2
        
        #4)Importer and exporter commodity specific reliability index
        #Importer index
        rim_dt = pd.DataFrame(trade_dt.groupby(["Importer"])["Reported_imports"].agg(sum)).rename({"Reported_imports":"Total_imports"},axis="columns")
        accurate_imports_dt = pd.DataFrame(trade_dt.loc[trade_dt["Accuracy_level"]<=accuracy_threshold].groupby(["Importer"])["Reported_imports"].agg(sum)).rename({"Reported_imports":"Total_accurate_imports"},axis="columns")
        #Combine data
        rim_dt = rim_dt.join(accurate_imports_dt)
        #Convert NaN values in 0
        rim_dt.loc[rim_dt["Total_accurate_imports"].isnull(),"Total_accurate_imports"] = 0
        #Calculate reliability index
        rim_dt["Reliability_index"] = rim_dt["Total_accurate_imports"].values/rim_dt["Total_imports"].values*100
        #Convert NaN values in 0. NaN means no total imports and no accurate. So we cannot judge the reliability.
        rim_dt.loc[rim_dt["Reliability_index"].isnull(),"Reliability_index"] = 0
        #Exporter index
        rix_dt = pd.DataFrame(trade_dt.groupby(["Exporter"])["Reported_exports"].agg(sum)).rename({"Reported_exports":"Total_exports"},axis="columns")
        accurate_exports_dt = pd.DataFrame(trade_dt.loc[trade_dt["Accuracy_level"]<=accuracy_threshold].groupby(["Exporter"])["Reported_exports"].agg(sum)).rename({"Reported_exports":"Total_accurate_exports"},axis="columns")
        #Combine data
        rix_dt = rix_dt.join(accurate_exports_dt)
        #Convert NaN values in 0
        rix_dt.loc[rix_dt["Total_accurate_exports"].isnull(),"Total_accurate_exports"] = 0
        #Calculate reliability index
        rix_dt["Reliability_index"] = rix_dt["Total_accurate_exports"].values/rix_dt["Total_exports"].values*100
        #Convert NaN values in 0. NaN means no total exports and no accurate. So we cannot judge the reliability.
        rix_dt.loc[rix_dt["Reliability_index"].isnull(),"Reliability_index"] = 0
        
        #5) Select the best trade data
        #Fill the reliability indexes
        trade_dt["RI_importer"] = [rim_dt.loc[rim_dt.index==trade_dt.loc[i,"Importer"],"Reliability_index"].values[0] for i in trade_dt.index]
        trade_dt["RI_exporter"] = [rix_dt.loc[rix_dt.index==trade_dt.loc[i,"Exporter"],"Reliability_index"].values[0] for i in trade_dt.index]
        #Choose the best value
        trade_dt["Value"] = 0
        for i in trade_dt.index:
            if trade_dt.loc[i,"RI_importer"]>=trade_dt.loc[i,"RI_exporter"]:
                trade_dt.loc[i,"Value"] = trade_dt.loc[i,"Reported_imports"]
            else:
                trade_dt.loc[i,"Value"] = trade_dt.loc[i,"Reported_exports"]
        
        #6) Create matrix
        #Update dataset with importer and exporter indexes. The rows are the importer. The columns are the exporter.
        trade_dt["index_importer"] = [uncomtrade_area_list[uncomtrade_area_list["ctyCode"]==trade_dt.loc[i,"Importer"]].index[0] for i in trade_dt.index]
        trade_dt["index_exporter"] = [uncomtrade_area_list[uncomtrade_area_list["ctyCode"]==trade_dt.loc[i,"Exporter"]].index[0] for i in trade_dt.index]
        #Create sparse matrix from previous dataset
        temp_mat_trad = sparse.csc_matrix((trade_dt["Value"],(trade_dt["index_importer"],trade_dt["index_exporter"])),shape=(len(uncomtrade_area_list),len(uncomtrade_area_list)))
    
    #Create total matrix of bilateral trade data from previous matrix per commodity.
    mat_trad = mat_trad+temp_mat_trad
    return mat_trad

def get_mat_prod(mineral="aluminium",year=2000):
    '''
    Construct the matrix of production
    Attributes:
        mineral is the mineral to consider
            aluminium | alumina | bauxite
        year
    Output:
        Unit:
    '''
    uncomtrade_area_list=pd.read_excel("inputs/Comtrade Country Code and ISO list.xlsx",sheet_name="Sheet1")
    file_name='inputs/'+mineral+'_production_data.csv'
    prod_ds = pd.read_csv(file_name).fillna(0)
    #Create production matrix
    array_prod=prod_ds.loc[prod_ds['Year']==year,['Country','Value']]
    array_prod.columns=["Country","Weight_ton"]
    array_prod["Index_producer"]=0
    for i in array_prod.index:
        array_prod.loc[i,"Index_producer"]=uncomtrade_area_list[uncomtrade_area_list["cty Name English"]==array_prod.loc[i,"Country"]].index[0]
    mat_prod_th_ton = sparse.csc_matrix((array_prod["Weight_ton"],(array_prod["Index_producer"],array_prod["Index_producer"])),(len(uncomtrade_area_list),len(uncomtrade_area_list)))
    mat_prod = mat_prod_th_ton.multiply(pow(10,6))
    return mat_prod

def calculate_mat_cons_kastner(mineral="aluminium",year=2000,trade_data_type="reconciliated"):
    '''
    Return matrix of apparent national level consumption according to country of origin based on Kastner et al. model
    The calculations include the iterative imports of imports
    Attributes:
        mineral to consider in the production
            'aluminium' | 'alumina' | 'bauxite'
        year
        from_local specifies if data are taken from online api of from local data
            "y" | "n"
    Output:
        R_hat is the matrix of apparent national level consumption according to country of origin
        Rows are the consuming countries, columns are the producing countries.
        Rij is the part of the apparent consumption of country i produced from country j.
        Unit:
    '''
    uncomtrade_area_list=pd.read_excel("inputs/Comtrade Country Code and ISO list.xlsx",sheet_name="Sheet1")
    #Get bilateral matrix data
    if trade_data_type=="imports":
        mat_trad = sparse.load_npz("inputs/internal/mat_imports_"+mineral+"_"+str(year)+".npz")
    elif trade_data_type=="exports":
        mat_trad = sparse.load_npz("inputs/internal/mat_exports_"+mineral+"_"+str(year)+".npz")
    elif trade_data_type=="reconciliated":
        mat_trad = sparse.load_npz("inputs/internal/mat_recon_trade_"+mineral+"_"+str(year)+".npz")
    #Get production matrix data
    mat_prod = get_mat_prod(mineral=mineral,year=year)
    #Convert tons in kg
    #Create the vector of domestic production plus imports (DMI) (x in the math model). Checked.
    vec_dmi = mat_prod.sum(axis=1) + mat_trad.sum(axis=1)
    #Consistency check: DMI should always by higher than the exports in a given region. If not, create an inventory change vector to add to the DMI vector.
    vec_inv = mat_trad.sum(axis=0).transpose()-vec_dmi
    #Assumption: Only countries with positive differences need to be readjusted (more export than DMI). So negative values are forced to 0.
    vec_inv[vec_inv<0]=0
    print("Inventory changes account for {} % of domestic production plus imports".format(round(vec_inv.sum()/vec_dmi.sum()*100,ndigits=2)))
    #Adjusting the DMI
    vec_dmi_adj=vec_dmi+vec_inv
    """
    Mathematical issue: Some elements in the diagonal may be zero. Impossible to invert.
    Reason: Means that a country/region has no production nor import. Either no consumption, rely on inventory changes, or inconsistencies
    Solution: Force one as a value. Invert it. Then Force to 0 all 1 values.
    """
    vec_dmi_adj[vec_dmi_adj==0]=1
    #Create the matrix containing DMI in diagonal (x_hat in the math model)
    mat_dmi=sparse.csc_matrix((vec_dmi_adj.A1,(np.array(range(0,len(uncomtrade_area_list))),np.array(range(0,len(uncomtrade_area_list))))),shape=(len(uncomtrade_area_list),len(uncomtrade_area_list)))
    #Create the reciprocal of vector of DMI (x_hat-1 in the math model)
    inv_mat_dmi=linalg.inv(mat_dmi)
    #Force the diagonal with 1 at 0
    inv_mat_dmi[inv_mat_dmi==1]=0
    #Calculate the export share matrix (A in the math model)  
    mat_ex_share = mat_trad*inv_mat_dmi
    #Check the consistency (the sums of the columns should be lower than 1). Otherwise means that the sum of the shares of exports is higher than the country's DMI
    #Diagonal matrix with one in diagonals
    mat_ones=sparse.eye(len(uncomtrade_area_list),len(uncomtrade_area_list))
    #Create the matrix of DMI according to country of origin  
    mat_dmi_ori=linalg.spsolve((mat_ones - mat_ex_share).tocsc(),mat_ones.tocsc())*mat_prod
    #Create the vector of apparent consumption
    #Represent the share of DMI used for national consumption
    vec_cons=(vec_dmi_adj-mat_trad.sum(axis=0).transpose())/vec_dmi_adj
    #Create the matrix of apparent consumption
    mat_cons=sparse.csc_matrix((vec_cons.A1,(np.array(range(0,len(uncomtrade_area_list))),np.array(range(0,len(uncomtrade_area_list))))),shape=(len(uncomtrade_area_list),len(uncomtrade_area_list)))
    #Create the matrix of apparent national level consumptionm according to country of origin (R_hat in the math model). 
    R_hat=mat_cons * mat_dmi_ori
    return R_hat

def calculate_mat_cons(mineral="aluminium",year=2000,trade_data_type="reconciliated"):
    '''
    Return matrix of apparent national level consumption according to country of origin
    The calculations only consider direct imports and not the iterative imports of imports    
    Attributes:
        mineral to consider in the production
            'aluminium' | 'alumina' | 'bauxite'
        year
        from_local specifies if data are taken from online api of from local data
            "y" | "n"
    Output:
        mat_cons is the matrix of apparent national level consumption according to country of origin
        Rows are the consuming countries, columns are the producing countries
        mat_cons(i,j) is the part of the apparent consumption of country i produced from country j
    '''
    #Get trades matrix
    #Get bilateral matrix data
    if trade_data_type=="imports":
        mat_trad = sparse.load_npz("inputs/internal/mat_imports_"+mineral+"_"+str(year)+".npz")
    elif trade_data_type=="exports":
        mat_trad = sparse.load_npz("inputs/internal/mat_exports_"+mineral+"_"+str(year)+".npz")
    elif trade_data_type=="reconciliated":
        mat_trad = sparse.load_npz("inputs/internal/mat_recon_trade_"+mineral+"_"+str(year)+".npz")
    #Get production matrix
    mat_prod = get_mat_prod(mineral=mineral,year=year)
    #Calculate consumption matrix.
    mat_cons = mat_prod+mat_trad
    #Adjust the consumption matrix to eliminate the commodities coming from countries without production
    mat_conv_prod = mat_prod
    mat_conv_prod[mat_conv_prod!=0] = 1
    R_hat = mat_cons*mat_conv_prod
    return R_hat

def calculate_emb_cons(production_of,consumption_of,year,trade_data_type="reconciliated"):
    '''
    Return matrix of embodied consumption   
    Attributes:
        production_of: The embodied mineral.
        consumption_of: The final mineral consumed.
        mineral to consider
            'aluminium' | 'alumina' | 'bauxite'
        year
        from_local specifies if data are taken from online api of from local data
            "y" | "n"
    Output:
        emb_cons is the matrix of embodied consumption for the consumption of "" of the production of ""
        Rows are the consuming countries, columns are the embodied producing countries
        mat_cons(i,j) is the part of the consumption of country i of mineral "consumption_of" produced from country j of mineral "production_of"
    '''
    mineral_list = {'alumina':'aluminium','bauxite':'alumina'}
    input_list = {'aluminium':1,'alumina':1.93538,'bauxite':2.8764}
    #Get apparent consumption matrix
    mat_cons = calculate_mat_cons_kastner(mineral=production_of,year=year,trade_data_type=trade_data_type)
    if production_of==consumption_of:
        emb_cons = mat_cons
    else:
        temp_mineral = mineral_list[production_of]
        #Get embodied consumption matrix for temp_mineral
        temp_emb_cons = calculate_emb_cons(production_of=temp_mineral,consumption_of=consumption_of,year=year,trade_data_type=trade_data_type)
        #Get relative apparent consumption matrix for production_of
        temp_mat_cons_rel = get_relative_matrix(mat=mat_cons,axis=1)
        #Input factor
        conversion_factor = input_list[production_of]
        emb_cons = temp_emb_cons*conversion_factor*temp_mat_cons_rel
    return emb_cons

def build_reg_matrix(country_mat,region_to_consider_for_col=None,region_to_consider_for_row=None):
    '''
    Build a matrix aggregated by region from matrix of country
    Attributes:
        country_mat is matrix with countries as rows and/or columns
        region_to_consider_for_row is the list of region to use for aggregation per rows (from number of countries to number of regions in rows)
            aluminium_region_ei34 | aluminium_region_ei35 | alumina_region
        region_to_consider_for_col is the list of region to use for aggregation per columns (from number of countries to number of regions in columns)
            aluminium_region_ei34 | aluminium_region_ei35 | alumina_region
    Outputs:
        reg_mat is a matrix in CSC format which aggregates country_mat as specified
        list_rows contains the list of row names (either country or regions)
        list_columns contains the list of column names (either countries or regions)
    '''
    #Inputs
    country_correspondence=pd.read_csv('inputs/country_correspondence.csv')
    #Aggregation over rows
    if region_to_consider_for_col==None and region_to_consider_for_row!=None:
        reg_list=sorted(country_correspondence.loc[:,region_to_consider_for_row].unique().tolist())
        shape_mat=(len(reg_list),country_mat.shape[1])
        #Create output
        reg_mat=sparse.lil_matrix(shape_mat,dtype=np.float64)
        for i in range(0,len(reg_list)):
            reg=reg_list[i]
            reg_index=country_correspondence.index[country_correspondence.loc[:,region_to_consider_for_row]==reg]
            reg_mat[i,:]=sparse.lil_matrix(country_mat[reg_index,:].sum(axis=0))
        #Get list of columns and row names
        list_rows=reg_list
        list_columns=country_correspondence.loc[:,'cty Name English'].tolist()
    #Aggregation over columns
    elif region_to_consider_for_col!=None and region_to_consider_for_row==None:
        reg_list=sorted(country_correspondence.loc[:,region_to_consider_for_col].unique().tolist())
        shape_mat=(country_mat.shape[0],len(reg_list))
        #Create output
        reg_mat=sparse.lil_matrix(shape_mat,dtype=np.float64)
        for i in range(0,len(reg_list)):
            reg=reg_list[i]
            reg_index=country_correspondence.index[country_correspondence.loc[:,region_to_consider_for_col]==reg]
            reg_mat[:,i]=sparse.lil_matrix(country_mat[:,reg_index].sum(axis=1))
        #Get list of columns and row names
        list_columns=reg_list
        list_rows=country_correspondence.loc[:,'cty Name English'].tolist()
    #Aggregation over rows and columns
    elif region_to_consider_for_col!=None and region_to_consider_for_row!=None:
        reg_list_col=sorted(country_correspondence.loc[:,region_to_consider_for_col].unique().tolist())
        reg_list_row=sorted(country_correspondence.loc[:,region_to_consider_for_row].unique().tolist())
        #Create output
        temp_reg_mat=sparse.lil_matrix((len(reg_list_row),country_mat.shape[1]),dtype=np.float64)
        #Aggregate per row
        for i in range(0,len(reg_list_row)):
            reg=reg_list_row[i]
            reg_index=country_correspondence.index[country_correspondence.loc[:,region_to_consider_for_row]==reg]
            temp_reg_mat[i,:]=sparse.lil_matrix(country_mat[reg_index,:].sum(axis=0))
        #Aggregate per col
        reg_mat=sparse.lil_matrix((len(reg_list_row),len(reg_list_col)),dtype=np.float64)
        for i in range(0,len(reg_list_col)):
            reg=reg_list_col[i]
            reg_index=country_correspondence.index[country_correspondence.loc[:,region_to_consider_for_col]==reg]
            reg_mat[:,i]=sparse.lil_matrix(temp_reg_mat[:,reg_index].sum(axis=1))
        #Get lists of columns and row names
        list_columns=reg_list_col
        list_rows=reg_list_row
    return reg_mat.tocsc(),list_rows,list_columns

def get_relative_matrix(mat,axis=1):
    '''
    Build matrix that contains the relative values according to the sum of the row or column
    Attributes:
        mat is the matrix to transform
        axis=0 means that for a given column, the values are normalized by the sum over rows (new_a(i,j) is old_a(i,j)/sum_i(old_a(i,j)))
        axis=1 means that for a given row, the values are normalized by the sum over columns (new_a(i,j) is old_a(i,j)/sum_j(old_a(i,j)))
    Output:
        rel_mat is the normalized matrix
    '''
    #Get sum over the rows
    normalizing_sum=mat.sum(axis=axis)
    #If sum is 0, force to 1
    normalizing_sum[normalizing_sum==0]=1
    #Divide the matrix per column
    rel_mat=sparse.csc_matrix(mat/normalizing_sum)
    return rel_mat
