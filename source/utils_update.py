# -*- coding: utf-8 -*-
"""
Functions to update the brightway activities and exchanges.

@author: Alexandre
"""
import brightway2 as bw
import pandas as pd
import numpy as np
import logging
import source.utils_mfa as utils_mfa
from scipy import sparse

def dbUpdate_ElecAluLiq(bw_db_name,year,china_alu_elec='iai'):
    '''
    Function that updates the brightway database with annual electricity mixes per region in aluminum liquid processes
    Attributes:
        bw_db_name is the name of the Brightway database to update. 
            Requirement: This database needs to be edited according to the project. Electricity inputs per aluminum liquid activites need to be available
        year is the year to consider
        
    '''
    #inputs
    iai_region_data=pd.read_csv('inputs/list_region_iai.csv')
    iai_region_col_name = "aluminium_region_ei34"
    iai_reg_l = iai_region_data[iai_region_col_name].unique().tolist()
    iai_reg_l.append('GLO')
    iai_elec_input = pd.read_csv('inputs/electricity_input_iai.csv')
    iai_elec_mix = pd.read_csv('inputs/elec_mix_iai_2000-2017.csv')
    cn_elec_mix = pd.read_csv('inputs/elec_mix_china_2017.csv')
    country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
    list_ei_country = country_correspondence.ecoinvent_country.unique().tolist()
    list_ei_country.append('GLO') #We add GLO in the list
    bw_db = bw.Database(bw_db_name)
    #Other parameters. bw_db is the brightway database to update
    alu_act_list = [act for act in bw_db if 'aluminium production, primary, liquid' in act['name'] and act['location'] in list_ei_country]
    for alu_act in alu_act_list:
        if alu_act['location']=='GLO':
            reg = 'World'
        else:
            iai_reg = country_correspondence.loc[country_correspondence['ecoinvent_country']==alu_act['location'],iai_region_col_name].values[0]
            reg = iai_region_data.loc[iai_region_data[iai_region_col_name]==iai_reg,'region'].values[0]
        #Get IAI input for year and reigon
        index_number = iai_elec_input[(iai_elec_input.Region == reg) & (iai_elec_input.Category == "AC") & (iai_elec_input.Year == year)].index
        #If ND value. Consider world value
        if iai_elec_input.loc[index_number,'value'].values[0]=="ND":
            index_number = iai_elec_input[(iai_elec_input.Region == 'World') & (iai_elec_input.Category == "AC") & (iai_elec_input.Year == year)].index
        elec_input_amount = float(iai_elec_input.loc[index_number,'value'])
        #print(iai_reg+" "+reg+" "+str(elec_input_amount))
        elec_exc = [exc for exc in alu_act.technosphere() if "electricity" in exc.input['name']][0]
        #Update electricity consumption value
        elec_exc['amount'] = elec_input_amount/10**3
        #Save update
        elec_exc.save()            
    #Update electricit mixes
    elec_alu_act_list = [act for act in bw_db if 'market for electricity, high voltage, aluminium industry' in act['name'] and 'RoW' not in act['location']]
    for elec_alu_act in elec_alu_act_list:
        if isinstance(elec_alu_act['location'],str):
            elec_mix_loc_input = elec_alu_act['location']
        elif isinstance(elec_alu_act['location'],tuple):
            elec_mix_loc_input = elec_alu_act['location'][1]
        if elec_mix_loc_input=='CN' and year==2017 and china_alu_elec=='national':
            tmp_elec_mix = cn_elec_mix
        else :
            tmp_elec_mix = iai_elec_mix
        elec_exc_list = [exc for exc in elec_alu_act.technosphere() if 'electricity production' in exc.input['name']]
        elec_mix_tot = 0
        for elec_exc in elec_exc_list:
            #Get mix value of activity
            elec_mix_value = tmp_elec_mix[(tmp_elec_mix.Year == year) & (tmp_elec_mix.Region == elec_mix_loc_input) & (tmp_elec_mix.ecoinvent_activity == elec_exc.input['name'])].value.tolist()[0]
            #Change value in exchange
            elec_exc['amount'] = elec_mix_value
            #Save value in activity
            elec_exc.save()
            #Check sum of mixes equal 1
            elec_mix_tot = elec_mix_tot+elec_mix_value
        elec_alu_act.save()
        #Check that mixes sum up to 1
        if elec_mix_tot != 1:
            logging.warning("Sum of electricity mixes is {elec_mix} for region: {region}!".format(elec_mix=elec_mix_tot,region=elec_mix_loc_input))
    return

def dbUpdate_EnerAlumina(bw_db_name,year):
    '''
    Function that updates the brightway database with annual energy mixes in alumina production processes by producing country
    Attributes:
        bw_db_name is the name of the Brightway database to update
            requiremnt: Database needs to be edited according to project.
        year is the year to consider
        
    '''
    region_data = pd.read_csv('inputs/iai_alumina_region_equivalency.csv')
    reg_l = list(region_data.alumina_region.unique())
    reg_l.append('GLO')
    country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
    energy_input_dt = pd.read_csv('inputs/alumina_energy_consumption.csv')
    energy_mixes_dt = pd.read_csv('inputs/alumina_energy_mix.csv')
    energy_act_dt = pd.read_csv('inputs/energy_alumina_act.csv')
    energy_input_list = energy_act_dt.ori_exc_input.tolist()
    bw_db = bw.Database(bw_db_name)
    #Other parameters. bw_db is the brightway database to update
    alumina_act_list = [act for act in bw_db if 'alumina production' == act['name']]
    for alumina_act in alumina_act_list:
        if alumina_act['location']=='GLO':
            reg='GLO'
        else:
            reg = country_correspondence.loc[country_correspondence['ecoinvent_country']==alumina_act['location'],'alumina_region'].values[0]
        if reg==0:
            logging.warning("Country {country} does not have an alumina region!".format(country=alumina_act['location']))
            #return[]
        else:
            #ene_input_amount is the energy intensity of alumina smelting [Mj/t]
            ene_input_amount = float(energy_input_dt.loc[(energy_input_dt.Region == reg) & (energy_input_dt.Year == year),'value'])
            #Update energetic inputs of alumina production activity
            ene_mix_tot=0
            for energy_exc_name in energy_input_list:
                search_list = [exc for exc in alumina_act.technosphere() if energy_exc_name in exc.input['name']]
                if len(search_list)!=1:
                    logging.warning("Multiple or no energy inputs entitled {exc_name} in alumina production for region: {region}!".format(exc_name=energy_exc_name,region=reg))
                    #return[]
                else:
                    energy_exc = search_list[0]
                #IAI energy source
                iai_source = energy_act_dt.loc[energy_act_dt.ori_exc_input==energy_exc_name,'source_iai'].values[0]
                #Take energy mixes
                ene_mix = float(energy_mixes_dt.loc[(energy_mixes_dt.Region == reg) & (energy_mixes_dt.Year == year) & (energy_mixes_dt.source == iai_source),'value'])
                ene_mix_tot = ene_mix_tot+ene_mix
                #If exchange is megajoule, convert energy input into MJ per kg of alumina
                if energy_exc.unit=='megajoule':
                    energy_exc_amount = ene_mix*ene_input_amount/10**3
                #If exchange is kWh, convert into kWh per kg of alumina. 1 kWh per 3.6 MJ
                elif energy_exc.unit=='kilowatt hour':
                    energy_exc_amount = ene_mix*ene_input_amount/10**3/3.6
                #Update exchange amount
                energy_exc['amount'] = energy_exc_amount
                energy_exc.save()
        #Check that mixes sum up to 1
        if ene_mix_tot < 0.99:
            logging.warning("Sum of energy mixes is {ene_mix_tot} for region: {region}!".format(ene_mix_tot=ene_mix_tot,region=reg))
        alumina_act.save()
    return

def dbUpdate_cons_mix(bw_db_name,year,mineral,adjustment="kastner",trade_data_type="reconciliated"):
    '''
    Function that updates the consumption mixes of mineral in the consuming countries based on the annual trade flows
    Attributes:
        bw_db_name: is the name of the Brightway database to update
            requiremnt: Database needs to be edited according to project.
        year: is the year to consider
        mineral: is the mineral consumed which consumption mixes are updated in market for consumption of 'mineral'
        transport: do we account for transport or not in the trades.
    '''
    #inputs
    country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
    bw_db = bw.Database(bw_db_name)
    #Get the name of the producing activity of mineral for the consumption mixes by producing region
    prod_act_name_dict={'bauxite':'bauxite mine operation',
                   'alumina':'alumina production',
                   'aluminium':'aluminium production, primary, ingot'}
    prod_act_name = prod_act_name_dict[mineral]
    #mat_cons contains the matrix of consumption. Rows are the consuming countries of mineral, columns are the producing countries of mineral
    if adjustment=="kastner":
        mat_cons = utils_mfa.calculate_mat_cons_kastner(mineral=mineral,year=year,trade_data_type=trade_data_type)
    elif adjustment=="no":
        mat_cons = utils_mfa.calculate_mat_cons(mineral=mineral,year=year,trade_data_type=trade_data_type)
    list_rows = country_correspondence.loc[:,'cty Name English'].tolist()
    list_columns = country_correspondence.loc[:,'cty Name English'].tolist()
    #tot_app_cons contains the total apparent consumption of mineral by country
    tot_app_cons = mat_cons.sum(axis=1)
    act_name = 'market for consumption of '+mineral
    #Get the list of market for consumption acitivites by consuming region/country
    act_list = [act for act in bw_db if act_name in act['name'] and 'GLO' not in act['location']]
    for act in act_list:
        #Get the list of countries associated with consuming location
        cons_country_list = country_correspondence.loc[country_correspondence['ecoinvent_country'] == act['location'],'cty Name English'].values.tolist()
        #loc_mineral_cons is the total local consumption of mineral in the given area
        loc_mineral_cons = sum([tot_app_cons.item(idx) for idx in [list_columns.index(country) for country in cons_country_list]])
        #Get the list of mineral producing activities in market for consumption of mineral
        prod_exc_list = [exc for exc in act.technosphere() if prod_act_name == exc.input['name']]
        #Check if total consumption different zero. Otherwise, link with GLO activity
        if loc_mineral_cons==0:
            assert 'GLO' in [exc.input['location'] for exc in prod_exc_list], 'No GLO producing region in '+act['location']
            if act['location'] in [exc.input['location'] for exc in prod_exc_list]:
                unique_location = act['location']
            else:
                unique_location = 'GLO'
            for prod_exc in prod_exc_list:
                if prod_exc.input['location']==unique_location:
                    amount = 1
                else:
                    amount = 0
                #Change amount to and save
                prod_exc['amount'] = amount
                prod_exc.save()
        else:
            #tot_input_mix is the sum of the consumption mixes. Has to be 1 at the end.
            tot_input_mix = 0
            for prod_exc in prod_exc_list:
                if prod_exc.input['location']=='GLO':
                    mix = 0
                else:
                    #Get the list of countries associated with the ecoinvent region of bauxite activity
                    prod_country_list = country_correspondence.loc[country_correspondence['ecoinvent_country']==prod_exc.input['location'],'cty Name English'].values.tolist()
                    mix = 0
                    for cons_country in cons_country_list:
                        index_row = list_rows.index(cons_country)
                        for prod_country in prod_country_list:
                            #Get index of region
                            index_col = list_columns.index(prod_country)
                            mix = mix + mat_cons[index_row,index_col]/loc_mineral_cons 
                #Change mix pf producing region and save
                prod_exc['amount'] = mix
                prod_exc.save()
                #Sum mixes
                tot_input_mix = tot_input_mix+mix 
            #Check that mixes sum up to alumina input
            if tot_input_mix < 1:
                logging.warning("Sum of produced mineral for {mark_name} is {tot_input_mix} for location {region}!".format(mark_name=act_name,tot_input_mix=tot_input_mix,region=act['location']))
        act.save()
    return

def dbUpdate_aluminum_global_production(bw_db_name,year):
    '''
    Function that updates the aluminum consumption activity with the global production data
    Attributes:
        bw_db_name: is the name of the Brightway database to update
            requiremnt: Database needs to be edited according to project.
        year: is the year to consider
    '''
    #inputs
    country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
    bw_db = bw.Database(bw_db_name)
    #mat_prod contains the matrix of production.
    mat_prod = utils_mfa.get_mat_prod(mineral='aluminium',year=year)
    list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
    act = bw_db.get('market for consumption of aluminium')
    act['location'] = 'GLO'
    act.save()
    prod_act_name = 'aluminium production, primary, ingot'
    #Get the list of mineral producing activities in market for consumption of mineral
    prod_exc_list = [exc for exc in act.technosphere() if prod_act_name == exc.input['name']]
    #tot_prod
    tot_prod = mat_prod.sum()
    #tot_inputs is the sum of the consumption mixes. Has to be 1 at the end.
    tot_inputs = 0
    for prod_exc in prod_exc_list:
        if prod_exc.input['location']=='GLO':
            mix = 0
        else:
            #Get the list of countries associated with the ecoinvent region of producing activity
            tmp_country_list = country_correspondence.loc[country_correspondence['ecoinvent_country']==prod_exc.input['location'],'cty Name English'].values.tolist()
            mix = 0
            for prod_country in tmp_country_list:
                #Get index of region
                index_cty = list_countries.index(prod_country)
                mix = mix + mat_prod[index_cty,index_cty]
        #Change mix pf producing region and save
        prod_exc['amount'] = mix
        prod_exc.save()
        #Sum mixes
        tot_inputs = tot_inputs + mix
    act.save()
    #Check that mixes sum up to alumina input
    if tot_inputs < tot_prod:
        logging.warning("Sum of produced mineral is {tot_inputs} compared to {tot_prod}!".format(tot_inputs=tot_inputs,tot_prod=tot_prod))
    return

def dbUpdate_localize(bw_db_name):
    '''
    Function that updates the localize the consumption of bauxite, alumina and aluminium for all countries. Using the function creates a localized product system.
    Attributes:
        bw_db_name: is the name of the Brightway database to update
            requiremnt: Database needs to be edited according to project.
    '''
    #inputs
    bw_db = bw.Database(bw_db_name)
    #Get the name of the producing activity of mineral for the consumption mixes by producing region
    prod_act_name_dict = {'bauxite':'bauxite mine operation',
                          'alumina':'alumina production',
                          'aluminium':'aluminium production, primary, ingot'}
    for mineral in prod_act_name_dict.keys():
        prod_act_name = prod_act_name_dict[mineral]
        act_name = 'market for consumption of '+mineral
        #Get the list of market for consumption acitivites by consuming region/country
        act_list = [act for act in bw_db if act_name in act['name'] and 'GLO' not in act['location']]
        for act in act_list:
            #Get the list of mineral producing activities in market for consumption of mineral
            prod_exc_list = [exc for exc in act.technosphere() if prod_act_name == exc.input['name']]
            assert 'GLO' in [exc.input['location'] for exc in prod_exc_list], 'No GLO producing region in '+act['location']
            #If local production exists, update the market with local production activity. Otherwise, consider GLO activity
            if act['location'] in [exc.input['location'] for exc in prod_exc_list]:
                localized_prod = act['location']
            else:
                localized_prod = 'GLO'
            #Update all exchanges
            for prod_exc in prod_exc_list:
                if prod_exc.input['location']==localized_prod:
                    amount = 1
                else:
                    amount = 0
                #Change amount to and save
                prod_exc['amount'] = amount
                prod_exc.save()
            act.save()
    return

def dbUpdate_globalize(bw_db_name,stage_to_globalize):
    '''
    Function that updates the localize the consumption of bauxite, alumina and aluminium for all countries. Using the function creates a localized product system.
    Attributes:
        bw_db_name: is the name of the Brightway database to update
            requiremnt: Database needs to be edited according to project.
    '''
    #inputs
    bw_db = bw.Database(bw_db_name)
    #Get the name of the producing activity of mineral for the consumption mixes by producing region
    prod_act_name_dict = {'bauxite':'bauxite mine operation',
                          'alumina':'alumina production',
                          'aluminium':'aluminium production, primary, ingot'}
    for mineral in stage_to_globalize:
        prod_act_name = prod_act_name_dict[mineral]
        act_name = 'market for consumption of '+mineral
        #Get the list of market for consumption acitivites by consuming region/country
        act_list = [act for act in bw_db if act_name in act['name'] and 'GLO' not in act['location']]
        for act in act_list:
            #Get the list of mineral producing activities in market for consumption of mineral
            prod_exc_list = [exc for exc in act.technosphere() if prod_act_name == exc.input['name']]
            assert 'GLO' in [exc.input['location'] for exc in prod_exc_list], 'No GLO producing region in '+act['location']
            #Update all exchanges
            for prod_exc in prod_exc_list:
                if prod_exc.input['location']=='GLO':
                    amount = 1
                else:
                    amount = 0
                #Change amount to and save
                prod_exc['amount'] = amount
                prod_exc.save()
            act.save()
    return

def get_embodied_impacts(year,mineral,recipe_method_abb):
    '''
    Calculate the coutry-level emissions of production, consumption and embodied from pre-calculated impact factors
    Requirement: Spatial distributions of the coutry-level impact factors
    '''
    #Inputs
    country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
    list_cty_ein = country_correspondence.loc[:,'ecoinvent_country'].tolist()
    if_spatial = pd.read_csv('outputs/spatial_if_aluminum.csv')
    if_spatial = if_spatial[(if_spatial.Midpoint_abb==recipe_method_abb) & (if_spatial.Year==year) & (if_spatial.Spatial_contribution!='GLO')].reset_index(drop=True)
    if_spatial["index_producer"]=0
    if_spatial["index_imp"]=0
    #Index for producer (rows)
    prod_cty_list = if_spatial.Producing_country.unique()
    for prod_cty in prod_cty_list:
        prod_cty_index = np.where(np.array(list_cty_ein) == prod_cty)[0]
        #Attention: ME has two names depending on the timing for production. Serbia and Montenegro before a certain year, montenegro after.
        if (len(prod_cty_index)==2) & (year<2006):
            prod_cty_index = prod_cty_index[1]
        elif (len(prod_cty_index)==2) & (year>=2006):
            prod_cty_index = prod_cty_index[0]
        if_spatial.loc[if_spatial.Producing_country==prod_cty,"index_producer"] = prod_cty_index
    #Index for spatial distribution (columns)
    cty_spat_list = if_spatial.Spatial_contribution.unique()
    for cty_spat in cty_spat_list:
        cty_spat_index = np.where(np.array(list_cty_ein) == cty_spat)[0]
        #Attention: ME has two rows. Only consider Montenergo in the spatial distribution.
        if (len(cty_spat_index)==2):
            cty_spat_index = cty_spat_index[0]
        if_spatial.loc[if_spatial.Spatial_contribution==cty_spat,"index_imp"] = cty_spat_index
    #Create matrix of spatial distribution
    mat_spat_imp_pro = sparse.csc_matrix((if_spatial["Score"],(if_spatial["index_producer"],if_spatial["index_imp"])),shape=(len(list_cty_ein),len(list_cty_ein)))
    #Get apparent consumption matrix
    mat_cons = utils_mfa.calculate_mat_cons_kastner(mineral=mineral,year=year)
    #Calculate the matrix of spatial distribution for consumption
    mat_spat_imp_cons = mat_cons*mat_spat_imp_pro
    return mat_spat_imp_cons

def get_glo_if(mineral):
    '''
    Calculate the impact factors of global production from pre-calculated national impact factors
    Requirement: Coutry-level impact factors
    '''
    #Inputs
    country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
    list_cty_ein = country_correspondence.loc[:,'ecoinvent_country'].tolist()
    if_dt = pd.read_csv('outputs/if_alumina_aluminium.csv')
    mineral_act ={'bauxite':'bauxite production','alumina':'alumina production','aluminium':'aluminium production, primary, ingot'}
    if_dt = if_dt[(if_dt.Location!='GLO') & (if_dt.Name==mineral_act[mineral])].reset_index(drop=True)
    #Output
    dtf = pd.DataFrame()
    for year in if_dt.Year.unique():
        for method in if_dt.Midpoint_abb.unique():
            tmp_if_dt = if_dt[(if_dt.Year==year) & (if_dt.Midpoint_abb==method)].reset_index(drop=True)
            tmp_if_dt["index_producer"]=0
            #Index for producer (rows)
            prod_cty_list = tmp_if_dt.Location.unique()
            for prod_cty in prod_cty_list:
                prod_cty_index = np.where(np.array(list_cty_ein) == prod_cty)[0]
                #Attention: ME has two names depending on the timing for production. Serbia and Montenegro before a certain year, montenegro after.
                if (len(prod_cty_index)==2) & (year<2006):
                    prod_cty_index = prod_cty_index[1]
                elif (len(prod_cty_index)==2) & (year>=2006):
                    prod_cty_index = prod_cty_index[0]
                tmp_if_dt.loc[tmp_if_dt.Location==prod_cty,"index_producer"] = prod_cty_index
            #Create matrix of spatial distribution
            mat_if_pro = sparse.csc_matrix((tmp_if_dt["Score"],(tmp_if_dt["index_producer"],[0 for l in range(0,len(tmp_if_dt))])),shape=(len(list_cty_ein),1))
            #Get apparent consumption matrix
            mat_cons = utils_mfa.calculate_mat_cons_kastner(mineral=mineral,year=year)
            #Calculate the matrix of spatial distribution for consumption
            mat_if_cons = mat_cons*mat_if_pro
            tmp_dict = {'Location':'GLO',
                    'Score':mat_if_cons.sum()/mat_cons.sum(),
                    'Midpoint_abb':method,
                    'Name':mineral_act[mineral],
                    'Year':year}
            dtf = dtf.append(pd.DataFrame(tmp_dict,index=[1]),ignore_index=True)
    return dtf