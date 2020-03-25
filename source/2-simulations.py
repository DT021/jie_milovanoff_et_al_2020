# -*- coding: utf-8 -*-
"""
Script to create the simulation results
@author: Alexandre
"""
# Setup the environment
import os
wd_path="C:/Users/Alex Milovanoff/GitHub/jie_milovanoff_et_al_2020"
os.chdir(wd_path)

import pandas as pd
import numpy as np
import brightway2 as bw
import source.utils_mfa as utils_mfa
import source.utils_update as utils_update
import source.utils_brightway as utils_bw

# Upload database
bw.projects.set_current('alu_project')
bw_db_name = "ecoinvent 3.4 cutoff to edit"
bw_db = bw.Database(bw_db_name)
gwp_recipe_method = [method for method in bw.methods if 'ReCiPe Midpoint (H) V1.13' == method[0] 
                                                        and any(impact == method[1] for impact in ['climate change'])]
mining_recipe_method = [method for method in bw.methods if 'ReCiPe Midpoint (H) V1.13' == method[0] 
                                                        and any(impact == method[1] for impact in ['climate change','terrestrial acidification','freshwater eutrophication','marine eutrophication','photochemical oxidant formation','ozone depletion'])]
#Create the multiLCA
country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
list_ei_country = country_correspondence.ecoinvent_country.unique().tolist()
list_ei_country.append('GLO') #We add GLO in the list
calculation_setup_list = {'bauxite_production_recipe':'bauxite mine operation',
                          'bauxite_consumption_recipe':'market for consumption of bauxite',
                          'alumina_production_recipe':'alumina production',
                          'alumina_consumption_recipe':'market for consumption of alumina',
                          'aluminium_ingot_production_recipe':'aluminium production, primary, ingot',
                          'aluminium_consumption_recipe':'market for consumption of aluminium'
                          }

for calculation_setup_name in calculation_setup_list.keys():
    act_name = calculation_setup_list[calculation_setup_name]
    act_list = [act for act in bw_db if act_name in act['name'] and  act['location'] in list_ei_country]
    fu_list = [{act:1} for act in act_list]
    bw.calculation_setups[calculation_setup_name] = {'inv':fu_list, 'ia':mining_recipe_method}

'''
Calculate country-level production and consumption quantity of bauxite, alumina and aluminum
'''
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
list_cty_ein = country_correspondence.loc[:,'ecoinvent_country'].tolist()
year_list = range(2000,2018)
mineral_list=['aluminium','alumina','bauxite']
dtf = pd.DataFrame()
for mineral in mineral_list:
    for year in year_list:
        mat_cons =  utils_mfa.calculate_mat_cons_kastner(mineral=mineral,year=year,trade_data_type="reconciliated")
        #Calculate the production mass by producing countries
        cty_index = np.nonzero(mat_cons.sum(axis=0))[1]
        tmp_dict = {'Country':[list_countries[index] for index in cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in cty_index],
                    'Quantity':[mat_cons.sum(axis=0).A1[index] for index in cty_index],
                    'Unit':'kg',
                    'Type': 'Production',
                    'Mineral':mineral,
                    'Year':year}
        tmp_dt = pd.DataFrame(tmp_dict)
        dtf = dtf.append(tmp_dt,ignore_index=True)
        #Calculate the consumption mass by consuming countries
        cty_index = np.nonzero(mat_cons.sum(axis=1))[0]
        tmp_dict = {'Country':[list_countries[index] for index in cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in cty_index],
                    'Quantity':[mat_cons.sum(axis=1).A1[index] for index in cty_index],
                    'Unit':'kg',
                    'Type': 'Consumption',
                    'Mineral':mineral,
                    'Year':year}
        tmp_dt = pd.DataFrame(tmp_dict)
        dtf = dtf.append(tmp_dt,ignore_index=True)
dtf.to_csv('outputs/country_prod_cons_quantity.csv',index=False)

'''
Calculate Independance ratio for producing countries
'''
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
list_cty_ein = country_correspondence.loc[:,'ecoinvent_country'].tolist()
year_list = range(2000,2018)
dtf = pd.DataFrame()
for year in year_list:
    for mineral in ['aluminium','alumina']:
        mat_cons =  utils_mfa.calculate_mat_cons_kastner(mineral=mineral,year=year,trade_data_type="reconciliated")
        #Calculate the producing countries with the lowest independant ratio
        prod_cty_index = np.flatnonzero(mat_cons.diagonal()).tolist()
        tmp_dict = {'Country':[list_countries[index] for index in prod_cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in prod_cty_index],
                    'Domestic_production':[mat_cons[index,index] for index in prod_cty_index],
                    'Apparent_consumption':[mat_cons.sum(axis=1).A1[index] for index in prod_cty_index],
                    'Domestic_production_ratio':[mat_cons[index,index]/mat_cons.sum(axis=1).A1[index] for index in prod_cty_index],
                    'Mineral':mineral,
                    'Year':year}
        tmp_dt = pd.DataFrame(tmp_dict)
        dtf = dtf.append(tmp_dt,ignore_index=True)
dtf.to_csv('outputs\country_domestic_production_ratio.csv',index=False)

'''
Calculate impact factors for selected ReCiPe midpoints of alumina production and consumption, aluminium production and consumption of the specified countries
'''
#Get list of top 20 producing countries
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
year=2017
mat_cons =  utils_mfa.calculate_mat_cons_kastner(mineral='aluminium',year=year,trade_data_type="reconciliated")
#Get the top list of consuming countries
country_list = [country_correspondence.loc[country_correspondence['cty Name English']==list_countries[index],'ecoinvent_country'].values[0] for index in np.argsort(mat_cons.sum(axis=1).A1,axis=0)[-21:].tolist() if str(country_correspondence.loc[country_correspondence['cty Name English']==list_countries[index],'ecoinvent_country'].values[0])!='nan']

year_list = range(2000,2018)
#country_list = ['IN', 'DE', 'US', 'CN','CA','ZA','QA','RU','BR','AU']
multilca_list = ['alumina_production_recipe','alumina_consumption_recipe','aluminium_ingot_production_recipe']
#Output
lca_df = pd.DataFrame()
for year in year_list:
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite')
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina')
    for calculation_setup_name in multilca_list:
        MultiLCA = bw.MultiLCA(calculation_setup_name)
        temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
        temp_dt['Year'] = year
        lca_df = lca_df.append(temp_dt,ignore_index=True)
    for country in country_list:
        alu_cons_act = bw_db.get('market for consumption of aluminium')
        alu_cons_act['location'] = country
        alu_cons_act.save()
        utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='aluminium')
        bw.calculation_setups['aluminium_consumption_recipe']['inv'] = [{alu_cons_act:1}]
        MultiLCA = bw.MultiLCA('aluminium_consumption_recipe')
        temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
        temp_dt['Year'] = year
        lca_df = lca_df.append(temp_dt,ignore_index=True)
lca_df.to_csv('outputs/if_bauxite_alumina_aluminium.csv',index=False)

#Calculate for global impact factors of primary aluminum ingot production
glo_dt = utils_update.get_glo_if(mineral='aluminium')
glo_dt.to_csv('outputs/if_glo_aluminium.csv',index=False)

'''
Impact Factors: Errors caused by use of globalized or localized alumina processes

'''
year_list = range(2000,2018)
lca_df = pd.DataFrame()
for year in year_list:
    print(year)
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    #Calculate the impact factors in case of globalized alumina processes
    utils_update.dbUpdate_globalize(bw_db_name=bw_db_name,stage_to_globalize=['alumina'])
    MultiLCA = bw.MultiLCA('aluminium_ingot_production_recipe')
    temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
    temp_dt['Year'] = year
    temp_dt['Type'] = 'globalized_for_alumina'
    lca_df = lca_df.append(temp_dt,ignore_index=True)
    #Calculate the production-based IF with production-based alumina
    utils_update.dbUpdate_localize(bw_db_name=bw_db_name)
    MultiLCA = bw.MultiLCA('aluminium_ingot_production_recipe')
    temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
    temp_dt['Year'] = year
    temp_dt['Type'] = 'localized_for_alumina'
    lca_df = lca_df.append(temp_dt,ignore_index=True)
    #Calculate the impact factors in regionalized case. First only alumina, then with aluminum
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite')
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina')
    MultiLCA = bw.MultiLCA('aluminium_ingot_production_recipe')
    temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
    temp_dt['Year'] = year
    temp_dt['Type'] = 'default'
    lca_df = lca_df.append(temp_dt,ignore_index=True)
lca_df.to_csv('outputs/if_with_errors_aluminium.csv',index=False)

'''
Sensitivity analysis of trade data on impact factors of primary aluminum ingot
'''
#Get list of producing countries
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
year=2017
mat_cons =  utils_mfa.calculate_mat_cons_kastner(mineral='aluminium',year=year,trade_data_type="reconciliated")
#Get the top 20 consuming countries
country_list = [country_correspondence.loc[country_correspondence['cty Name English']==list_countries[index],'ecoinvent_country'].values[0] for index in np.argsort(mat_cons.sum(axis=1).A1,axis=0)[-21:].tolist() if str(country_correspondence.loc[country_correspondence['cty Name English']==list_countries[index],'ecoinvent_country'].values[0])!='nan']
year_list = [2000,2017]
#Output
lca_df = pd.DataFrame()
for year in year_list:
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    for trade_data_type in ["imports","exports","reconciliated"]:
        utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite',trade_data_type=trade_data_type)
        utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina',trade_data_type=trade_data_type)
        for country in country_list:
            alu_cons_act = bw_db.get('market for consumption of aluminium')
            alu_cons_act['location'] = country
            alu_cons_act.save()
            utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='aluminium',trade_data_type=trade_data_type)
            bw.calculation_setups['aluminium_consumption_recipe']['inv'] = [{alu_cons_act:1}]
            MultiLCA = bw.MultiLCA('aluminium_consumption_recipe')
            temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
            temp_dt['Year'] = year
            temp_dt['Trade_data_type'] = trade_data_type
            lca_df = lca_df.append(temp_dt,ignore_index=True)
lca_df.to_csv('outputs/sensitivity_trade_data_if_alu_cons.csv',index=False)

'''
Sensitivity analysis of apparent consumption model on impact factors of primary aluminum ingot
'''
#Get list of producing countries
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
year=2017
mat_cons =  utils_mfa.calculate_mat_cons_kastner(mineral='aluminium',year=year,trade_data_type="reconciliated")
#Get the top 20 consuming countries
country_list = [country_correspondence.loc[country_correspondence['cty Name English']==list_countries[index],'ecoinvent_country'].values[0] for index in np.argsort(mat_cons.sum(axis=1).A1,axis=0)[-21:].tolist() if str(country_correspondence.loc[country_correspondence['cty Name English']==list_countries[index],'ecoinvent_country'].values[0])!='nan']

year_list = [2000,2017]
#Output
lca_df = pd.DataFrame()
for year in year_list:
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    for adj_model in ["kastner","no"]:
        utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite',adjustment=adj_model)
        utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina',adjustment=adj_model)
        for country in country_list:
            alu_cons_act = bw_db.get('market for consumption of aluminium')
            alu_cons_act['location'] = country
            alu_cons_act.save()
            utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='aluminium',adjustment=adj_model)
            bw.calculation_setups['aluminium_consumption_recipe']['inv'] = [{alu_cons_act:1}]
            MultiLCA = bw.MultiLCA('aluminium_consumption_recipe')
            temp_dt = utils_bw.get_multilca_to_dataframe(MultiLCA)
            temp_dt['Year'] = year
            temp_dt['Consumption_model'] = adj_model
            lca_df = lca_df.append(temp_dt,ignore_index=True)
lca_df.to_csv('outputs/sensitivity_cons_mdl_if_alu_cons.csv',index=False)

'''
Calculate the spatial distribution of IFs for all producing countries. 
Calculation time: 82 hours
'''
#Get all aluminum ingot production activities
alu_cons_act = bw_db.get('market for consumption of aluminium')
act_list = [exc.input for exc in alu_cons_act.technosphere() if exc.input['location']!='GLO']
year_list = [2000,2010,2017]
product_system_depth=4
#output
contribution_df = pd.DataFrame()
for year in year_list:
    print(year)
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite')
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina')
    for act in act_list:
        for method in mining_recipe_method:
            Method = bw.Method(method)
            functional_unit = {act:1}
            #Get the spatial contribution
            temp_dtf = utils_bw.traverse_tagged_databases_to_dataframe(functional_unit, method, label="location_tag",default_tag='GLO', secondary_tag=(None,None),product_system_depth=product_system_depth)
            temp_dtf = temp_dtf.rename(columns={'location_tag':'Spatial_contribution'})
            temp_dtf = temp_dtf.join(pd.DataFrame(
                    {'Year':year,
                     'Producing_country':act['location'],
                     'Method_name':method[0],
                     'Midpoint':method[1],
                     'Midpoint_abb':method[2],
                     'Unit':Method.metadata['unit']
                     },index=temp_dtf.index
                    ))
            contribution_df = contribution_df.append(temp_dtf,ignore_index=True)
contribution_df.to_csv('outputs/spatial_if_aluminum.csv',index=False)

'''
Calculate the stage contribution of the LC impacts of global primary aluminum production
'''
year_list = [2000,2010,2017]
product_system_depth = 5
#Output
contribution_df = pd.DataFrame()
for year in year_list:
    print(year)
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite')
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina')
    utils_update.dbUpdate_aluminum_global_production(bw_db_name=bw_db_name,year=year)
    for method in mining_recipe_method:
        Method = bw.Method(method)
        alu_cons_act = bw_db.get('market for consumption of aluminium')
        functional_unit = {alu_cons_act:1}
        #Get the spatial contribution
        temp_dtf = utils_bw.traverse_tagged_databases_to_dataframe(functional_unit, method, label="tag",default_tag="other", secondary_tag=(None,None),product_system_depth=product_system_depth)
        temp_dtf = temp_dtf.rename(columns={'tag':'Stage'})
        temp_dtf = temp_dtf.join(pd.DataFrame(
                {'Year':year,
                 'Method_name':method[0],
                 'Midpoint':method[1],
                 'Midpoint_abb':method[2],
                 'Unit':Method.metadata['unit']
                 },index=temp_dtf.index
                ))
        contribution_df = contribution_df.append(temp_dtf,ignore_index=True)
contribution_df.to_csv('outputs/global_aluminium_prod_stage.csv',index=False)

'''
Calculate the spatial distribution by stage of the impacts of global primary aluminum ingot production
'''
year_list = [2000,2017]
product_system_depth = 6
#Output
contribution_df = pd.DataFrame()
for year in year_list:
    print(year)
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite')
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina')
    utils_update.dbUpdate_aluminum_global_production(bw_db_name=bw_db_name,year=year)
    for method in mining_recipe_method:
        Method = bw.Method(method)
        alu_cons_act = bw_db.get('market for consumption of aluminium')
        functional_unit = {alu_cons_act:1}
        #Get the spatial contribution
        temp_dtf = utils_bw.traverse_tagged_databases_to_dataframe(functional_unit, method, label="tag",default_tag="other", secondary_tag=('location_tag','GLO'),product_system_depth=product_system_depth)
        temp_dtf = temp_dtf.rename(columns={'tag':'Stage','location_tag':'Location'})
        temp_dtf = temp_dtf.join(pd.DataFrame(
                {'Year':year,
                 'Method_name':method[0],
                 'Midpoint':method[1],
                 'Midpoint_abb':method[2],
                 'Unit':Method.metadata['unit']
                 },index=temp_dtf.index
                ))
        contribution_df = contribution_df.append(temp_dtf,ignore_index=True)
contribution_df.to_csv('outputs/global_aluminium_prod_stage_spatial.csv',index=False)

'''
Calculate the domestic production, consumption, embodied and exported emissions
Requirememt: if_aluminium_prod_spatial_all.csv
'''
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
list_cty_ein = country_correspondence.loc[:,'ecoinvent_country'].tolist()
year_list = [2000,2010,2017]
mineral='aluminium'
dtf = pd.DataFrame()
for year in year_list:
    for method in mining_recipe_method:
        tmp_dt = pd.DataFrame()
        Method = bw.Method(method)
        mat_spat_imp_cons = utils_update.get_embodied_impacts(year=year,mineral='aluminium',recipe_method_abb=method[2])
        #Calculate the consumption impacts by consuming countries
        cty_index = np.nonzero(mat_spat_imp_cons.sum(axis=1))[0]
        tmp_dict = {'Country':[list_countries[index] for index in cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in cty_index],
                    'Score':[mat_spat_imp_cons.sum(axis=1).A1[index] for index in cty_index],
                    'Type': 'Consumption',
                    }
        tmp_dt = tmp_dt.append(pd.DataFrame(tmp_dict),ignore_index=True)
        #Calculate the domestic production impacts by consuming countries
        cty_index = np.nonzero(mat_spat_imp_cons.sum(axis=0))[1]
        tmp_dict = {'Country':[list_countries[index] for index in cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in cty_index],
                    'Score':[mat_spat_imp_cons.sum(axis=0).A1[index] for index in cty_index],
                    'Type': 'Production',
                    }
        tmp_dt = tmp_dt.append(pd.DataFrame(tmp_dict),ignore_index=True)
        #Calculate the embodied emissions with imports by consuming countries
        cty_index = list(set(np.nonzero(mat_spat_imp_cons.sum(axis=1))[0]) | set(np.nonzero(mat_spat_imp_cons.sum(axis=0))[1]))
        tmp_dict = {'Country':[list_countries[index] for index in cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in cty_index],
                    'Score':[mat_spat_imp_cons.sum(axis=1).A1[index]-mat_spat_imp_cons[index,index] for index in cty_index],
                    'Type': 'Imports',
                    }
        tmp_dt = tmp_dt.append(pd.DataFrame(tmp_dict),ignore_index=True)
        #Calculate the embodied emissions with exports by consuming countries
        cty_index = list(set(np.nonzero(mat_spat_imp_cons.sum(axis=1))[0]) | set(np.nonzero(mat_spat_imp_cons.sum(axis=0))[1]))
        tmp_dict = {'Country':[list_countries[index] for index in cty_index],
                    'Ecoinvent_name':[list_cty_ein[index] for index in cty_index],
                    'Score':[mat_spat_imp_cons[index,index]-mat_spat_imp_cons.sum(axis=0).A1[index] for index in cty_index],
                    'Type': 'Exports',
                    }
        tmp_dt = tmp_dt.append(pd.DataFrame(tmp_dict),ignore_index=True)
        #Format
        tmp_dt = tmp_dt.join(pd.DataFrame(
                {'Mineral':mineral,
                 'Year':year,
                 'Method_name':method[0],
                'Midpoint':method[1],
                'Midpoint_abb':method[2],
                'Unit':Method.metadata['unit']
                 },index=tmp_dt.index
                ))
        dtf = dtf.append(tmp_dt,ignore_index=True)
dtf.to_csv('outputs/country_impacts_aluminium.csv',index=False)
'''
Top activities and emissions by direct emsisions for global aluminium production in 2000 and 2017
Calculation time: Minutes
'''
year_list = [2000,2017]
#Output
act_contribution_df = pd.DataFrame()
emi_contribution_dt = pd.DataFrame()
for year in year_list:
    print(year)
    utils_update.dbUpdate_ElecAluLiq(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_EnerAlumina(bw_db_name=bw_db_name,year=year)
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='bauxite')
    utils_update.dbUpdate_cons_mix(bw_db_name=bw_db_name,year=year,mineral='alumina')
    utils_update.dbUpdate_aluminum_global_production(bw_db_name=bw_db_name,year=year)
    for method in mining_recipe_method:
        Method = bw.Method(method)
        alu_cons_act = bw_db.get('market for consumption of aluminium')
        functional_unit = {alu_cons_act:1}
        lca = bw.LCA(functional_unit,method)
        lca.lci()    # Builds matrices, solves the system, generates an LCI matrix.
        lca.lcia()
        #Get activity contribution
        temp_dtf = pd.DataFrame([(tmp_list[0],tmp_list[0]/lca.score,tmp_list[1],str(tmp_list[2])) for tmp_list in lca.top_activities()],columns=['Score',"Rel_score",'Supply','Activity'])
        temp_dtf = temp_dtf.join(pd.DataFrame(
                    {'Year':year,
                     'Country':alu_cons_act['location'],
                     'Method_name':method[0],
                     'Midpoint':method[1],
                     'Midpoint_abb':method[2],
                     'Unit':Method.metadata['unit']
                     },index=temp_dtf.index
                    ))
        act_contribution_df = act_contribution_df.append(temp_dtf,ignore_index=True)
        #Get emission contribution
        temp_dtf = pd.DataFrame([(tmp_list[0],tmp_list[0]/lca.score,tmp_list[1],str(tmp_list[2])) for tmp_list in lca.top_emissions()],columns=['Score','Rel_score','Inventory_amount','Biosphere_flow'])
        temp_dtf = temp_dtf.join(pd.DataFrame(
                    {'Year':year,
                     'Country':alu_cons_act['location'],
                     'Method_name':method[0],
                     'Midpoint':method[1],
                     'Midpoint_abb':method[2],
                     'Unit':Method.metadata['unit']
                     },index=temp_dtf.index
                    ))
        emi_contribution_dt = emi_contribution_dt.append(temp_dtf,ignore_index=True)
act_contribution_df.to_csv('outputs/top_activities_aluminium.csv',index=False)
emi_contribution_dt.to_csv('outputs/top_emissions_aluminium.csv',index=False)
'''
Create Sankey diagram inputs: Material Flow Analysis
'''
country_correspondence = pd.read_csv('inputs/country_correspondence.csv')
list_cty_code =  country_correspondence.loc[:,'ctyCode'].tolist()
list_countries = country_correspondence.loc[:,'cty Name English'].tolist()
list_cty_ein = country_correspondence.loc[:,'ecoinvent_country'].tolist()
year_list = [2000,2017]
mineral_list=['aluminium','alumina','bauxite']
dtf=pd.DataFrame()
for year in year_list:
    for mineral in mineral_list:
        #Get matrix of apparent consumption
        mat_cons =  utils_mfa.calculate_mat_cons_kastner(year=year,mineral=mineral,trade_data_type="reconciliated")
        rel_mat_cons = mat_cons/mat_cons.sum()
        for row in np.where(rel_mat_cons.sum(axis=1)!=0)[0]:
            for col in np.where((rel_mat_cons[row,]!=0).toarray()[0])[0]:
                if row==col:
                    type_link = 'Domestic'
                else:
                    type_link = 'Embodied'
                tmp_dict = {'Producer':list_cty_code[col],'Consumer':list_cty_code[row],'value':rel_mat_cons[row,col],'type':type_link,'Mineral':mineral,'Year':year}
                tmp_dt = pd.DataFrame(tmp_dict,index=[0])
                dtf = dtf.append(tmp_dt,ignore_index=True)
#Aggregate by region
dtf["Producer_region"] = [country_correspondence.loc[country_correspondence["ctyCode"]==dtf.loc[i,"Producer"],"graph_region"].values[0] for i in dtf.index]
dtf["Consumer_region"] = [country_correspondence.loc[country_correspondence["ctyCode"]==dtf.loc[i,"Consumer"],"graph_region"].values[0] for i in dtf.index]
#All china producer/consumer have domestic type
dtf.loc[(dtf["Producer_region"]=="China") & (dtf["Consumer_region"]=="China"),"type"] = "Domestic"
dtf_reg = dtf.groupby(['Year','Mineral','Producer_region','Consumer_region','type'],as_index=False)['value'].sum()
dtf_reg.to_csv('outputs/sankey_diagram_data_mfa.csv',index=False)
