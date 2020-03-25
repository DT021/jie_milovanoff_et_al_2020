# -*- coding: utf-8 -*-
"""
Script to set-up the product system using Brightway. 
Objective: Create all processes associated with production and consumption of Bauxite, Alumina and aluminum
Requirement: Brightway installed with the ecoinvent 3.4 cutoff database
@author: Alexandre
"""

import os
wd_path="C:/Users/Alex Milovanoff/GitHub/jie_milovanoff_et_al_2020"
os.chdir(wd_path)
#
import brightway2 as bw
import numpy as np
import pandas as pd 
import time
import source.utils_brightway as utils_bw

bw.projects.set_current('alu_project')
''' ONLY RUN INITIALLY OR IF ISSUE WITH DATABASE
del bw.databases["ecoinvent 3.4 cutoff to edit"]
bw.databases
eidb_original = bw.Database("ecoinvent 3.4 cutoff")
eidb_original.copy("ecoinvent 3.4 cutoff to edit")
'''
ei_version=3.4
bw_db_name = "ecoinvent 3.4 cutoff to edit"
bw_db = bw.Database(bw_db_name)

'''
First step: Create and complete the electricity production activities of aluminium industries by location.
For each location-specific aluminium liquid production activity, ecoinvent input "market for electricity, medium voltage, aluminium industry".
"market for electricity, medium voltage, aluminium industry" input "electricity voltage transformation from high to medium voltage, aluminium industry"
"electricity voltage transformation from high to medium voltage, aluminium industry" input "market for electricity, medium voltage, aluminium industry" and "market for electricity, high voltage, aluminium industry"
"electricity, high voltage, aluminium industry" by source input "market for electricity, high voltage, aluminium industry"  
'''
#Import inputs
eiv_geo_overlap=pd.read_excel('inputs/eiv3.4_geographies_names_coordinates_shortcuts_overlaps20180822.xlsx',sheet_name='geographies overlaps',dtype=np.str)
conv_elec_techno_iai=pd.read_csv('inputs/conv_elec_techno_iai.csv')
iai_reg_list=pd.read_csv('inputs/list_region_iai.csv')
iai_reg_l=iai_reg_list.aluminium_region_ei34.unique().tolist()
iai_reg_l.insert(0,'GLO')

#1) Create 'electricity high voltage' for GLO with common exchanges.

act_name="market for electricity, high voltage, aluminium industry"
utils_bw.utils_bw.create_act_new_location(bw_db_name,act_name,new_location='GLO',cut_off_occurence=1,copy_multiple_same_inputs='n',keep_location_multiple_inputs='n',copy_from_glo='n')

#Check 
act_list = [act for act in bw_db if act_name in act['name'] and 'GLO' in act['location']]
new_act = act_list[0]
[exc for exc in new_act.technosphere()]

#3) For all 'electricity high voltage' activities, ensure that all energy sources are linked in input. Otherwise, create the acvitity in input and link it with existing ones.
act_list=[act for act in bw_db if act_name in act['name']]
#For each activity. Ensure all electricity sources are inputs and have exchanges.
for market_act in act_list:
    location = market_act['location']
    exc_list = [exc.input['name'] for exc in market_act.technosphere() if "electricity production" in exc.input['name']]
    diff_set = set(conv_elec_techno_iai.ecoinvent_activity.tolist()).difference(set(exc_list))
    #If some sources are not linked. Link them
    if len(diff_set) != 0:
        for input_name in list(diff_set):
            #If the input does not exist. Create it and link it with existing proxies
            if len([act for act in bw_db if input_name in act['name'] and location in act['location']])==0:
                new_act=bw_db.new_activity(input_name)
                new_act['name']=input_name
                new_act['code']=input_name+location
                new_act['location']=location
                new_act['reference product']='electricity, high voltage, aluminium industry'
                new_act['type']='process'
                new_act['unit']='kilowatt hour'
                new_act['production amount']=1
                new_act.save()
                #Activity as dictionary
                #new_act.as_dict()
            else:
                new_act=[act for act in bw_db if input_name in act['name'] and location in act['location']][0]
            #Check the list of exchanges in this input. If null, then link it to existing proxies
            if len([exc for exc in new_act.technosphere()])==0:
                new_exc_source_name=input_name.replace(", aluminium industry","").replace("electricity production,","")
                #location_keywords is list of geography shortcuts included in location
                location_keywords=eiv_geo_overlap.loc[eiv_geo_overlap["geography shortcut"]==location]["contained geography shortcut"].tolist()
                location_keywords.append(location)
                name_to_include = [new_exc_source_name,'electricity production']
                name_to_exclude = ['aluminium industry']
                unit = 'kilowatt hour'
                #bounding_act_dic return the median activities by score that respect the criteria. 
                #First, try the acitivites in a limited zone. If no activity, consider all proxies
                try:
                    median_act_dic = utils_bw.get_median_act(bw_db_name=bw_db_name,name_to_include=name_to_include,name_to_exclude=name_to_exclude,unit=unit,ia_method=('IPCC 2013', 'climate change', 'GWP 100a'),location_keywords=location_keywords)
                    print('Inputs have been found in the location keywords for market '+location+' and source '+new_exc_source_name)
                except KeyError:
                    median_act_dic = utils_bw.get_median_act(bw_db_name=bw_db_name,name_to_include=name_to_include,name_to_exclude=name_to_exclude,unit=unit,ia_method=('IPCC 2013', 'climate change', 'GWP 100a'),location_keywords=None)
                    print('No inputs have been found in the location keywords for market '+location+' and source '+new_exc_source_name)
                #Create the exchange
                new_exc_input = [act for act in bw_db if act['code']==median_act_dic['Median']][0]
                new_exc = new_act.new_exchange(input=new_exc_input, amount=1, type='technosphere')
                new_exc.save()
                print(new_exc)
                new_act.save()
            #Check exchange is in the activity
            #[exc for exc in new_act.technosphere()]
            #Add the input (newly created or not to the market activity)
            new_exc_market = market_act.new_exchange(input=new_act, amount=0, type='technosphere')
            new_exc_market.save()
''' If issue with GLO activity. Delete all 
act_name="market for electricity, high voltage, aluminium industry"
act = [act for act in bw_db if act_name in act['name'] and 'GLO' in act['location']][0]
elec_act_exc_list = [exc for exc in act.technosphere() if 'electricity production' in exc.input['name']]
for exc in elec_act_exc_list:
    exc.input.delete()
    exc.delete()
[exc for exc in act.technosphere()]
'''
#Check the LCA score are equivalent for all locations
act_name="market for electricity, high voltage, aluminium industry"
act_list = [act for act in bw_db if act_name in act['name']]
fu_list = [{act:1} for act in act_list]
method = ('IPCC 2013', 'climate change', 'GWP 100a')
bw.calculation_setups['aluminium_elec_high_ipcc'] = {'inv':fu_list, 'ia':[method]}
MultiLCA = bw.MultiLCA('aluminium_elec_high_ipcc')
pd.DataFrame(index=[list(fu.keys())[0]['location'] for fu in fu_list], columns=[method], data=MultiLCA.results)

    
#2) Create 'electricity medium voltage' for GLO without any exchange.

act_name = "market for electricity, medium voltage, aluminium industry"
act_list = [act for act in bw_db if act_name in act['name']]
#Check all locations exist
diff_set=set(iai_reg_l).difference(set([act['location'] for act in act_list]))
#If GLO does not exist. Create the activity WITHOUT EXCHANGES
if 'GLO' in diff_set:
    new_act=bw_db.new_activity(act_name)
    new_act['name']=act_name
    new_act['code']=act_name+'GLO'
    new_act['location']='GLO'
    new_act['reference product']=act_list[0]['reference product']
    new_act['type']='process'
    new_act['unit']=act_list[0]['unit']
    new_act['production amount']=1
    new_act.save()
#Check [act for act in bw_db if act_name in act['name']]
    
#3) Create 'electricity voltage transformation' for GLO without any exchange.

act_name="electricity voltage transformation from high to medium voltage, aluminium industry"
utils_bw.create_act_new_location(bw_db_name,act_name,new_location='GLO',cut_off_occurence=0.9,copy_multiple_same_inputs='y',keep_location_multiple_inputs='n',copy_from_glo='n')

#Check 
act = [act for act in bw_db if act_name in act['name'] and 'GLO' in act['location']][0]
[exc for exc in act.technosphere()]

#4) Create the exchanges in 'electricity medium voltage' for GLO
                
act_name="market for electricity, medium voltage, aluminium industry"
utils_bw.create_act_new_location(bw_db_name,act_name,new_location='GLO',cut_off_occurence=0.9,copy_multiple_same_inputs='y',keep_location_multiple_inputs='n',copy_from_glo='n')
#Check 
act = [act for act in bw_db if act_name in act['name'] and 'GLO' in act['location']][0]
[exc for exc in act.technosphere()]

'''
Create Bauxite production activities by producing country. And link them to alumina activities.

'''    

#1) Duplicate the bauxite mine operation activities by producing country location

#In this study, the depth of product system is only 1 as we regionalize the LCA results in another way using recurse_tagged_graph
country_correspondence=pd.read_csv('inputs/country_correspondence.csv').fillna(0)
prod_ds = pd.read_csv('inputs/bauxite_production_data.csv').fillna(0)
producing_country_list = prod_ds.Country.unique().tolist()
act_name = 'bauxite mine operation'
act_ref = [act for act in bw_db if act_name in act['name'] and 'GLO' in act['location']][0]
for country in producing_country_list:
    ei_country = country_correspondence.loc[country_correspondence['cty Name English']==country,'ecoinvent_country'].values[0]
    if ei_country==0:
        print(country+' has no equivalency')
    else:
        beg_ts = time.time()
        new_act,duplicated_act,non_duplicated_act = utils_bw.duplicate_act_new_location(bw_db_name=bw_db_name,act_key=act_ref.key,new_location=ei_country,prod_system_depth=1)
        end_ts = time.time()
        print("elapsed time (seconds): %f" % (end_ts - beg_ts))

#Check the LCA score are equivalent for all locations
act_list = [act for act in bw_db if act_name in act['name']]
fu_list = [{act:1} for act in act_list]
method = ('IPCC 2013', 'climate change', 'GWP 100a')
bw.calculation_setups['bauxite_production_ipcc'] = {'inv':fu_list, 'ia':[method]}
MultiLCA = bw.MultiLCA('bauxite_production_ipcc')
pd.DataFrame(index=[list(fu.keys())[0]['location'] for fu in fu_list], columns=[method], data=MultiLCA.results)

#2) Create the transportation activities for all producing countries. Assumption: We assume only transoceanic ship
act_name = 'market for transport, freight, sea, transoceanic ship'
transport_act = [act for act in bw_db if act_name == act['name']][0]
#This activity has only one input. We duplicate it by producing region (origin)
baux_loc_list = [act['location'] for act in bw_db if 'bauxite mine operation' in act['name'] and 'kilogram' in act['unit'] and 'GLO' not in act['location']]
for loc in baux_loc_list:
    beg_ts = time.time()
    new_act,duplicated_act,non_duplicated_act = utils_bw.duplicate_act_new_location(bw_db_name=bw_db_name,act_key=transport_act.key,new_location=loc,prod_system_depth=1)
    end_ts = time.time()
    print("elapsed time (seconds): %f" % (end_ts - beg_ts))

#3) Create market for consumption of bauxite for all alumina producing countries
    
#Create activity for GLO
act_ref = [act for act in bw_db if 'bauxite mine operation' in act['name'] and 'GLO' in act['location']][0]
#list(act_ref.technosphere())
act_name = 'market for consumption of bauxite'
if len([act for act in bw_db if act_name==act['name'] and 'GLO'==act['location']])==0:
    glo_act = bw_db.new_activity(act_name)
    glo_act['name'] = act_name
    glo_act['code'] = act_name+'GLO'
    glo_act['location'] = 'GLO'
    glo_act['reference product'] = act_ref['reference product']
    glo_act['type'] = 'process'
    glo_act['unit'] = act_ref['unit']
    glo_act['production amount'] = 1
    glo_act.save()
else:
    glo_act = [act for act in bw_db if act_name==act['name'] and 'GLO'==act['location']][0]
#glo_act.as_dict()
list(glo_act.technosphere())
    
#Create exchanges of all bauxite producing countries and transportation acitivities
baux_act_list = [act for act in bw_db if 'bauxite mine operation' in act['name'] and 'kilogram' in act['unit']]
mar_tran_act_list = [act for act in bw_db if 'market for transport, freight, sea, transoceanic ship' == act['name']]
baux_loc_list = [act['location'] for act in baux_act_list]
for loc in baux_loc_list:
    if loc not in [exc.input['location'] for exc in glo_act.technosphere() if 'bauxite mine operation' == exc.input['name']]:
        #Create a new bauxite exchange from location
        baux_in_act = [act for act in baux_act_list if loc in act['location']][0]
        new_exc = glo_act.new_exchange(amount=1/len(baux_loc_list),input=baux_in_act,type="technosphere")
        new_exc.save()
        glo_act.save()
        #Create the transport market activity
        trans_in_act = [act for act in mar_tran_act_list if loc in act['location']][0]
        new_exc = glo_act.new_exchange(amount=0,input=trans_in_act,type="technosphere")
        new_exc.save()
        glo_act.save()
list(glo_act.technosphere())

#Update the exchanges to only account for GLO inputs in GLO market.
for exc in [exc for exc in glo_act.technosphere() if 'bauxite mine operation'==exc.input['name']]:
    if exc.input['location']=='GLO':
        exc['amount'] = 1
    else:
        exc['amount'] = 0
    exc.save()
    glo_act.save()
#list(glo_act.technosphere())

#Copy activity for all alumina producing countries
country_correspondence=pd.read_csv('inputs/country_correspondence.csv').fillna(0)
prod_ds = pd.read_csv('inputs/alumina_production_data.csv').fillna(0)
producing_country_list = prod_ds.Country.unique().tolist()
act_list = [act for act in bw_db if act_name==act['name']]
for country in producing_country_list:
    act_list = [act for act in bw_db if 'market for consumption of bauxite'==act['name']]
    ei_country = country_correspondence.loc[country_correspondence['cty Name English']==country,'ecoinvent_country'].values[0]
    print(ei_country)
    if ei_country==0:
        print(country+' has no equivalency')
    elif ei_country not in [act['location'] for act in act_list]:
        new_loc_act = glo_act.copy()
        new_loc_act['location'] = ei_country
        new_loc_act.save()
        print('Activity for '+ei_country+' created')
#list(new_loc_act.technosphere())
        
#Check no duplicates
loc_to_delete = list(set([act['location'] for act in act_list if [act['location'] for act in act_list].count(act['location']) > 1]))
for loc in loc_to_delete:
    act = [act for act in act_list if act['location']==loc][0]
    act.delete()
act_list = [act for act in bw_db if 'market for consumption of bauxite' in act['name']]
[act['location'] for act in act_list if [act['location'] for act in act_list].count(act['location']) > 1]

'''
Create alumina process that aggregate aluminium oxide and aluminium hydroxide processes GLO.
'''

#1) Create the alumina production activity GLO if non-existing

orig_alumina_act=[act for act in bw_db if "aluminium oxide production" in act['name'] and "GLO" in act['location'] and 'kilogram' in act['unit']][0]
search_list=[act for act in bw_db if "alumina production" in act['name'] and "GLO" in act['location'] and 'kilogram' in act['unit']]
if  len(search_list)==0:
    alumina_act=orig_alumina_act.copy()
    alumina_act['name']='alumina production'
    alumina_act['comment']='from aluminium oxide production, GLO process. Aggregates inputs of aluminium hydroxide production adjusted to the ouput/input of aluminium hydroxide in aluminium oxide'
    alumina_act.save()
elif len(search_list)==1:
    alumina_act=search_list[0]
alumina_act.as_dict()

#2) Input value of market for aluminium hydroxide into this new process (if still existing).

search_list=[exc for exc in alumina_act.technosphere() if 'market for aluminium hydroxide' in exc.input['name'] and 'kilogram' in exc.input['unit']]
if len(search_list)==0:
    print("No market for aluminium hydroxide input in alumina production")
    alu_hydro_input=1.53
else:
    print("Input exists")
    alu_hydro_exc=search_list[0]
    alu_hydro_input=alu_hydro_exc.amount

#3) Copy and adapt all inputs and outputs (except production of aluminium hydroxide production activity into alumina production activity
al_hydroxide_act=[act for act in bw_db if 'aluminium hydroxide production' in act['name'] and "GLO" in act['location'] and 'kilogram' in act['unit']][0]
exc_list=[exc for exc in al_hydroxide_act.exchanges() if exc['type']!='production']
for old_exc in exc_list:
    #Check if exchange does not already exist
    if len([exc for exc in alumina_act.exchanges() if exc.input.key==old_exc.input.key and exc['comment']=="copied and adapted from aluminium hydroxide production"])==0:
        new_exc=alumina_act.new_exchange(input=old_exc.input.key,amount=alu_hydro_input*old_exc.amount,unit=old_exc['unit'],type=old_exc['type'],comment='copied and adapted from aluminium hydroxide production')
        new_exc.save()
    else:
        print("Exchange already copied and adapted")
len([exc for exc in alumina_act.exchanges()])

#4) Delete market for aluminium hydroxide input into alumina production

alu_hydro_exc.delete()
alumina_act.save()

#5) Check if duplicate inputs

alumina_exc_list=[exc for exc in alumina_act.exchanges()]
alumina_exc_input_key=[exc.input.key for exc in alumina_exc_list]
duplicate_input_key=list(set([x for x in alumina_exc_input_key if alumina_exc_input_key.count(x) >= 2]))
if len(duplicate_input_key)>0:
    for input_key in duplicate_input_key:
        #Exchange to adapt (not to delete)
        exc_to_adapt = [exc for exc in alumina_exc_list if exc.input.key==input_key and exc['comment']!="copied and adapted from aluminium hydroxide production"][0]
        #List of exchanges to delete (once amount extracted)
        exc_to_del_list = [exc for exc in alumina_exc_list if exc.input.key==input_key and exc['comment']=="copied and adapted from aluminium hydroxide production"]
        initial_amount = exc_to_adapt.amount
        amount_to_add = sum([exc.amount for exc in exc_to_del_list])
        exc_to_adapt['amount'] = initial_amount+amount_to_add
        exc_to_adapt.save()
        #Delete other exchanges
        for exc_to_del in exc_to_del_list:
            exc_to_del.delete()
    alumina_act.save()
else:
    print("No duplicate inputs in exchanges")
    
#Check [exc for exc in alumina_act.exchanges()]
#alumina_act.delete() IF PROBLEM ONLY
    
#6) Check that LCA scores are the same

ipcc2013 = ('IPCC 2013', 'climate change', 'GWP 100a')
#New activity
delta_lca_calc = bw.LCA({alumina_act:1,orig_alumina_act:-1},ipcc2013)
delta_lca_calc.lci()
delta_lca_calc.lcia()
delta_lca_calc.score

#7) Unlink the market of bauxite, without water. And link the new market for consumption of bauxite for GLO

act_name = 'alumina production'
alumina_act = [act for act in bw_db if act['name']==act_name and 'GLO'==act['location']][0]
mar_baux_act = [act for act in bw_db if 'market for consumption of bauxite' == act['name'] and 'GLO'==act['location']][0]

#print(str(sum([exc.amount for exc in alumina_act.technosphere() if 'bauxite mine operation' in exc.input['name']])))
alumina_exc_list = [exc for exc in alumina_act.technosphere()]
#Check if market is still present and bauxite not linked
if any('market for bauxite, without water' in exc.input['name'] for exc in alumina_exc_list):
    #Get the market activity
    baux_exc = [exc for exc in alumina_exc_list if 'market for bauxite, without water'==exc.input['name']][0]
    baux_input = baux_exc.amount
    #Delete the exchange
    baux_exc.delete()
    alumina_act.save()
    #check [exc for exc in alumina_act.technosphere() if 'market for bauxite, without water'==exc.input['name']]
else:
    baux_input = 2.8764
if 'GLO' not in [exc.input['location'] for exc in alumina_act.technosphere() if 'market for consumption of bauxite' == exc.input['name']]:
    #Create a new bauxite exchange from location
    new_exc = alumina_act.new_exchange(amount=baux_input,input=mar_baux_act,type="technosphere")
    new_exc.save()
    alumina_act.save()
#Check list(alumina_act.technosphere())

'''
Duplicate alumina production processes for alumina producing country. Create market for consumption for all aluminium consuming countries
'''

#1) Duplicate the alumina production activites for all producing countries

country_correspondence=pd.read_csv('inputs/country_correspondence.csv').fillna(0)
prod_ds = pd.read_csv('inputs/alumina_production_data.csv').fillna(0)
producing_country_list = prod_ds.Country.unique().tolist()
act_name = 'alumina production'
act_ref = [act for act in bw_db if act_name in act['name'] and 'GLO' in act['location']][0]
for country in producing_country_list:
    ei_country = country_correspondence.loc[country_correspondence['cty Name English']==country,'ecoinvent_country'].values[0]
    print(ei_country)
    if ei_country==0:
        print(country+' has no equivalency')
    else:
        beg_ts = time.time()
        new_act,duplicated_act,non_duplicated_act = utils_bw.duplicate_act_new_location(bw_db_name=bw_db_name,act_key=act_ref.key,new_location=ei_country,prod_system_depth=1)
        end_ts = time.time()
        print("elapsed time (seconds): %f" % (end_ts - beg_ts))

#Check for all alumina act, appropriate market for consumption of bauxite is in
country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
list_ei_country = country_correspondence.ecoinvent_country.unique().tolist()
list_ei_country.append('GLO') #We add GLO in the list
act_name = 'alumina production'
act_list = [act for act in bw_db if act_name in act['name'] and act['location'] in list_ei_country]
for act in act_list:
    search_list = [exc for exc in act.technosphere() if 'market for consumption of bauxite' in exc.input['name']]
    assert len(search_list)==1
    assert act['location'] in [exc.input['location'] for exc in search_list]

#Check the LCA score are equivalent for all locations
act_name = 'alumina production'
act_list = [act for act in bw_db if act_name in act['name']]
fu_list = [{act:1} for act in act_list]
method = ('IPCC 2013', 'climate change', 'GWP 100a')
bw.calculation_setups['alumina_production_ipcc'] = {'inv':fu_list, 'ia':[method]}
MultiLCA = bw.MultiLCA('alumina_production_ipcc')
pd.DataFrame(index=[list(fu.keys())[0]['location'] for fu in fu_list], columns=[method], data=MultiLCA.results)

#9) Create the transportation activities for all producing countries. Assumption: We assume only transoceanic ship
act_name = 'market for transport, freight, sea, transoceanic ship'
transport_act = [act for act in bw_db if act_name == act['name'] and 'GLO' == act['location']][0]
#This activity has only one input. We duplicate it by producing region (origin)
alumina_loc_list = [act['location'] for act in bw_db if 'alumina production' in act['name'] and 'kilogram' in act['unit'] and 'GLO' not in act['location']]
for loc in alumina_loc_list:
    beg_ts = time.time()
    new_act,duplicated_act,non_duplicated_act = utils_bw.duplicate_act_new_location(bw_db_name=bw_db_name,act_key=transport_act.key,new_location=loc,prod_system_depth=1)
    end_ts = time.time()
    print("elapsed time (seconds): %f" % (end_ts - beg_ts))

#2) Create the market for consumption of alumina for primary aluminium liquid countries
    
#Create activity for GLO
act_ref = [act for act in bw_db if 'alumina production' in act['name'] and 'GLO' in act['location']][0]
act_name = 'market for consumption of alumina'
if len([act for act in bw_db if act_name==act['name'] and 'GLO'==act['location']])==0:
    glo_act = bw_db.new_activity(act_name)
    glo_act['name'] = act_name
    glo_act['code'] = act_name+'GLO'
    glo_act['location'] = 'GLO'
    glo_act['reference product'] = act_ref['reference product']
    glo_act['type'] = 'process'
    glo_act['unit'] = act_ref['unit']
    glo_act['production amount'] = 1
    glo_act.save()
else:
    glo_act = [act for act in bw_db if act_name==act['name'] and 'GLO'==act['location']][0]
#glo_act.as_dict()
list(glo_act.technosphere())

#Create exchanges of all alumina producing countries and transportation activities. Include GLO activity
alumina_act_list = [act for act in bw_db if 'alumina production' in act['name'] and 'kilogram' in act['unit']]
mar_tran_act_list = [act for act in bw_db if 'market for transport, freight, sea, transoceanic ship' == act['name']]
alumina_loc_list = [act['location'] for act in alumina_act_list]
for loc in alumina_loc_list:
    if loc not in [exc.input['location'] for exc in glo_act.technosphere() if 'alumina production' == exc.input['name']]:
        #Create a new bauxite exchange from location
        alumina_in_act = [act for act in alumina_act_list if loc in act['location']][0]
        new_exc = glo_act.new_exchange(amount=1/len(alumina_act_list),input=alumina_in_act,type="technosphere")
        new_exc.save()
        glo_act.save()
        #Create the transport market activity
        trans_in_act = [act for act in mar_tran_act_list if loc in act['location']][0]
        new_exc = glo_act.new_exchange(amount=0,input=trans_in_act,type="technosphere")
        new_exc.save()
        glo_act.save()
#list(glo_act.technosphere())

#Update the exchanges to only account for GLO inputs in GLO market.
for exc in [exc for exc in glo_act.technosphere() if 'alumina production'==exc.input['name']]:
    if exc.input['location']=='GLO':
        exc['amount'] = 1
    else:
        exc['amount'] = 0
    exc.save()
    glo_act.save()
#list(glo_act.technosphere())

#3) Copy activity for all primary aluminium liquid producing countries
country_correspondence=pd.read_csv('inputs/country_correspondence.csv').fillna(0)
prod_ds = pd.read_csv('inputs/aluminium_production_data.csv').fillna(0)
producing_country_list = prod_ds.Country.unique().tolist()
for country in producing_country_list:
    act_list = [act for act in bw_db if 'market for consumption of alumina' in act['name']]
    ei_country = country_correspondence.loc[country_correspondence['cty Name English']==country,'ecoinvent_country'].values[0]
    print(ei_country)
    if ei_country==0:
        print(country+' has no equivalency')
    elif ei_country not in [act['location'] for act in act_list]:
        new_loc_act = glo_act.copy()
        new_loc_act['location'] = ei_country
        new_loc_act.save()
        
#list(new_loc_act.technosphere())
#Check no duplicated location
loc_to_delete = list(set([act['location'] for act in act_list if [act['location'] for act in act_list].count(act['location']) > 1]))
for loc in loc_to_delete:
    act = [act for act in act_list if act['location']==loc][0]
    act.delete()
act_list = [act for act in bw_db if 'market for consumption of alumina' in act['name']]
[act['location'] for act in act_list if [act['location'] for act in act_list].count(act['location']) > 1]
  

'''
Update the alumina exchange in the primary aluminium liquid production of existing activities. Create GLO activity for liquid and ingot production
'''
#1)Link market for consumption of alumina GLO in primary liquid activities

act_list = [act for act in bw_db if 'aluminium production' in act['name'] and 'primary, liquid' in act['name']]
act_ref = [act for act in bw_db if 'market for consumption of alumina' in act['name'] and 'GLO' in act['location']][0]
for liq_alu_act in act_list:
    search_list = [exc for exc in liq_alu_act.technosphere() if  'market for aluminium oxide' in exc.input['name']]
    if len(search_list)==1:
        exc_to_update = search_list[0]
        input_to_alu_liquid = exc_to_update['amount']
        exc_to_update.delete()
        liq_alu_act.save()
    else:
        input_to_alu_liquid = 1.93538
    search_list=[exc for exc in liq_alu_act.technosphere() if  act_ref.key == exc.input.key]
    if len(search_list)==0:
        new_exc = liq_alu_act.new_exchange(amount=input_to_alu_liquid,input=act_ref,type="technosphere")
        new_exc.save()
        liq_alu_act.save()

#2) Create the primary aluminium liquid production activities for GLO
for liquid_type in ['Söderberg','prebake']:
    act_name = 'aluminium production, primary, liquid, '+liquid_type
    #Create the GLO activity WITHOUT ALUMINA EXCHANGE
    utils_bw.create_act_new_location(bw_db_name=bw_db_name,act_name=act_name,new_location='GLO',cut_off_occurence=0.9,copy_multiple_same_inputs='y',keep_location_multiple_inputs='y',copy_from_glo='n')

act_list = [act for act in bw_db if 'aluminium production' in act['name'] and 'primary, liquid' in act['name'] and 'GLO' in act['location']]
for act in act_list:
    print(list(act.technosphere()))


#Check LCA scores for all regions
act_name = 'aluminium production, primary, liquid'
act_list = [act for act in bw_db if act_name in act['name']]
fu_list = [{act:1} for act in act_list]
method = ('IPCC 2013', 'climate change', 'GWP 100a')
bw.calculation_setups['primary_alu_liquid_iai_ipcc'] = {'inv':fu_list, 'ia':[method]}
MultiLCA = bw.MultiLCA('primary_alu_liquid_iai_ipcc')
pd.DataFrame(index=[list(fu.keys())[0]['location'] for fu in fu_list], columns=[method], data=MultiLCA.results)

#3) Create the primary aluminum ingot activities for GLO, and check it is linked with primary aluminium liquid activitiy GLO
act_name = 'aluminium production, primary, ingot'
utils_bw.create_act_new_location(bw_db_name=bw_db_name,act_name=act_name,new_location='GLO',cut_off_occurence=0.2,copy_multiple_same_inputs='y',keep_location_multiple_inputs='n',copy_from_glo='n')
new_act = [act for act in bw_db if act_name in act['name'] and 'kilogram' in act['unit'] and 'GLO' in act['location']][0]
exc_list = [exc for exc in new_act.technosphere()]
#Update inputs of soderberg and prebake. Only consider prebake at 1
sod_exc = [exc for exc in exc_list if 'Söderberg' in exc.input['name']][0]
sod_exc['amount'] = 0
sod_exc.save()
preb_exc = [exc for exc in exc_list if 'prebake' in exc.input['name']][0]
preb_exc['amount'] = 1
preb_exc.save()
[exc for exc in new_act.technosphere()]
'''
Duplicate the primary aluminium liquid production and and primary ingot production activities by country (from regional activities) 
'''

#1) Duplicate primary aluminium liquid
country_correspondence=pd.read_csv('inputs/country_correspondence.csv').fillna(0)
prod_ds = pd.read_csv('inputs/aluminium_production_data.csv').fillna(0)
producing_country_list = prod_ds.Country.unique().tolist()
for country in producing_country_list:
    act_list = [act for act in bw_db if 'aluminium production' in act['name'] and 'primary, liquid' in act['name']]
    ei_country = country_correspondence.loc[country_correspondence['cty Name English']==country,'ecoinvent_country'].values[0]
    print(ei_country)
    assert ei_country!=0, country+' has no equivalency'
    iai_aluminium_reg = country_correspondence.loc[country_correspondence['cty Name English']==country,'aluminium_region_ei34'].values[0]        
    act_ref_list = [act for act in act_list if act['location']==iai_aluminium_reg]
    for act_ref in act_ref_list:
        beg_ts = time.time()
        new_act,duplicated_act,non_duplicated_act = utils_bw.duplicate_act_new_location(bw_db_name=bw_db_name,act_key=act_ref.key,new_location=ei_country,prod_system_depth=1)
        end_ts = time.time()
        print("elapsed time (seconds): %f" % (end_ts - beg_ts))
        #list(new_act.technosphere())

#Check for all act that market for consumption of alumina is in input
country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
list_ei_country = country_correspondence.ecoinvent_country.unique().tolist()
list_ei_country.append('GLO') #We add GLO in the list
act_list = [act for act in bw_db if 'aluminium production' in act['name'] and 'primary, liquid' in act['name'] and act['location'] in list_ei_country]
for act in act_list:
    search_list = [exc for exc in act.technosphere() if 'market for consumption of alumina' in exc.input['name']]
    assert len(search_list)==1
    exc = search_list[0]
    assert act['location']==exc.input['location']
    
#Check
act_name = 'aluminium production, primary, liquid'
act_list = [act for act in bw_db if act_name in act['name']]
fu_list = [{act:1} for act in act_list]
method = ('IPCC 2013', 'climate change', 'GWP 100a')
bw.calculation_setups['primary_alu_liquid_all_ipcc'] = {'inv':fu_list, 'ia':[method]}
MultiLCA = bw.MultiLCA('primary_alu_liquid_all_ipcc')
pd.DataFrame(index=[list(fu.keys())[0]['location'] for fu in fu_list], columns=[method], data=MultiLCA.results)

#2) Duplicate primary aluminium ingot activities by country

country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
prod_ds = pd.read_csv('inputs/aluminium_production_data.csv').fillna(0)
producing_country_list = prod_ds.Country.unique().tolist()
for country in producing_country_list:
    act_list = [act for act in bw_db if 'aluminium production, primary, ingot' in act['name']]
    ei_country = country_correspondence.loc[country_correspondence['cty Name English']==country,'ecoinvent_country'].values[0]
    print(ei_country)
    assert ei_country!=0, country+' has no equivalency'
    iai_aluminium_reg = country_correspondence.loc[country_correspondence['cty Name English']==country,'aluminium_region_ei34'].values[0]  
    act_ref_list = [act for act in act_list if act['location']==iai_aluminium_reg]
    assert len(act_ref_list)==1
    act_ref = act_ref_list[0]
    beg_ts = time.time()
    new_act,duplicated_act,non_duplicated_act = utils_bw.duplicate_act_new_location(bw_db_name=bw_db_name,act_key=act_ref.key,new_location=ei_country,prod_system_depth=1)
    end_ts = time.time()
    print("elapsed time (seconds): %f" % (end_ts - beg_ts))

     
#Check for all primary aluminum ingot activities that primary aluminum liquid are input with appropriate location
country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
list_ei_country = country_correspondence.ecoinvent_country.unique().tolist()
list_ei_country.append('GLO') #We add GLO in the list
act_list = [act for act in bw_db if 'aluminium production, primary, ingot' in act['name'] and act['location'] in list_ei_country]
for act in act_list:
    search_list = [exc for exc in act.technosphere() if 'aluminium production' in exc.input['name'] and 'primary, liquid' in exc.input['name']]
    assert len(search_list)!=0
    for exc in search_list:
        assert act['location']==exc.input['location']
act_name='market group for electricity, medium voltage'
#Check LCA score
act_name = 'aluminium production, primary, ingot'
act_list = [act for act in bw_db if act_name in act['name']]
fu_list = [{act:1} for act in act_list]
method = ('IPCC 2013', 'climate change', 'GWP 100a')
bw.calculation_setups['primary_alu_ingot_all_ipcc'] = {'inv':fu_list, 'ia':[method]}
MultiLCA = bw.MultiLCA('primary_alu_ingot_all_ipcc')
pd.DataFrame(index=[list(fu.keys())[0]['location'] for fu in fu_list], columns=[method], data=MultiLCA.results)

'''
Fourth Step: Create consumption activity of primary aluminium ingot
'''

##Create primary aluminum consumption activity
#1) Create the new activity
act_name = 'market for consumption of aluminium'
search_list = [act for act in bw_db if act_name in act['name']]
if len(search_list)==0:
    new_act = bw_db.new_activity(act_name)
    new_act['name'] = act_name
    new_act['code'] = act_name
    new_act['location'] = 'GLO'
    new_act['reference product'] = 'aluminium, primary, ingot'
    new_act['type'] = 'process'
    new_act['unit'] = 'kilogram'
    new_act['production amount'] = 1
    new_act.save()
    alu_cons_act = new_act
else:
    alu_cons_act = search_list[0]
    
#2) Link the primary aluminium ingot production activities to the market for consumption. Only country are considered, not the IAI reg (except CN))

country_correspondence = pd.read_csv('inputs/country_correspondence.csv').fillna(0)
list_ei_country = country_correspondence.ecoinvent_country.unique().tolist()
list_ei_country.append('GLO') #We add GLO in the list
act_name = 'aluminium production, primary, ingot'
act_list = [act for act in bw_db if act_name in act['name'] and act['location'] in list_ei_country]
for act in act_list:
    exc_input_list = [exc.input for exc in alu_cons_act.technosphere()] 
    if act not in exc_input_list:
        new_exc = alu_cons_act.new_exchange(amount=1/len(act_list),input=act,type="technosphere")
        new_exc.save()
        alu_cons_act.save()
    else:
        print('Aluminium ingot production inputs from '+act['location']+' already exists')
#Check
list(alu_cons_act.technosphere())
    
'''
Tag some activities to for the spatial analysis
'''
#Tag aluminium consumption (highest level activity)
alu_cons_act=bw_db.get('market for consumption of aluminium')
alu_cons_act['tag'] = 'Aluminium consumption'
alu_cons_act['location_tag'] = 'GLO'
alu_cons_act.save()

#Tag aluminium ingot production
aluminum_ingot_act_list = [act for act in bw_db if 'aluminium production, primary, ingot' in act['name'] and 'kilogram' in act['unit']]
for act in aluminum_ingot_act_list:
    act['location_tag'] = act['location']
    act['tag'] = 'Primary Aluminium Ingot production'
    act.save()

#Tag aluminium liquid production    
act_name = 'aluminium production, primary, liquid'
act_list = [act for act in bw_db if act_name in act['name'] and 'kilogram' in act['unit']]
for act in act_list:
    act['location_tag'] = act['location']
    act['tag'] = 'Primary Aluminium Liquid production'
    act.save()
    
#Tag aluminium liquid electricity market    
act_name = 'market for electricity, medium voltage, aluminium industry'
act_list = [act for act in bw_db if act_name in act['name']]
for act in act_list:
    act['location_tag'] = None
    act['tag'] = 'Primary Aluminium Liquid production'
    act.save()
    
#Tag aluminium liquid electricity market   
act_name='electricity voltage transformation from high to medium voltage, aluminium industry'
act_list = [act for act in bw_db if act_name in act['name']]
for act in act_list:
    act['location_tag'] = None
    act['tag'] = 'Primary Aluminium Liquid production'
    act.save()
    
#Tag alumina consumption
alumina_act_list=[act for act in bw_db if "market for consumption of alumina" in act['name'] and 'kilogram' in act['unit']]
for act in alumina_act_list:
    act['location_tag'] = act['location']
    act['tag'] = 'Alumina consumption'
    act.save()
    
#Tag alumina production
alumina_act_list=[act for act in bw_db if "alumina production" in act['name'] and 'kilogram' in act['unit']]
for act in alumina_act_list:
    act['location_tag'] = act['location']
    act['tag'] = 'Alumina production'
    act.save()

#Tag bauxite consumption
act_list=[act for act in bw_db if "market for consumption of bauxite" in act['name'] and 'kilogram' in act['unit']]
for act in act_list:
    act['location_tag'] = act['location']
    act['tag'] = 'Bauxite consumption'
    act.save()

#Tag bauxite production
act_list=[act for act in bw_db if "bauxite mine operation" in act['name'] and 'kilogram' in act['unit']]
for act in act_list:
    act['location_tag'] = act['location']
    act['tag'] = 'Bauxite production'
    act.save()