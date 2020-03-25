# -*- coding: utf-8 -*-
"""
Additional functions based on Brightway

@author: Alexandre
"""

import brightway2 as bw
from bw2data import get_activity
from bw2data import Method
from bw2calc import LCA
import pandas as pd
from collections import defaultdict
import numpy as np
   
def redo_lca_score(lca,new_fu):
        lca.redo_lcia(new_fu)
        return lca.score

#Copy from bw package. Adapted to make it work.
def recurse_tagged_database(activity, amount, method_dict, lca, label, default_tag, secondary_tag=(None,None),product_system_depth=5):
    '''Traverse a foreground database and assess activities and biosphere flows by tags.

    Input arguments:

        * ``activity``: Activity tuple or object. Key
        * ``amount``: float associated with the functional unit.
        * ``method``: Method 
        * ``lca``: An ``LCA`` object that is already initialized, i.e. has already calculated LCI and LCIA with same method as in ``method_dict``
        * ``label``: string
        * ``default_tag``: string
        * ``secondary_tag``: Tuple in the format (secondary_label, secondary_default_tag). Default is empty tuple.

    Returns:

    .. code-block:: python

        {
            'activity': activity object,
            'amount': float,
            'tag': string,
            'secondary_tags': string,
            'impact': float (impact of inputs from outside foreground database),
            'biosphere': [{
                'amount': float,
                'impact': float,
                'tag': string,
                'secondary_tag': string,
            }],
            'technosphere': [this data structure]
        }

    '''
    if isinstance(activity, tuple):
        activity = get_activity(activity)
    inputs = list(activity.technosphere())
    #Check if the depth of the product system is attained. Or check if the activity does not have label. If no label, then we stop the recursive calculations.
    if(product_system_depth > 0 and activity.get(label)!=None):
        return {
            'activity': activity,
            'amount': amount,
            'tag': activity.get(label) or default_tag,
            'secondary_tag':activity.get(secondary_tag[0]) or secondary_tag[1],
            'impact': 0,
            'biosphere': [{
                'amount': exc['amount'] * amount,
                'impact': exc['amount'] * amount * method_dict.get(exc['input'], 0),
                'tag': exc.get(label) or activity.get(label) or default_tag,
                'secondary_tag':exc.get(secondary_tag[0]) or activity.get(secondary_tag[0]) or secondary_tag[1]
            } for exc in activity.biosphere()],
            'technosphere': [recurse_tagged_database(exc.input, exc['amount'] * amount,
                                                     method_dict, lca, label,exc.get(label) or activity.get(label) or default_tag, (secondary_tag[0],exc.get(secondary_tag[0]) or activity.get(secondary_tag[0]) or secondary_tag[1]),product_system_depth=product_system_depth-1)
                             for exc in inputs]
        }
    else:
        return {
            'activity': activity,
            'amount': amount,
            'tag': activity.get(label) or default_tag,
            'secondary_tag':activity.get(secondary_tag[0]) or secondary_tag[1],
            'impact': redo_lca_score(lca,{activity:amount}),
            'biosphere': [],
            'technosphere': []
        }

def aggregate_tagged_graph(graph):
    """Aggregate a graph produced by ``recurse_tagged_database`` by the provided tags.

    Outputs a dictionary with keys of tags and numeric values.

    .. code-block:: python

        {'a tag': summed LCIA scores}

    """
    def recursor(obj, scores):
        if obj['secondary_tag']!=None:
            if type(scores[obj['tag']]) is int:
                scores[obj['tag']] = defaultdict(int)
            scores[obj['tag']][obj['secondary_tag']] += obj['impact']
            for flow in obj['biosphere']:
                if type(scores[flow['tag']]) is int:
                    scores[flow['tag']] = defaultdict(int)
                scores[flow['tag']][flow['secondary_tag']] += flow['impact']
        else:
            scores[obj['tag']] += obj['impact']
            for flow in obj['biosphere']:
                scores[flow['tag']] += flow['impact']             
        for exc in obj['technosphere']:
            scores = recursor(exc, scores)
        return scores

    scores = defaultdict(int)
    for obj in graph:
        scores = recursor(obj, scores)
    return scores

def traverse_tagged_databases_to_dataframe(functional_unit, method, label="tag",default_tag="other", secondary_tag=(None,None),product_system_depth=5):
    """Traverse a functional unit throughout its foreground database(s), and
    group impacts by tag label.
    
    Input arguments:
        * ``functional_unit``: A functional unit dictionary, e.g. ``{("foo", "bar"): 42}``.
        * ``method``: A method name, e.g. ``("foo", "bar")``
        * ``label``: The label of the tag classifier. Default is ``"tag"``
        * ``default_tag``: The tag classifier to use if none was given. Default is ``"other"``
        * ``secondary_tags``: List of tuples in the format (secondary_label, secondary_default_tag). Default is empty list.

    Returns:

        Aggregated tags dictionary from ``aggregate_tagged_graph``, and tagged supply chain graph from ``recurse_tagged_database``.

    """    
    lca = LCA(functional_unit, method)
    lca.lci(factorize=True)
    lca.lcia()
    method_dict = {o[0]: o[1] for o in Method(method).load()}
    graph = [recurse_tagged_database(key, amount, method_dict, lca, label, default_tag, secondary_tag, product_system_depth)
             for key, amount in functional_unit.items()]
    agg_graph = aggregate_tagged_graph(graph)
    if secondary_tag==(None,None):
        dtf = pd.Series(agg_graph,name='Score')
        dtf.index.name = label
        dtf = dtf.reset_index()
    else:
        dtf = pd.DataFrame(agg_graph)
        dtf[secondary_tag[0]] = dtf.index
        dtf = dtf.reset_index(drop=True)
        dtf = dtf.melt(id_vars=[secondary_tag[0]],value_vars=[key for key in agg_graph.keys()])
        dtf = dtf.rename({"variable":label,'value':'Score'},axis="columns")
        dtf = dtf.dropna()
        
    redo_lca_score(lca,functional_unit)
    dtf['Rel_Score'] = [imp/lca.score for imp in dtf.Score]
    return dtf

def get_multilca_to_dataframe(MultiLCA):
    '''
    Return a long dataframe with the LCA scores of the multi LCA.
    
    Input arguments:
        *``MultiLCA``: a MultiLCA object already calculated
    
    Returns:
        *Return a long dataframe. Columns: ('Database', 'Code', 'Name', 'Location', 'Unit', 'Amount_fu','Method_name','Midpoint','Midpoint_abb','Score')
    '''
    as_activities = [
        (bw.get_activity(key), amount) 
        for dct in MultiLCA.func_units 
        for key, amount in dct.items()
    ]
    scores = pd.DataFrame(data=MultiLCA.results, columns=[method[1] for method in MultiLCA.methods],index=[act[0]['code'] for act in as_activities])
    nicer_fu = pd.DataFrame(
        [
            (x['database'], x['code'], x['name'], x['location'], x['unit'], y, method[0], method[1], method[2],bw.Method(method).metadata['unit'], scores.loc[x['code'],method[1]]) 
            for x, y in as_activities
            for method in MultiLCA.methods  
        ], 
        columns=('Database', 'Code', 'Name', 'Location', 'Unit', 'Amount_fu','Method_name','Midpoint','Midpoint_abb','Midpoint_unit','Score')
    )
    return nicer_fu
    
def get_activity_to_dataframe(act):
    '''
    Return a long dataframe with an activity.
    
    Input arguments:
        *``MultiLCA``: a MultiLCA object already calculated
    
    Returns:
        *Return a long dataframe. Columns: ('Database', 'Code', 'Name', 'Location', 'Unit', 'Amount_fu','Method_name','Midpoint','Midpoint_abb','Score')
    '''
    #
    techno_dtf = pd.DataFrame(
            [
                (exc.input['name'], exc.input['location'], exc.amount, exc.unit)
                for exc in act.technosphere()
            ],
            columns=('Input_name', 'Input_location', 'Input_amount', 'Input_unit')
        )
    #
    bio_dtf = pd.DataFrame(
            [
                
                (exc.input['name'], exc.amount, exc.unit)
                for exc in act.biosphere()
            ],
            columns=('Input_name', 'Input_amount', 'Input_unit')
        )
    bio_dtf['Input_location'] = "biosphere"
    #
    dtf = techno_dtf.append(bio_dtf,ignore_index=True,sort=True)
    dtf['Activity'] = act['name']+' '+act['location']
    return dtf

def get_median_act(bw_db_name,name_to_include,name_to_exclude,unit,ia_method,location_keywords=None):
    '''
    Function that return the median case of LCA scores of activiies specified by keywords
    Attributes:
        bw_db_name is the name of the Brightway database to update.
        name_to_include is a list of strings that should be individually contained in the activity name
        name_to_exclude is a list of strings that should not be individually contained in the activity name
        location_keywords is a list of strings that should be individually contained in the activity name
            None specified no location
        unit is the string of activity unit.
        ia_method is a bw Impact Assessment method        
    '''    
    bw_db = bw.Database(bw_db_name)
    #Get the list of all potential activities
    if (location_keywords==None):
        act_list = [act for act in bw_db if all(keyword in act['name'] for keyword in name_to_include) and all(keyword not in act['name'] for keyword in name_to_exclude) and unit in act['unit']]
    else:
        act_list = [act for act in bw_db if all(keyword in act['name'] for keyword in name_to_include) and all(keyword not in act['name'] for keyword in name_to_exclude) and any(keyword==act['location'] for keyword in location_keywords) and unit in act['unit']]
    #Check the number of activities
    if len(act_list)==0:
        raise KeyError("No activities found")
    else:
        dtf = pd.DataFrame()
        for act in act_list:
            try:
                lca_calc = bw.LCA({act:1},ia_method)
            except:
                raise StopIteration('LCA calculations do not work')
            lca_calc.lci()
            lca_calc.lcia()
            new_row= pd.DataFrame({'Activity':act['name'],'Activity Location':act['location'],'Score':lca_calc.score,'Code':act['code']},index=[0])
            dtf = dtf.append(new_row,ignore_index=True)
        dtf.sort_values(by='Score',inplace=True)
        #Get the index of the median. If odd number of activities, exact one. Otherwise, the closest lower score   
        return {'Median':dtf[dtf['Score'] <= dtf['Score'].median()].iloc[-1].Code}
   
def create_act_new_location(bw_db_name,act_name,new_location,cut_off_occurence=0.9,copy_multiple_same_inputs='n',keep_location_multiple_inputs='n',copy_from_glo='n'):
    '''
    Function that creates an activity from an existing one with new location, and update exchanges
    '''   
    bw_db = bw.Database(bw_db_name)
    act_list=[act for act in bw_db if act_name == act['name']]
    #Create activity if not existing
    if new_location not in [act['location'] for act in act_list]:
         #Create the new activity
        new_act=bw_db.new_activity(act_name)
        new_act['name']=act_name
        new_act['code']=act_name+new_location
        new_act['location']=new_location
        new_act['reference product']=act_list[0]['reference product']
        new_act['type']='process'
        new_act['unit']=act_list[0]['unit']
        new_act['production amount']=act_list[0]['production amount']
        new_act.save()
    else:
        new_act = [act for act in act_list if new_location in act['location']][0]
    #For the rest, only consider the other activities except RoW
    if copy_from_glo=='y':
        act_list=[act for act in bw_db if act_name == act['name'] and 'GLO' in act['location']]
    elif copy_from_glo=='n':
        act_list=[act for act in bw_db if act_name == act['name'] and new_location not in act['location'] and 'RoW' not in act['location']]
    #Create a data frame with all exchanges by activity (only technosphere) for already existing activities
    techno_exc_dt=pd.DataFrame()
    for act in act_list:
        exc_list = [exc for exc in act.technosphere()]
        for exc in exc_list:
            new_row= pd.DataFrame({'Activity':act['name'],'Activity Location':act['location'],'Input':exc.input['name'],'Input Location':exc.input['location'],'Input Amount':exc.amount,'Input Unit':exc.input['unit']},index=[0])
            techno_exc_dt = techno_exc_dt.append(new_row,ignore_index=True)        
    #Create the list of technosphere inputs for the new activity and see for each input if it is present in most of the existing activities.
    input_name_list = techno_exc_dt['Input'].unique().tolist()
    for input_name in input_name_list:
        #Get the list of exchanges already in the new activity
        new_act_exc_list = [exc for exc in new_act.technosphere()]
        #occurentce_nb is the number of activities that have the input
        occurence_nb = sum([input_name in techno_exc_dt.loc[techno_exc_dt['Activity Location']==loc,'Input'].tolist() for loc in techno_exc_dt['Activity Location'].unique().tolist()])
        #tot_occurence_nbis the total number of occurence of the input
        tot_occurence_nb = sum(techno_exc_dt['Input']==input_name)
        if occurence_nb >= cut_off_occurence*len(act_list) and tot_occurence_nb <= len(act_list) and input_name not in [exc.input['name'] for exc in new_act_exc_list]:
            #Amount to use. consider uncertainty in the exchange
            ##Assumption: We only consider uniform uncertainties in the exchange amount from min to max of distribution.
            input_amount = np.mean(techno_exc_dt.loc[techno_exc_dt.Input==input_name,'Input Amount'])
            input_min = np.min(techno_exc_dt.loc[techno_exc_dt.Input==input_name,'Input Amount'])
            inpu_max = np.max(techno_exc_dt.loc[techno_exc_dt.Input==input_name,'Input Amount'])
            #Activity to use
            #input_loc_list is the list of locations for the inputs
            input_loc_list = techno_exc_dt.loc[techno_exc_dt.Input==input_name,'Input Location'].unique().tolist()
            #act_location_list is the list of locations for the activities
            act_location_list = techno_exc_dt.loc[techno_exc_dt.Input==input_name,'Activity Location'].unique().tolist()
            #Check if single location
            if len(input_loc_list)==1:
                input_act = [act for act in bw_db if input_name == act['name'] and input_loc_list[0] in act['location']][0]
                new_exc = new_act.new_exchange(type='technosphere',input=input_act, amount=input_amount)
                new_exc['uncertainty type']=4
                new_exc['minimum']=input_min
                new_exc['maximum']=inpu_max
                new_exc.save()
            #Check if all locations of the inputs are also in the activity locations list. In that case, it is an exchange with a location-specific activity
            elif all(elem in act_location_list for elem in input_loc_list):
                input_act_list = [act for act in bw_db if input_name == act['name'] and new_location == act['location']]
                if len(input_act_list)==0:
                    print('Issue with exchange creation: '+input_name+' for '+new_location+' does not exist')
                elif len(input_act_list)==1:
                    input_act = input_act_list[0]
                    new_exc = new_act.new_exchange(type='technosphere',input=input_act, amount=input_amount)
                    new_exc['uncertainty type']=4
                    new_exc['minimum']=input_min
                    new_exc['maximum']=inpu_max
                    new_exc.save()
                else:
                    print('Issue with exchange creation: Many activities for '+input_name+' in '+new_location)
            else:
                #Impossible to determine a generic location. We are using the functions previously developped
                unit=techno_exc_dt.loc[techno_exc_dt.Input==input_name,'Input Unit'].unique().tolist()[0]
                ipcc2013 = ('IPCC 2013', 'climate change', 'GWP 100a')
                median_act_dic = get_median_act(bw_db_name=bw_db_name,name_to_include=[input_name],name_to_exclude=[],unit=unit,ia_method=ipcc2013,location_keywords=None)
                #Create exchange
                input_act = [act for act in bw_db if act['code']==median_act_dic['Median']][0]
                new_exc = new_act.new_exchange(type='technosphere',input=input_act, amount=input_amount)
                new_exc['uncertainty type']=4
                new_exc['minimum']=input_min
                new_exc['maximum']=inpu_max
                new_exc.save()
                print('Many locations for the input '+input_name)
        #If input does not appear in all activities.
        elif occurence_nb < cut_off_occurence*len(act_list) and tot_occurence_nb <= len(act_list):
            print(input_name+' appears in only '+str(occurence_nb)+' activities')
        #If input appears in many activities, multiple times with different locations
        elif occurence_nb >= cut_off_occurence*len(act_list) and tot_occurence_nb > len(act_list) and copy_multiple_same_inputs=='y':
            print(input_name+' appears '+str(tot_occurence_nb)+' times in '+str(occurence_nb)+' activities')
            location_list = techno_exc_dt.loc[techno_exc_dt['Input']==input_name,'Input Location'].unique().tolist()
            #If we keep all the inputs with the same locations
            if keep_location_multiple_inputs=='y':
                for location in location_list:
                    ##Assumption: We only consider uniform uncertainties in the exchange amount from min to max of distribution.
                    input_amount = np.mean(techno_exc_dt.loc[(techno_exc_dt['Input']==input_name) & (techno_exc_dt['Input Location']==location) ,'Input Amount'])
                    input_min = np.min(techno_exc_dt.loc[(techno_exc_dt['Input']==input_name) & (techno_exc_dt['Input Location']==location),'Input Amount'])
                    inpu_max = np.max(techno_exc_dt.loc[(techno_exc_dt['Input']==input_name) & (techno_exc_dt['Input Location']==location),'Input Amount'])
                    #Create exchange
                    input_act = [act for act in bw_db if input_name == act['name'] and location == act['location']][0]
                    new_exc = new_act.new_exchange(type='technosphere',input=input_act, amount=input_amount)
                    new_exc['uncertainty type']=4
                    new_exc['minimum']=input_min
                    new_exc['maximum']=inpu_max
                    new_exc.save()
            #If we aggregate the inputs into one location
            elif keep_location_multiple_inputs=='n':
                input_amount = 0
                input_min = 0
                inpu_max = 0
                for location in location_list:
                    ##Assumption: We only consider uniform uncertainties in the exchange amount from min to max of distribution.
                    input_amount = input_amount + np.mean(techno_exc_dt.loc[(techno_exc_dt['Input']==input_name) & (techno_exc_dt['Input Location']==location) ,'Input Amount'])
                    input_min = input_min + np.min(techno_exc_dt.loc[(techno_exc_dt['Input']==input_name) & (techno_exc_dt['Input Location']==location),'Input Amount'])
                    inpu_max = inpu_max + np.max(techno_exc_dt.loc[(techno_exc_dt['Input']==input_name) & (techno_exc_dt['Input Location']==location),'Input Amount'])
                #List of potential activities
                input_act_list = [act for act in bw_db if input_name == act['name']]
                available_loc_list = [act['location'] for act in input_act_list]
                if new_location in available_loc_list:
                    location = new_location
                elif 'GLO' in available_loc_list:
                    location = 'GLO'
                elif any('RoW' in loc for loc in available_loc_list):
                    location = [loc for loc in available_loc_list if 'RoW' in loc][0]
                else:
                    location = available_loc_list[0]
                #Create exchange
                input_act = [act for act in input_act_list if location == act['location']][0]
                new_exc = new_act.new_exchange(type='technosphere',input=input_act, amount=input_amount)
                new_exc['uncertainty type']=4
                new_exc['minimum']=input_min
                new_exc['maximum']=inpu_max
                new_exc.save()            
    #Create a data frame with biosphere exchanges by flows
    bio_exc_dt=pd.DataFrame()
    for act in act_list:
        exc_list = [exc for exc in act.biosphere()]
        for exc in exc_list:
            new_row= pd.DataFrame({'Activity':act['name'],'Activity Location':act['location'],'Input':exc.input['name'],'Input code':exc.input['code'],'Input Amount':exc.amount},index=[0])
            bio_exc_dt = bio_exc_dt.append(new_row,ignore_index=True)  
    if len(bio_exc_dt)!=0:
        bio_db = bw.Database('biosphere3')
        #Create the list of technosphere inputs for the new activity and see for each input if it is present in most of the existing activities.
        input_code_list = bio_exc_dt['Input code'].unique().tolist()
        for input_code in input_code_list:
            #Get the list of exchanges already in the new activity
            new_act_exc_list = [exc for exc in new_act.biosphere()]
            #occurentce_nb is the number of activities that have the input
            occurence_nb = sum([input_code in bio_exc_dt.loc[bio_exc_dt['Activity Location']==loc,'Input code'].tolist() for loc in bio_exc_dt['Activity Location'].unique().tolist()])
            #tot_occurence_nbis the total number of occurence of the input
            tot_occurence_nb = sum(bio_exc_dt['Input code']==input_code)
            if occurence_nb > cut_off_occurence*len(act_list) and input_code not in [exc.input['name'] for exc in new_act_exc_list]:
                #Amount to use. consider uncertainty in the exchange
                ##Assumption: We only consider uniform uncertainties in the exchange amount from min to max of distribution.
                input_amount = np.mean(bio_exc_dt.loc[bio_exc_dt['Input code']==input_code,'Input Amount'])
                input_min = np.min(bio_exc_dt.loc[bio_exc_dt['Input code']==input_code,'Input Amount'])
                inpu_max = np.max(bio_exc_dt.loc[bio_exc_dt['Input code']==input_code,'Input Amount'])
                #Activity to use
                input_act = [act for act in bio_db if input_code == act['code']][0]
                new_exc = new_act.new_exchange(type='biosphere',input=input_act, amount=input_amount)
                new_exc['uncertainty type']=4
                new_exc['minimum']=input_min
                new_exc['maximum']=inpu_max
                new_exc.save()  
    return

def duplicate_act_new_location(bw_db_name,act_key,new_location,prod_system_depth=3):
    '''
    Function that duplicates an activity from a given location to a new location by systematically changing the inputs to the new location until the provided depth of product system
        Necessary for regionalized LCA as the activities become highly regionalized
        However, requires a high depth of product system to regionalized all the impacts to the given new location (and assume that all the previous impacts are in the new location)

    Input arguments:
        *``act_key``: Activity tuple or object
    '''    
    bw_db = bw.Database(bw_db_name)
    activity = bw.get_activity(act_key)
    #Output
    duplicated_act = {}
    non_duplicated_act = {}
    if prod_system_depth < 1:
        #print('No duplicates for '+ activity['name']+' at location '+new_location)
        non_duplicated_act[activity['name']] = activity['location']
        return activity,duplicated_act,non_duplicated_act
    elif len([act for act in bw_db if act['name']==activity['name'] and act['location']==new_location])>0:
        print('Activity '+ activity['name']+' already exists in '+new_location)
        new_activity = [act for act in bw_db if act['name']==activity['name'] and act['location']==new_location][0]
    else:
        new_activity = activity.copy()
        new_activity['location'] = new_location
        new_activity.save()
        #add to list of duplicates
        duplicated_act[new_activity['name']] = new_location
    prod_system_depth = prod_system_depth - 1
    #Change the inputs of the exchanges
    inputs = list(new_activity.technosphere())
    input_name_list = list(set([exc.input['name'] for exc in inputs]))
    #Aggregate inputs by names. If it is a 'market for consumption', then update the market by the location but not inside the market (Assume market for consumption activity exist)
    for exc_name in input_name_list:
        temp_exc_list = [exc for exc in inputs if exc.input['name']==exc_name]
        #If only one exchange with such input name. Take this input.
        if len(temp_exc_list)==1:
            exc_input = temp_exc_list[0].input
        #If many exchanges with such input name. Aggregate them in one input
        else:
            all_exc_input_list = [act for act in bw_db if act['name']==exc_name]
            if new_location in [act['location'] for act in all_exc_input_list]:
                exc_input = [act for act in all_exc_input_list if act['location']==new_location][0]
            elif 'GLO' in [act['location'] for act in all_exc_input_list]:
                exc_input = [act for act in all_exc_input_list if act['location']=='GLO'][0]
            elif any('RoW' in act['location'] for act in all_exc_input_list):
                exc_input = [act for act in all_exc_input_list if 'RoW' in act['location']][0]
            else:
                exc_input = all_exc_input_list[0]
            #Get the sum of amounts of exchanges with the specific input name
            exc_amount = sum([exc.amount for exc in temp_exc_list])
            #Delete all exchanges associated with this input name
            for exc in temp_exc_list:
                exc.delete()
            new_activity.save()
            #Create new exchange with the input name
            new_activity.new_exchange(amount=exc_amount,input=exc_input,type="technosphere").save()
        #Consider the exc_to_update
        exc_to_update = [exc for exc in new_activity.technosphere() if exc.input['name']==exc_name][0]
        #Update the exc_input
        if exc_input['location']!=new_location:
            #Search if activity exist with new location
            search_list = [act for act in bw_db if act['name']==exc_input['name'] and act['location']==new_location]
            if len(search_list)>0:
                new_exc_input = search_list[0]
            #If not, create the new inputs with the new location (recursive function)
            elif len(search_list)==0:
                new_exc_input,temp_duplicated_act,temp_non_duplicated_act = duplicate_act_new_location(bw_db_name,exc_input.key,new_location,prod_system_depth)
                #add the duplicates activites
                duplicated_act = {**duplicated_act, **temp_duplicated_act}
                non_duplicated_act = {**non_duplicated_act, **temp_non_duplicated_act}
            exc_to_update['input'] = new_exc_input
            exc_to_update.save()
    return new_activity,duplicated_act,non_duplicated_act