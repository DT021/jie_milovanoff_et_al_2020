# Repository Introduction
This repository contains the input data, codes and results of the model developed in the paper "**Quantifying environmental impacts of primary aluminum ingot production and consumption: A trade-linked multilevel life cycle assessment**" published in the Journal of Industrial Ecology (2020) by Alexandre Milovanoff<sup>a</sup>, I. Daniel Posen<sup>a</sup>, Heather L. MacLean<sup>a,c</sup>.  
<sup>a</sup> Department of Civil & Mineral Engineering, University of Toronto, 35 St. George Street, Toronto, Ontario, M5S 1A4 Canada  

# How to use this repository
This repository will not be updated but can be cloned and serve as an example to develop trade-linked multilevel life cycle assessment. There are two ways to use this repository:
* Instal the model, reproduce it and re-use it for your own research.
* Extract the numerical values of simulations: (https://github.com/amilovanoff/jie_milovanoff_et_al_2020/outputs).  

Feel free me to contact if you have any questions via email (alexandre.milovanoff@mail.utoronto.ca) or by GitHub @amilovanoff.  

# Repository description
The repository comprises 3 folders.
* inputs: Contains the external and internal inputs to run the model.
* source: Contains the python script to develop the model and run the simulations.
* outputs: Contains the .csv files of the results presented in the manuscript and SI.

# How to set up the model
The python-based model requires the Brightway2 software (https://brightway.dev/), and the ecoinvent 3.4 cutoff database (https://www.ecoinvent.org/) uploaded in the Brightway2 environment.
* Run the 1-model_set_up.py script. It will adjust the local database to the model.
* Run the 2-simulations.py script. It will simulate the different results of the manuscript.
The other scripts contain functions developed for the purpose of the model.

# Results description
The [outputs](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/outputs) folder contains all simulated results that can be directly used for further research. The files are:
* country_prod_cons_quantity.csv: Country-level production and consumption quantity of bauxite, alumina and aluminum.
* sankey_diagram_data_mfa.csv: Results of the Trade-Linked Material flow analysis from bauxite mining to primary aluminum ingot consumption in 2000 and 2017. Used in **Figure 2 of the manuscript**.
* if_alumina_aluminium.csv: Country-level impact factors of alumina refining and primary aluminum ingot production from 2000 to 2017. Used in **Figures 3, 4 and 5 of the manuscript**, and **Figures S1.8, S1.9 and S1.10 of the SI**.
* if_glo_aluminium.csv: Global average impact factors of primary aluminum ingot. Used in **Figure 3 of the manuscript** and **Figure S1.9 of the SI**.
* country_domestic_production_ratio.csv: Country-level domestic production ratio for producing countries of alumina and aluminum. Used in **Figure 4 of the manuscript**.
* if_with_errors_aluminium.csv: Country-level impact factors of primary aluminum ingot production in case of aggregated globalized alumina processes (Type=globalzied_for_alumina) and in case of localized alumina processes (Type=localized_for_alumina). Used in **Figure 5 of the manuscript** and **Figure S1.8 of the SI**.
* sensitivity_trade_data_if_alu_cons.csv: Results of sensitivity analyses of bilateral trade data on impact factors of primary aluminum ingot. Used in **Figure S1.6 of the SI**.
* sensitivity_cons_mdl_if_alu_cons.csv: Results of sensitivity analyses of consumption matrix calculations on impact factors of primary aluminum ingot. Used in **Figure S1.7 of the SI**.
* spatial_if_aluminum.csv: Spatial distribution of the impact factors of primary aluminum ingot production.
* global_aluminium_prod_stage.csv: LC impacts of global primary aluminum ingot production by stage. Used in **Figure 7 of the manuscript**.
* global_aluminium_prod_stage_spatial.csv: Spatial distribution of LC impacts of global primary aluminum ingot production. Used in **Figure 7 of the manuscript**.
* country_impacts_aluminium.csv: Country-level production-based and consumption-based impacts. Used in **Figure 8 of the manuscript**.
