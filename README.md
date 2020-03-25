This repository contains the input data, codes and results of the model developed in the paper "**Quantifying environmental impacts of primary aluminum ingot production and consumption: A trade-linked multilevel life cycle assessment**" published in the Journal of Industrial Ecology (2020) by Alexandre Milovanoff<sup>a</sup>, I. Daniel Posen<sup>a</sup>, Heather L. MacLean<sup>a</sup>.  
<sup>a</sup> Department of Civil & Mineral Engineering, University of Toronto, 35 St. George Street, Toronto, Ontario, M5S 1A4 Canada  

# How to use this repository
This repository will not be updated but can be cloned and serve as an example to develop trade-linked multilevel life cycle assessment. There are two ways to use this repository:
* Install the model, reproduce it and re-use it for your own research. Note: After downloading the repository, change the working directory of the 1-model_set_up.py and 2-simulations.py scripts to the repository directory (e.g., C:/Users/Alexandre/GitHub/jie_milovanoff_et_al_2020).
* Download the repository and extract the numerical values of the simulations presented in [outputs](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/tree/master/outputs) folder.

Feel free me to contact if you have any questions via email (alexandre.milovanoff@mail.utoronto.ca) or by GitHub @amilovanoff.  

# Repository description
The repository comprises 3 folders.
* [inputs](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/tree/master/inputs): Contains the external and internal inputs to run the model.
* [source](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/tree/master/source): Contains the python script to develop the model and run the simulations.
* [outputs](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/tree/master/outputs): Contains the .csv files of the results presented in the manuscript and SI.

# How to set up the model
The python-based model requires the Brightway2 software (https://brightway.dev/), and the ecoinvent 3.4 cutoff database (https://www.ecoinvent.org/) uploaded in the Brightway2 environment.
* Run the [1-model_set_up.py](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/blob/master/source/1-model_set_up.py) script. It will adjust the local database to the model.
* Run the [2-simulations.py](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/blob/master/source/2-simulations.py) script. It will simulate the different results of the manuscript.

The other scripts contain functions developed for the purpose of the model.
