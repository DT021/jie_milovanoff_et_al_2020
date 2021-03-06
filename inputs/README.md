# Inputs description
This folder contains the inputs necessary to generate the simulation results divided in two categories: the internal inputs (generated by the model itself) and the exogenous inputs. 

## Internal inputs
The internal inputs are the bilateral trade data derived from the [UN Comtrade database](https://comtrade.un.org/) using the functions get_commodity_data, get_mat_trad_raw and get_mat_trad_reconciliation of the [utils_mfa](https://github.com/amilovanoff/jie_milovanoff_et_al_2020/blob/master/source/utils_mfa.py) script.

## Exogenous inputs
### Production data
* bauxite_production_data.csv: Country-level bauxite production from [USGS](https://www.usgs.gov/centers/nmic/aluminum-statistics-and-information).
* alumina_production_data.csv: Country-level alumina production from [USGS](https://www.usgs.gov/centers/nmic/aluminum-statistics-and-information).
* aluminium_production_data.csv: Country-level primary aluminum ingot production from [USGS](https://www.usgs.gov/centers/nmic/aluminum-statistics-and-information).

### Alumina-related data
* alumina_energy_consumption.csv: Metallurgical alumina refining energy intensity downloaded from [World Aluminium](http://www.world-aluminium.org/statistics/metallurgical-alumina-refining-energy-intensity/).
* alumina_energy_mix.csv: Metallurgical alumina refining fuel consumption downloaded from [World Aluminium](http://www.world-aluminium.org/statistics/metallurgical-alumina-refining-fuel-consumption/).

### Aluminum smelting-related data
* elec_mix_iai_2000-2017.csv: Primary aluminum smelting power consumption from [World Aluminium](http://www.world-aluminium.org/statistics/primary-aluminium-smelting-power-consumption/).
* electricity_input_iai.csv: Primary aluminum smelting energy intensity from [World Aluminium](http://www.world-aluminium.org/statistics/primary-aluminium-smelting-energy-intensity/).

### Modelling-related inputs
* Comtrade Country Code and ISO list.xlsx and UN Comtrade Commodity Classifications.xlsx: Country and commodity information in the [UN Comtrade database](https://comtrade.un.org/).
* country_correspondence.csv: List of countries, and correspondence with regions and World Aluminium regions.
* eiv3.4_geographies_names_coordinates_shortcuts_overlaps20180822.xlsx: Geography information in the [ecoinvent v3.4 database](https://www.ecoinvent.org/support/documents-and-files/information-on-ecoinvent-3/information-on-ecoinvent-3.html).
* list_region_iai.csv: List of regions between World Aluminium and ecoinvent.
* energy_alumina_act.csv: List of matching activities with energy inputs in alumina refining processes.
* conv_elec_techno_iai.csv: List of matching activities with energy inputs in aluminium smelting processes.