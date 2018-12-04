#! /usr/bin/env python
""" docstring"""

import logging
import yaml
from marbl_diags import analysis_elements_class

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s (%(funcName)s): %(message)s', level=logging.DEBUG)
    with open('input.yml') as file_in:
        config_dicts = yaml.load(file_in)
    with open('variables.yml') as file_in:
        var_dict = yaml.load(file_in)

    AnalysisElements = dict()
    for config_key, config_dict in config_dicts.items():
        AnalysisElements[config_key] = analysis_elements_class.AnalysisElements(config_key, config_dict, var_dict) # pylint: disable=invalid-name
    for AnalysisElement in AnalysisElements.values():
        AnalysisElement.do_analysis()
