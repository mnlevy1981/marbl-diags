#! /usr/bin/env python
""" docstring"""

import logging
import yaml
from marbl_diags import analysis_elements_class

#######################################

def _parse_args():
    """ Parse command line arguments
    """

    import argparse

    parser = argparse.ArgumentParser(description="Generate plots based on MARBL diagnostic output",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Input file
    parser.add_argument('-i', '--input_file', action='store', dest='input_file', required=True,
                        help='YAML file defining analysis element(s) and data sources')

    # Variable definitions
    parser.add_argument('-v', '--variable_file', action='store', dest='variable_file',
                        default='./variables.yml',
                        help="YAML file defining variables and plot settings")

    return parser.parse_args()

#######################################

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s (%(funcName)s): %(message)s', level=logging.DEBUG)
    args = _parse_args()

    with open(args.input_file) as file_in:
        config_dicts = yaml.load(file_in)
    with open(args.variable_file) as file_in:
        var_dict = yaml.load(file_in)

    AnalysisElements = dict()
    for config_key, config_dict in config_dicts.items():
        AnalysisElements[config_key] = analysis_elements_class.AnalysisElements(config_key, config_dict, var_dict) # pylint: disable=invalid-name
    for AnalysisElement in AnalysisElements.values():
        AnalysisElement.do_analysis()
