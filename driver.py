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

    return parser.parse_args()

#######################################

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s (%(funcName)s): %(message)s', level=logging.DEBUG)
    args = _parse_args()

    with open(args.input_file) as file_in:
        full_input = yaml.load(file_in)
    # Check for correct keys
    err_found = False
    for key in ['config', 'data_sources', 'variables', 'analysis']:
        if key not in full_input:
            err_found = True
            print("ERROR: can not find {} key in {}".format(key, args.input_file))
    if err_found:
        raise KeyError("One or more missing keys in {}".format(args.input_file))

    # Create dictionary for data sources
    ds_dict = dict()
    for ds_file in full_input['data_sources']:
        with open(ds_file) as file_in:
            ds_dict_in = yaml.load(file_in)
            for ds_name in full_input['data_sources'][ds_file]:
                if ds_name not in ds_dict_in:
                    raise KeyError("Can not find {} in {}".format(ds_name, ds_file))
                if ds_name in ds_dict:
                    raise KeyError("Data source named {} has already been processed".format(ds_name))
                ds_dict[ds_name] = dict(ds_dict_in[ds_name])
            del(ds_dict_in)

    # Create dictionary of variables from requested files
    var_dict = dict()
    for var_file in full_input['variables']:
        with open(var_file) as file_in:
            var_dict_in = yaml.load(file_in)
            for var_name in full_input['variables'][var_file]:
                if var_name not in var_dict_in:
                    raise KeyError("Can not find {} in {}".format(var_name, var_file))
                if var_name in var_dict:
                    raise KeyError("Variable named {} has already been processed".format(var_name))
                var_dict[var_name] = dict(var_dict_in[var_name])
            del(var_dict_in)

    AnalysisElements = dict()
    for analysis_sname, analysis_dict in full_input['analysis'].items():
        AnalysisElements[analysis_sname] = \
            analysis_elements_class.AnalysisElements(analysis_sname, analysis_dict, ds_dict,
                                                     var_dict, full_input['config'])
    for AnalysisElement in AnalysisElements.values():
        AnalysisElement.do_analysis()
