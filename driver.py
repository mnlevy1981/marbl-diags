#! /usr/bin/env python
""" docstring"""

import logging
import yaml
from marbl_diags import analysis_class

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
    parser.add_argument('-d', '--debug', action='store_true', dest='debug', required=False,
                        help='Write additional messages to stdout')

    return parser.parse_args()

#######################################

if __name__ == "__main__":
    args = _parse_args()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(format='%(levelname)s (%(funcName)s): %(message)s', level=log_level)

    with open(args.input_file) as file_in:
        full_input = yaml.load(file_in)
    # Check for correct keys
    err_found = False
    for key in ['global_config', 'data_sources', 'variable_definitions', 'analysis']:
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
    with open(full_input['variable_definitions']) as file_in:
        var_dict = yaml.load(file_in)

    AnalysisCategories = dict()
    for category_name, analysis_dict in full_input['analysis'].items():
        AnalysisCategories[category_name] = \
            analysis_class.AnalysisCategory(category_name, analysis_dict, ds_dict,
                                            var_dict, full_input['global_config'])
    for AnalysisCategory in AnalysisCategories.values():
        AnalysisCategory.do_analysis()
