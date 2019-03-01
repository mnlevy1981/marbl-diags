""" Classes that may eventually be moved to ESMlab? """

import logging
import os
import json
from subprocess import call
from datetime import datetime
import esmlab

######################################################################

class GenericDataSource(object): # pylint: disable=useless-object-inheritance
    """ Class containing functions used regardless of data source """
    def __init__(self, child_class=None, **kwargs):
        if child_class:
            self.logger = logging.getLogger(child_class)
        self._files = None
        self.source = kwargs['source']
        self.ds = None # pylint: disable=invalid-name
        self._var_dict = None
        self._set_var_dict()

    ###################
    # PUBLIC ROUTINES #
    ###################

    def compute_mon_climatology(self):
        """ Compute a monthly climatology """

        ds = esmlab.climatology.compute_mon_climatology(self.ds)
        self.ds = ds

    def cache_dataset(self, cached_location, cached_var_dict):
        """
        Function to write output:
           - optionally add some file-level attrs
           - switch method based on file extension
        """

        diro = os.path.dirname(cached_var_dict)
        if not os.path.exists(diro):
            self.logger.info('creating %s', diro)
            call(['mkdir', '-p', diro])

        if os.path.exists(cached_var_dict):
            call(['rm', '-f', cached_var_dict])

        # Write json dictionary for var_dict
        with open(cached_var_dict, "w") as file_out:
            json.dump(self._var_dict, file_out, separators=(',', ': '), sort_keys=True, indent=3)

        diro = os.path.dirname(cached_location)
        if not os.path.exists(diro):
            self.logger.info('creating %s', diro)
            call(['mkdir', '-p', diro])

        if os.path.exists(cached_location):
            call(['rm', '-fr', cached_location])

        dsattrs = {
            'history': 'created by {} on {}'.format(os.environ['USER'],
                                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            }

        # if add_attrs:
        #     dsattrs.update(dsattrs)
        self.ds.attrs.update(dsattrs)

        ext = os.path.splitext(cached_location)[1]
        if ext == '.nc':
            self.logger.info('writing %s', cached_location)
            self.ds.to_netcdf(cached_location, compute=True)

        elif ext == '.zarr':
            self.logger.info('writing %s', cached_location)
            self.ds.to_zarr(cached_location, compute=True)

        else:
            raise ValueError('Unknown output file extension: {ext}')

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _time_bound_var(self):
        """ Determine time bound var name and dimension """
        tb_name = ''
        if 'bounds' in self.ds['time'].attrs:
            tb_name = self.ds['time'].attrs['bounds']
        elif 'time_bound' in self.ds:
            tb_name = 'time_bound'
        else:
            raise ValueError('No time_bound variable found')
        tb_dim = self.ds[tb_name].dims[1]
        return tb_name, tb_dim

    def _set_var_dict(self):
        """
        Each class derived from GenericDataSource needs to map from generic variable names to
        model-specific variable names.
        """
        raise NotImplementedError('_set_var_dict needs to be defined in child classes')

######################################################################

class GenericAnalysisElement(object):
    """
    Objects in this class
        * datasets: datasets[ds_sname] is a specific dataset to analyze
                    E.g. datasets['WOA2013'] is the World Ocean Atlas reanalysis
        * variables: variables[var_name] is a list of alternative names for the variable
                     E.g. variables['nitrate'] = ['NO3', 'n_an']
    """
    def __init__(self, analysis_sname, analysis_dict, var_dict, config):
        """ construct class object
            * analysis_sname is unique identifier
            * analysis_dict must contain datestrs
              - can also override values from argument passed in to config (e.g. levels)
            * var_dict defines all variables
            * config is full list of element configuration
        """

        # Define logger on type and save analysis short name
        self.logger = logging.getLogger(analysis_sname)
        self.analysis_sname = analysis_sname

        # (1) Error check: analysis_dict keys are either "datestrs" or already in config
        #                  ("datestrs" required)
        if 'datestrs' not in analysis_dict:
            raise KeyError("'{}' must contain 'datestrs'".format(analysis_sname))
        for config_key in analysis_dict:
            if config_key not in config and config_key != 'datestrs':
                raise KeyError("'{}' is not a valid key in '{}'".format(config_key, analysis_sname))

        # (2) Define datestrs and _global_config
        self.datestrs = analysis_dict['datestrs']
        #     - Force datestrs[data_source] to be a list
        for data_source in self.datestrs:
            if not isinstance(self.datestrs[data_source], list):
                self.datestrs[data_source] = [self.datestrs[data_source]]
        self._global_config = dict()
        for key in config:
            if key in analysis_dict:
                self._global_config[key] = analysis_dict[key]
            else:
                self._global_config[key] = config[key]

        # (3) Define _var_dict
        self._var_dict = var_dict

        # (4) Set up objects for plotting
        self.fig = dict()
        self.axs = dict()
