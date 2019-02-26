""" Classes that may eventually be moved to ESMlab? """

import logging
import os
import json
from subprocess import call
from datetime import datetime
import esmlab

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

class GenericAnalysisElement(object):
    """
    Objects in this class
        * datasets: datasets[ds_sname] is a specific dataset to analyze
                    E.g. datasets['WOA2013'] is the World Ocean Atlas reanalysis
        * variables: variables[var_name] is a list of alternative names for the variable
                     E.g. variables['nitrate'] = ['NO3', 'n_an']
    """
    def __init__(self, analysis_sname, analysis_dict, ds_dict, var_dict, config_dict):
        """ construct class object based on config_file_in (YAML format) """
        if 'config' not in analysis_dict:
            analysis_dict['config'] = dict()
        analysis_sname = analysis_sname

        # Set default values for _config_dict
        config_defaults = dict()
        config_defaults['dirout'] = None
        config_defaults['reference'] = None
        config_defaults['plot_bias'] = False
        config_defaults['cache_data'] = False
        config_defaults['stats_in_title'] = False
        config_defaults['plot_format'] = 'png'
        config_defaults['keep_figs'] = False
        config_defaults['grid'] = None
        config_defaults['depth_list'] = [0]

        # Define logger on type and save analysis short name
        self.logger = logging.getLogger(analysis_sname)
        self.analysis_sname = analysis_sname

        # Populate _config_dict
        self._config_dict = dict()
        # (1) If present in analysis_dict['config'] use that value
        # (2) Otherwise, if present in config_dict use that value
        # (3) Otherwise use value from config_defaults
        for config_opt in config_defaults:
            if config_opt in analysis_dict['config']:
                self._config_dict[config_opt] = analysis_dict['config'][config_opt]
            elif config_opt in config_dict:
                self._config_dict[config_opt] = config_dict[config_opt]
            else:
                self._config_dict[config_opt] = config_defaults[config_opt]

        # No default for cache_dir, this needs to be set by user if cache_data is True
        if self._config_dict['cache_data']:
            if 'cache_dir' in analysis_dict['config']:
                self._config_dict['cache_dir'] = analysis_dict['config']['cache_dir']
            elif 'cache_dir' in config_dict:
                self._config_dict['cache_dir'] = config_dict['cache_dir']

        self._var_dict = var_dict
        self._analysis_dict = analysis_dict
        if 'variables' not in self._analysis_dict.keys():
            self._analysis_dict['variables'] = self._var_dict.keys()
        self._ds_dict = ds_dict

        # Set up objects for plotting
        self.fig = dict()
        self.axs = dict()

        self._check()
        self._open_datasets()

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _check(self):
        """
        Configuration of AnalysisElement must be laid out as follows:

        # self._config_dict
            keep_figs: False
            plot_format: png
            cache_data: False
            cache_dir: None # only required if cache_data is true
            reference: None # Not all plot types show comparison to reference
            plot_bias: False # Not all plot types can include a difference
            stats_in_title: False # Not all plot types have meaningful statistics to print
            dirout: {{ path_to_write_plot_files }}
            cache_dir: {{ path_to_save_cached_data }}

        # self._ds_dict (need at least one data source in list)
            - ds_one
            - ds_two
            - ds_three

        # self._var_dict (need at least one variable in list)
            - var_one
            - var_two
            - var_three

        # self._analysis_dict
            op: operation to perform
            sources: # need at least one source
                - ds_one
                - ds_two
                - ds_three

        """
        self.logger.info("Checking contents of %s", self.analysis_sname)

        # Set up lists of required fields for self._config_dict and self._analysis_dict
        consistency_dict = dict()
        consistency_dict['_config_dict'] = ['keep_figs', 'plot_format', 'cache_data', 'reference',
                                            'plot_bias', 'stats_in_title', 'dirout']
        if self._config_dict['cache_data']:
            consistency_dict['config_dict'].append('cache_dir')
        consistency_dict['_analysis_dict'] = ['op', 'sources']

        for dict_name, expected_keys in consistency_dict.items():
            for expected_key in expected_keys:
                if  expected_key not in getattr(self, dict_name):
                    raise KeyError("Can not find '%s' in '%s' section of configuration" %
                                (expected_key, dict_name))

        # Check for required fields in data_sources
        for data_source in self._ds_dict:
            for expected_key in ['source', 'open_dataset']:
                if expected_key not in self._ds_dict[data_source]:
                    raise KeyError("Can not find '%s' in '%s' section of data_sources" %
                                   (expected_key, data_source))
        self.logger.info("Contents of %s contain all necessary data", self.analysis_sname)

    # Drop is_climo, since we may be opening multiple datasets depending on requested analyses
    def _open_datasets(self):
        raise NotImplementedError('_open_datasets needs to be defined in child classes')
