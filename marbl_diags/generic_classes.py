""" Classes that may eventually be moved to ESMlab? """

import logging
import os
import json
from subprocess import call
from datetime import datetime
import cftime
import xarray as xr
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
    def __init__(self, config_key, config_dict, var_dict, is_climo):
        """ construct class object based on config_file_in (YAML format) """
        # Read YAML configuration
        self.logger = logging.getLogger(config_key)
        self._config_key = config_key
        self._config_dict = config_dict
        self._var_dict = var_dict
        if 'reference' in config_dict:
            self.reference = config_dict['reference']
        else:
            self.reference = None
        if 'cache_data' in config_dict:
            self.cache_data = config_dict['cache_data']
        else:
            self.cache_data = False
        self.data_sources = None
        self._check()
        self._open_datasets(is_climo)

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _check(self):
        """
        Configuration file must be laid out as follows.
        analysis_element:
          description: {{ description_text }}
          dirout: {{ path_to_save_temp_files }}
          source: {{ module_for_compute }}
          operations: {{ List of methods of form: ? = func(data_source,data_sources)}}
          variable_list: {{ list of variables to include in analysis (might be derived) }}
          [ climo_time_periods: {{ list of climatological time periods to plot (e.g. ANN, DJF, etc) }} ]
          data_sources:
            data_source:
              source:
              open_dataset:


        data_sources: a collection of data_sources;
        data_source: stores attributes of the data_source, specified in the yaml file.
        """
        if not self._config_dict:
            raise ValueError("configuration dictionary is empty")
        if not isinstance(self._config_dict, dict):
            raise TypeError("configuration dictionary is not a dictionary")

        if 'climo_time_periods' not in self._config_dict:
            self._config_dict['climo_time_periods'] = ['DJF', 'MAM', 'JJA', 'SON', 'ANN']

        self.logger.info("Checking contents of %s", self._config_key)
        # Check for required fields in top level analysis element
        for expected_key in ['dirout', 'source', 'data_sources', 'operations']:
            if  expected_key not in self._config_dict:
                raise KeyError("Can not find '%s' in '%s' section of configuration" %
                               (expected_key, self._config_key))
        # Check for required fields in data_sources
        for data_source in self._config_dict['data_sources']:
            for expected_key in ['source', 'open_dataset']:
                if expected_key not in self._config_dict['data_sources'][data_source]:
                    raise KeyError("Can not find '%s' in '%s' section of data_sources" %
                                   (expected_key, data_source))
        self.logger.info("Contents of %s contain all necessary data", self._config_key)

    def _open_datasets(self, is_climo):
        pass
