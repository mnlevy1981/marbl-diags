""" Classes that may eventually be moved to ESMlab? """

import logging
import os
import json
from subprocess import call
from datetime import datetime
import cftime
import xarray as xr

class GenericCollection(object): # pylint: disable=useless-object-inheritance
    """ Class containing functions used regardless of data source """
    def __init__(self, child_class=None, **kwargs):
        if child_class:
            self.logger = logging.getLogger(child_class)
        self._files = None
        self.role = kwargs['role']
        self.source = kwargs['source']
        self.ds = None # pylint: disable=invalid-name
        self._var_dict = None
        self._set_var_dict()

    ###################
    # PUBLIC ROUTINES #
    ###################

    def compute_mon_climatology(self):
        """ Compute a monthly climatology """

        tb_name, tb_dim = self._time_bound_var()

        grid_vars = [v for v in self.ds.variables if 'time' not in self.ds[v].dims]

        # save attrs
        attrs = {v:self.ds[v].attrs for v in self.ds.variables}
        encoding = {v:{key:val for key, val in self.ds[v].encoding.items()
                       if key in ['dtype', '_FillValue', 'missing_value']}
                    for v in self.ds.variables}

        #-- compute time variable
        date = cftime.num2date(self.ds[tb_name].mean(tb_dim),
                               units=self.ds.time.attrs['units'],
                               calendar=self.ds.time.attrs['calendar'])
        self.ds.time.values = date
        if len(date)%12 != 0:
            raise ValueError('Time axis not evenly divisible by 12!')

        #-- compute climatology
        ds = self.ds.drop(grid_vars).groupby('time.month').mean('time').rename({'month':'time'})

        #-- put grid_vars back
        ds = xr.merge((ds, self.ds.drop([v for v in self.ds.variables if v not in grid_vars])))

        attrs['time'] = {'long_name':'Month', 'units':'month'}
        del encoding['time']

        # put the attributes back
        for v in ds.variables:
            ds[v].attrs = attrs[v]

        # put the encoding back
        for v in ds.variables:
            if v in encoding:
                ds[v].encoding = encoding[v]

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
        Each class derived from GenericCollection needs to map from generic variable names to
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
        self.collections = None
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
          operations: {{ List of methods of form: ? = func(collection,collections)}}
          variable_list: {{ list of variables to include in analysis (might be derived) }}
          collections:
            collection:
              role:
              source:
              open_dataset:


        collections: is a collection of datasets;
        collection: stores attributes of the collection, specified in the yaml file.
        """
        if not self._config_dict:
            raise ValueError("configuration dictionary is empty")

        self.logger.info("Checking contents of %s", self._config_key)
        # Check for required fields in top level analysis element
        for expected_key in ['dirout', 'source', 'collections', 'operations']:
            if  expected_key not in self._config_dict:
                raise KeyError("Can not find '%s' in '%s' section of configuration" %
                               (expected_key, self._config_key))
        # Check for required fields in collections
        for collection in self._config_dict['collections']:
            for expected_key in ['source', 'open_dataset']:
                if expected_key not in self._config_dict['collections'][collection]:
                    raise KeyError("Can not find '%s' in '%s' section of collections" %
                                   (expected_key, collection))
        self.logger.info("Contents of %s contain all necessary data", self._config_key)

    def _open_datasets(self, is_climo):
        pass
