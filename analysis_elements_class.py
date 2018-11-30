"""
The AnalysisElements class adds source-specific methods for opening or operating
on collections of data."""

import os
import importlib
import collection_classes
from generic_classes import GenericAnalysisElement

class AnalysisElements(GenericAnalysisElement): # pylint: disable=useless-object-inheritance,too-few-public-methods

    def __init__(self, config_key, config_dict, var_dict):
        """ Determine if operators require monthly climatology """
        is_climo = False

        # This needs to be preceded (or replaced?) with a consistency check
        # that ensures that all the operations requested at this level want
        # the data sources in the collection to be in the same format
        # E.g. we do not want to combine "plot zonal averages" with "plot
        # monthly climatology" because reducing the original dataset to zonal
        # averages would make it impossible to get global data for monthly
        # climatologies
        for op in config_dict['operations']:
            if 'climo' in op:
                is_climo=True
                break

        super(AnalysisElements, self).__init__(config_key, config_dict, var_dict, is_climo)

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _open_datasets(self, is_climo=False):
        """ Open requested datasets """
        self.collections = dict()
        self._cached_locations = dict()
        self._cached_var_dicts = dict()
        for collection in self._config_dict['collections']:
            self.logger.info("Creating data object for %s in %s", collection, self._config_key)
            if is_climo:
                climo_str = 'climo'
            else:
                climo_str = 'no_climo'
            self._cached_locations[collection] = "{}/work/{}.{}.{}.{}".format(
                self._config_dict['dirout'],
                self._config_key,
                collection,
                climo_str,
                'zarr')
            self._cached_var_dicts[collection] = "{}/work/{}.{}.{}.json".format(
                self._config_dict['dirout'],
                self._config_key,
                collection,
                climo_str)
            if os.path.exists(self._cached_locations[collection]):
                self.logger.info('Opening %s', self._cached_locations[collection])
                self.collections[collection] = collection_classes.CachedClimoData(
                    data_root=self._cached_locations[collection],
                    var_dict_in=self._cached_var_dicts[collection],
                    data_type='zarr',
                    **self._config_dict['collections'][collection])
            else:
                self.logger.info('Opening %s',
                                 self._config_dict['collections'][collection]['source'])
                if self._config_dict['collections'][collection]['source'] == 'cesm':
                    self.collections[collection] = collection_classes.CESMData(
                        **self._config_dict['collections'][collection])
                elif self._config_dict['collections'][collection]['source'] == 'woa2013':
                    self.collections[collection] = collection_classes.WOA2013Data(
                        var_dict=self._var_dict,
                        **self._config_dict['collections'][collection])
                else:
                    raise ValueError("Unknown source '%s'" %
                                     self._config_dict['collections'][collection]['source'])
            self.logger.info('ds = %s', self.collections[collection].ds)

        # Call any necessary operations on datasets
        ops_list = []
        for op in self._config_dict['operations']:
            if op in ['plot_climo']:
                ops_list.append('compute_mon_climatology')
        if ops_list:
            self._operate_on_datasets(ops_list)

    def _operate_on_datasets(self, ops_list):
        """ perform requested operations on datasets """
        for collection in self._config_dict['collections']:
            for op in ops_list:
                self.logger.info('Computing %s on %s', op, collection)
                func = getattr(self.collections[collection], op)
                func()
                self.logger.info('ds = %s', self.collections[collection].ds)

                # write to cache
                if op == 'compute_mon_climatology':
                    if not self.collections[collection]._is_climo:
                        self.collections[collection].cache_dataset(self._cached_locations[collection],
                                                                   self._cached_var_dicts[collection])

    ###################
    # PUBLIC ROUTINES #
    ###################

    def do_analysis(self):
        """ Perform requested analysis operations on each dataset """
        for op in self._config_dict['operations']:
            self.logger.info('Calling %s for %s', op, self._config_key)
            module = importlib.import_module('analysis_ops')
            func = getattr(module, op)
            func(self)
