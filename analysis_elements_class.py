"""
The AnalysisElements class adds source-specific methods for opening or operating
on collections of data."""

import os
import importlib
import collection_classes
from generic_classes import GenericAnalysisElement

class AnalysisElements(GenericAnalysisElement): # pylint: disable=useless-object-inheritance,too-few-public-methods

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _open_datasets(self):
        """ Open requested datasets """
        self.collections = dict()
        self._cached_locations = dict()
        self._cached_var_dicts = dict()
        for collection in self._config_dict['collections']:
            self.logger.info("Creating data object for %s in %s", collection, self._config_key)
            self._cached_locations[collection] = "{}/work/{}.{}.{}".format(
                self._config_dict['dirout'],
                self._config_key,
                collection,
                'zarr')
            self._cached_var_dicts[collection] = "{}/work/{}.{}.json".format(
                self._config_dict['dirout'],
                self._config_key,
                collection)
            if os.path.exists(self._cached_locations[collection]):
                self.logger.info('Opening %s', self._cached_locations[collection])
                self.collections[collection] = collection_classes.CachedData(
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
        self._operate_on_datasets()

    def _operate_on_datasets(self):
        """ perform requested operations on datasets """
        for collection in self._config_dict['collections']:
            if isinstance(self.collections[collection],
                          collection_classes.CachedData):
                self.logger.info('No operations for %s, data was cached', collection)
                continue
            if not self._config_dict['collections'][collection]['operations']:
                self.logger.info('No operations requested for %s', collection)
                continue
            for op in self._config_dict['collections'][collection]['operations']:
                self.logger.info('Computing %s', op)
                func = getattr(self.collections[collection], op)
                func()
                self.logger.info('ds = %s', self.collections[collection].ds)
                # write to cache
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
