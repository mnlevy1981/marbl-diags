"""
The AnalysisElements class adds source-specific methods for opening or operating
on data_sources of data."""

import os
from . import data_source_classes
from . import analysis_ops
from .generic_classes import GenericAnalysisElement

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
                if 'climo_time_periods' not in config_dict:
                    if 'ann_climo' in op:
                        config_dict['climo_time_periods'] = ['ANN']
                    elif 'mon_climo' in op:
                        config_dict['climo_time_periods'] = ['ANN', 'DJF', 'MAM', 'JJA', 'SON']
                    else:
                        raise ValueError("'{}' is not a valid operation".format(op))
                self.fig = dict()
                self.axs = dict()
                break

        super(AnalysisElements, self).__init__(config_key, config_dict, var_dict, is_climo)

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _open_datasets(self, is_climo=False):
        """ Open requested datasets """
        self.data_sources = dict()
        self._cached_locations = dict()
        self._cached_var_dicts = dict()
        for data_source in self._config_dict['data_sources']:
            self.logger.info("Creating data object for %s in %s", data_source, self._config_key)
            if is_climo:
                climo_str = 'climo'
            else:
                climo_str = 'no_climo'
            self._cached_locations[data_source] = "{}/{}.{}.{}.{}".format(
                self._config_dict['cache_dir'],
                self._config_key,
                data_source,
                climo_str,
                'zarr')
            self._cached_var_dicts[data_source] = "{}/{}.{}.{}.json".format(
                self._config_dict['cache_dir'],
                self._config_key,
                data_source,
                climo_str)
            if self.cache_data and os.path.exists(self._cached_locations[data_source]):
                self.logger.info('Opening %s', self._cached_locations[data_source])
                self.data_sources[data_source] = data_source_classes.CachedClimoData(
                    data_root=self._cached_locations[data_source],
                    var_dict_in=self._cached_var_dicts[data_source],
                    data_type='zarr',
                    **self._config_dict['data_sources'][data_source])
            else:
                self.logger.info('Opening %s',
                                 self._config_dict['data_sources'][data_source]['source'])
                if self._config_dict['data_sources'][data_source]['source'] == 'cesm':
                    self.data_sources[data_source] = data_source_classes.CESMData(
                        **self._config_dict['data_sources'][data_source])
                elif self._config_dict['data_sources'][data_source]['source'] in ['woa2005', 'woa2013']:
                    self.data_sources[data_source] = data_source_classes.WOAData(
                        var_dict=self._var_dict,
                        **self._config_dict['data_sources'][data_source])
                else:
                    raise ValueError("Unknown source '%s'" %
                                     self._config_dict['data_sources'][data_source]['source'])
            self.logger.info('ds = %s', self.data_sources[data_source].ds)

        # Call any necessary operations on datasets
        ops_list = []
        for op in self._config_dict['operations']:
            if op in ['plot_mon_climo', 'plot_ann_climo']:
                # For now, we'll take the average of the monthly ann_climatology
                # FIXME: add compute_ann_climatology function (from esmlab)
                ops_list.append('compute_mon_climatology')
        if ops_list:
            self._operate_on_datasets(ops_list)

    def _operate_on_datasets(self, ops_list):
        """ perform requested operations on datasets """
        for data_source in self._config_dict['data_sources']:
            for op in ops_list:
                self.logger.info('Computing %s on %s', op, data_source)
                func = getattr(self.data_sources[data_source], op)
                func()
                self.logger.info('ds = %s', self.data_sources[data_source].ds)

                # write to cache
                if self.cache_data:
                    if op == 'compute_mon_climatology':
                        if not (self.data_sources[data_source]._is_mon_climo or self.data_sources[data_source]._is_ann_climo):
                            self.data_sources[data_source].cache_dataset(self._cached_locations[data_source],
                                                                       self._cached_var_dicts[data_source])

    ###################
    # PUBLIC ROUTINES #
    ###################

    def do_analysis(self):
        """ Perform requested analysis operations on each dataset """
        for op in self._config_dict['operations']:
            self.logger.info('Calling %s for %s', op, self._config_key)
            func = getattr(analysis_ops, op)
            func(self, self._config_dict)
