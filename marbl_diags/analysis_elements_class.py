"""
The AnalysisElements class adds source-specific methods for opening or operating
on data_sources of data."""

import os
from . import data_source_classes
from . import analysis_ops
from .generic_classes import GenericAnalysisElement

class AnalysisElements(GenericAnalysisElement): # pylint: disable=useless-object-inheritance,too-few-public-methods

    def __init__(self, analysis_sname, analysis_dict, ds_dict, var_dict, config_dict):
        """ Determine if operators require monthly climatology """

        super(AnalysisElements, self).__init__(analysis_sname, analysis_dict, ds_dict, var_dict, config_dict)

        # Is this analysis a climatology?
        # This needs to be preceded (or replaced?) with a consistency check
        # that ensures that all the operations requested at this level want
        # the data sources in the collection to be in the same format
        # E.g. we do not want to combine "plot zonal averages" with "plot
        # monthly climatology" because reducing the original dataset to zonal
        # averages would make it impossible to get global data for monthly
        # climatologies

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _open_datasets(self):
        """ Open requested datasets """
        # Determine if operator acts on climatology
        is_climo = False
        if 'climo' in self._analysis_dict['op']:
            is_climo=True
            if 'climo_time_periods' in self._analysis_dict['config']:
                self._config_dict['climo_time_periods'] =  self._analysis_dict['config']['climo_time_periods']
            else:
                if 'ann_climo' in self._analysis_dict['op']:
                    self._config_dict['climo_time_periods'] = ['ANN']
                elif 'mon_climo' in self._analysis_dict['op']:
                    self._config_dict['climo_time_periods'] = ['ANN', 'DJF', 'MAM', 'JJA', 'SON']
                else:
                    raise ValueError("'{}' is not a valid operation".format(self._analysis_dict['op']))

        self.data_sources = dict()
        for data_source in self._ds_dict:
            self.logger.info("Creating data object for %s in %s", data_source, self.analysis_sname)
            if self._config_dict['cache_data']:
                self._cached_locations = dict()
                self._cached_var_dicts = dict()
                if is_climo:
                    climo_str = 'climo'
                else:
                    climo_str = 'no_climo'
                self._cached_locations[data_source] = "{}/{}.{}.{}.{}".format(
                    self._config_dict['cache_dir'],
                    self.analysis_sname,
                    data_source,
                    climo_str,
                    'zarr')
                self._cached_var_dicts[data_source] = "{}/{}.{}.{}.json".format(
                    self._config_dict['cache_dir'],
                    self.analysis_sname,
                    data_source,
                    climo_str)
                if os.path.exists(self._cached_locations[data_source]):
                    self.logger.info('Opening %s', self._cached_locations[data_source])
                    self.data_sources[data_source] = data_source_classes.CachedClimoData(
                        data_root=self._cached_locations[data_source],
                        var_dict_in=self._cached_var_dicts[data_source],
                        data_type='zarr',
                        **self._ds_dict[data_source])
            else:
                self.logger.info('Opening %s', self._ds_dict[data_source]['source'])
                if self._ds_dict[data_source]['source'] == 'cesm':
                    self.data_sources[data_source] = data_source_classes.CESMData(
                        **self._ds_dict[data_source])
                elif self._ds_dict[data_source]['source'] in ['woa2005', 'woa2013']:
                    self.data_sources[data_source] = data_source_classes.WOAData(
                        var_dict=self._var_dict,
                        **self._ds_dict[data_source])
                else:
                    raise ValueError("Unknown source '%s'" %
                                     self._ds_dict[data_source]['source'])
            self.logger.info('ds = %s', self.data_sources[data_source].ds)

        # Call any necessary operations on datasets
        self._operate_on_datasets()

    def _operate_on_datasets(self):
        """ perform requested operations on datasets """
        if self._analysis_dict['op'] in ['plot_mon_climo', 'plot_ann_climo']:
            op = 'compute_mon_climatology'
            for data_source in self._ds_dict:
                self.logger.info('Computing %s on %s', op, data_source)
                func = getattr(self.data_sources[data_source], op)
                func()
                self.logger.info('ds = %s', self.data_sources[data_source].ds)

                # write to cache
                if self._config_dict['cache_data']:
                    if not (self.data_sources[data_source]._is_mon_climo or self.data_sources[data_source]._is_ann_climo):
                        self.data_sources[data_source].cache_dataset(self._cached_locations[data_source],
                                                                   self._cached_var_dicts[data_source])

    ###################
    # PUBLIC ROUTINES #
    ###################

    def do_analysis(self):
        """ Perform requested analysis operations on each dataset """
        self.logger.info('Calling %s for %s', self._analysis_dict['op'], self.analysis_sname)
        func = getattr(analysis_ops, self._analysis_dict['op'])
        func(self)
