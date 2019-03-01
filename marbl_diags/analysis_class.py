"""
The AnalysisElement class adds source-specific methods for opening or operating
on data_sources of data."""

import logging
import os
from . import data_source_classes
from . import analysis_ops
from .generic_classes import GenericAnalysisElement

######################################################################

class AnalysisCategory(object):

    def __init__(self, category_name, analysis_dicts, ds_dict, var_dict, global_config):
        """ Set up many AnalysisElement objects for the same type of plots """

        # (1) Define logger on type, save category name, and save ds_dict
        self.logger = logging.getLogger(category_name)
        self.logger.info("Initializing %s category...", category_name)
        self.category_name = category_name
        self._ds_dict = ds_dict

        # (2) Define operations based on category
        if category_name == "3d_ann_climo_maps_on_levels":
            self.operation = 'plot_ann_climo'
        elif category_name == "plot_regional_time_series":
            pass
        else:
            raise ValueError("'{}' is not a valid analysis category".format(category_name))
        # (2) Store analysis category configuration in self.category_settings
        #     (a) build dictionary of default settings
        category_settings_defaults = dict()
        category_settings_defaults['dirout'] = None
        category_settings_defaults['cache_data'] = False
        category_settings_defaults['plot_format'] = 'png'
        category_settings_defaults['keep_figs'] = False
        #         (some settings may be category-specific)
        if category_name == "3d_ann_climo_maps_on_levels":
            # Set up dictionary of default values to use for this category
            category_settings_defaults['variables'] = ['nitrate', 'phosphate', 'silicate', 'oxygen',
                                                     'dic', 'alkalinity', 'iron']
            category_settings_defaults['levels'] = [depth for depth in range(0, 4001, 500)]
            category_settings_defaults['reference'] = None
            category_settings_defaults['plot_diff_from_reference'] = False
            category_settings_defaults['stats_in_title'] = True
            category_settings_defaults['grid'] = None
            category_settings_defaults['climo_time_periods'] = ['ANN']

        #     (b) To start, category_settings = analysis_dicts['_settings'] (if it exists)
        if '_settings' in analysis_dicts:
            self.logger.debug("Getting %s from analysis_dicts", analysis_dicts['_settings'].keys())
            self.category_settings = analysis_dicts['_settings']
        else:
            self.category_settings = dict()

        #     (c) next add any settings from global_config that were not in '_settings'
        for settings_key in global_config:
            if settings_key not in self.category_settings:
                self.logger.debug("Getting %s from global_config", settings_key)
                self.category_settings[settings_key] = global_config[settings_key]

        #     (d) Lastly, add any settings from config_settings_defaults
        for settings_key, category_settings_default in category_settings_defaults.items():
            if settings_key not in self.category_settings:
                self.logger.debug("Getting %s from default values", settings_key)
                self.category_settings[settings_key] = category_settings_default

        # (3) Make sure no extraneous keys were included
        #     (a) Only allowable keys are ones in category_settings_defaults
        #         (with exception that cache_dir is REQUIRED if cache_data is True)
        expected_keys = category_settings_defaults.keys()
        if self.category_settings['cache_data']:
            if 'cache_dir' not in self.category_settings:
                raise KeyError("Must provide 'cache_dir' if setting 'cache_data' to True")
            expected_keys.append('cache_dir')

        #     (b) Abort if any category_settings keys are not in expected_keys
        for settings_key in self.category_settings:
            if settings_key not in expected_keys:
                raise KeyError("Unrecognized setting: '{}'".format(settings_key))

        # (4) Create analysis elements
        self.AnalysisElements = dict()
        for element_key, analysis_dict in analysis_dicts.items():
            # _settings is not an Analysis Element
            if element_key == '_settings':
                continue
            self.AnalysisElements[element_key] = AnalysisElement(element_key, analysis_dict,
                                                                 var_dict,
                                                                 config=self.category_settings)
            self._open_datasets(element_key)

    ###################
    # PUBLIC ROUTINES #
    ###################

    def do_analysis(self):
        """ Perform requested analysis operations on each dataset """
        for AnalysisElement in self.AnalysisElements.values():
            self.logger.info('Calling %s for %s', self.operation, AnalysisElement.analysis_sname)
            func = getattr(analysis_ops, self.operation)
            func(AnalysisElement)

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _open_datasets(self, element_key):
        """ Open datasets requested by AnalysisElement[element_key] """
        AnalysisElement = self.AnalysisElements[element_key]
        # Determine if operator acts on climatology
        AnalysisElement.climo = None
        if 'climo' in self.operation:
            if 'ann_climo' in self.operation:
                AnalysisElement.climo = 'ann_climo'
            elif 'mon_climo' in self.operation:
                AnalysisElement.climo = 'mon_climo'
            else:
                raise ValueError("'{}' is not a valid operation".format(self.operation))

        # AnalysisElement.datestrs details what years of data to read from each source
        # E.g. {JRA: 0033-0052} => will be working with years 33 - 52 of CESM run using JRA forcing
        # Data will be stored in AnalysisElement.data_sources['JRA.0033-0052']
        # (AnalysisElement.data_sources is a new dictionary, can be thought of as intent(out))
        AnalysisElement.data_sources = dict()
        for data_source in AnalysisElement.datestrs:
            self.logger.info("Creating data object for %s in %s", data_source, element_key)

            # (1) Save both datestr ('0033-0052') and data_source_key ('JRA.0033-0052')
            data_source_labels = [data_source + '.' + datestr for datestr in AnalysisElement.datestrs[data_source]]
            data_sources = dict(zip(AnalysisElement.datestrs[data_source], data_source_labels))

            # (2) Read data from source
            for datestr, data_source_label in data_sources.items():
                # Is dataset already cached?
                if AnalysisElement._global_config['cache_data']:
                    AnalysisElement._cached_locations = dict()
                    AnalysisElement._cached_var_dicts = dict()
                    if AnalysisElement.climo:
                        climo_str = AnalysisElement.climo
                    else:
                        climo_str = 'no_climo'
                    AnalysisElement._cached_locations[data_source_label] = "{}/{}.{}.{}.{}".format(
                        AnalysisElement._global_config['cache_dir'],
                        AnalysisElement.category_name,
                        datestr,
                        climo_str,
                        'zarr')
                    AnalysisElement._cached_var_dicts[data_source_label] = "{}/{}.{}.{}.json".format(
                        AnalysisElement._global_config['cache_dir'],
                        AnalysisElement.category_name,
                        datestr,
                        climo_str)
                    if os.path.exists(AnalysisElement._cached_locations[data_source_label]):
                        AnalysisElement.logger.debug('Reading %s', AnalysisElement._cached_locations[data_source_label])
                        AnalysisElement.data_sources[data_source_label] = data_source_classes.CachedClimoData(
                            data_root=AnalysisElement._cached_locations[data_source_label],
                            var_dict_in=AnalysisElement._cached_var_dicts[data_source_label],
                            data_type='zarr',
                            **self._ds_dict[data_source])
                else:
                    self.logger.debug('Reading %s output', self._ds_dict[data_source]['source'])
                    if self._ds_dict[data_source]['source'] == 'cesm':
                        AnalysisElement.data_sources[data_source_label] = data_source_classes.CESMData(
                            datestr, **self._ds_dict[data_source])
                    elif self._ds_dict[data_source]['source'] in ['woa2005', 'woa2013']:
                        AnalysisElement.data_sources[data_source_label] = data_source_classes.WOAData(
                            var_dict=AnalysisElement._var_dict,
                            **self._ds_dict[data_source])
                    else:
                        raise ValueError("Unknown source '%s'" %
                                         self._ds_dict[data_source]['source'])
                self.logger.debug('ds = %s', AnalysisElement.data_sources[data_source_label].ds)

        # Call any necessary operations on datasets
        AnalysisElement._operate_on_datasets(self.operation)

######################################################################

class AnalysisElement(GenericAnalysisElement): # pylint: disable=useless-object-inheritance,too-few-public-methods

    def __init__(self, analysis_sname, analysis_dict, var_dict, config):
        """ Determine if operators require monthly climatology """

        super(AnalysisElement, self).__init__(analysis_sname, analysis_dict, var_dict, config)

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

    def _operate_on_datasets(self, operation):
        """ perform requested operations on datasets """
        if operation in ['plot_mon_climo', 'plot_ann_climo']:
            op = 'compute_mon_climatology'
            for data_source in self.data_sources:
                self.logger.info('Computing %s on %s', op, data_source)
                func = getattr(self.data_sources[data_source], op)
                func()
                self.logger.debug('ds = %s', self.data_sources[data_source].ds)

                # write to cache
                if self._global_config['cache_data']:
                    if not (self.data_sources[data_source]._is_mon_climo or self.data_sources[data_source]._is_ann_climo):
                        self.data_sources[data_source].cache_dataset(self._cached_locations[data_source],
                                                                   self._cached_var_dicts[data_source])
