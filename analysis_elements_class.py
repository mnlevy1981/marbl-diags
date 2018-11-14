"""
The AnalysisElements class is a container for multiple datasets
and operators for comparing them."""

import logging
import os
import yaml
import data_source_classes

class AnalysisElements(object): # pylint: disable=useless-object-inheritance,too-few-public-methods
    """
    Objects in this class
        * datasets: datasets[ds_sname] is a specific dataset to analyze
                    E.g. datasets['WOA2013'] is the World Ocean Atlas reanalysis
        * variables: variables[var_name] is a list of alternative names for the variable
                     E.g. variables['nitrate'] = ['NO3', 'n_an']
    """
    def __init__(self, config_file_in):
        """ construct class object based on config_file_in (YAML format) """
        # Read YAML configuration
        with open(config_file_in) as file_in:
            self._configuration = yaml.load(file_in)
        self.logger = logging.getLogger('AnalysisElements')
        self._check()
        self._open_datasets()
        self._operate_on_datasets()

    def _check(self):
        """
        Configuration file must be laid out as follows.
        analysis_element:
          description: {{ description_text }}
          dirout: {{ path_to_save_temp_files }}
          source: {{ module_for_compute }}
          operations: {{ List of methods of form: ? = func(collection,data_sources)}}
          variable_list: {{ list of variables to include in analysis (might be derived) }}
          data_sources:
            data_source:
              role:
              source:
              open_dataset:
              operations:
                {{ List of methods of form: ds = func(ds) }}


        collection: is a collection of datasets;
        data_sources: stores attributes of the collection, specified in the yaml
                      file.
        """
        if not self._configuration:
            raise ValueError("configuration dictionary is empty")
        for config_key, config_dict in self._configuration.items():
            self.logger.info("Checking contents of %s", config_key)
            # Check for required fields in top level analysis element
            for expected_key in ['dirout', 'source', 'data_sources', 'operations']:
                if  expected_key not in config_dict:
                    raise KeyError("Can not find '%s' in '%s' section of configuration" %
                                   (expected_key, config_key))
            # Check for required fields in data_sources
            for data_source in config_dict['data_sources']:
                for expected_key in ['source', 'open_dataset', 'operations']:
                    if expected_key not in config_dict['data_sources'][data_source]:
                        raise KeyError("Can not find '%s' in '%s' section of data_sources" %
                                       (expected_key, data_source))
            self.logger.info("Contents of %s contain all necessary data", config_key)

    def _open_datasets(self):
        """ Open requested datasets """
        self.collection = dict()
        for config_key, config_dict in self._configuration.items():
            self.collection[config_key] = dict()
            for data_source in config_dict['data_sources']:
                self.logger.info("Creating data object for %s in %s", data_source, config_key)
                cached_location = "{}/work/{}.{}.{}".format(
                    self._configuration[config_key]['dirout'],
                    config_key,
                    data_source,
                    'zarr')
                if os.path.exists(cached_location):
                    self.logger.info('Opening %s', cached_location)
                    self.collection[config_key][data_source] = data_source_classes.CachedData(
                        data_root=cached_location, data_type='zarr')
                else:
                    self.logger.info('Opening %s',
                                     config_dict['data_sources'][data_source]['source'])
                    if config_dict['data_sources'][data_source]['source'] == 'cesm':
                        self.collection[config_key][data_source] = data_source_classes.CESMData(
                            **config_dict['data_sources'][data_source]['open_dataset'])
                    elif config_dict['data_sources'][data_source]['source'] == 'woa2013':
                        self.collection[config_key][data_source] = data_source_classes.WOA2013Data(
                            **config_dict['data_sources'][data_source]['open_dataset'])
                    else:
                        raise ValueError("Unknown source '%s'" %
                                         config_dict['data_sources'][data_source]['source'])
                self.logger.info('ds = %s', self.collection[config_key][data_source].ds)

    def _operate_on_datasets(self):
        """ perform requested operations on datasets """
        for config_key, config_dict in self._configuration.items():
            for data_source in config_dict['data_sources']:
                if isinstance(self.collection[config_key][data_source],
                              data_source_classes.CachedData):
                    self.logger.info('No operations for %s, data was cached', data_source)
                    continue
                if not config_dict['data_sources'][data_source]['operations']:
                    self.logger.info('No operations requested for %s', data_source)
                    continue
                for op in config_dict['data_sources'][data_source]['operations']:
                    self.logger.info('Computing %s', op)
                    func = getattr(self.collection[config_key][data_source], op)
                    func()
                    self.logger.info('ds = %s', self.collection[config_key][data_source].ds)
                    # write to cache
                    cached_location = "{}/work/{}.{}.{}".format(
                        self._configuration[config_key]['dirout'],
                        config_key,
                        data_source,
                        'zarr')
                    self.collection[config_key][data_source].cache_dataset(cached_location)
