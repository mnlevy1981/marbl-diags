"""
The AnalysisElements class is a container for multiple datasets
and operators for comparing them."""

import logging
import os
from subprocess import call
import yaml
import cartopy
import cartopy.crs as ccrs
import numpy as np
import matplotlib.pyplot as plt
import data_source_classes
import plottools as pt

class AnalysisElements(object): # pylint: disable=useless-object-inheritance,too-few-public-methods
    """
    Objects in this class
        * datasets: datasets[ds_sname] is a specific dataset to analyze
                    E.g. datasets['WOA2013'] is the World Ocean Atlas reanalysis
        * variables: variables[var_name] is a list of alternative names for the variable
                     E.g. variables['nitrate'] = ['NO3', 'n_an']
    """
    def __init__(self, config_key, config_dict):
        """ construct class object based on config_file_in (YAML format) """
        # Read YAML configuration
        self.logger = logging.getLogger(config_key)
        self._config_key = config_key
        self._config_dict = config_dict
        self._check()
        self._open_datasets()
        self._operate_on_datasets()

    ###################
    # PUBLIC ROUTINES #
    ###################

    def do_analysis(self):
        """ Perform requested analysis operations on each dataset """
        for op in self._config_dict['operations']:
            self.logger.info('Calling %s for %s', op, self._config_key)
            func = getattr(self, op)
            func(self.collection, **self._config_dict)

    def plot_state(self, collections, **kwargs):
        """ Regardless of data source, generate png """
        contour_specs = {'O2' : {'levels' : np.concatenate((np.arange(0,5,1),np.arange(5,10,2),np.arange(10,30,5),np.arange(30,70,10),np.arange(80,150,20),np.arange(150,325,25))),
                                 'norm' : pt.MidPointNorm(midpoint=50.),
                                 'extend' : 'max','cmap':'PuOr'},
                         'NO3' : {'levels' : [0,0.1,0.2,0.3,0.4,0.6,0.8,1.,1.5,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,34,38,42],
                                  'norm' : pt.MidPointNorm(midpoint=2.),
                                  'extend' : 'max','cmap':'PRGn'},
                         'PO4' : {'levels' : [0,0.01,0.02,0.04,0.06,0.08,0.1,0.14,0.18,0.22,0.26,0.3,0.34,0.38,0.42,0.46,0.5,0.6,0.7,0.8,0.9,1,1.2,1.4,1.6,1.8,2,2.4,2.8,3.2],
                                  'norm' : pt.MidPointNorm(midpoint=0.8),
                                  'extend' : 'max','cmap':'PRGn'},
                         'SiO3' : {'levels' : np.concatenate((np.arange(0,10,1),np.arange(10,50,5),np.arange(50,100,10),np.arange(100,200,20))),
                                   'norm' : pt.MidPointNorm(midpoint=5.),
                                   'extend' : 'max','cmap':'PRGn'}
                         }


        # look up grid (move to known grids database)
        if kwargs['grid'] == 'POP_gx1v7':
            # and is tracer....
            depth_coord_name = 'z_t'
        else:
            raise ValueError('unknown grid')

        # where will plots be written?
        dirout = kwargs['dirout']+'/plots'
        if not os.path.exists(dirout):
            call(['mkdir', '-p', dirout])

        nrow, ncol = pt.get_plot_dims(len(collections))
        self.logger.info('dimensioning plot canvas: %d x %d (%d total plots)',
                         nrow, ncol, len(collections))

        # identify reference (if any provided)
        ref_cname = None
        for cname, collection in collections.items():
            if collection.role == 'reference':
                if ref_cname:
                    raise ValueError('More that one reference dataset specified')
                ref_cname = cname
        if ref_cname:
            self.logger.info("Reference dataset: '%s'", ref_cname)
        else:
            self.logger.info("No reference dataset specified")

        #-- loop over variables
        for v in kwargs['variable_list']:

            for sel_z in kwargs['depth_list']:

                #-- build indexer for depth
                if isinstance(sel_z, list): # fragile?
                    is_depth_range = True
                    indexer = {depth_coord_name:slice(sel_z[0], sel_z[1])}
                    depth_str = '{:.0f}-{:.0f}m'.format(sel_z[0], sel_z[1])
                else:
                    is_depth_range = False
                    indexer = {depth_coord_name: sel_z, 'method': 'nearest'}
                    depth_str = '{:.0f}m'.format(sel_z)

                #-- name of the plot
                plot_name = '{}/state-map-{}.{}.{}.png'.format(dirout, kwargs['short_name'], v, depth_str)
                logging.info('generating plot: %s', plot_name)

                #-- generate figure object
                fig = plt.figure(figsize=(ncol*6,nrow*4))

                #-- loop over datasets
                cname_list = collections.keys()
                if ref_cname:
                    cname_list = [ref_cname] + [cname for cname in cname_list if cname != ref_cname]
                for i, ds_name in enumerate(cname_list):

                    ds = collections[ds_name].ds
                    self.logger.info('Plotting %s', ds_name)

                    #-- need to deal with time dimension here....
                    field = ds[v].sel(**indexer).isel(time=0)
                    if is_depth_range:
                        field = field.mean(depth_coord_name)

                    ax = fig.add_subplot(nrow, ncol, i+1, projection=ccrs.Robinson(central_longitude=305.0))

                    if kwargs['grid'] == 'POP_gx1v7':
                        lon, lat, field = pt.adjust_pop_grid(ds.TLONG.values, ds.TLAT.values, field)

                    if v not in contour_specs:
                        contour_specs[v] = {}

                    cf = ax.contourf(lon,lat,field,transform=ccrs.PlateCarree(),
                                     **contour_specs[v])
                    land = ax.add_feature(cartopy.feature.NaturalEarthFeature(
                        'physical','land','110m',
                        edgecolor='face',
                        facecolor='gray'))

                    ax.set_title(ds_name)
                    ax.set_xlabel('')
                    ax.set_ylabel('')

                fig.subplots_adjust(hspace=0.45, wspace=0.02, right=0.9)
                cax = plt.axes((0.93, 0.15, 0.02, 0.7))
                fig.colorbar(cf, cax=cax)

                fig.savefig(plot_name, bbox_inches='tight', dpi=300)
                plt.close(fig)

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
        if not self._config_dict:
            raise ValueError("configuration dictionary is empty")

        self.logger.info("Checking contents of %s", self._config_key)
        # Check for required fields in top level analysis element
        for expected_key in ['dirout', 'source', 'data_sources', 'operations']:
            if  expected_key not in self._config_dict:
                raise KeyError("Can not find '%s' in '%s' section of configuration" %
                               (expected_key, self._config_key))
        # Check for required fields in data_sources
        for data_source in self._config_dict['data_sources']:
            for expected_key in ['source', 'open_dataset', 'operations']:
                if expected_key not in self._config_dict['data_sources'][data_source]:
                    raise KeyError("Can not find '%s' in '%s' section of data_sources" %
                                   (expected_key, data_source))
        self.logger.info("Contents of %s contain all necessary data", self._config_key)

    def _open_datasets(self):
        """ Open requested datasets """
        self.collection = dict()
        for data_source in self._config_dict['data_sources']:
            self.logger.info("Creating data object for %s in %s", data_source, self._config_key)
            cached_location = "{}/work/{}.{}.{}".format(
                self._config_dict['dirout'],
                self._config_key,
                data_source,
                'zarr')
            if os.path.exists(cached_location):
                self.logger.info('Opening %s', cached_location)
                self.collection[data_source] = data_source_classes.CachedData(
                    data_root=cached_location, data_type='zarr',
                    **self._config_dict['data_sources'][data_source])
            else:
                self.logger.info('Opening %s',
                                 self._config_dict['data_sources'][data_source]['source'])
                if self._config_dict['data_sources'][data_source]['source'] == 'cesm':
                    self.collection[data_source] = data_source_classes.CESMData(
                        **self._config_dict['data_sources'][data_source])
                elif self._config_dict['data_sources'][data_source]['source'] == 'woa2013':
                    self.collection[data_source] = data_source_classes.WOA2013Data(
                        **self._config_dict['data_sources'][data_source])
                else:
                    raise ValueError("Unknown source '%s'" %
                                     self._config_dict['data_sources'][data_source]['source'])
            self.logger.info('ds = %s', self.collection[data_source].ds)

    def _operate_on_datasets(self):
        """ perform requested operations on datasets """
        for data_source in self._config_dict['data_sources']:
            if isinstance(self.collection[data_source],
                          data_source_classes.CachedData):
                self.logger.info('No operations for %s, data was cached', data_source)
                continue
            if not self._config_dict['data_sources'][data_source]['operations']:
                self.logger.info('No operations requested for %s', data_source)
                continue
            for op in self._config_dict['data_sources'][data_source]['operations']:
                self.logger.info('Computing %s', op)
                func = getattr(self.collection[data_source], op)
                func()
                self.logger.info('ds = %s', self.collection[data_source].ds)
                # write to cache
                cached_location = "{}/work/{}.{}.{}".format(
                    self._config_dict['dirout'],
                    self._config_key,
                    data_source,
                    'zarr')
                self.collection[data_source].cache_dataset(cached_location)
