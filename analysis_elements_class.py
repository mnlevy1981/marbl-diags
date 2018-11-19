"""
The AnalysisElements class is a container for multiple datasets
and operators for comparing them."""

import logging
import os
from subprocess import call
import cartopy
import cartopy.crs as ccrs
import numpy as np
import matplotlib.pyplot as plt
import collection_classes
import plottools as pt
from generic_classes import GenericAnalysisElement

class AnalysisElements(GenericAnalysisElement): # pylint: disable=useless-object-inheritance,too-few-public-methods

    ####################
    # PRIVATE ROUTINES #
    ####################

    def _open_datasets(self):
        """ Open requested datasets """
        self.collections = dict()
        for collection in self._config_dict['collections']:
            self.logger.info("Creating data object for %s in %s", collection, self._config_key)
            cached_location = "{}/work/{}.{}.{}".format(
                self._config_dict['dirout'],
                self._config_key,
                collection,
                'zarr')
            if os.path.exists(cached_location):
                self.logger.info('Opening %s', cached_location)
                self.collections[collection] = collection_classes.CachedData(
                    data_root=cached_location, data_type='zarr',
                    **self._config_dict['collections'][collection])
            else:
                self.logger.info('Opening %s',
                                 self._config_dict['collections'][collection]['source'])
                if self._config_dict['collections'][collection]['source'] == 'cesm':
                    self.collections[collection] = collection_classes.CESMData(
                        **self._config_dict['collections'][collection])
                elif self._config_dict['collections'][collection]['source'] == 'woa2013':
                    self.collections[collection] = collection_classes.WOA2013Data(
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
                cached_location = "{}/work/{}.{}.{}".format(
                    self._config_dict['dirout'],
                    self._config_key,
                    collection,
                    'zarr')
                self.collections[collection].cache_dataset(cached_location)

    ###################
    # PUBLIC ROUTINES #
    ###################

    def do_analysis(self):
        """ Perform requested analysis operations on each dataset """
        for op in self._config_dict['operations']:
            self.logger.info('Calling %s for %s', op, self._config_key)
            func = getattr(self, op)
            func(**self._config_dict)

    def plot_state(self, **kwargs):
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

        nrow, ncol = pt.get_plot_dims(len(self.collections))
        self.logger.info('dimensioning plot canvas: %d x %d (%d total plots)',
                         nrow, ncol, len(self.collections))

        # identify reference (if any provided)
        ref_cname = None
        for cname, collection in self.collections.items():
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
                cname_list = self.collections.keys()
                if ref_cname:
                    cname_list = [ref_cname] + [cname for cname in cname_list if cname != ref_cname]
                for i, ds_name in enumerate(cname_list):

                    ds = self.collections[ds_name].ds
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
