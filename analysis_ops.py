"""
Functions that can be called from analysis elements"""

import os
from subprocess import call
import cartopy
import cartopy.crs as ccrs
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import plottools as pt

def plot_state(AnalysisElement):
    """ Regardless of data source, generate png """
    # look up grid (move to known grids database)
    if AnalysisElement._config_dict['grid'] == 'POP_gx1v7':
        # and is tracer....
        depth_coord_name = 'z_t'
    else:
        raise ValueError('unknown grid')

    # where will plots be written?
    dirout = AnalysisElement._config_dict['dirout']+'/plots'
    if not os.path.exists(dirout):
        call(['mkdir', '-p', dirout])

    # identify reference (if any provided)
    ref_cname = None
    for cname, collection in AnalysisElement.collections.items():
        if collection.role == 'reference':
            if ref_cname:
                raise ValueError('More that one reference dataset specified')
            ref_cname = cname
    if ref_cname:
        AnalysisElement.logger.info("Reference dataset: '%s'", ref_cname)
    else:
        AnalysisElement.logger.info("No reference dataset specified")

    #-- loop over datasets
    cname_list = AnalysisElement.collections.keys()
    if ref_cname:
        cname_list = [ref_cname] + [cname for cname in cname_list if cname != ref_cname]

    #-- loop over variables
    for v in AnalysisElement._config_dict['variable_list']:

        nrow, ncol = pt.get_plot_dims(len(cname_list))
        AnalysisElement.logger.info('dimensioning plot canvas: %d x %d (%d total plots)',
                         nrow, ncol, len(cname_list))

        for sel_z in AnalysisElement._config_dict['depth_list']:

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
            plot_name = '{}/state-map-{}.{}.{}.png'.format(dirout, AnalysisElement._config_dict['short_name'], v, depth_str)
            AnalysisElement.logger.info('generating plot: %s', plot_name)

            #-- generate figure object
            fig = plt.figure(figsize=(ncol*6,nrow*4))

            for i, ds_name in enumerate(cname_list):

                ds = AnalysisElement.collections[ds_name].ds
                #-- need to deal with time dimension here....

                # Find appropriate variable name in dataset
                var_name = AnalysisElement.collections[ds_name]._var_dict[v]
                if var_name not in ds:
                    raise KeyError('Can not find {} in {}'.format(var_name, ds_name))
                field = ds[var_name].sel(**indexer).isel(time=0)
                AnalysisElement.logger.info('Plotting %s from %s', var_name, ds_name)

                if is_depth_range:
                    field = field.mean(depth_coord_name)

                ax = fig.add_subplot(nrow, ncol, i+1, projection=ccrs.Robinson(central_longitude=305.0))

                if AnalysisElement._config_dict['grid'] == 'POP_gx1v7':
                    lon, lat, field = pt.adjust_pop_grid(ds.TLONG.values, ds.TLAT.values, field)

                if v not in AnalysisElement._var_dict:
                    raise KeyError('{} not defined in variable YAML dict'.format(v))

                cf = ax.contourf(lon,lat,field,transform=ccrs.PlateCarree(),
                                 levels=AnalysisElement._var_dict[v]['contours']['levels'],
                                 extend=AnalysisElement._var_dict[v]['contours']['extend'],
                                 cmap=AnalysisElement._var_dict[v]['contours']['cmap'],
                                 norm=pt.MidPointNorm(midpoint=AnalysisElement._var_dict[v]['contours']['midpoint']))
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
