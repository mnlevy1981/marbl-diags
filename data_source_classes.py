""" The DataSource class contains a data set and metadata regarding how to analyze the set """

import glob
import logging
import os
import xarray as xr

class BaseDataSource(object): # pylint: disable=useless-object-inheritance
    """ Class containing functions used regardless of data source """
    def __init__(self, **kwargs): # pylint: disable=unused-argument
        self.logger = None
        self._files = None
        self.ds = None # pylint: disable=invalid-name

    def gen_plots(self):
        """ Regardless of data source, generate png """
        pass

    def gen_html(self):
        """ Regardless of data source, generate html """
        pass

class CachedData(BaseDataSource):
    """ Class built around reading previously-cached data """
    def __init__(self, data_root, data_type):
        super().__init__()
        self.logger = logging.getLogger('CachedData')
        self._get_dataset(data_root, data_type)

    def _get_dataset(self, data_root, data_type):
        self.logger.info('calling _get_dataset, data_type = %s', data_type)
        if data_type == 'zarr':
            self.ds = xr.open_zarr(data_root, decode_times=False, decode_coords=False) # pylint: disable=invalid-name

class CESMData(BaseDataSource):
    """ Class built around reading CESM history files """
    def __init__(self, **kwargs):
        super().__init__()
        self.logger = logging.getLogger('CESMData')
        self._get_dataset(**kwargs)

    def _get_dataset(self, filetype, dirin, case, stream, datestr, variable_list):
        """ docstring """
        xr_open_ds = {'decode_coords' : False, 'decode_times' : False, 'data_vars' : 'minimal'}
        if isinstance(variable_list, str):
            variable_list = [variable_list]

        if filetype == 'hist':

            file_name_pattern = '{}/{}.{}.{}.nc'.format(dirin, case, stream, datestr)
            self._list_files(file_name_pattern)

            self.logger.info('Opening %d files: ', len(self._files))
            for n, file_name in enumerate(self._files): # pylint: disable=invalid-name
                self.logger.info('%d: %s', n+1, file_name)

            self.ds = xr.open_mfdataset(self._files, **xr_open_ds)

            tb_name = ''
            if 'bounds' in self.ds['time'].attrs:
                tb_name = self.ds['time'].attrs['bounds']
            elif 'time_bound' in self.ds:
                tb_name = 'time_bound'

            if variable_list:

                static_vars = [v for v, da in self.ds.variables.items()
                               if 'time' not in da.dims]
                self.logger.debug('static vars: %s', static_vars)

                keep_vars = ['time', tb_name]+variable_list+static_vars
                self.logger.debug('keep vars: %s', keep_vars)

                drop_vars = [v for v, da in self.ds.variables.items()
                             if 'time' in da.dims and v not in keep_vars]

                self.logger.debug('dropping vars: %s', drop_vars)
                self.ds = self.ds.drop(drop_vars)

        elif filetype == 'single_variable':

            if not variable_list:
                raise ValueError('Format %s requires variable_list.' % filetype)

            self.ds = xr.Dataset()
            for variable in variable_list:
                file_name_pattern = '{}/{}.{}.{}.{}.nc'.format(
                    dirin, case, stream, variable, datestr)
                self._list_files(file_name_pattern)
                self.ds = xr.merge((self.ds, xr.open_mfdataset(self._files, **xr_open_ds)))

        else:
            raise ValueError('Uknown format: %s' % filetype)

        #-- do unit conversions belong here?
        # maybe there should be a "conform_collections" method?
        if 'z_t' in self.ds:
            self.ds.z_t.values = self.ds.z_t.values * 1e-2

        # should this method handle making the 'time' variable functional?
        # (i.e., take mean of time_bound, convert to date object)

    def _list_files(self, glob_pattern):
        '''Glob for files and check that some were found.'''

        self.logger.debug('glob file search: %s', glob_pattern)
        self._files = sorted(glob.glob(glob_pattern))
        if not self._files:
            raise ValueError('No files: %s' % glob_pattern)

class WOA2013Data(BaseDataSource):
    """ Class built around reading World Ocean Atlas 2013 reanalysis """
    def __init__(self, **kwargs):
        super().__init__()
        self.woa_names = {'T':'t', 'S':'s', 'NO3':'n', 'O2':'o', 'O2sat':'O', 'AOU':'A', 'SiO3':'i', 'PO4':'p'}
        self.logger = logging.getLogger('WOA2013Data')
        self._get_dataset(**kwargs)

    def _get_dataset(self, varname_list, freq='ann', grid='1x1d'):
        """ docstring """
        mlperl_2_mmolm3 = 1.e6 / 1.e3 / 22.3916
        long_names = {'NO3':'Nitrate', 'O2':'Oxygen', 'O2sat':'Oxygen saturation', 'AOU':'AOU',
                      'SiO3':'Silicic acid', 'PO4':'Phosphate', 'S':'Salinity', 'T':'Temperature'}
        if not isinstance(varname_list, list):
            varname_list = [varname_list]

        self.ds = xr.Dataset()
        for varname in varname_list:
            v = self.woa_names[varname] # pylint: disable=invalid-name

            self._list_files(varname=varname, freq=freq, grid=grid)
            dsi = xr.open_mfdataset(self._files, decode_times=False)

            if f'{v}_an' in dsi.variables and varname != f'{v}_an':
                dsi.rename({'{}_an'.format(v):varname}, inplace=True)

            dsi = dsi.drop([k for k in dsi.variables if f'{v}_' in k])

            if varname in ['O2', 'AOU', 'O2sat']:
                dsi[varname] = dsi[varname] * mlperl_2_mmolm3
                dsi[varname].attrs['units'] = 'mmol m$^{-3}$'

            if dsi[varname].attrs['units'] == 'micromoles_per_liter':
                dsi[varname].attrs['units'] = 'mmol m$^{-3}$'
            dsi[varname].attrs['long_name'] = long_names[varname]

            if self.ds.variables:
                self.ds = xr.merge((self.ds, dsi))
            else:
                self.ds = dsi

    def _list_files(self, varname, freq='ann', grid='1x1d'):
        """ docstring """
        woapth = '/glade/work/mclong/woa2013v2'
        v = self.woa_names[varname] # pylint: disable=invalid-name

        if grid == '1x1d':
            res_code = '01'
        elif grid == 'POP_gx1v7':
            res_code = 'gx1v7'

        if v in ['t', 's']:
            files = ['woa13_decav_{}{}_{}v2.nc'.format(v, code, res_code) for code in woa_time_freq(freq)]
        elif v in ['o', 'p', 'n', 'i', 'O', 'A']:
            files = ['woa13_all_{}{}_{}.nc'.format(v, code, res_code) for code in woa_time_freq(freq)]
        else:
            raise ValueError('no file template defined')

        self._files = [os.path.join(woapth, grid, f) for f in files]

def woa_time_freq(freq):
    """ docstring """
    # 13: jfm, 14: amp, 15: jas, 16: ond

    if freq == 'ann':
        time_freq = ['00']
    elif freq == 'mon':
        time_freq = ['%02d' % m for m in range(1, 13)]
    elif freq == 'jfm':
        time_freq = ['13']
    elif freq == 'amp':
        time_freq = ['14']
    elif freq == 'jas':
        time_freq = ['15']
    elif freq == 'ond':
        time_freq = ['16']
    return time_freq