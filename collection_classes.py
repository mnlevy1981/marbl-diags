""" These classes build on GenericCollection to open data from specific sources """

import glob
import logging
import os
import xarray as xr
from generic_classes import GenericCollection

class CachedData(GenericCollection):
    """ Class built around reading previously-cached data """
    def __init__(self, data_root, data_type, **kwargs):
        super(CachedData, self).__init__(**kwargs)
        self.logger = logging.getLogger('CachedData')
        self._get_dataset(data_root, data_type)

    def _get_dataset(self, data_root, data_type):
        self.logger.info('calling _get_dataset, data_type = %s', data_type)
        if data_type == 'zarr':
            self.ds = xr.open_zarr(data_root, decode_times=False, decode_coords=False) # pylint: disable=invalid-name

class CESMData(GenericCollection):
    """ Class built around reading CESM history files """
    def __init__(self, **kwargs):
        super(CESMData, self).__init__(**kwargs)
        self.logger = logging.getLogger('CESMData')
        self._get_dataset(**kwargs['open_dataset'])

    def _get_dataset(self, filetype, dirin, case, stream, datestr, variable_dict):
        """ docstring """
        xr_open_ds = {'decode_coords' : False, 'decode_times' : False, 'data_vars' : 'minimal'}
        if isinstance(datestr, str):
            datestr = [datestr]

        if filetype == 'hist':

            file_name_pattern = []
            for date_str in datestr:
                file_name_pattern.append('{}/{}.{}.{}.nc'.format(dirin, case, stream, date_str))
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

            static_vars = [v for v, da in self.ds.variables.items()
                           if 'time' not in da.dims]
            self.logger.debug('static vars: %s', static_vars)

            keep_vars = ['time', tb_name]+[var for var in variable_dict.values()]+static_vars
            self.logger.debug('keep vars: %s', keep_vars)

            drop_vars = [v for v, da in self.ds.variables.items()
                         if 'time' in da.dims and v not in keep_vars]

            self.logger.debug('dropping vars: %s', drop_vars)
            self.ds = self.ds.drop(drop_vars)

        elif filetype == 'climo':

            file_name_pattern = []
            for date_str in datestr:
                file_name_pattern.append('{}/{}.{}.nc'.format(dirin, stream, date_str))
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

            static_vars = [v for v, da in self.ds.variables.items()
                           if 'time' not in da.dims]
            self.logger.debug('static vars: %s', static_vars)

            keep_vars = ['time', tb_name]+[var for var in variable_dict.values()]+static_vars
            self.logger.debug('keep vars: %s', keep_vars)

            drop_vars = [v for v, da in self.ds.variables.items()
                         if 'time' in da.dims and v not in keep_vars]

            self.logger.debug('dropping vars: %s', drop_vars)
            self.ds = self.ds.drop(drop_vars)

        elif filetype == 'single_variable':

            if not variable_dict:
                raise ValueError('Format %s requires variable_dict.' % filetype)

            self.ds = xr.Dataset()
            for variable in variable_dict.values():
                file_name_pattern = []
                for date_str in datestr:
                    file_name_pattern.append('{}/{}.{}.{}.{}.nc'.format(
                        dirin, case, stream, variable, date_str))
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

        self._files = []
        for glob_pat in glob_pattern:
            self.logger.debug('glob file search: %s', glob_pat)
            self._files += sorted(glob.glob(glob_pat))
        if not self._files:
            raise ValueError('No files: %s' % glob_pattern)

class WOA2013Data(GenericCollection):
    """ Class built around reading World Ocean Atlas 2013 reanalysis """
    def __init__(self, var_dict, **kwargs):
        super(WOA2013Data, self).__init__(**kwargs)
        self._set_woa_names()
        self.logger = logging.getLogger('WOA2013Data')
        self._get_dataset(var_dict, **kwargs['open_dataset'])

    def _set_woa_names(self):
        """ Define the _woa_names dictionary """
        # self.woa_names = {'T':'t', 'S':'s', 'NO3':'n', 'O2':'o', 'O2sat':'O', 'AOU':'A',
        #                   'SiO3':'i', 'PO4':'p'}
        self._woa_names = dict()
        self._woa_names['nitrate'] = 'n'
        self._woa_names['phosphate'] = 'p'
        self._woa_names['oxygen'] = 'o'
        self._woa_names['silicate'] = 'i'

    def _get_dataset(self, var_dict, dirin, variable_dict, freq='ann', grid='1x1d'):
        """ docstring """
        mlperl_2_mmolm3 = 1.e6 / 1.e3 / 22.3916
        long_names = {'NO3':'Nitrate', 'O2':'Oxygen', 'O2sat':'Oxygen saturation', 'AOU':'AOU',
                      'SiO3':'Silicic acid', 'PO4':'Phosphate', 'S':'Salinity', 'T':'Temperature'}

        self.ds = xr.Dataset()
        for varname_generic, varname in variable_dict.items():
            v = self._woa_names[varname_generic] # pylint: disable=invalid-name

            self._list_files(dirin=dirin, v=v, freq=freq, grid=grid)
            dsi = xr.open_mfdataset(self._files, decode_times=False)

            if '{}_an'.format(v) in dsi.variables and varname != '{}_an'.format(v):
                dsi.rename({'{}_an'.format(v):varname}, inplace=True)

            dsi = dsi.drop([k for k in dsi.variables if '{}_'.format(v) in k])

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

    def _list_files(self, dirin, v, freq='ann', grid='1x1d'):
        """ docstring """

        if grid == '1x1d':
            res_code = '01'
        elif grid == 'POP_gx1v7':
            res_code = 'gx1v7'

        files = []
        for code in woa_time_freq(freq):
            if v in ['t', 's']:
                files.append('woa13_decav_{}{}_{}v2.nc'.format(v, code, res_code))
            elif v in ['o', 'p', 'n', 'i', 'O', 'A']:
                files.append('woa13_all_{}{}_{}.nc'.format(v, code, res_code))
            else:
                raise ValueError('no file template defined for {}'.format(v))

        self._files = [os.path.join(dirin, grid, f) for f in files]

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
