#!/usr/bin/env python
"""
A script that does some basic unit testing on compute_mon_climatology()
"""

import sys
import xarray as xr
import numpy as np
from marbl_diags import generic_classes

# Create Unit Test child object of GenericDataSource
class UnitTestDataSource(generic_classes.GenericDataSource):
    """ Extend GenericDataSource for unit testing """
    def __init__(self):
        super(UnitTestDataSource, self).__init__(child_class='unit_test', source='memory')
        self._test_names = []
        self._test_results = []
        self.fail_cnt = 0

    def _set_var_dict(self):
        pass

    def _append_result(self, test_result):
        if test_result:
            result = 'PASS'
        else:
            result = 'FAIL'
            self.fail_cnt += 1
        self._test_results.append(result)

    def create_data_set(self):
        """ Create a small xarray dataset for testing """
        # Some arrays necessary to generate dataset
        start_date = np.array([0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334])
        start_date = np.append(start_date, start_date+365)
        end_date = np.array([31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365])
        end_date = np.append(end_date, end_date+365)

        # Populate ds
        self.ds = xr.Dataset(coords={'time':24, 'lat':1, 'lon':1, 'd2':2})
        self.ds['time'] = xr.DataArray(end_date, dims='time')
        self.ds['lat'] = xr.DataArray([0], dims='lat')
        self.ds['lon'] = xr.DataArray([0], dims='lon')
        self.ds['d2'] = xr.DataArray([0, 1], dims='d2')
        self.ds['time_bound'] = xr.DataArray(np.array([start_date, end_date]).transpose(), dims=['time', 'd2'])
        self.ds['var_to_average'] = xr.DataArray(np.append(np.zeros([12, 1, 1]), np.ones([12, 1, 1]), axis=0), dims=['time', 'lat', 'lon'])
        self.ds.time.attrs['units'] = "days since 0001-01-01 00:00:00"
        self.ds.time.attrs['calendar'] = "noleap"
        self.ds.time.attrs['bounds'] = "time_bound"

    def unit_tests(self):
        """ Run unit tests """
        # Create dataset:
        self.create_data_set()

        # Test 1: time dimension is len 24
        self._test_names.append('Initial time dimension is 24')
        self._append_result(self.ds.dims['time'] == 24)

        # Run compute_mon_climatology
        self.compute_mon_climatology()

        # Test 2: time dimension is now len 12
        self._test_names.append('After computing climatology, time dimension is 12')
        self._append_result(self.ds.dims['time'] == 12)

        # Test 3: All 'var_to_average' values are 1/2
        self._test_names.append('All climatological averages are 0.5')
        self._append_result(all(abs(self.ds.var_to_average.values - 0.5) < 1e-10))

    def print_test_results(self):
        """ print unit test results to screen """
        for n, (name, result) in enumerate(zip(self._test_names, self._test_results)):
            print('Test {} ({}): {}'.format(n+1, name, result))
        print('{} test failure(s)'.format(self.fail_cnt))

data_source = UnitTestDataSource()
data_source.unit_tests()
data_source.print_test_results()

sys.exit(min(data_source.fail_cnt,1))
