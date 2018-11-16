#! /usr/bin/env python
""" docstring"""

import logging
import analysis_elements_class

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s (%(funcName)s): %(message)s', level=logging.DEBUG)
    AnalysisElement = analysis_elements_class.AnalysisElements('input.yml') # pylint: disable=invalid-name
    AnalysisElement.do_analysis()
