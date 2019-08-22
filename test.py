import json
import unittest
import os

from download_and_unzip_result import ObjectiveEvidence
from parse_result_json import Data


class TestResult(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bucket = 'compote-bldr-staging-result-s3'
        cls.flowcell = '000000000-{}'.format(os.getenv('flowcell'))
        cls.oe_ins = ObjectiveEvidence(cls.bucket, cls.flowcell)
        cls.oe_ins.download_dir()
        cls.oe_ins.unzip_file()

    def test_spaint0005_7(self):
        for unzipped_result_json_file in self.oe_ins.unzipped_result_json_files_path:
                data_ins = Data(unzipped_result_json_file)
                self.assertTrue(data_ins.existence_of_elements_for_spaint0005_7(self.oe_ins.flowcell))

    def test_spaint0005_8(self):
        for unzipped_result_json_file in self.oe_ins.unzipped_result_json_files_path:
                data_ins = Data(unzipped_result_json_file)
                self.assertTrue(data_ins.existence_of_elements_for_spaint0005_8)

    def test_spaint0005_10(self):
        for unzipped_result_json_file in self.oe_ins.unzipped_result_json_files_path:
                data_ins = Data(

                    unzipped_result_json_file)
                self.assertTrue(data_ins.existence_of_elements_for_spaint0005_10)

    def test_spaint0004_13(self):

        if self.oe_ins.unzipped_result_json_files_path:
            for unzipped_result_json_file in self.oe_ins.unzipped_result_json_files_path:
                data_ins = Data(unzipped_result_json_file)
                self.assertTrue(data_ins.spaint0004_13)
        else:
            self.fail('unzipped json files do not exist')


if __name__ == '__main__':
    unittest.main()
