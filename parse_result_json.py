import json
import time
import os

from selenium import webdriver


class Data:

    def __init__(self, result_json):

        self.result_json = result_json
        with open(self.result_json) as f:
            self.result_data = json.load(f)

    @staticmethod
    def check_key_value_pair_existence(data_set, key, element=None):
        for item in data_set:
            try:
                metric = item[key]
                value = item['value']
                if metric == element and value:
                    return True
            except KeyError:
                return False

    @staticmethod
    def check_key_existence(field, data_set):
        return field in data_set

    @property
    def sequencer_id_and_value_existence(self):
        return self.check_key_value_pair_existence(self.result_data['sequencing_run']['metrics'], 'metric',
                                                   'Sequencer ID')

    @property
    def q30_and_value_existence(self):
        return self.check_key_value_pair_existence(self.result_data['sequencing_run']['metrics'], 'metric', '% >= Q30')

    @property
    def sequencing_run_qc_existence(self):
        return self.check_key_existence('qc', self.result_data['sequencing_run'])

    @property
    def metrics_in_q30_qc_existence(self):
        if self.sequencing_run_qc_existence:
            return self.check_key_existence('metrics', self.result_data['sequencing_run']['qc'])
        else:
            return False

    @property
    def q30_in_qc_threshold_existence(self):
        if self.metrics_in_q30_qc_existence:
            return self.check_key_value_pair_existence(self.result_data['sequencing_run']['qc']['metrics'],
                                                       'metric', '% >= Q30')
        else:
            return False

    @property
    def q30_critersion_result_value_in_existence_and_value_not_none(self):
        if self.metrics_in_q30_qc_existence:
            return not self.check_key_value_pair_existence(
                self.result_data['sequencing_run']['qc']['metrics'], 'criterion') and \
                   not self.check_key_value_pair_existence(
                       self.result_data['sequencing_run']['qc']['metrics'], 'result') and \
                   not self.check_key_value_pair_existence(
                       self.result_data['sequencing_run']['qc']['metrics'], 'value')
        else:
            return False

    @property
    def q30_and_associated_qc_threshold(self):
        return self.q30_and_value_existence and self.q30_in_qc_threshold_existence and \
               self.q30_critersion_result_value_in_existence_and_value_not_none

    @property
    def sequencer_run_start_time_and_value_existence(self):
        return self.check_key_value_pair_existence(self.result_data['sequencing_run']['metrics'], 'metric',
                                                   'Run start time')

    @property
    def total_sample_complexity_and_value_existence(self):
        return self.check_key_value_pair_existence(
            self.result_data['sample']['metrics'], 'metric', 'Total Sample Complexity')

    @property
    def sample_qc_existence(self):
        return self.check_key_existence('qc', self.result_data['sample'])

    @property
    def metrics_in_sample_qc_existence(self):
        if self.sample_qc_existence:
            return self.check_key_existence('metrics', self.result_data['sample']['qc'])
        else:
            return False

    @property
    def total_sample_complexity_in_qc_threshold_existence(self):
        if self.metrics_in_sample_qc_existence:
            return self.check_key_value_pair_existence(self.result_data['sample']['qc']['metrics'],
                                                       'metric', 'Total Sample Complexity')
        else:
            return False

    @property
    def total_sample_complexity_critersion_result_value_key_existence_and_value_not_none(self):
        if self.total_sample_complexity_in_qc_threshold_existence:
            for item in self.result_data['sample']['qc']['metrics']:
                if item['metric'] == 'Total Sample Complexity':
                    try:
                        return item['criterion'] is not None and item['result'] is not None and \
                               item['value'] is not None
                    except KeyError:
                        return False
        else:
            return False

    @property
    def total_sample_complexity_and_associated_qc_threshold_existence(self):
        return self.total_sample_complexity_and_value_existence and \
               self.total_sample_complexity_in_qc_threshold_existence and \
               self.total_sample_complexity_critersion_result_value_key_existence_and_value_not_none

    @property
    def per_sample_read_count_and_value_existence(self):
        return self.check_key_value_pair_existence(
            self.result_data['sample']['metrics'], 'metric', 'Per-Sample Read Count')

    @property
    def per_sample_read_count_in_qc_threshold_existence(self):
        if self.metrics_in_sample_qc_existence:
            return self.check_key_value_pair_existence(self.result_data['sample']['qc']['metrics'],
                                                       'metric', 'Per-Sample Read Count')
        else:
            return False

    @property
    def per_sample_read_count_critersion_result_value_key_existence_and_value_not_none(self):
        if self.total_sample_complexity_in_qc_threshold_existence:
            for item in self.result_data['sample']['qc']['metrics']:
                if item['metric'] == 'Per-Sample Read Count':
                    try:
                        return item['criterion'] is not None and item['result'] is not None and \
                               item['value'] is not None
                    except KeyError:
                        return False

    @property
    def per_sample_read_count_and_associated_qc_threshold_existence(self):
        return self.per_sample_read_count_and_value_existence and \
               self.per_sample_read_count_in_qc_threshold_existence and \
               self.per_sample_read_count_critersion_result_value_key_existence_and_value_not_none

    def flowcell_and_value_existence(self, flowcell):
        try:
            return self.result_data['sample']['metadata']['sample_runner']['flowcell'] == flowcell
        except KeyError:
            return False

    @property
    def demux_counter_and_value_existance(self):
        try:
            return self.result_data['sample']['metadata']['sample_runner']['demux_counter'] == '1'
        except KeyError:
            return False

    @property
    def ssr_version_and_value_existance(self):
        try:
            return self.result_data['sample']['metadata']['sample_runner']['ssr_version'] == '0.0.12.post1'
        except KeyError:
            return False

    @property
    def aggregated_result_and_value_existence(self):
        if self.sample_qc_existence:
            return self.check_key_value_pair_existence(self.result_data['sample']['qc'], 'aggregated_result', 'PASS')
        else:
            return False

    def existence_of_elements_for_spaint0005_7(self, flowcell):
        return self.sequencer_id_and_value_existence and \
               self.q30_and_associated_qc_threshold and \
               self.total_sample_complexity_and_associated_qc_threshold_existence and \
               self.per_sample_read_count_and_associated_qc_threshold_existence and \
               self.sequencer_run_start_time_and_value_existence and \
               self.flowcell_and_value_existence(flowcell) and \
               self.demux_counter_and_value_existance and \
               self.ssr_version_and_value_existance

    @property
    def metamp_ide_study_existence(self):

        for item in self.result_data['sample']['analyte_groups']:

            try:
                return item['analyte_group'] == 'METamp IDE Study'
            except KeyError:
                return False

    @property
    def analyte_for_metamp_existence(self):

        for item in self.result_data['sample']['analyte_groups']['analytes']:
            try:
                return item['analyte'] == 'METamp'
            except KeyError:
                return False

    def existence_of_elements_for_spaint0005_8(self):

        return self.metamp_ide_study_existence and self.analyte_for_metamp_existence

    @property
    def flowcell_from_result_json(self):

        return self.result_data['sample']['metadata']['sample_runner']['flowcell']

    @property
    def demux_counter_from_result_json(self):
        return self.result_data['sample']['metadata']['sample_runner']['demux_counter']

    @property
    def library_id_from_result_json(self):
        return self.result_data['sample']['metadata']['sample_runner']['library']

    def library_id_embedded_in_fastq_file(self, fastq):
        for item in self.result_data['sample']['metadata']['inputs']:
            try:
                fastq_s3_object_key = item['key']
                if fastq in fastq_s3_object_key:
                    fastq_file_name = os.path.basename(fastq_s3_object_key)
                    library_id = fastq_file_name.split('_')[0]
                    return library_id
            except KeyError:
                return False

    @property
    def library_id_embeded_in_r1_fastq_file_name(self):
        return self.library_id_embedded_in_fastq_file('R1_001.fastq.gz')


    @property
    def library_id_embeded_in_r2_fastq_file_name(self):
        return self.library_id_embedded_in_fastq_file('R2_001.fastq.gz')

    def spaint0004_13(self):
        return self.library_id_from_result_json is not None and \
               self.library_id_from_result_json == self.library_id_embeded_in_r1_fastq_file_name and \
               self.library_id_from_result_json == self.library_id_embeded_in_r2_fastq_file_name


    @property
    def pipeline_name_from_result_json(self):
        return self.result_data['sample']['metadata']['sample_runner']['pipeline']

    @property
    def pipeline_version_from_result_json(self):
        return self.result_data['sample']['metadata']['sample_runner']['pipeline_version']

    @staticmethod
    def log_in_aws(flowcell):
        driver = webdriver.Firefox()
        driver.get('https://aws.amazon.com/')
        driver.find_element_by_xpath('//a[@role="button"]').click()
        driver.find_element_by_id('account').send_keys('859070845359')
        driver.find_element_by_id('username').send_keys('ompote-bldr-staging-admin1')
        driver.find_element_by_id('password').send_keys('P@ss1234')
        driver.find_element_by_id('signin_button').click()
        driver.find_element_by_id('search-box-input').send_keys('S3')
        driver.find_element_by_class_name('awsui-select-option-label-content').click()
        time.sleep(3)
        driver.find_element_by_xpath('//a[contains(text(), "compote-bldr-staging-result-s3")]').click()
        driver.find_element_by_xpath('//a[contains(text(), {})]'.format(flowcell))

    def exsitence_of_field_in_result_json_for_different_obj(self, field, obj):
        for item in self.result_data['sample']['metadata']['inputs']:
            if obj in item['key']:
                try:
                    return item[field] is not None
                except KeyError:
                    return False

    @property
    def existence_of_r1_fastq_s3_object_key_and_value(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('key', 'R1_001.fastq')

    @property
    def existence_of_r1_fastq_s3_version_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('version', 'R1_001.fastq')

    @property
    def existence_of_r1_fastq_s3_bucket_name_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('bucket', 'R1_001.fastq')

    @property
    def existence_of_r1_fastq_expected_checksum_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('expected_checksum', 'R1_001.fastq')

    @property
    def existence_of_r1_fastq_observed_checksum_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('expected_checksum', 'R1_001.fastq')

    @property
    def existence_of_fields_associated_with_r1_fastq(self):
        return self.existence_of_r1_fastq_s3_object_key_and_value and \
               self.existence_of_r1_fastq_s3_version_in_result_json and \
               self.existence_of_r1_fastq_s3_bucket_name_in_result_json and \
               self.existence_of_r1_fastq_expected_checksum_in_result_json and \
               self.existence_of_r1_fastq_observed_checksum_in_result_json

    @property
    def existence_of_r2_fastq_s3_object_key_and_value(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('key', 'R2_001.fastq')

    @property
    def existence_of_r2_fastq_s3_version_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('version', 'R2_001.fastq')

    @property
    def existence_of_r2_fastq_s3_bucket_name_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('bucket', 'R2_001.fastq')

    @property
    def existence_of_r2_fastq_expected_checksum_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('expected_checksum', 'R2_001.fastq')

    @property
    def existence_of_r2_fastq_observed_checksum_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('expected_checksum', 'R2_001.fastq')

    @property
    def existence_of_fields_associated_with_r2_fastq(self):
        return self.existence_of_r2_fastq_s3_object_key_and_value and \
               self.existence_of_r2_fastq_s3_version_in_result_json and \
               self.existence_of_r2_fastq_s3_bucket_name_in_result_json and \
               self.existence_of_r2_fastq_expected_checksum_in_result_json and \
               self.existence_of_r2_fastq_observed_checksum_in_result_json

    @property
    def existence_of_sequencer_metric_s3_object_key_and_value_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('key', 'sequencer_metrics.tsv')

    @property
    def existence_of_sequencer_metric_s3_version_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('version', 'sequencer_metrics.tsv')

    @property
    def existence_of_sequencer_metric_s3_bucket_name_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('bucket', 'sequencer_metrics.tsv')

    @property
    def existence_of_sequencer_metric_expected_checksum_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('expected_checksum', 'sequencer_metrics.tsv')

    @property
    def existence_of_sequencer_metric_observed_checksum_in_result_json(self):
        return self.exsitence_of_field_in_result_json_for_different_obj('observed_checksum', 'sequencer_metrics.tsv')

    @property
    def existence_of_fields_associated_with_sequencer_metrics(self):
        return self.existence_of_sequencer_metric_s3_object_key_and_value_in_result_json and \
               self.existence_of_sequencer_metric_s3_version_in_result_json and \
               self.existence_of_sequencer_metric_s3_bucket_name_in_result_json and \
               self.existence_of_sequencer_metric_expected_checksum_in_result_json and \
               self.existence_of_sequencer_metric_observed_checksum_in_result_json

    @property
    def existence_of_elements_for_spaint0005_10(self):
        return self.existence_of_fields_associated_with_r1_fastq and \
               self.existence_of_fields_associated_with_r2_fastq and \
               self.existence_of_fields_associated_with_sequencer_metrics


if __name__ == '__main__':
    data = Data('./000000000-mId7T/sample-1/000000000-mId7T_sample-1_results/sample-1_S0_L001_R1_001.results.json')
    print(data.log_in_aws('mId7T'))
