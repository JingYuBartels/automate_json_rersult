import argparse
import boto3
import json
import os
import zipfile

_PACKAGE_ROOT = os.path.dirname(os.path.realpath(__file__))


class ObjectiveEvidence:

    def __init__(self, bucket, flowcell):
        self.bucket = bucket
        self.flowcell = flowcell
        self.base_dir = os.path.join(_PACKAGE_ROOT, self.flowcell)
        self.client = boto3.client('s3')
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    @property
    def object_keys(self):
        kwargs = {
            'Bucket': self.bucket,
            'Prefix': self.flowcell
        }
        result = self.client.list_objects_v2(**kwargs)
        contents = result.get('Contents')
        for i in contents:
            k = i.get('Key')
            yield k

    @staticmethod
    def library_id(key):
        sample_index = key.index('sample')
        return key[slice(sample_index, sample_index+8)]

    def download_dir(self):
        for k in self.object_keys:
            file_name = os.path.basename(k)
            dest_pathname = os.path.join(self.base_dir, self.library_id(file_name), file_name)
            sample_dir = os.path.dirname(dest_pathname)
            if not os.path.exists(sample_dir):
                os.makedirs(sample_dir)
            if not os.path.isfile(dest_pathname):
                self.client.download_file(self.bucket, k, dest_pathname)

    @property
    def library_id_dirs(self):
        return os.listdir(self.base_dir)

    @property
    def library_id_dirs_path(self):
        for library_id_dir in self.library_id_dirs:
            path = os.path.join(self.base_dir, library_id_dir)
            if not path.endswith('.DS_Store'):
                yield path

    @property
    def zip_result_files_path(self):
        for library_id_dir_path in self.library_id_dirs_path:
            for file in os.listdir(library_id_dir_path):
                if file.endswith('results.zip'):
                    result_file_path = os.path.join(library_id_dir_path, file)
                    yield result_file_path

    def unzip_file(self):

        for zip_result_file in self.zip_result_files_path:
            library_id_dir_path, zip_file_name = os.path.split(zip_result_file)
            unzip_dir_name = zip_file_name.replace('.zip', '')
            unzip_file_dir = os.path.join(library_id_dir_path, unzip_dir_name)
            with zipfile.ZipFile(zip_result_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_file_dir)

    @property
    def unzip_dirs(self):
        for zip_result_file_path in self.zip_result_files_path:
            library_id_dir_path, zip_file_name = os.path.split(zip_result_file_path)
            unzip_dir_name = zip_file_name.replace('.zip', '')
            unzip_file_dir = os.path.join(library_id_dir_path, unzip_dir_name)
            yield unzip_file_dir

    @property
    def unzipped_result_json_files_path(self):
        for unzip_dir in self.unzip_dirs:
            library_id = os.path.basename(os.path.dirname(unzip_dir))
            result_file_name = library_id + '_S0_L001_R1_001.results.json'
            result_file_path = os.path.join(unzip_dir, result_file_name)
            yield result_file_path

    def get_evidence_for_spaint0005_7(self):

        for unzipped_result_json_file in self.unzipped_result_json_files_path:
            library_id = os.path.basename(os.path.dirname(os.path.dirname(unzipped_result_json_file)))
            with open(unzipped_result_json_file) as f:
                result_data = json.load(f)

            sequencer_id = result_data['sequencing_run']['metrics'][1]
            Q30 = result_data['sequencing_run']['metrics'][6]
            Q30_qc_threshold = result_data['sequencing_run']['qc']
            total_sample_complexity = result_data['sample']['metrics'][7]
            per_sample_read_count = result_data['sample']['metrics'][9]
            sample_qc = result_data['sample']['qc']
            sequencer_run_start_time = result_data['sequencing_run']['metrics'][3]
            sample_runner = result_data['sample']['metadata']['sample_runner']
            data = {
                'sequencing_run': {
                    'metrics': [sequencer_id, Q30, sequencer_run_start_time],
                    'qc': Q30_qc_threshold,
                },
                'sample': {
                    'metrics': [total_sample_complexity, per_sample_read_count],
                    'qc': sample_qc,
                    'sample_runner': sample_runner

                }
            }
            evidence_dir = os.path.join(self.base_dir, 'spaint0005_7', library_id)
            if not os.path.exists(evidence_dir):
                os.makedirs(evidence_dir)
            evidence_path = os.path.join(evidence_dir, library_id + '.json')
            with open(evidence_path, 'w') as f:
                json.dump(data, f, indent=2)


def shellmain():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--s3_bucket",
        help='The s3 bucket from where we download files',
        default='compote-bldr-staging-result-s3'
    )
    parser.add_argument(
        '--flowcell',
        help='The flowcell of the current sequencer run'
    )
    args = parser.parse_args()
    my_ins = ObjectiveEvidence(args.s3_bucket, args.prefix)
    my_ins.download_dir()
    my_ins.unzip_file()
    my_ins.get_evidence_for_spaint0005_7()


if __name__ == '__main__':
    shellmain()
