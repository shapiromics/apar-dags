from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
from zipfile import ZipFile

class ZipOperator(BaseOperator):
    """
    From the zip_operator_plugin by rssanders3
    https://github.com/rssanders3/airflow-zip-operator-plugin/
    """

    @apply_defaults
    def __init__(self,
                path_to_zip,
                path_to_save,
                *args, **kwargs):
        super(ZipOperator, self).__init__(*args, **kwargs)
        self.path_to_zip = path_to_zip
        self.path_to_save = path_to_save

    def execute(self, context):
        self.log.info("Running ZipOperator")

        file_to_zip = os.path.basename(self.path_to_size)
        file_to_save = os.path.basename(self.path_to_save)

        dir_path_to_zip = os.path.dirname(os.path.abspath(self.path_to_zip))
        os.chdir(dir_path_to_zip)

        with ZipFile(file_to_save, 'w') as zip_file:
            
            if os.path.isfile(self.path_to_zip):
                zip_file.write(file_to_zip)
            else:
                for dirname, subdirs, files in os.walk(file_to_zip):
                    zip_file.write(dirname)
                    for file in files:
                        file_to_write = os.path.join(dirname, file)
                        zip_file.write(file_to_write)

            zip_file.close()

        os.rename(file_to_save, self.path_to_save)
        self.log.info("ZipOperator is finished.")