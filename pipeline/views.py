import os
import logging
import flask
from flask import views
import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Split(apache_beam.DoFn):

    def process(self, element):
        """
        PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
        """
        
        passengerid,survived,pclass,name,sex,age,sibsp,parch,ticket,fare,cabin,embarked = element.split(",")
        
        return [{
            'passengerid': int(passengerid),
            'survived': bool(int(survived)),
            'pclass': pclass.encode('ascii'),
            'name': name.encode('ascii'),
            'sex': sex.encode('ascii'),
            'age': age.encode('ascii'),
            'sibsp': sibsp.encode('ascii'),
            'parch': parch.encode('ascii'),
            'ticket': ticket.encode('ascii'),
            'fare': fare.encode('ascii'),
            'cabin': cabin.encode('ascii'),
            'embarked': embarked.encode('ascii')
        }]


class CollectSurvived(apache_beam.DoFn):

    def process(self, element):
        """
            Returns a list of tuples containing name and sex
        """
        print(element["survived"])

        if element["survived"] == True:
            result = [
                (element['name'], element['sex'])
            ]
            return result
        else:
            return None


class WriteToCSV(apache_beam.DoFn):

    def process(self, element):
        result = [
            "{},{},{}".format(
                1,
                element[0],
                element[1]
            )
        ]
        return result


class FromTextView(views.MethodView):

    def get(self):
        """
        Flask view that triggers the execution of the pipeline
        """
        input_filename = 'data/input/titanic.txt'
        output_filename = 'data/output/titanic.txt'

        # project_id = os.environ['DATASTORE_PROJECT_ID']
        # credentials_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        # client = datastore.Client.from_service_account_json(credentials_file)

        options = PipelineOptions()
        gcloud_options = options.view_as(GoogleCloudOptions)
        # gcloud_options.project = project_id
        gcloud_options.job_name = 'test-job'

        # Dataflow runner
        runner = os.environ['DATAFLOW_RUNNER']
        options.view_as(StandardOptions).runner = runner

        with apache_beam.Pipeline(options=options) as p:
            rows = (
                p |
                ReadFromText(input_filename) |
                apache_beam.ParDo(Split())
            )

            survived = (
                rows |
                apache_beam.ParDo(CollectSurvived()) |
                apache_beam.GroupByKey() |
                apache_beam.ParDo(WriteToCSV()) |
                WriteToText(output_filename)
            )

        return 'All Titanic survivors are writte to data/output/titanic.txt-00000-of-00001'
