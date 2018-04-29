from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm.attributes import flag_modified
from flask import request
import docker
import enum
import requests
import sys
import json
import os
import tempfile
import re
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from apscheduler.triggers.interval import IntervalTrigger
from SPARQLWrapper import SPARQLWrapper, CSV


TEST_MODE = True

################################################################################################
# STARTUP TESTS
################################################################################################
CONFIG_KEYS = [
    'PHT_PORT',
    'PHT_HOSTNAME',
    'PHT_PROTOCOL',
    'PHT_URI_STATION_OFFICE',
    'PHT_DOCKER_SOCKET_PATH',
    'PHT_STATION_NAME',
    'PHT_ENDPOINT_SPARQL',
    'PHT_SCRATCH'
]
for key in CONFIG_KEYS:
    if key not in os.environ:
        print("FATAL: Missing configuration env variable: {}".format(key), file=sys.stderr)
        sys.exit(1)

# TODO Add Regex tests for config values


################################################################################################
# HELPER FUNCTIONS
################################################################################################
def is_quoted(s):
    return (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'"))


def sanitize_config_value(config_value):
    config_value = config_value.strip()
    if is_quoted(config_value):
        config_value = config_value[1:-1]
    return config_value


def pprint(msg):
    print(msg, file=sys.stderr)
    sys.stderr.flush()


def clean_query(query):
    return re.sub("\s+", ' ', query)


KEEP_OPEN_CMD = "tail -f /dev/null"

################################################################################################
# Load Config
################################################################################################
PORT = int(sanitize_config_value(os.environ['PHT_PORT']))
HOSTNAME = sanitize_config_value(os.environ['PHT_HOSTNAME'])
PROTOCOL = sanitize_config_value(os.environ['PHT_PROTOCOL'])
URI_STATION_OFFICE = sanitize_config_value(os.environ['PHT_URI_STATION_OFFICE'])
DOCKER_SOCKET_PATH = sanitize_config_value(os.environ['PHT_DOCKER_SOCKET_PATH'])
STATION_NAME = sanitize_config_value(os.environ['PHT_STATION_NAME'])
ENDPOINT_SPARQL = sanitize_config_value(os.environ['PHT_ENDPOINT_SPARQL'])
SCRATCH = sanitize_config_value(os.environ['PHT_SCRATCH'])


# Derived config
URI_WEBHOOK = '{}://{}:{}/train'.format(PROTOCOL, HOSTNAME, PORT)

################################################################################################
#  TrainState
################################################################################################

class TrainState(enum.Enum):
    """
    Represents the state of each train job that is going to be executed at the station
    """
    TRAIN_REGISTERED = "TRAIN_REGISTERED"  # Train has been registered, application will try to download train

    PULL_BEING_PERFORMED = "PULL_BEING_PERFORMED"
    PULL_PERFORMED = "PULL_PERFORMED"                       # Train was downloaded from its registry

    SPARQL_BEING_FETCHED = "SPARQL_BEING_FETCHED"
    SPARQL_FETCHED = 'SPARQL_FETCHED'

    TRAIN_DATA_BEING_FETCHED = "TRAIN_DATA_BEING_FETCHED"  # Train data is being fetched from the SPARQL endpoint
    TRAIN_DATA_FETCHED = "TRAIN_DATA_FETCHED"

    TRAIN_PROCESSED_SUCCESS = "TRAIN_PROCESSED_SUCCESS"  # Train was successfully processed at the station
    TRAIN_PROCESSED_ERROR = "TRAIN_PROCESSED_ERROR"      # Execution of the train resulted in error state
    TRAIN_PUSHED_BACK = "TRAIN_PUSHED_BACK"              # Train has been pushed back to the station
    ERROR = "ERROR"                                      # Some other error


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite://"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Init Docker client
docker_client = docker.DockerClient(base_url='unix:/{}'.format(DOCKER_SOCKET_PATH))
cli = docker.APIClient(base_url='unix:/{}'.format(DOCKER_SOCKET_PATH))


class Train(db.Model):

    # Regular primary key column (only used internally in this database)
    id = db.Column(db.Integer, primary_key=True)

    # Id of the train (from the TrainSubmissionRecord)
    train_id = db.Column(db.String(80), unique=False, nullable=False)

    # Docker Registry URI (from the TrainSubmissionRecord)
    registry_uri = db.Column(db.String(80), unique=False, nullable=False)

    # Tag of the train that this station is allowed to use
    from_tag = db.Column(db.String(80), unique=False, nullable=False)

    # To Tag
    to_tag = db.Column(db.String(80), unique=False, nullable=False)

    # State of this archive job
    state = db.Column(db.Enum(TrainState))

    #The query to be executed
    query = db.Column(db.String(5000), unique=False, nullable=True)


db.drop_all()
db.create_all()


def create_dummy_job():
    train_id = 1000
    train_docker_registry_uri = '193.196.20.86'
    from_tag = '1'
    to_tag = '2'
    train_job = Train(train_id=train_id,
                      registry_uri=train_docker_registry_uri,
                      from_tag=from_tag,
                      to_tag=to_tag,
                      state=TrainState.TRAIN_REGISTERED)
    db.session.add(train_job)
    db.session.commit()
    print("Dummy Job Registered", file=sys.stderr)


if TEST_MODE:
    create_dummy_job()


def update_job_state(job, state, query=None):
    """
    Updates the job state in the persistence
    :param job:
    :param state:
    :param query
    :return:
    """
    job.state = state

    if query:
        job.query = query

    flag_modified(job, "state")

    if query:
        flag_modified(job, "query")

    db.session.merge(job)
    db.session.flush()
    db.session.commit()


@app.route('/train', methods=['POST'])
def train():
    train_visit = request.json
    print(train_visit, file=sys.stderr)
    return json.dumps({'success': True}), 200, {'ContentType': 'application/json'}


def register_request():
    """
    Request sent to the TrainController to keep this station registered
    """
    try:
        resp = requests.post(
            URI_STATION_OFFICE,
            json={"stationURI": URI_WEBHOOK, 'stationName': STATION_NAME})
    except requests.exceptions.ConnectionError:
        pass


def work_on_job(f, from_state, via_state, to_state):
    job = db.session.query(Train).filter_by(state=from_state).first()
    if job is not None:
        update_job_state(job, via_state)
        f(job)
        update_job_state(job, to_state)


# Define the individual steps here
# def dataCollection(endpoint, query):
# 	print("Data collection started: ")
# 	endpointURL = endpointParser(endpoint)
# 	#print(endpointURL)
# 	queryString = queryParser(query)
# 	#print(queryString)
#
# 	SPARQL = SPARQLWrapper(endpointURL)
# 	SPARQL.setQuery(queryString)
# 	SPARQL.setReturnFormat(JSON)
# 	results = SPARQL.query().convert()
#
# 	queryString = queryString[:queryString.find("WHERE")]
# 	schema = [i.replace("?", "") for i in re.findall("\?\w+", queryString)]
#
# 	with open('DIC.csv', 'w+') as csvfile:
# 		writer = csv.writer(csvfile, delimiter=',')
# 		writer.writerow([g for g in schema])
# 		#row = [result[column]["value"] for column in schema]
#
# 		for result in results["results"]["bindings"]:
# 			row = [result[column]["value"] for column in schema]
# 			writer.writerow(row)
#
# 		file_name = csvfile.name
# 		csvfile.close()
# 		print("	Data has been collected and saved! \n")
#		return file_name


scheduler = BackgroundScheduler()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())


def pull(job: Train):
    repository = job.registry_uri + "/" + job.train_id
    tag = job.from_tag
    docker_client.images.pull(repository, tag)


def fetch_sparql(job: Train):

    # Run the container and fetch the sparql query from the filesystem
    image = job.registry_uri + "/" + job.train_id + ":" + job.from_tag

    query = ''
    container_id = cli.create_container(
        image, KEEP_OPEN_CMD, detach=True, working_dir="/")['Id']
    cli.start(container_id)
    cmds = 'cat /query.sparql'  # comand to execute

    exe = cli.exec_create(container=container_id, cmd=cmds)
    exe_start = cli.exec_start(exec_id=exe, stream=True)

    for val in exe_start:
        query += val.decode("utf-8")

    job.query = clean_query(query)

    cli.stop(container_id)
    cli.remove_container(container_id)

    flag_modified(job, "query")
    db.session.merge(job)
    db.session.flush()
    db.session.commit()



def fetch_data(job: Train):

    query = job.query
    SPARQL = SPARQLWrapper(ENDPOINT_SPARQL)
    SPARQL.setQuery(query)
    SPARQL.setReturnFormat(CSV)
    results = SPARQL.query().convert()
    pprint(results)



scheduler.add_job(
    func=lambda: work_on_job(pull,
                             TrainState.TRAIN_REGISTERED,
                             TrainState.PULL_BEING_PERFORMED,
                             TrainState.PULL_PERFORMED),
    trigger=IntervalTrigger(seconds=3),
    id='pull',
    name='Loads the content from the submitted archive file',
    replace_existing=True)


scheduler.add_job(
    func=lambda: work_on_job(fetch_sparql,
                             TrainState.PULL_PERFORMED,
                             TrainState.SPARQL_BEING_FETCHED,
                             TrainState.SPARQL_FETCHED),
    trigger=IntervalTrigger(seconds=3),
    id='fetch_sparql',
    name='Loads the content from the submitted archive file',
    replace_existing=True)

scheduler.add_job(
    func=lambda: work_on_job(fetch_data,
                             TrainState.SPARQL_FETCHED,
                             TrainState.TRAIN_DATA_BEING_FETCHED,
                             TrainState.TRAIN_DATA_FETCHED),
    trigger=IntervalTrigger(seconds=3),
    id='fetch_train_data',
    name='Loads the content from the submitted archive file',
    replace_existing=True)


if __name__ == '__main__':
    app.run(port=PORT, host='0.0.0.0')
