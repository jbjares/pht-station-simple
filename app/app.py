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
import re
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from apscheduler.triggers.interval import IntervalTrigger
from SPARQLWrapper import SPARQLWrapper, CSV
from docker.types import Mount


TEST_MODE = False

# Path to the SPARQL query in the train container
PATH_SPARQL = '/train_package/query.sparql'

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
    'PHT_DATA_DIR'
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


def remove_trailing_slash(text: str):
    return text[:-1] if text.endswith('/') else text


def build_image_name(remote, image_name, tag=None):
    base = remove_trailing_slash(remote) + '/' + image_name
    if tag:
        base += ( ':' + tag)
    return base


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
DATA_DIR = sanitize_config_value(os.environ['PHT_DATA_DIR'])


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

    TRAIN_ALGORITHM_BEING_EXECUTED = "TRAIN_ALGORITHM_BEING_EXECUTED"
    TRAIN_ALGORITHM_EXECUTED = 'TRAIN_ALGORITHM_EXECUTED'

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

    # The query to be executed
    query = db.Column(db.String(5000), unique=False, nullable=True)

    # Data dir (The directory where the data for the train is mounted)
    job_data = db.Column(db.String(400), unique=True, nullable=True)

    def from_image(self, tag=True):
        return build_image_name(self.registry_uri, str(self.train_id), self.from_tag if tag else None)

    def to_image(self, tag=True):
        return build_image_name(self.registry_uri, str(self.train_id), self.to_tag if tag else None)


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


def update_job_property(job: Train, prop: str, value):

    job.__setattr__(prop, value)
    flag_modified(job, prop)
    db.session.merge(job)
    db.session.flush()
    db.session.commit()


def update_job_state(job, state):
    update_job_property(job, 'state', state)


def update_job_query(job, query):
    update_job_property(job, 'query', query)


def update_job_data(job, job_data):
    update_job_property(job, 'job_data', job_data)


@app.route('/train', methods=['POST'])
def train():
    train_visit = request.json

    # Construct new train from train_visit_push
    train_id = train_visit['trainID']
    train_docker_registry_uri = train_visit['trainDockerRegistryURI']
    from_tag = train_visit['fromTag']
    to_tag = train_visit['toTag']
    existing_train = db.session.query(Train).filter_by(from_tag=from_tag, to_tag=to_tag, train_id=train_id).first()

    # Add the train to database if we do not have it yet
    # TODO Needs to be synchronized
    if existing_train is None:
        train_job = Train(train_id=train_id,
                          registry_uri=train_docker_registry_uri,
                          from_tag=from_tag,
                          to_tag=to_tag,
                          state=TrainState.TRAIN_REGISTERED)
        db.session.add(train_job)
        db.session.commit()

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


scheduler = BackgroundScheduler()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())


def pull(job: Train):
    repository = job.registry_uri + "/" + job.train_id
    tag = job.from_tag
    docker_client.images.pull(repository, tag)


def fetch_sparql(job: Train):

    # Run the container and fetch the sparql query from the filesystem
    image = job.from_image()
    container_id = cli.create_container(image, KEEP_OPEN_CMD,
                                        detach=True,
                                        working_dir="/",
                                        entrypoint=KEEP_OPEN_CMD)['Id']

    cli.start(container_id)

    exe = cli.exec_create(container=container_id, cmd='cat {}'.format(PATH_SPARQL))
    exe_start = cli.exec_start(exec_id=exe, stream=True)

    query = ''
    for val in exe_start:
        query += val.decode("utf-8")

    cli.stop(container_id)
    cli.remove_container(container_id)

    update_job_query(job, clean_query(query))


def fetch_data(job: Train):

    # Set up the SPARQL command (only support CSV for now)
    sparql_command = SPARQLWrapper(ENDPOINT_SPARQL)
    sparql_command.setQuery(str(job.query))
    sparql_command.setReturnFormat(CSV)
    results = sparql_command.query().convert().split()

    # Create a directory for this train in the data dir (use the job id for this)
    job_data = os.path.abspath(os.path.join(DATA_DIR, str(job.id)))

    # write the results to the data file
    with open(job_data, 'w') as f:
        for line in results:
            f.write(line.decode("utf-8"))
            f.write(os.linesep)

    update_job_data(job, job_data)


def run_algorithm(job: Train):

    image = job.from_image()

    # Define the mount point inside the container
    mount_data = Mount(target='/data/data.csv',   # Mount data to /data/data.csv as bind mount
                       source=str(job.job_data),
                       type='bind',
                       read_only=True)

    # Run the container and mount the data
    running_algorithm = docker_client.containers.run(image, mounts=[mount_data], detach=True)

    # TODO Timeout!
    exit_code = running_algorithm.wait()

    # Check the status code. If 0, we can commit and push the new train
    if exit_code['StatusCode'] == 0:
        repository = job.to_image(tag=False)
        tag = job.to_tag
        running_algorithm.commit(repository, tag)
        push_result = docker_client.images.push(repository, tag)
        pprint(push_result)


# Define the jobs
def job(name, fun, start, via, end, max_instances=4, replace_existing=True, trigger=IntervalTrigger(seconds=3)):
    return {
        'trigger': trigger,
        'name': name,
        'max_instances': max_instances,
        'replace_existing': replace_existing,
        'func': lambda: work_on_job(fun, start, via, end)
    }


jobs = {

    "pull": job(
        name='Pulls the train from the Docker Registry',
        fun=pull,
        start=TrainState.TRAIN_REGISTERED,
        via=TrainState.PULL_BEING_PERFORMED,
        end=TrainState.PULL_PERFORMED),

    "fetch_sparql": job(
        name='Extracts the SPARQL query from the downloaded image and stores them in the job database',
        fun=fetch_sparql,
        start=TrainState.PULL_PERFORMED,
        via=TrainState.SPARQL_BEING_FETCHED,
        end=TrainState.SPARQL_FETCHED),

    'fetch_data': job(
        name='Gets the data from the SPARQL endpoint that belongs to this station',
        fun=fetch_data,
        start=TrainState.SPARQL_FETCHED,
        via=TrainState.TRAIN_DATA_BEING_FETCHED,
        end=TrainState.TRAIN_DATA_FETCHED),

    'run_algorithm': job(
        name='Gets the data from the SPARQL endpoint that belongs to this station',
        fun=run_algorithm,
        start=TrainState.TRAIN_DATA_FETCHED,
        via=TrainState.TRAIN_ALGORITHM_BEING_EXECUTED,
        end=TrainState.TRAIN_ALGORITHM_EXECUTED)
}


# Register all jobs
for (key, value) in jobs.items():

    value['id'] = key
    scheduler.add_job(**value)


if __name__ == '__main__':
    app.run(port=PORT, host='0.0.0.0')
