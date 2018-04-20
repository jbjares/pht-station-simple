from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm.attributes import flag_modified
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import docker
import enum
import requests


class JobState(enum.Enum):
    """
    Represents the state of each train job that is going to be executed at the station
    """
    TRAIN_REGISTERED = "TRAIN_REGISTERED"  # Train has been registered, application will try to download the train
    TRAIN_DOWNLOADED = "TRAIN_DOWNLOADED"  # Train was downloaded from its registry
    TRAIN_PROCESSED_SUCCESS = "TRAIN_PROCESSED_SUCCESS"  # Train was successfully processed at the station
    TRAIN_PROCESSED_ERROR = "TRAIN_PROCESSED_ERROR"      # Execution of the train resulted in error state
    TRAIN_PUSHED_BACK = "TRAIN_PUSHED_BACK"              # Train has been pushed back to the station
    ERROR = "ERROR"                                      # Some other error


app = Flask(__name__)

# Configure station route of train controller and train route here
app.config.from_envvar('TRAIN_SIMPLE_STATION_CONFIG_FILE')
PROTOCOL = app.config['PROTOCOL']
HOSTNAME = app.config['HOSTNAME']
PORT = app.config['PORT']

app.config['URI_WEBHOOK'] = '{}://{}:{}/train'.format(PROTOCOL, HOSTNAME, PORT)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////data/train.db'
db = SQLAlchemy(app)

# Init Docker client
docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')


@app.before_first_request
def setup_database():
    db.create_all()


class Train(db.Model):

    # Regular primary key column (only used internally in this database)
    id = db.Column(db.Integer, primary_key=True)

    # Id of the train (from the TrainSubmissionRecord)
    trainID = db.Column(db.String(80), unique=True, nullable=False)

    # Docker Registry URI (from the TrainSubmissionRecord)
    registry_uri = db.Column(db.String(80), unique=True, nullable=False)

    # Tag of the train that this station is allowed to use
    train_tag = db.Column(db.String(80), unique=False, nullable=False)

    # State of this archive job
    state = db.Column(db.Enum(JobState))

    def serialize(self):
        return {
            'id': self.id,
            'trainID': self.trainID,
            'registry_uri': self.registry_uri,
            'train_tag': self.filepath,
            'state': str(self.state)
        }


def update_job_state(job, state):
    """
    Updates the job state in the persistence
    :param job:
    :param state:
    :return:
    """
    job.state = state
    flag_modified(job, "state")
    db.session.merge(job)
    db.session.flush()
    db.session.commit()


@app.route('/train', methods=['POST'])
def train():
    print("Message received")


def register_request():
    """
    Request sent to the TrainController to keep this station registered
    """
    requests.post(
        app.config['URI_TRAINCONTROLLER'],
        json={"uri": app.config['URI_WEBHOOK']})


# Start the AP Scheduler
scheduler = BackgroundScheduler()
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

# Add the register_request job
scheduler.add_job(
    func=register_request,
    trigger=IntervalTrigger(seconds=3),
    id='register_request',
    name='Loads the content from the submitted archive file',
    replace_existing=True)


if __name__ == '__main__':
    app.run(port=PORT, host='0.0.0.0')


