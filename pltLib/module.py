try:
    from streamsets.sdk.config import *
except:
    pass
try:
    from streamsets.sdk.examples import *
except:
    pass

import streamsets
import streamsets.sdk
from cryptography.fernet import *
from streamsets.sdk import *
from streamsets.sdk.sch_models import *
from streamsets.sdk.utils import *
from streamsets.sdk.sch import *
from streamsets.sdk.sch_models import *
from streamsets.sdk.sdc import *
from streamsets.sdk.utils import *

warnings.filterwarnings("ignore")

os.environ["CYPHER_KEY"] = "CHjQEGk9Cvk9J5sqmuvvx96gjzp17SFkX2n2xDLCm08="

versions = ["4", "5", "6"]


class Config:
    __CONFIG = {}
    __USER = None
    __PASSWORD = None
    __PATH = None
    __KEY = None
    __CYPHER = None
    __ERROR = False
    __URL_SCH = None
    __URL_SDC = None
    __SDK_KEY = None
    __URL_TX = None
    __ID = None
    __TOKEN = None

    def __init__(self, path):
        Config.__PATH = path
        Config.__KEY = os.environ["CYPHER_KEY"]
        Config.__CYPHER = Fernet(Config.__KEY)

        ControlHub.VERIFY_SSL_CERTIFICATES = False
        DataCollector.VERIFY_SSL_CERTIFICATES = False
        Transformer.VERIFY_SSL_CERTIFICATES = False

    @staticmethod
    def load():
        if not os.path.exists(Config.__PATH):
            Config.__ERROR = True
            print(f"Config path or file {Config.__PATH} is invalid.")
            return

        Config.__ERROR = False
        Config.__CONFIG = yaml.safe_load(open(Config.__PATH, "r"))
        Config.__SDK_KEY = Config.__CONFIG["__SDK_KEY__"]
        Config.__URL_SCH = Config.__CONFIG["__URL_SCH__"]
        Config.__URL_SDC = Config.__CONFIG["__URL_SDC__"]
        Config.__URL_TX = Config.__CONFIG["__URL_TX__"]

        Config.__USER = Config.__CONFIG["_USER_"]
        Config.__PASSWORD = Config.__CONFIG["_PASSWORD_"]

        try:
            Config.__ID = Config.__CONFIG["__ID__"]
        except:
            Config.__ID = None
            if streamsets.sdk.__version__[:1] not in versions:
                print("if >= 3.x then id not set")
        try:
            Config.__TOKEN = Config.__CONFIG["__TOKEN__"]
        except:
            Config.__TOKEN = None
            if streamsets.sdk.__version__[:1] not in versions:
                print("if => 3.x then token not set")

    @staticmethod
    def dump():
        if Config.__ERROR == True:
            return
        _ = yaml.dump(Config.__CONFIG, open(Config.__PATH, "w"))

    @staticmethod
    def getUser():
        return Config.__CYPHER.decrypt(Config.__USER).decode("utf-8")

    @staticmethod
    def setUser(n):
        Config.__CONFIG["_USER_"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getPassword():
        return Config.__CYPHER.decrypt(Config.__PASSWORD).decode("utf-8")

    @staticmethod
    def setPassword(n):
        Config.__CONFIG["_PASSWORD_"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getUrlSch():
        return Config.__CYPHER.decrypt(Config.__URL_SCH).decode("utf-8")

    @staticmethod
    def setUrlSch(n):
        Config.__CONFIG["__URL_SCH__"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getUrlSdc():
        return Config.__CYPHER.decrypt(Config.__URL_SDC).decode("utf-8")

    @staticmethod
    def setUrlSdc(n):
        Config.__CONFIG["__URL_SDC__"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getUrlTx():
        return Config.__CYPHER.decrypt(Config.__URL_TX).decode("utf-8")

    @staticmethod
    def setUrlTx(n):
        Config.__CONFIG["__URL_TX__"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getSdkKey():
        return Config.__CYPHER.decrypt(Config.__SDK_KEY).decode("utf-8")

    @staticmethod
    def setSdkKey(n):
        Config.__CONFIG["__SDK_KEY__"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getSdkId():
        return Config.__CYPHER.decrypt(Config.__ID).decode("utf-8")

    @staticmethod
    def setSdkId(n):
        Config.__CONFIG["__ID__"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def getSdkToken():
        return Config.__CYPHER.decrypt(Config.__TOKEN).decode("utf-8")

    @staticmethod
    def setSdkToken(n):
        Config.__CONFIG["__TOKEN__"] = Config.__CYPHER.encrypt(n.encode("utf-8"))

    @staticmethod
    def connectSCH():
        if streamsets.sdk.__version__[:1] in versions:
            return ControlHub(credential_id=Config.__CYPHER.decrypt(Config.__ID).decode("utf-8"),
                              token=Config.__CYPHER.decrypt(Config.__TOKEN).decode("utf-8"), )
        else:
            os.environ["STREAMSETS_SDK_ACTIVATION_KEY"] = Config.__CYPHER.decrypt(Config.__SDK_KEY).decode("utf-8")
            return ControlHub(Config.__CYPHER.decrypt(Config.__URL_SCH).decode("utf-8"),
                              username=Config.__CYPHER.decrypt(Config.__USER).decode("utf-8"),
                              password=Config.__CYPHER.decrypt(Config.__PASSWORD).decode("utf-8"), )


class sch_job_ctrl:
    """
    StreamSets Python SDK job wrapper
    """

    __JOB = None
    __JOB_NAME = None
    __STATUS = None
    __CONTROL_HUB = None
    __ALL = None
    __JSONLD = None

    def __init__(self, control_hub, job_name):
        sch_job_ctrl.__CONTROL_HUB = control_hub
        sch_job_ctrl.__JOB_NAME = job_name
        if isinstance(job_name, str):
            sch_job_ctrl.__JOB = sch_job_ctrl.__CONTROL_HUB.jobs.get(job_name=sch_job_ctrl.__JOB_NAME)
        else:
            sch_job_ctrl.__JOB = job_name

        self.__JSONLD = None

    def getjson(self):
        if self.__JSONLD is None:
            self.__JSONLD = json.loads(
                sch_job_ctrl.__CONTROL_HUB.get_current_job_status(sch_job_ctrl.__JOB).response.text)
        return self.__JSONLD

    @staticmethod
    def get_start_time():
        # sch_job_ctrl.__JOB = sch_job_ctrl.__CONTROL_HUB.pipelines.get(
        #     job_name=sch_job_ctrl.__JOB_NAME
        # )
        return (f"{datetime.fromtimestamp(sch_job_ctrl.__JOB.history[0].start_time / 1000)}")

    @staticmethod
    def get_finish_time():
        # sch_job_ctrl.__JOB = sch_job_ctrl.__CONTROL_HUB.pipelines.get(
        #     job_name=sch_job_ctrl.__JOB_NAME
        # )
        return f"{datetime.fromtimestamp(sch_job_ctrl.__JOB.history[0].finish_time / 1000)}"

    def get_all(self):
        # sch_job_ctrl.__JOB = sch_job_ctrl.__CONTROL_HUB.pipelines.get(
        #     job_name=sch_job_ctrl.__JOB_NAME
        # )
        self.getjson()
        return self.__JSONLD

    def get_id(self):
        self.getjson()
        return self.__JSONLD["id"]

    def get_status(self):
        # sch_job_ctrl.__JOB = sch_job_ctrl.__CONTROL_HUB.pipelines.get(
        #     job_name=sch_job_ctrl.__JOB_NAME
        # )
        self.getjson()
        return self.__JSONLD["status"]

    def get_errorMessage(self):
        # sch_job_ctrl.__JOB = sch_job_ctrl.__CONTROL_HUB.pipelines.get(
        #     job_name=sch_job_ctrl.__JOB_NAME
        # )
        self.getjson()
        return self.__JSONLD["errorMessage"]

    def get_errorInfos(self):
        self.getjson()
        return self.__JSONLD["errorInfos"]

    def get_inputRecordCount(self):
        self.getjson()
        return self.__JSONLD["inputRecordCount"]

    def get_outputRecordCount(self):
        self.getjson()
        return self.__JSONLD["outputRecordCount"]

    def get_errorRecordCount(self):
        self.getjson()
        return self.__JSONLD["errorRecordCount"]

    def get_color(self):
        self.getjson()
        return self.__JSONLD["color"]

    def get_runCount(self):
        self.getjson()
        return self.__JSONLD["runCount"]

    @staticmethod
    def get_metrics_run_count():
        if sch_job_ctrl.__JOB.metrics is not None:
            return (sch_job_ctrl.__JOB.metrics[0]).run_count
        else:
            return None

    @staticmethod
    def get_metrics_input_count():
        if (sch_job_ctrl.__JOB.metrics is not None and len(sch_job_ctrl.__JOB.metrics) > 0):
            return (sch_job_ctrl.__JOB.metrics[0]).input_count
        else:
            return None

    @staticmethod
    def get_metrics_output_count(self):
        if (sch_job_ctrl.__JOB.metrics is not None and len(sch_job_ctrl.__JOB.metrics) > 0):
            return (sch_job_ctrl.__JOB.metrics[0]).output_count
        else:
            return None

    @staticmethod
    def get_metrics_total_error_count(self):
        if (sch_job_ctrl.__JOB.metrics is not None and len(sch_job_ctrl.__JOB.metrics) > 0):
            return (sch_job_ctrl.__JOB.metrics[0]).total_error_count
        else:
            return None

    def start_job(self, wait=False):
        sch_job_ctrl.__CONTROL_HUB.start_job(sch_job_ctrl.__JOB, wait=wait)
        return self.get_status()

    def wait_for_job_status(self, status, timeout_sec):
        try:
            sch_job_ctrl.__CONTROL_HUB.wait_for_job_status(sch_job_ctrl.__JOB, status, timeout_sec=120 + timeout_sec)
        except:
            pass
        return self.get_status()

    def show_job_history_status(self, errorOnly=False):
        for h in self.__JOB.history:
            if errorOnly:
                if len(h.error_infos) > 0 or h.error_message is not None:
                    print(
                        f"job name:{self.__JOB_NAME}  Time start:{datetime.utcfromtimestamp(int(h.start_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')} "
                        f"end:{datetime.utcfromtimestamp(int(h.finish_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')} Error info:{h.error_infos}  message:{h.error_message}")
                else:
                    pass
            else:
                print(
                    f"job name:{self.__JOB_NAME}  Time start:{datetime.utcfromtimestamp(int(h.start_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
                    f" end:{datetime.utcfromtimestamp(int(h.finish_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')} Error info:{h.error_infos}  message:{h.error_message}")
