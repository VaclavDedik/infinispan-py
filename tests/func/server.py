
import os
import signal
import shutil
import subprocess
import zipfile


class Mode(object):
    STANDALONE = "standalone.sh"
    DOMAIN = "domain.sh"


class InfinispanServer(object):
    DOWNLOAD_URL = "http://downloads.jboss.org/infinispan/%s.Final/"
    ZIP_NAME = "infinispan-server-%s.Final-bin.zip"
    DIR_NAME = "infinispan-server-%s.Final"

    def __init__(self, version="8.2.4", mode=Mode.STANDALONE):
        self.version = version
        self.mode = mode
        self.process = None

        zip_name = self.ZIP_NAME % version
        zip_path = "server/" + zip_name
        dir_name = self.DIR_NAME % version
        self.dir_path = "server/" + dir_name
        url = (self.DOWNLOAD_URL + zip_name) % version

        if not os.path.exists(zip_path):
            print("Downloading %s" % zip_name)
            ret = subprocess.call(['./download_server.sh', url, zip_name])
            if ret != 0:
                raise RuntimeError("Failed to download %s" % zip_name)

        if os.path.exists(self.dir_path):
            shutil.rmtree(self.dir_path)
        print("Unzipping %s" % zip_name)
        zip_ref = zipfile.ZipFile(zip_path, 'r')
        zip_ref.extractall("server/")
        zip_ref.close()

    def start(self):
        if self.process:
            raise RuntimeError("Server already running")
        launch_script = os.path.join(self.dir_path, "bin", self.mode)

        os.chmod(launch_script, 0o775)
        self.process = subprocess.Popen(
            [launch_script], shell=True, preexec_fn=os.setsid)

    def stop(self):
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        self.process = None
