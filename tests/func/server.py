
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

        server_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "server")
        zip_name = self.ZIP_NAME % version
        zip_path = os.path.join(server_dir, zip_name)
        dir_name = self.DIR_NAME % version
        self.dir_path = os.path.join(server_dir, dir_name)
        url = (self.DOWNLOAD_URL + zip_name) % version

        if not os.path.exists(zip_path):
            print("Downloading %s" % zip_name)
            download_script = os.path.join(server_dir, "download_server.sh")
            ret = subprocess.call([download_script, url, zip_name, server_dir])
            if ret != 0:
                raise RuntimeError("Failed to download %s" % zip_name)

        if os.path.exists(self.dir_path):
            shutil.rmtree(self.dir_path)
        print("Unzipping %s" % zip_name)
        zip_ref = zipfile.ZipFile(zip_path, 'r')
        zip_ref.extractall(server_dir)
        zip_ref.close()

    def start(self):
        if self.process:
            raise RuntimeError("Server already running")
        launch_script = os.path.join(self.dir_path, "bin", self.mode)

        os.chmod(launch_script, 0o775)
        self.process = subprocess.Popen(
            [launch_script], shell=True, preexec_fn=os.setsid)

    def stop(self):
        if not self.process:
            raise RuntimeError("Server is already stopped")
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        self.process = None
