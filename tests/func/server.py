# -*- coding: utf-8 -*-

import os
import signal
import time
import subprocess
import pytest


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

        this_dir = os.path.dirname(os.path.realpath(__file__))
        server_dir = os.path.join(this_dir, "server")
        zip_name = self.ZIP_NAME % version
        dir_name = self.DIR_NAME % version
        self.dir_path = os.path.join(server_dir, dir_name)
        url = (self.DOWNLOAD_URL + zip_name) % version

        print("Downloading and unzipping %s" % zip_name)
        download_script = os.path.join(this_dir, "download_server.sh")
        ret = subprocess.call(
            [download_script, url, zip_name, server_dir, dir_name])
        if ret != 0:
            raise RuntimeError("Failed to download %s" % zip_name)

    def start(self):
        if self.process:
            raise RuntimeError("Server already running")
        launch_script = os.path.join(self.dir_path, "bin", self.mode)

        self.process = subprocess.Popen(
            [launch_script], shell=True, preexec_fn=os.setsid)
        if pytest.config.getoption("--waitlong"):
            time.sleep(20)
        else:
            time.sleep(5)

    def stop(self):
        if not self.process:
            raise RuntimeError("Server is already stopped")
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        if pytest.config.getoption("--waitlong"):
            time.sleep(3)
        else:
            time.sleep(1)
        self.process = None
