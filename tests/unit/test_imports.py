# -*- coding: utf-8 -*-


class TestDefaultImports(object):
    def test_infinispan_class_import(self):
        from infinispan import Infinispan
        Infinispan

    def test_exceptions_import(self):
        import infinispan as ispn
        ispn.ClientError
        ispn.ServerError
        ispn.DecodeError

    def test_connections_import(self):
        import infinispan as ispn
        ispn.SocketConnection
