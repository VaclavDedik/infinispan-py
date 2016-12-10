# -*- coding: utf-8 -*-


def generate_async(cls):
    def build_async(fn):
        def async(self, *args, **kwargs):
            return self.executor.submit(fn, self, *args, **kwargs)
        return async

    methods = [(n, m) for n, m in cls.__dict__.items()]
    for name, method in methods:
        if hasattr(method, "sync_op") and getattr(method, "sync_op") is True:
            setattr(cls, name + "_async", build_async(method))
    return cls


def op(method):
    method.sync_op = True
    return method
