
class CreatedCounter(object):
    _count = 0

    @staticmethod
    def count():
        count = CreatedCounter._count
        CreatedCounter._count += 1
        return count


class DataType(object):
    def __init__(self, **kwargs):
        self._created = CreatedCounter.count()
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    @property
    def type(self):
        return type(self).__name__.lower()


class Message(object):
    def __init__(self, **kwargs):
        m_fields = filter(
            lambda f: not f.startswith('__') and
            isinstance(getattr(self, f), DataType), dir(self))

        self.fields = \
            sorted(m_fields, key=lambda fn: getattr(self, fn)._created)

        for f_name in self.fields:
            f_cls = getattr(self.__class__, f_name)
            if f_name in kwargs:
                setattr(self, f_name, kwargs[f_name])
            elif hasattr(f_cls, 'default'):
                setattr(self, f_name, f_cls.default)
            else:
                setattr(self, f_name, None)

    @property
    def cls(self):
        return self.__class__


class Composite(DataType):
    pass


class Byte(DataType):
    pass


class SplitByte(DataType):
    pass


class Uvarint(DataType):
    pass


class Uvarlong(DataType):
    pass


class Lenstr(DataType):
    pass
