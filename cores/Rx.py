import re
import types
import logging

core_types = []


class Error(Exception):
    pass


class Util(object):
    @staticmethod
    def make_range_check(opt):
        range = {}
        for entry in opt.keys():
            if entry not in ('min', 'max', 'min-ex', 'max-ex'):
                raise ("illegal argument to make_range_check")

            range[entry] = opt[entry]

        def check_range(value):
            if range.get('min') is not None and value < range['min']:
                return False, "value: %s < min: %s " % (
                    value, range['min'])
            if range.get('min-ex') is not None and value <= range['min-ex']:
                return False, "value: %s < min-ex: %s " % (
                    value, range['min'])

            if range.get('max-ex') is not None and value >= range['max-ex']:
                return False, "value: %s >= max-ex: %s " % (
                    value, range['min'])

            if range.get('max') is not None and value > range['max']:
                return False, "value: %s > max: %s " % (
                    value, range['min'])

            return True, None

        return check_range


class Factory(object):
    def __init__(self, opt={}):
        self.prefix_registry = {
            '': 'tag:codesimply.com,2008:rx/core/',
            '.meta': 'tag:codesimply.com,2008:rx/meta/',
        }

        self.type_registry = {}
        if opt.get("register_core_types", False):
            for t in core_types:
                self.register_type(t)

    @staticmethod
    def _default_prefixes():
        pass

    def expand_uri(self, type_name):
        if re.match('^\w+:', type_name):
            return type_name

        m = re.match('^/([-._a-z0-9]*)/([-._a-z0-9]+)$', type_name)

        if not m:
            raise "couldn't understand type name '%s'" % type_name

        if not self.prefix_registry.get(m.group(1)):
            raise "unknown prefix '%s' in type name '%s'" % (m.group(1), type_name)

        return '%s%s' % (self.prefix_registry[m.group(1)], m.group(2))

    def register_type(self, t):
        t_uri = t.uri()

        if self.type_registry.get(t_uri, None):
            raise "type already registered for %s" % t_uri

        self.type_registry[t_uri] = t

    def make_schema(self, schema):
        if type(schema) in (str, bytes):
            schema = {"type": schema}

        if not type(schema) is dict:
            raise Error('invalid schema argument to make_schema')

        uri = self.expand_uri(schema["type"])

        if not self.type_registry.get(uri):
            raise "unknown type %s" % uri

        type_class = self.type_registry[uri]

        return type_class(schema, self)


class _CoreType(object):
    @classmethod
    def uri(self):
        return 'tag:codesimply.com,2008:rx/core/' + self.subname()

    def __init__(self, schema, rx): pass

    def check(self, value): return False, None


class AllType(_CoreType):
    @staticmethod
    def subname():
        return 'all'

    def __init__(self, schema, rx):
        if not (schema.get('of') and len(schema.get('of'))):
            raise Error('no alternatives given in //all of')

        self.alts = [rx.make_schema(s) for s in schema['of']]

    def check(self, value):
        for schema in self.alts:
            status, msg = schema.check(value)
            if (not status):
                return False, msg
        return True, None


class AnyType(_CoreType):
    @staticmethod
    def subname():
        return 'any'

    def __init__(self, schema, rx):
        self.alts = None

        if schema.get('of') is not None:
            if not schema['of']:
                raise Error('no alternatives given in //any of')
            self.alts = [rx.make_schema(alt) for alt in schema['of']]

    def check(self, value):
        if self.alts is None:
            return True, None

        msg = None
        for alt in self.alts:
            status, msg = alt.check(value)
            if status:
                return True, None

        return False, msg


class ArrType(_CoreType):
    @staticmethod
    def subname():
        return 'arr'

    def __init__(self, schema, rx):
        self.length = None

        if not set(schema.keys()).issubset(set(('type', 'contents', 'length'))):
            raise Error('unknown parameter for //arr')

        if not schema.get('contents'):
            raise Error('no contents provided for //arr')

        self.content_schema = rx.make_schema(schema['contents'])

        if schema.get('length'):
            self.length = Util.make_range_check(schema["length"])

    def check(self, value):
        if not (type(value) in [type([]), type(())]):
            return False, "type of %s is not in ( [], () )" % value
        if self.length:
            status, msg = self.length(len(value))
            if not status:
                return False, msg

        for item in value:
            status, msg = self.content_schema.check(item)
            if not status:
                return False, msg

        return True, None


class BoolType(_CoreType):
    @staticmethod
    def subname(): return 'bool'

    def check(self, value):
        if value is True or value is False:
            return True, None
        return False, "type of %s is not bool" % value


class DefType(_CoreType):
    @staticmethod
    def subname(): return 'def'

    def check(self, value):
        if value is None:
            return False, "%s is undefined" % value
        return True, None


class FailType(_CoreType):
    @staticmethod
    def subname(): return 'fail'

    def check(self, value):
        return False, "type is FailType"


class IntType(_CoreType):
    @staticmethod
    def subname():
        return 'int'

    def __init__(self, schema, rx):
        if not set(schema.keys()).issubset(set(('type', 'range', 'value'))):
            raise Error('unknown parameter for //int')

        self.value = None
        if schema.__contains__('value'):
            if not type(schema['value']) in (float, int):
                raise Error('invalid value parameter for //int')
            if schema['value'] % 1 != 0:
                raise Error('invalid value parameter for //int')
            self.value = schema['value']

        self.range = None
        if schema.__contains__('range'):
            self.range = Util.make_range_check(schema["range"])

    def check(self, value):
        if not (type(value) in (float, int)):
            return False, "type of %s is not in (float, int)" % value
        if value % 1 != 0:
            return False, "type of %s is not int" % value
        if self.range:
            status, msg = self.range(value)
            if not status:
                return False, msg
        if (self.value is not None) and value != self.value:
            return False, "%s != value: %s" % (value, self.value)
        return True, None


class MapType(_CoreType):
    @staticmethod
    def subname():
        return 'map'

    def __init__(self, schema, rx):
        self.allowed = set()

        if not schema.get('values'):
            raise Error('no values given for //map')

        self.value_schema = rx.make_schema(schema['values'])

    def check(self, value):
        if not isinstance(value, type({})):
            return False, "type of %s is not {}" % value

        for v in value.values():
            status, msg = self.value_schema.check(v)
            if not status:
                return False, msg

        return True, None


class NilType(_CoreType):
    @staticmethod
    def subname():
        return 'nil'

    def check(self, value):
        if value is None:
            return True, None
        else:
            return False, "value of %s is not None" % value


class NumType(_CoreType):
    @staticmethod
    def subname():
        return 'num'

    def __init__(self, schema, rx):
        if not set(schema.keys()).issubset(set(('type', 'range', 'value'))):
            raise Error('unknown parameter for //num')

        self.value = None
        if schema.__contains__('value'):
            if not type(schema['value']) in (float, int):
                raise Error('invalid value parameter for //num')
            self.value = schema['value']

        self.range = None

        if schema.get('range'):
            self.range = Util.make_range_check(schema["range"])

    def check(self, value):
        if not (type(value) in (float, int)):
            return False, "type of %s is not in (float, int)" % value
        if self.range:
            status, msg = self.range(value)
            if not status:
                return False, msg
        if (self.value is not None) and value != self.value:
            return False, "%s != value: %s" % (value, self.value)
        return True, None


class OneType(_CoreType):
    @staticmethod
    def subname(): return 'one'

    def check(self, value):
        if type(value) in (int, float, bool, str, bytes):
            return True, None

        return False, "type of %s is not in (int, float, bool, str, bytes)" % value


class RecType(_CoreType):
    @staticmethod
    def subname():
        return 'rec'

    def __init__(self, schema, rx):
        if not set(schema.keys()).issubset(set(('type', 'rest', 'required', 'optional'))):
            raise Error('unknown parameter for //rec')

        self.known = set()
        self.rest_schema = None
        if schema.get('rest'):
            self.rest_schema = rx.make_schema(schema['rest'])

        for which in ('required', 'optional'):
            self.__setattr__(which, {})
            for field in schema.get(which, {}).keys():
                if field in self.known:
                    raise Error('%s appears in both required and optional' % field)

                self.known.add(field)

                self.__getattribute__(which)[field] = rx.make_schema(
                    schema[which][field]
                )

    def check(self, value):
        if not (isinstance(value, type({}))):
            return False, "type of %s is not {}" % value

        unknown = []
        for field in value.keys():
            if field not in self.known:
                unknown.append(field)

        if len(unknown) and not self.rest_schema:
            return False, "unknown field %s but no rest schema" % unknown

        for field in self.required.keys():
            if not value.__contains__(field):
                return False, "field: %s required" % field
            status, msg = self.required[field].check(value[field])
            if not status:
                return False, msg

        for field in self.optional.keys():
            if not value.__contains__(field):
                continue
            status, msg = self.optional[field].check(value[field])
            if not status:
                return False, msg

        if len(unknown):
            rest = {}
            for field in unknown:
                rest[field] = value[field]
            status, msg = self.rest_schema.check(rest)
            if not status:
                return False, msg

        return True, None


class SeqType(_CoreType):
    @staticmethod
    def subname():
        return 'seq'

    def __init__(self, schema, rx):
        if not schema.get('contents'):
            raise Error('no contents provided for //seq')

        self.content_schema = [rx.make_schema(s) for s in schema["contents"]]

        self.tail_schema = None
        if (schema.get('tail')):
            self.tail_schema = rx.make_schema(schema['tail'])

    def check(self, value):
        if not (type(value) in [type([]), type(())]):
            return False, "type of %s is not in ( [], () )" % value

        if len(value) < len(self.content_schema):
            return False, "length of %s < length of content schema" % value

        for i in range(0, len(self.content_schema)):
            status, msg = self.content_schema[i].check(value[i])
            if not status:
                return False, msg

        if len(value) > len(self.content_schema):
            if not self.tail_schema:
                return False, "no tail schema defined"
            status, msg = self.tail_schema.check(value[len(self.content_schema):])
            if not status:
                return False, msg

        return True, None


class StrType(_CoreType):
    @staticmethod
    def subname():
        return 'str'

    def __init__(self, schema, rx):
        if not set(schema.keys()).issubset(set(('type', 'value'))):
            raise Error('unknown parameter for //str')

        self.value = None
        if schema.__contains__('value'):
            if not type(schema['value']) in (str, bytes):
                raise Error('invalid value parameter for //str')
            self.value = schema['value']

    def check(self, value):
        if not type(value) in (str, bytes):
            return False, "type of %s not in (str, bytes)" % value
        if (self.value is not None) and value != self.value:
            return False, "%s != value: %s"(value, self.value)
        return True, None


core_types = [
    AllType, AnyType, ArrType, BoolType, DefType,
    FailType, IntType, MapType, NilType, NumType,
    OneType, RecType, SeqType, StrType
]
