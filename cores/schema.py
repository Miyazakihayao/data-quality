import os

import rx
import utils
from exception import ValidateError

SCHEMA_DIR = 'schema' + os.sep
CORE_SCHEMA = SCHEMA_DIR + 'core.yaml.schema'
TABLE_META_SCHEMA = SCHEMA_DIR + 'file.meta.schema'
DB_META_SCHEMA = SCHEMA_DIR + 'db.meta.schema'
TASK_SCHEMA = SCHEMA_DIR + 'task.yaml.schema'


def check_schema(schema, data, file_name, home=True):
    """
    Arguments:
    - `schema`: the file name of schema to check
    - `data`: the data to check
    """
    rxo = rx.Factory({"register_core_types": True})
    s = utils.load_yaml(schema, home)
    rx_s = rxo.make_schema(s)
    status, msg = rx_s.check(data)
    if status:
        return True
    else:
        raise ValidateError("[%s] format check fail, %s" % (file_name, msg))


if __name__ == '__main__':
    print(check_schema('schema/a.yaml', {"from": "to"}, "test"))
