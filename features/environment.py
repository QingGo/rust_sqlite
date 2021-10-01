from util import del_database, compile_rust_sqlite


def before_all(context):
    compile_rust_sqlite()


def before_scenario(context, scenario):
    del_database()


def after_all(context):
    del_database()
