from subprocess import PIPE, Popen


def compile_rust_sqlite(db_name="default.db"):
    commands = ["cargo", "build"]
    proc = Popen(commands, stdin=PIPE, stdout=PIPE)
    proc.wait()


def del_database(db_name="default.db"):
    commands = ["rm", db_name]
    proc = Popen(commands, stdin=PIPE, stdout=PIPE)
    proc.wait()
