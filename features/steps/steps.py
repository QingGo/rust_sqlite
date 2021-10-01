# -- FILE: features/steps/example_steps.py
import os
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from typing import List
from behave import given, when, then, step


def run_script(proc: Popen[bytes], commands: List[str]):
    # results = []
    for command in commands:
        proc.stdin.write(f"{command}\n".encode("utf-8"))
        proc.stdin.flush()
        # results.append(proc.stdout.readline().decode("utf-8").strip())
    results = proc.stdout.read().decode("utf-8").strip().split("\n")
    return results


@given("open rust_sqlite binary")
def step_impl(context):
    context.proc = Popen(["./target/debug/rust_sqlite"],
                         stdin=PIPE, stdout=PIPE)


@when("reopen rust_sqlite binary")
def step_impl(context):
    context.proc.kill()
    context.proc = Popen(["./target/debug/rust_sqlite"],
                         stdin=PIPE, stdout=PIPE)


@when("execute some sql commands")
def step_impl(context):
    commands = context.text.strip().split("\n")
    results = run_script(context.proc, commands)
    context.results = results


@then("get expected stdout")
def step_impl(context):
    expected_result = context.text.strip().split("\n")
    # print stdout if AssertionError
    print(f"actual results is : {context.results}")
    assert context.results == expected_result


@when("insert many rows")
def step_impl(context):
    commands = []
    for i in range(1, 1402):
        commands.append(f"insert {i} user{i} person{i}@example.com")
    commands.append(".exit")
    context.results = run_script(context.proc, commands)


@then("get expected error")
def step_impl(context):
    assert 'db > table is full of rows: 1400'.startswith(
        "db > table is full of rows")
    assert context.results[-2].startswith("db > table is full of rows")


@when("inserting strings that are the maximum length")
def step_impl(context):
    long_username = "a"*32
    long_email = "a"*255
    commands = [
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ]
    context.long_username = long_username
    context.long_email = long_email
    context.results = run_script(context.proc, commands)


@then("get expected maximum string stdout")
def step_impl(context):
    expected_results = [
        "db > Executed",
        f"db > (1, {context.long_username}, {context.long_email})",
        "Executed",
        "db >"
    ]
    print(context.results)
    assert context.results == expected_results


@when("inserting strings that are longer than the maximum length")
def step_impl(context):
    long_username = "a"*33
    long_email = "a"*256
    commands = [
        f"insert 1 {long_username} {long_email}",
        "select",
        ".exit",
    ]
    context.long_username = long_username
    context.long_email = long_email
    context.results = run_script(context.proc, commands)


@then("get string is too long error")
def step_impl(context):
    expected_results = [
        "db > String is too long",
        "db > Executed",
        "db >"
    ]
    print(context.results)
    assert context.results == expected_results
