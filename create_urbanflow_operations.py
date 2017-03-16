#!/usr/bin/env python
"""Create an operation and run it, to demonstrate the architecture"""
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import datetime
import json
from pprint import pprint
import sys
import time
import uuid

from dateutil.tz import tzlocal
import requests
from requests.auth import HTTPBasicAuth

import common_dibbs.auth as dibbs_auth


RESERVATION_ID = 'REPLACE_BY_RESERVATION_ID'
SCRIPTS_PATH = '/home/cc/scripts'


def check_response(response, dumpfile='response-dump.log'):
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print("   {}".format(str(e)))
        if dumpfile:
            with open(dumpfile, 'wb') as f:
                print("dumping response to '{}'...".format(dumpfile))
                f.write(response.content)
        raise
    else:
        print("   OK")


def create_upload_operation(or_url, headers):
    # Creation of an Operation
    operation_dict = {
        "name": "UrbanFlow",
        "description": "The data upload stage of the UrbanFlow application",
        "string_parameters": json.dumps(
            []
        ),
        "logo_url": "https://raw.githubusercontent.com/DIBBS-project/DIBBS-Architecture-Demo/master/misc/dibbs/linecounter.png",
        "file_parameters": json.dumps(
            ["input_file"]
        ),
    }
    print(" - Creating the urbanflow operation...", end="")
    r = requests.post(
        "{}/operations/".format(or_url),
        json=operation_dict,
        headers=headers,
    )
    print(r.status_code)
    check_response(r)
    operation = r.json()

    return operation


def create_upload_implementation(or_url, headers, operation_id):
    # Implementing the Operation based on the hadoop appliance.
    implementation_dict = {
        "name": "urbanflow_hadoop",
        "appliance": "hadoop",
        "operation": operation_id,
        "cwd": "~",
        "script": r"%s/upload_data.sh &> /usr/hadoop/ChameleonHadoopWebservice/tmp/output.txt" % SCRIPTS_PATH,
        "output_type": "file",
        "output_parameters": json.dumps(
            {"file_path": "output.txt"}
        ),
    }
    print(" - implementing of the urbanflow operation... ", end="")
    r = requests.post(
        "{}/operationversions/".format(or_url),
        json=implementation_dict,
        headers=headers,
    )
    print(r.status_code)
    check_response(r)
    implementation = r.json()
    return implementation


def create_upload_instance(om_url, headers, operation_id):
    # Creating an instance of the Operation
    instance_dict = {
        "name": "urbanflow_instance",
        "process_definition_id": operation_id,
        "parameters": json.dumps({
        }),
        "files": json.dumps({
            "input_file": "https://raw.githubusercontent.com/DIBBS-project/DIBBS-Architecture-Demo/master/misc/input.txt",
        }),
    }
    print(" - Creating an instance of the urbanflow operation... ", end="")
    r = requests.post(
        "%s/instances/" % (om_url),
        json=instance_dict,
        headers=headers,
    )
    print(r.status_code)
    check_response(r)

    instance = r.json()
    return instance


def create_filter_operation(or_url, headers):
    # Creation of an Operation
    operation_dict = {
        "name": "UrbanFlow",
        "description": "The filter stage of the UrbanFlow application",
        "string_parameters": json.dumps(
            ["minX", "maxX", "minY", "maxY"]
        ),
        "logo_url": "https://raw.githubusercontent.com/DIBBS-project/DIBBS-Architecture-Demo/master/misc/dibbs/linecounter.png",
        "file_parameters": json.dumps(
            ["input_file"]
        ),
    }
    print(" - Creating the urbanflow operation...", end="")
    r = requests.post(
        "{}/operations/".format(or_url),
        json=operation_dict,
        headers=headers,
    )
    print(r.status_code)
    check_response(r)
    operation = r.json()

    return operation


def create_filter_implementation(or_url, headers, operation_id):
    # Implementing the Operation based on the hadoop appliance.
    implementation_dict = {
        "name": "urbanflow_hadoop",
        "appliance": "hadoop",
        "operation": operation_id,
        "cwd": "~",
        "script": r"%s/chicago_run_pipeline.sh &> /usr/hadoop/ChameleonHadoopWebservice/tmp/output.txt" % SCRIPTS_PATH,
        "output_type": "file",
        "output_parameters": json.dumps(
            {"file_path": "output.txt"}
        ),
    }
    print(" - implementing of the urbanflow operation... ", end="")
    r = requests.post(
        "{}/operationversions/".format(or_url),
        json=implementation_dict,
        headers=headers,
    )
    print(r.status_code)
    check_response(r)
    implementation = r.json()
    return implementation


def create_filter_instance(om_url, headers, operation_id):
    # Creating an instance of the Operation
    instance_dict = {
        "name": "urbanflow_instance",
        "process_definition_id": operation_id,
        "parameters": json.dumps({
            "minX": "-87.9395",
            "maxX": "-87.5245",
            "minY": "41.6446",
            "maxY": "42.0229",
        }),
        "files": json.dumps({
            "input_file": "https://raw.githubusercontent.com/DIBBS-project/DIBBS-Architecture-Demo/master/misc/input.txt",
        }),
    }
    print(" - Creating an instance of the urbanflow operation... ", end="")
    r = requests.post(
        "%s/instances/" % (om_url),
        json=instance_dict,
        headers=headers,
    )
    print(r.status_code)
    check_response(r)

    instance = r.json()
    return instance


def prepare_execution(om_url, headers, instance_id, hints):
    execution_dict = {
        "operation_instance": instance_id,
        "callback_url": "http://plop.org",
        "force_spawn_cluster": "",
        "hints": hints,
    }
    print(" - Preparing an execution of the urbanflow operation")
    r = requests.post(
        "%s/executions/" % (om_url),
        json=execution_dict,
        headers=headers,
    )
    check_response(r)

    execution = r.json()
    return execution


def wait_for_execution(om_url, headers, execution_id):
    print(" - Waiting for the execution to finish")

    current_status = None
    previous_status = ""
    while True:
        r = requests.get(
            url="{}/executions/{}/".format(om_url, execution_id),
            headers=headers,
        )

        data = r.json()
        current_status = data["status"]

        if current_status != previous_status:
            now = datetime.datetime.now(tz=tzlocal())
            print(" ({})  => {}".format(now.strftime('%H:%M:%S'), current_status))
            previous_status = current_status

        if current_status == "FINISHED":
            return

        time.sleep(5)


def download_output(om_url, headers, execution_id):
    print(" - Download the output of the execution")
    r = requests.get(
        url="{}/executions/{}/".format(om_url, execution_id),
        headers=headers,
    )

    data = r.json()

    download_url = data["output_location"]
    if download_url is not None:
        r = requests.get(download_url, auth=HTTPBasicAuth('admin', 'pass'))

        # Write the downloaded file in a temporary file
        output_file_path = "/tmp/%s" % (uuid.uuid4())
        with open(output_file_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)

        print("   => output has been downloaded in %s" % (output_file_path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('action', choices=['deploy', 'run', 'both'])
    parser.add_argument('--run-on-roger',
        action='store_true', help='Run on Roger, rather than Chameleon')
    parser.add_argument('-H', '--host', type=str,
        help='DIBBs host address', default='127.0.0.1')
    parser.add_argument('-u', '--username', type=str,
        help='DIBBs username', default='alice')
    parser.add_argument('-p', '--password', type=str,
        help='Password for user. Defaults to uppercased username.')
    parser.add_argument('-i', '--instance-id', type=int,
        help='instance_id to use (needed if run-only)')
    parser.add_argument('-d', '--upload-data', action='store_true',
        help='Perform data upload prior to running computation', default=False)

    args = parser.parse_args()

    cas_url = "http://%s:7000" % (args.host)
    or_url = "http://%s:8000" % (args.host)
    om_url = "http://%s:8001" % (args.host)
    # rm_url = "http://%s:8002" % (args.host)

    username = args.username
    password = username.upper() if args.password is None else args.password

    auth_response = requests.post(
        '{}/auth/tokens'.format(cas_url),
        json={'username': username, 'password': password},
    )
    if auth_response.status_code != 200:
        print(auth_response.text, file=sys.stderr)
        return -1
    auth_data = auth_response.json()
    headers = {'Dibbs-Authorization': auth_data['token']}

    if args.run_on_roger:
        hints = """{"credentials": ["kvm@roger_dibbs"], "lease_id": ""}"""
    else:
        hints = """{{"credentials": ["chi@tacc_fg392"], "lease_id": "{}"}}""".format(RESERVATION_ID)

    do_deploy = args.action in {'both', 'deploy'}
    do_run = args.action in {'both', 'run'}

    if do_deploy:
        if args.upload_data:
            upload_operation = create_upload_operation(or_url, headers)
            upload_implementation = create_upload_implementation(or_url, headers, upload_operation['id'])
            upload_instance = create_upload_instance(om_url, headers, upload_operation['id'])
            print(' - Created instance:')
            pprint(upload_instance) # so the user can call it later

        filter_operation = create_filter_operation(or_url, headers)
        filter_implementation = create_filter_implementation(or_url, headers, filter_operation['id'])
        instance = filter_instance = create_filter_instance(om_url, headers, filter_operation['id'])

        print(' - Created instance:')
        pprint(filter_instance) # so the user can call it later

    else:
        if args.instance_id is None:
            print("Must provide instance id", file=sys.stderr)
            return -1
        instance = {'id': args.instance_id}

    if args.upload_data:
        execution = prepare_execution(om_url, headers, upload_instance['id'], hints)
        # Wait for the execution to finish
        wait_for_execution(om_url, headers, execution['id'])
        # Download the output of the execution
        download_output(om_url, headers, execution['id'])

    if do_run:
        execution = prepare_execution(om_url, headers, instance['id'], hints)

        # Wait for the execution to finish
        wait_for_execution(om_url, headers, execution['id'])

        # Download the output of the execution
        download_output(om_url, headers, execution['id'])


if __name__ == "__main__":
    sys.exit(main())
