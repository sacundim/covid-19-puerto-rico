def lambda_handler(event, context):
    """Check that the array of results contains no failed Batch tasks."""
    if not isinstance(event, list):
        raise AssertionError(f"event is not list: {event}")
    for output in event:
        if not isinstance(output, dict):
            raise AssertionError(f"subtask output is not a dict: {output}")

        if 'Status' not in output:
            raise AssertionError(f"subtask output doesn't have Status field: {output}")

        status = output['Status']
        if status == 'FAILED' or status != 'SUCCEEDED':
            raise AssertionError(f'{output["JobName"]} produced status {status}')
