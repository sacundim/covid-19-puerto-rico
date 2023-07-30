import json

def lambda_handler(event, context):
    """Check that the array of results contains no failed Batch tasks."""
    if not isinstance(event, list):
        raise AssertionError(f"event is not a list: {event}")

    failures = list(filter(
        lambda result: result.failure,
        map(diagnose, event)
    ))
    if failures:
        raise AssertionError(f'One or more subtasks failed: {failures}')

def diagnose(output):
    if not isinstance(output, dict):
        return Result.fail(f"subtask output is not a dict: {output}")

    elif 'Error' in output:
        error = output['Error']
        cause = output['Cause']
        if error == 'States.TaskFailed':
            return analyze_task_failed(error, json.loads(cause))
        else:
            return Result.fail(f'subtask failed with error = "{error}", cause = "{cause}"')

    else:
        return Result.ok()

def analyze_task_failed(error: str, cause: dict):
    if 'Status' not in cause:
        return Result.fail(f"subtask failure cause doesn't have Status field: {cause}")

    status = cause['Status']
    if status == 'FAILED' or status != 'SUCCEEDED':
        return Result.fail(f'{cause["JobName"]} produced status {status}')

    return Result.ok()

class Result():
    """Simple class to represent all the validation results instead of throwing an exception"""

    def __init__(self, success, value, error):
        self.success = success
        self.error = error
        self.value = value

    @property
    def failure(self):
        """True if operation failed, False if successful (read-only)."""
        return not self.success

    def __str__(self):
        if self.success:
            return f'[Success]'
        else:
            return f'[Failure] "{self.error}"'

    def __repr__(self):
        if self.success:
            return f"<Result success={self.success}>"
        else:
            return f'<Result success={self.success}, message="{self.error}">'

    @classmethod
    def fail(cls, error):
        """Create a Result object for a failed operation."""
        return cls(False, value=None, error=error)

    @classmethod
    def ok(cls, value=None):
        """Create a Result object for a successful operation."""
        return cls(True, value=value, error=None)
