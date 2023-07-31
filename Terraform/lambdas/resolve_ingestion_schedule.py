from datetime import date, datetime, time, timedelta, timezone
import json
import logging
from zoneinfo import ZoneInfo


def lambda_handler(event, context):
    """Resolve a 'wall clock' schedule input to a UTC time schedule.

    Example input:

    ```json
    {
      "localTimezone": "America/Puerto_Rico",
      "localSchedule": {
        "biostatistics": {
          "timezone": "America/Puerto_Rico",
          "localTime": "05:55:00"
        },

        "covid19datos_v2": {
          "timezone": "America/Puerto_Rico",
          "localTime": "12:25:00"
        },

        "hhs": {
          "timezone": "America/New_York",
          "localTime": "13:25:00"
        }
      }
    }
    ```

    Example output:

    ```json
    {
      "utcSchedule": {
        "biostatistics": "2023-07-30T09:55:00Z",
        "covid19datos_v2": "2023-07-30T16:25:00Z",
        "hhs": "2023-07-30T17:25:00Z"
      }
    }
    ```
    """
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    reference_timezone = ZoneInfo(event['localTimezone'])
    local_date = datetime.now(tz=reference_timezone).date()
    resolved = resolve_utc_schedule(local_date, event['localSchedule'])

    failures = list(filter(lambda result: result.failure, validate_schedule(resolved)))
    if failures:
        logging.warning("Pushing schedule 1 day forward because of: %s", failures)
        local_date = local_date + timedelta(days=1)
        resolved = resolve_utc_schedule(local_date, event['localSchedule'])

    return {
        'utcSchedule': {
            key: format_timestamp_for_sfn(ts)
            for key, ts in resolved.items()
        }
    }

def resolve_utc_schedule(local_date: date, schedules: dict):
    return {
        key: resolve_one_utc_schedule(local_date, unresolved)
        for key, unresolved in schedules.items()
    }

def resolve_one_utc_schedule(local_date: date, schedule: dict):
    local_timezone = ZoneInfo(schedule['timezone'])
    local_time = time.fromisoformat(schedule['localTime'])
    return datetime.combine(local_date, local_time, tzinfo=local_timezone).astimezone(timezone.utc)

def validate_schedule(resolved_schedule):
    def validate_one(key, utc, now):
        if utc <= now + timedelta(minutes=5):
            return Result.fail(
                f'{key} scheduled too late in the day; wanted to run at {utc}, but now is {now}'
            )
        else:
            return Result.ok()

    now = datetime.now(timezone.utc)
    return [
        validate_one(key, utc, now)
        for key, utc in resolved_schedule.items()
    ]

def format_timestamp_for_sfn(ts):
    """Step Functions' Wait action demands this format"""
    return ts.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

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


def run_example():
    """Example invocation"""
    event = json.loads("""
    {
      "localTimezone": "America/Puerto_Rico",
      "localSchedule": {
        "biostatistics": {
          "timezone": "America/Puerto_Rico",
          "localTime": "05:55:00"
        },

        "covid19datos_v2": {
          "timezone": "America/Puerto_Rico",
          "localTime": "12:25:00"
        },

        "hhs": {
          "timezone": "America/New_York",
          "localTime": "13:25:00"
        }
      }
    }
""")
    output = lambda_handler(event, {})
    print(output)
