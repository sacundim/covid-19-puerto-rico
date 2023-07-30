from datetime import date, datetime, time, timedelta, timezone
import json
import logging
from zoneinfo import ZoneInfo


def lambda_handler(event, context):
    """Resolve a 'wall clock' schedule input to a UTC time schedule.

    Example input:

    ```json
    {
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

    run_after = resolve_run_after(event)
    resolved = resolve_utc_schedule(run_after, event['localSchedule'])
    return {
        'utcSchedule': {
            key: format_timestamp_for_sfn(ts)
            for key, ts in resolved.items()
        }
    }

def resolve_run_after(event):
    """Figure out which is the next run date based on the schedules and current time."""
    if 'runAfter' in event:
        logging.info('Used input-supplied run after time: %s', event['runAfter'])
        return date.fromisoformat(event['runAfter'])

    now = datetime.now(tz=timezone.utc)
    tentative_schedule = resolve_utc_schedule(now, event['localSchedule'])
    for key, value in tentative_schedule.items():
        if value <= now:
            logging.info(
                "Now is %s but %s has a tentative schedule for %s. Scheduling for tomorrow (%s).",
                now.isoformat(), key, value.isoformat(), (now + timedelta(days=1)).isoformat()
            )
            return now + timedelta(days=1)
    return now


def resolve_utc_schedule(run_after: datetime, schedules: dict):
    return {
        key: resolve_one_utc_schedule(run_after, unresolved)
        for key, unresolved in schedules.items()
    }

def resolve_one_utc_schedule(run_after: datetime, schedule: dict):
    local_timezone = ZoneInfo(schedule['timezone'])
    local_time = time.fromisoformat(schedule['localTime'])
    local_date = run_after.astimezone(local_timezone).date()
    return datetime.combine(local_date, local_time, tzinfo=local_timezone).astimezone(timezone.utc)

def format_timestamp_for_sfn(ts):
    """Step Functions' Wait action demands this format"""
    return ts.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def run_example():
    """Example invocation"""
    event = json.loads("""
    {
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
