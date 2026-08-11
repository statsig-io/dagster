from contextlib import contextmanager

import packaging.version
import pendulum

_IS_PENDULUM_2 = (
    hasattr(pendulum, "__version__")
    and getattr(packaging.version.parse(getattr(pendulum, "__version__")), "major") == 2
)

_IS_PENDULUM_3 = (
    hasattr(pendulum, "__version__")
    and getattr(packaging.version.parse(getattr(pendulum, "__version__")), "major") == 3
)


@contextmanager
def mock_pendulum_timezone(override_timezone):
    # Pendulum 2 and 3 share test_local_timezone / timezone APIs.
    if _IS_PENDULUM_2 or _IS_PENDULUM_3:
        with pendulum.tz.test_local_timezone(pendulum.tz.timezone(override_timezone)):
            yield
    else:
        with pendulum.tz.LocalTimezone.test(pendulum.Timezone.load(override_timezone)):
            yield


def create_pendulum_time(year, month, day, *args, **kwargs):
    if _IS_PENDULUM_2 or _IS_PENDULUM_3:
        return pendulum.datetime(year, month, day, *args, **kwargs)
    return pendulum.create(year, month, day, *args, **kwargs)


# pendulum.Pendulum was removed in pendulum 3; DateTime is the shared type.
PendulumDateTime = pendulum.DateTime


# Workaround for issues with .in_tz() in pendulum:
# https://github.com/sdispater/pendulum/issues/535
def to_timezone(dt, tz):
    import dagster._check as check

    check.inst_param(dt, "dt", PendulumDateTime)
    return pendulum.from_timestamp(dt.timestamp(), tz=tz)
