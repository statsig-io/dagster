from contextlib import contextmanager

import packaging.version
import pendulum

_PENDULUM_VERSION = packaging.version.parse(getattr(pendulum, "__version__", "0.0.0"))
_IS_PENDULUM_2 = _PENDULUM_VERSION.major == 2
_IS_PENDULUM_3_PLUS = _PENDULUM_VERSION.major >= 3


@contextmanager
def mock_pendulum_timezone(override_timezone):
    if _IS_PENDULUM_2:
        with pendulum.tz.test_local_timezone(pendulum.tz.timezone(override_timezone)):
            yield
    elif _IS_PENDULUM_3_PLUS:
        # Pendulum v3+ API
        with pendulum.tz.test_local_timezone(pendulum.tz.timezone(override_timezone)):
            yield
    else:
        with pendulum.tz.LocalTimezone.test(pendulum.Timezone.load(override_timezone)):
            yield


def create_pendulum_time(year, month, day, *args, **kwargs):
    if _IS_PENDULUM_2 or _IS_PENDULUM_3_PLUS:
        return pendulum.datetime(year, month, day, *args, **kwargs)
    else:
        return pendulum.create(year, month, day, *args, **kwargs)


if _IS_PENDULUM_2 or _IS_PENDULUM_3_PLUS:
    PendulumDateTime = pendulum.DateTime
else:
    PendulumDateTime = pendulum.Pendulum  # type: ignore[attr-defined]


# Workaround for issues with .in_tz() in pendulum:
# https://github.com/sdispater/pendulum/issues/535
def to_timezone(dt, tz):
    import dagster._check as check

    check.inst_param(dt, "dt", PendulumDateTime)
    return pendulum.from_timestamp(dt.timestamp(), tz=tz)
