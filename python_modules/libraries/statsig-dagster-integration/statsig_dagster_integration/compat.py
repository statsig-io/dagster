from importlib.metadata import version


def require_supported_versions() -> None:
    for package, expected in (("dagster", "1.13.25"), ("dagster-postgres", "0.29.25")):
        actual = version(package)
        if actual != expected:
            raise RuntimeError(f"Daemon adapters require {package}=={expected}; found {actual}")
