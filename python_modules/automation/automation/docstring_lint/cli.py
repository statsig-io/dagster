"""Command-line interface for docstring validation."""

import argparse
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

from automation.docstring_lint.validator import DocstringValidator, SymbolImporter
from automation.docstring_lint.watcher import DocstringFileWatcher


def main() -> int:
    """Main entry point for the docstring validation CLI."""
    parser = argparse.ArgumentParser(
        description="Validate Python docstrings using Sphinx parsing pipeline"
    )
    parser.add_argument(
        "symbol_path", help="Dotted path to the Python symbol (e.g., 'dagster.asset')"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument(
        "--all-public",
        action="store_true",
        help="Validate all public symbols in the specified module",
    )
    parser.add_argument(
        "--public-methods",
        action="store_true",
        help="Validate all methods marked with @public decorator on @public classes in the specified module",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch the file containing the symbol for changes and re-validate automatically",
    )

    args = parser.parse_args()

    # Validate argument combinations
    if args.watch and (args.all_public or args.public_methods):
        print("Error: --watch cannot be used with --all-public or --public-methods")  # noqa: T201
        return 1

    if args.all_public and args.public_methods:
        print("Error: --all-public and --public-methods cannot be used together")  # noqa: T201
        return 1

    # Core use case - validate single docstring efficiently
    validator = DocstringValidator()

    try:
        if args.watch:
            return _run_watch_mode(args.symbol_path, validator, args.verbose)
        elif args.all_public:
            # Batch validation mode for all public symbols
            importer = SymbolImporter()
            symbols = importer.get_all_public_symbols(args.symbol_path)
            print(f"Validating {len(symbols)} public symbols in {args.symbol_path}\n")  # noqa: T201

            total_errors = 0
            total_warnings = 0

            for symbol_info in symbols:
                result = validator.validate_docstring_text(
                    symbol_info.docstring or "", symbol_info.dotted_path
                )

                if result.has_errors() or result.has_warnings():
                    print(f"--- {symbol_info.dotted_path} ---")  # noqa: T201

                    for error in result.errors:
                        print(f"  ERROR: {error}")  # noqa: T201
                        total_errors += 1

                    for warning in result.warnings:
                        print(f"  WARNING: {warning}")  # noqa: T201
                        total_warnings += 1

                    print()  # noqa: T201

            print(f"Summary: {total_errors} errors, {total_warnings} warnings")  # noqa: T201
            return 1 if total_errors > 0 else 0

        elif args.public_methods:
            # Batch validation mode for @public methods on @public classes
            importer = SymbolImporter()
            methods = importer.get_all_public_class_methods(args.symbol_path)
            print(  # noqa: T201
                f"Validating {len(methods)} @public methods on @public classes in {args.symbol_path}\n"
            )

            total_errors = 0
            total_warnings = 0

            for method_info in methods:
                result = validator.validate_docstring_text(
                    method_info.docstring or "", method_info.dotted_path
                )

                if result.has_errors() or result.has_warnings():
                    print(f"--- {method_info.dotted_path} ---")  # noqa: T201

                    for error in result.errors:
                        print(f"  ERROR: {error}")  # noqa: T201
                        total_errors += 1

                    for warning in result.warnings:
                        print(f"  WARNING: {warning}")  # noqa: T201
                        total_warnings += 1

                    print()  # noqa: T201

            print(f"Summary: {total_errors} errors, {total_warnings} warnings")  # noqa: T201
            return 1 if total_errors > 0 else 0

        else:
            # Single symbol validation (core use case)
            result = validator.validate_symbol_docstring(args.symbol_path)

            print(f"Validating docstring for: {args.symbol_path}")  # noqa: T201

            if result.has_errors():
                print("\nERRORS:")  # noqa: T201
                for error in result.errors:
                    print(f"  - {error}")  # noqa: T201

            if result.has_warnings():
                print("\nWARNINGS:")  # noqa: T201
                for warning in result.warnings:
                    print(f"  - {warning}")  # noqa: T201

            if result.is_valid() and not result.has_warnings():
                print("✓ Docstring is valid!")  # noqa: T201
            elif result.is_valid():
                print("✓ Docstring is valid (with warnings)")  # noqa: T201
            else:
                print("✗ Docstring validation failed")  # noqa: T201

            return 0 if result.is_valid() else 1

    except Exception as e:
        print(f"Error: {e}")  # noqa: T201
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


def _run_watch_mode(symbol_path: str, validator: DocstringValidator, verbose: bool) -> int:
    """Run the validation in watch mode, monitoring file changes."""
    print(f"Setting up watch mode for symbol: {symbol_path}")  # noqa: T201

    # First, resolve the symbol to get its file path
    try:
        importer = SymbolImporter()
        symbol_info = importer.import_symbol(symbol_path)

        if not symbol_info.file_path:
            print(f"Error: Cannot determine source file for symbol '{symbol_path}'")  # noqa: T201
            return 1

        target_file = Path(symbol_info.file_path)
        if not target_file.exists():
            print(f"Error: Source file does not exist: {target_file}")  # noqa: T201
            return 1

        print(f"Watching file: {target_file}")  # noqa: T201
        if verbose:
            print("Debug mode enabled - will show file system events")  # noqa: T201
        print("Press Ctrl+C to stop watching\n")  # noqa: T201

    except Exception as e:
        print(f"Error resolving symbol: {e}")  # noqa: T201
        if verbose:
            import traceback

            traceback.print_exc()
        return 1

    # Define validation callback
    def validate_and_report() -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] File changed, validating {symbol_path}...")  # noqa: T201

        try:
            result = validator.validate_symbol_docstring(symbol_path)

            if result.has_errors():
                print("ERRORS:")  # noqa: T201
                for error in result.errors:
                    print(f"  - {error}")  # noqa: T201

            if result.has_warnings():
                print("WARNINGS:")  # noqa: T201
                for warning in result.warnings:
                    print(f"  - {warning}")  # noqa: T201

            if result.is_valid() and not result.has_warnings():
                print("✓ Docstring is valid!")  # noqa: T201
            elif result.is_valid():
                print("✓ Docstring is valid (with warnings)")  # noqa: T201
            else:
                print("✗ Docstring validation failed")  # noqa: T201

        except Exception as e:
            print(f"Validation error: {e}")  # noqa: T201
            if verbose:
                import traceback

                traceback.print_exc()

        print("-" * 50)  # noqa: T201

    # Run initial validation
    validate_and_report()

    # Setup file watcher
    watcher = DocstringFileWatcher(target_file, validate_and_report, verbose)

    # Setup signal handler for graceful shutdown
    def signal_handler(signum, frame):
        print("\nStopping file watcher...")  # noqa: T201
        watcher.stop_watching()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        watcher.start_watching()
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except Exception as e:
        print(f"Watch mode error: {e}")  # noqa: T201
        if verbose:
            import traceback

            traceback.print_exc()
        return 1
    finally:
        watcher.stop_watching()

    return 0


if __name__ == "__main__":
    sys.exit(main())
