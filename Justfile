fix:
    find inzicht/ tests/ -name "*.py" -type f | xargs -I {} pyupgrade --py310-plus {}
    ruff check inzicht/ tests/ --fix
    ruff format inzicht/ tests/


type-check:
    mypy inzicht/
    mypy tests/

test:
    pytest -s --cov=inzicht/ --cov-report=html:tests/coverage tests/

show-coverage:
    #!/usr/bin/env python3
    import pathlib
    import webbrowser

    report_path = f"file://{pathlib.Path.cwd()/'tests/coverage/index.html'}"
    webbrowser.open(report_path)

check:
    just fix
    just type-check
    just test

clean:
    find inzicht tests -type d -name ".*_cache" -exec rm -rf {} +
    find inzicht tests -type d -name "__pycache__" -exec rm -rf {} +
    find inzicht tests -type f \( -name "*.pyc" -o -name "*.pyo" \) -exec rm -f {} +
