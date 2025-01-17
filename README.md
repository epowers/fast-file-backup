# Fast File Backup

(pre-alpha prototype, not working)

Local filesystem backup sync with deduplication using a custom filesystem index database,
and fast directory and file stats.

## Setup

    python -m venv .venv
    pip install -r requirements.txt

## Deploy

    source .venv/bin/activate
    python -m build
    deactivate
    pip install --force-reinstall dist/fast_file_backup-0.0.1-py3-none-any.whl

## Use

    fast-file-backup-ingest .
    fast-file-backup-ingest -q
