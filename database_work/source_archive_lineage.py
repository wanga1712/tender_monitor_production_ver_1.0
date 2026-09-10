"""Persist source archive/XML lineage without changing procurement parsing."""

import hashlib
import os
import re
from datetime import date
from pathlib import Path
from typing import Iterable

import psycopg2
from dotenv import load_dotenv


def _db_connection():
    load_dotenv(Path(__file__).with_name("db_credintials.env"))
    return psycopg2.connect(
        host=os.environ["DB_HOST_TENDER"],
        port=os.environ.get("DB_PORT_TENDER", "5432"),
        dbname=os.environ["DB_DATABASE_TENDER"],
        user=os.environ["DB_USER_TENDER"],
        password=os.environ["DB_PASSWORD_TENDER"],
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _notice_from_filename(filename: str) -> str | None:
    match = re.search(r"(?:^|_)(\d{10,})(?:_|$)", Path(filename).stem)
    return match.group(1) if match else None


def persist_archive(
    archive_path: Path,
    law_family: str,
    ingestion_direction: str,
    source_date: date,
    xml_members: Iterable[str],
    document_type: str | None = None,
) -> int:
    """Store one archive and its extracted XML manifest in one transaction."""
    archive_path = Path(archive_path)
    checksum = sha256_file(archive_path)
    xml_members = list(xml_members)
    with _db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_archive_manifest
                    (law_family, ingestion_direction, source_date,
                     archive_filename, archive_path, file_size, sha256,
                     processing_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'EXTRACTED')
                ON CONFLICT (sha256) DO UPDATE SET
                    archive_path = EXCLUDED.archive_path,
                    processing_status = EXCLUDED.processing_status
                RETURNING archive_id
                """,
                (
                    law_family,
                    ingestion_direction,
                    source_date,
                    archive_path.name,
                    str(archive_path),
                    archive_path.stat().st_size,
                    checksum,
                ),
            )
            archive_id = cursor.fetchone()[0]
            for member_path in xml_members:
                filename = Path(member_path).name
                cursor.execute(
                    """
                    INSERT INTO source_xml_manifest
                        (archive_id, archive_member_path, xml_filename, xml_document_type,
                         notice_number, parser_status)
                    VALUES (%s, %s, %s, %s, %s, 'EXTRACTED')
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        archive_id,
                        member_path,
                        filename,
                        document_type,
                        _notice_from_filename(filename),
                    ),
                )
    return archive_id
