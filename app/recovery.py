"""Primary disaster recovery from the backup.

Scenario: the primary's SQLite volume was deleted and re-initialised to
the empty seed zone (serial 0), while the backup still holds a
resolvable, complete copy of the last published zone. An operator
triggers ``POST /api/recover`` on the primary, which then:

1. pulls the backup's current complete zone over HTTP;
2. validates the zone name against the locally configured fixed zone
   and validates every record with the same rules as a normal publish
   (in-zone names, IPv4 only, no duplicates, at most ``MAX_RECORDS``);
3. restores the primary *only while it is still the empty seed zone*,
   assigning a serial strictly higher than the backup's current serial;
4. lets the backup catch the restored version up through the existing
   synchronisation mechanism afterwards.

Every failure mode (backup unreachable, invalid data, storage error,
primary no longer empty) leaves the primary state untouched and reports
an explicit reason.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from pydantic import ValidationError

from .config import settings
from .models import RecordInput, ZonePayload
from .storage import PrimaryNotEmpty, storage, utcnow_iso

log = logging.getLogger("recovery")


class RecoveryError(RuntimeError):
    """Base class for recovery failures; carries an operator-facing reason."""

    http_status = 409

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class BackupUnreachable(RecoveryError):
    """The backup API could not be contacted or returned a bad HTTP status."""

    http_status = 502


class BackupDataInvalid(RecoveryError):
    """The backup answered, but its zone payload failed validation."""

    http_status = 502


class PrimaryConflict(RecoveryError):
    """The primary already has a real zone; recovery must not overwrite it."""

    http_status = 409


class RecoveryStorageError(RecoveryError):
    """The restore transaction failed; the primary kept its previous state."""

    http_status = 500


@dataclass
class RecoveryResult:
    status: str
    serial: int
    backup_serial: int
    record_count: int
    published_at: str
    zone: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "serial": self.serial,
            "backup_serial": self.backup_serial,
            "record_count": self.record_count,
            "published_at": self.published_at,
            "zone": self.zone,
        }


async def _fetch_backup_zone(
    client: httpx.AsyncClient,
) -> Dict[str, Any]:
    url = f"{settings.backup_api_url.rstrip('/')}/api/zone"
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        payload = resp.json()
    except httpx.HTTPError as exc:
        raise BackupUnreachable(
            f"backup API unreachable at {url}: {type(exc).__name__}: {exc}"
        ) from exc
    except ValueError as exc:  # response body is not valid JSON
        raise BackupDataInvalid(
            f"backup returned a non-JSON zone payload: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise BackupDataInvalid("backup zone payload is not a JSON object")
    return payload


def _validate_backup_payload(
    payload: Dict[str, Any],
) -> ZonePayload:
    zone = settings.dns_zone.rstrip(".").lower()
    remote_zone_raw = payload.get("zone")
    if not isinstance(remote_zone_raw, str):
        raise BackupDataInvalid("backup payload lacks a string 'zone' field")
    remote_zone = remote_zone_raw.strip().rstrip(".").lower()
    if remote_zone != zone:
        raise BackupDataInvalid(
            f"backup zone {remote_zone!r} does not match the configured "
            f"fixed zone {zone!r}"
        )

    serial_raw = payload.get("serial")
    if not isinstance(serial_raw, int) or isinstance(serial_raw, bool):
        raise BackupDataInvalid("backup payload lacks an integer 'serial'")
    if serial_raw < 1:
        raise BackupDataInvalid(
            f"backup holds no publishable zone (serial={serial_raw}); "
            "there is nothing to recover"
        )

    records = payload.get("records")
    if not isinstance(records, list) or not records:
        raise BackupDataInvalid(
            "backup payload must carry a non-empty list of A records"
        )
    try:
        # Reuse the exact publish-time validation: IPv4, in-zone names,
        # label syntax, no duplicates, maximum record count.
        zone_payload = ZonePayload.model_validate({"records": records})
    except ValidationError as exc:
        raise BackupDataInvalid(
            f"backup zone records failed validation: {exc.errors()[0]['msg']}"
        ) from exc

    published_at = payload.get("published_at")
    if not isinstance(published_at, str) or not published_at:
        raise BackupDataInvalid(
            "backup payload lacks a string 'published_at' timestamp"
        )
    return zone_payload


async def recover_from_backup() -> RecoveryResult:
    """Run one operator-triggered recovery. Never partially mutates state."""
    if storage.active_serial() != 0:
        # Fast pre-check before even contacting the backup; the storage
        # layer performs the authoritative re-check inside the transaction.
        raise PrimaryConflict(
            "primary already holds a published zone (serial>0); recovery is "
            "only allowed while the primary is the empty seed zone"
        )

    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        payload = await _fetch_backup_zone(client)
        backup_serial = payload["serial"]
        zone_payload = _validate_backup_payload(payload)

    records: List[Dict[str, str]] = [
        r.model_dump() for r in zone_payload.sorted_records()
    ]
    # The restored version is a fresh primary version: stamp it now.
    published_at = utcnow_iso()
    try:
        new_serial = storage.recover_empty_zone(
            int(backup_serial), records, published_at
        )
    except PrimaryNotEmpty as exc:
        # A normal publish landed while the backup pull was in flight.
        log.warning("recovery aborted, primary not empty: %s", exc)
        raise PrimaryConflict(str(exc)) from exc
    except Exception as exc:  # storage/SQLite failure: keep original state
        log.error("recovery storage failed, primary state unchanged: %s", exc)
        raise RecoveryStorageError(
            f"failed to persist the restored zone: {type(exc).__name__}: {exc}"
        ) from exc

    log.info(
        "recovered primary from backup: backup_serial=%s new_serial=%s "
        "records=%s",
        backup_serial, new_serial, len(records),
    )
    return RecoveryResult(
        status="recovered",
        serial=new_serial,
        backup_serial=int(backup_serial),
        record_count=len(records),
        published_at=published_at,
        zone=settings.dns_zone,
    )
