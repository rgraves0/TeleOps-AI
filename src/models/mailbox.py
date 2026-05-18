from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


# =========================================================
# MAILBOX ACCESS LEVELS
# =========================================================


class MailboxAccessLevel(
    str,
    Enum,
):

    READ_ONLY = (
        "read_only"
    )

    READ_WRITE = (
        "read_write"
    )

    FULL_CONTROL = (
        "full_control"
    )


# =========================================================
# MAILBOX TYPES
# =========================================================


class MailboxType(
    str,
    Enum,
):

    PRIVATE = "private"

    SHARED = "shared"


# =========================================================
# MAILBOX PERMISSION
# =========================================================


@dataclass
class MailboxPermission:

    mailbox_id: str

    telegram_id: int

    access_level: (
        MailboxAccessLevel
    )

    granted_by: int

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )


# =========================================================
# SHARED MAILBOX
# =========================================================


@dataclass
class SharedMailbox:

    mailbox_id: str

    display_name: str

    email_address: str

    created_by: int

    members: list[
        MailboxPermission
    ] = field(
        default_factory=list
    )

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    updated_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    is_active: bool = True


# =========================================================
# PRIVATE MAILBOX
# =========================================================


@dataclass
class PrivateMailbox:

    mailbox_id: str

    owner_telegram_id: int

    email_address: str

    display_name: str

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    updated_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    is_active: bool = True


# =========================================================
# MAILBOX ACCESS ENGINE
# =========================================================


class MailboxAccessManager:

    # =====================================================
    # CHECK ACCESS
    # =====================================================

    @staticmethod
    def has_access(
        mailbox: SharedMailbox,
        telegram_id: int,
        required_level: (
            MailboxAccessLevel
        ),
    ) -> bool:

        hierarchy = {

            MailboxAccessLevel.READ_ONLY:
            1,

            MailboxAccessLevel.READ_WRITE:
            2,

            MailboxAccessLevel.FULL_CONTROL:
            3,
        }

        for member in mailbox.members:

            if (
                member.telegram_id
                != telegram_id
            ):

                continue

            member_level = (
                hierarchy[
                    member.access_level
                ]
            )

            required = (
                hierarchy[
                    required_level
                ]
            )

            return (
                member_level
                >= required
            )

        return False

    # =====================================================
    # GRANT ACCESS
    # =====================================================

    @staticmethod
    def grant_access(
        mailbox: SharedMailbox,
        permission: (
            MailboxPermission
        ),
    ) -> None:

        mailbox.members.append(
            permission
        )

        mailbox.updated_at = (
            datetime.utcnow()
            .isoformat()
        )

    # =====================================================
    # REVOKE ACCESS
    # =====================================================

    @staticmethod
    def revoke_access(
        mailbox: SharedMailbox,
        telegram_id: int,
    ) -> None:

        mailbox.members = [

            member

            for member
            in mailbox.members

            if member.telegram_id
            != telegram_id
        ]

        mailbox.updated_at = (
            datetime.utcnow()
            .isoformat()
        )

    # =====================================================
    # UPDATE ACCESS LEVEL
    # =====================================================

    @staticmethod
    def update_access_level(
        mailbox: SharedMailbox,
        telegram_id: int,
        new_level: (
            MailboxAccessLevel
        ),
    ) -> None:

        for member in (
            mailbox.members
        ):

            if (
                member.telegram_id
                == telegram_id
            ):

                member.access_level = (
                    new_level
                )

        mailbox.updated_at = (
            datetime.utcnow()
            .isoformat()
        )
