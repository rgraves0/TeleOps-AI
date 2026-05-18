from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

logger = logging.getLogger(__name__)


# =========================================================
# PERMISSIONS
# =========================================================


class Permission(
    str,
    Enum,
):

    # =====================================================
    # USER MANAGEMENT
    # =====================================================

    MANAGE_USERS = (
        "manage_users"
    )

    VIEW_USERS = (
        "view_users"
    )

    DELETE_USERS = (
        "delete_users"
    )

    # =====================================================
    # MAILBOX
    # =====================================================

    VIEW_MAILBOX = (
        "view_mailbox"
    )

    MANAGE_MAILBOX = (
        "manage_mailbox"
    )

    SEND_EMAIL = (
        "send_email"
    )

    # =====================================================
    # SYSTEM
    # =====================================================

    VIEW_SYSTEM = (
        "view_system"
    )

    MANAGE_SYSTEM = (
        "manage_system"
    )

    # =====================================================
    # AI
    # =====================================================

    USE_AI = (
        "use_ai"
    )

    CLEAR_MEMORY = (
        "clear_memory"
    )

    # =====================================================
    # STORAGE
    # =====================================================

    SEARCH_STORAGE = (
        "search_storage"
    )

    MANAGE_STORAGE = (
        "manage_storage"
    )


# =========================================================
# ROLES
# =========================================================


class Role(
    str,
    Enum,
):

    OWNER = "owner"

    ADMIN = "admin"

    USER = "user"

    GUEST = "guest"


# =========================================================
# ROLE PERMISSIONS
# DEFAULT DENY
# =========================================================


ROLE_PERMISSIONS: dict[
    Role,
    set[Permission],
] = {

    Role.OWNER: {
        permission
        for permission
        in Permission
    },

    Role.ADMIN: {

        Permission.VIEW_USERS,
        Permission.VIEW_MAILBOX,
        Permission.MANAGE_MAILBOX,
        Permission.SEND_EMAIL,
        Permission.VIEW_SYSTEM,
        Permission.USE_AI,
        Permission.CLEAR_MEMORY,
        Permission.SEARCH_STORAGE,
    },

    Role.USER: {

        Permission.VIEW_MAILBOX,
        Permission.SEND_EMAIL,
        Permission.USE_AI,
        Permission.CLEAR_MEMORY,
        Permission.SEARCH_STORAGE,
    },

    Role.GUEST: set(),
}


# =========================================================
# ACCESS CONTEXT
# =========================================================


@dataclass
class AccessContext:

    telegram_id: int

    role: Role

    is_active: bool


# =========================================================
# RBAC ENGINE
# =========================================================


class RBACManager:

    def __init__(
        self,
    ) -> None:

        logger.info(
            "RBACManager initialized"
        )

    # =====================================================
    # CHECK SINGLE PERMISSION
    # =====================================================

    def has_permission(
        self,
        context: AccessContext,
        permission: Permission,
    ) -> bool:

        # =================================================
        # DEFAULT DENY
        # =================================================

        if not context.is_active:

            logger.warning(
                "Inactive user denied "
                "telegram_id=%s",
                context.telegram_id,
            )

            return False

        allowed_permissions = (
            ROLE_PERMISSIONS.get(
                context.role,
                set(),
            )
        )

        allowed = (
            permission
            in allowed_permissions
        )

        logger.info(
            "Permission check "
            "telegram_id=%s "
            "role=%s "
            "permission=%s "
            "allowed=%s",
            context.telegram_id,
            context.role,
            permission,
            allowed,
        )

        return allowed

    # =====================================================
    # REQUIRE PERMISSION
    # =====================================================

    def require_permission(
        self,
        context: AccessContext,
        permission: Permission,
    ) -> None:

        if not self.has_permission(
            context,
            permission,
        ):

            raise PermissionError(
                f"Permission denied: "
                f"{permission}"
            )

    # =====================================================
    # MULTI PERMISSION CHECK
    # =====================================================

    def has_any_permission(
        self,
        context: AccessContext,
        permissions: Iterable[
            Permission
        ],
    ) -> bool:

        return any(
            self.has_permission(
                context,
                permission,
            )
            for permission
            in permissions
        )

    def has_all_permissions(
        self,
        context: AccessContext,
        permissions: Iterable[
            Permission
        ],
    ) -> bool:

        return all(
            self.has_permission(
                context,
                permission,
            )
            for permission
            in permissions
        )

    # =====================================================
    # ROLE HELPERS
    # =====================================================

    def is_owner(
        self,
        context: AccessContext,
    ) -> bool:

        return (
            context.role
            == Role.OWNER
        )

    def is_admin(
        self,
        context: AccessContext,
    ) -> bool:

        return context.role in (
            Role.OWNER,
            Role.ADMIN,
        )

    def can_access_admin_panel(
        self,
        context: AccessContext,
    ) -> bool:

        return self.has_any_permission(
            context,
            [
                Permission.MANAGE_USERS,
                Permission.MANAGE_SYSTEM,
            ],
        )
