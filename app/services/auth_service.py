from __future__ import annotations

import os

from dotenv import load_dotenv
from telegram import User

from app.core.security import (
    AuthenticationException,
)
from app.database.repositories.roles import (
    RoleRepository,
)
from app.database.repositories.users import (
    UserRepository,
)

load_dotenv()


class AuthService:
    def __init__(self):
        self.user_repository = UserRepository()
        self.role_repository = RoleRepository()

        self.owner_ids = {
            int(user_id.strip())
            for user_id in os.getenv(
                "TELEGRAM_ADMIN_IDS",
                ""
            ).split(",")
            if user_id.strip().isdigit()
        }

    async def authenticate_telegram_user(
        self,
        telegram_user: User
    ) -> dict:
        if telegram_user is None:
            raise AuthenticationException(
                "Telegram user is missing"
            )

        telegram_id = telegram_user.id

        existing_user = (
            await self.user_repository
            .get_by_telegram_id(telegram_id)
        )

        if existing_user:
            if existing_user["is_banned"]:
                raise AuthenticationException(
                    "User is banned"
                )

            if not existing_user["is_active"]:
                raise AuthenticationException(
                    "User account is inactive"
                )

            return existing_user

        role_name = self._determine_default_role(
            telegram_id
        )

        full_name = (
            telegram_user.full_name
            or telegram_user.first_name
            or "Unknown User"
        )

        await self.user_repository.create_user(
            telegram_id=telegram_id,
            username=telegram_user.username,
            full_name=full_name,
            role_name=role_name
        )

        created_user = (
            await self.user_repository
            .get_by_telegram_id(telegram_id)
        )

        if created_user is None:
            raise AuthenticationException(
                "Failed to create user"
            )

        return created_user

    async def verify_permission(
        self,
        telegram_id: int,
        permission: str
    ) -> bool:
        user = (
            await self.user_repository
            .get_by_telegram_id(telegram_id)
        )

        if user is None:
            return False

        role_name = user["role_name"]

        return (
            await self.role_repository
            .has_permission(
                role_name,
                permission
            )
        )

    async def verify_role(
        self,
        telegram_id: int,
        allowed_roles: list[str]
    ) -> bool:
        user = (
            await self.user_repository
            .get_by_telegram_id(telegram_id)
        )

        if user is None:
            return False

        return (
            user["role_name"]
            in allowed_roles
        )

    async def assign_role(
        self,
        telegram_id: int,
        role_name: str
    ) -> bool:
        role = (
            await self.role_repository
            .get_role_by_name(role_name)
        )

        if role is None:
            raise ValueError(
                f"Role '{role_name}' does not exist"
            )

        user = (
            await self.user_repository
            .get_by_telegram_id(telegram_id)
        )

        if user is None:
            raise AuthenticationException(
                "User not found"
            )

        return (
            await self.user_repository
            .assign_role(
                user["id"],
                role_name
            )
        )

    def _determine_default_role(
        self,
        telegram_id: int
    ) -> str:
        if telegram_id in self.owner_ids:
            return "owner"

        return "user"
