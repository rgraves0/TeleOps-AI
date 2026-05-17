from __future__ import annotations

# =========================================================
# SQLITE TABLE DEFINITIONS
# =========================================================

CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    telegram_id INTEGER NOT NULL UNIQUE,
    username TEXT,
    full_name TEXT,

    role_id INTEGER NOT NULL,

    is_active INTEGER NOT NULL DEFAULT 1,
    is_banned INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (role_id) REFERENCES roles(id)
);
"""

CREATE_ROLES_TABLE = """
CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    name TEXT NOT NULL UNIQUE,
    description TEXT,

    permissions TEXT NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INBOXES_TABLE = """
CREATE TABLE IF NOT EXISTS inboxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('private', 'shared')),

    owner_user_id INTEGER,
    telegram_chat_id INTEGER,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (owner_user_id) REFERENCES users(id)
);
"""

CREATE_REMINDERS_TABLE = """
CREATE TABLE IF NOT EXISTS reminders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    user_id INTEGER NOT NULL,

    title TEXT NOT NULL,
    description TEXT,

    remind_at TIMESTAMP NOT NULL,

    is_sent INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(id)
);
"""

CREATE_AUDIT_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    user_id INTEGER,
    action TEXT NOT NULL,

    target_type TEXT,
    target_id TEXT,

    details TEXT,

    ip_address TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(id)
);
"""

CREATE_USER_INBOXES_TABLE = """
CREATE TABLE IF NOT EXISTS user_inboxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    user_id INTEGER NOT NULL,
    inbox_id INTEGER NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(user_id, inbox_id),

    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (inbox_id) REFERENCES inboxes(id)
);
"""

CREATE_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_users_telegram_id
    ON users(telegram_id);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_users_role_id
    ON users(role_id);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_reminders_user_id
    ON reminders(user_id);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_reminders_remind_at
    ON reminders(remind_at);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id
    ON audit_logs(user_id);
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_inboxes_owner
    ON inboxes(owner_user_id);
    """
]

DEFAULT_ROLES = [
    {
        "name": "owner",
        "description": "System owner with full permissions",
        "permissions": [
            "*"
        ]
    },
    {
        "name": "admin",
        "description": "Administrative user",
        "permissions": [
            "users.read",
            "users.write",
            "roles.read",
            "inboxes.read",
            "inboxes.write",
            "reminders.read",
            "reminders.write",
            "audit.read",
            "ai.use",
            "system.manage"
        ]
    },
    {
        "name": "user",
        "description": "Default user role",
        "permissions": [
            "inboxes.read_own",
            "reminders.read_own",
            "reminders.write_own",
            "ai.use"
        ]
    }
]
