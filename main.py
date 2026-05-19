from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

from dotenv import load_dotenv

# =========================================================
# CORE / DATABASE
# =========================================================
from app.core.scheduler import scheduler_service
from app.database.base import close_database, init_db
from app.database.repositories.chat_memory import chat_memory_repository
from app.database.repositories.rclone_meta import RcloneMetaRepository
from app.interfaces.telegram.bot import TelegramBot
from app.plugins.loader import plugin_loader
from app.services.reminder_service import ReminderService

# =========================================================
# PHASE 11 — COORDINATION LAYER
# =========================================================
from src.core.message_bus import MessageBus

from src.agents.memory_agent import MemoryAgent
from src.agents.monitoring_agent import MonitoringAgent
from src.agents.recovery_agent import RecoveryAgent
from src.agents.coordination_engine import CoordinationEngine

# =========================================================
# PHASE 14 — AUTONOMOUS OPS BRAIN
# =========================================================
from src.brain.operational_planner import OperationalPlanner
from src.brain.predictive_planner import PredictivePlanner
from src.brain.adaptive_orchestrator import AdaptiveOrchestrator
from src.brain.decision_engine import OperationalDecisionEngine

# =========================================================
# PHASE 15 — SECURITY
# =========================================================
from src.security.audit_logger import AuditLogger
from src.security.abuse_preventer import AbusePreventer

# =========================================================
# PHASE 16 — RELIABILITY
# =========================================================
from src.reliability.runtime_manager import RuntimeManager
from src.reliability.crash_recovery import CrashRecoveryManager
from src.reliability.degraded_orchestrator import DegradedModeOrchestrator
from src.reliability.persistent_queue import PersistentQueue

# =========================================================
# PHASE 17 — TELEGRAM CONSOLE
# =========================================================
from src.console.live_dashboard import LiveDashboard
from src.console.workflow_controls import WorkflowControls
from src.console.runtime_controls import RuntimeControls
from src.console.memory_inspector import MemoryInspector
from src.console.abuse_review import AbuseReviewConsole

# =========================================================
# PHASE 20 — LIFECYCLE
# =========================================================
from src.lifecycle.db_maintenance import DatabaseMaintenanceManager
from src.lifecycle.audit_exporter import AuditExporter
from src.lifecycle.key_rotator import KeyRotationManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class TeleOpsApplication:
    """
    Main TeleOps AI bootstrap runtime.

    Responsibilities:
    - Database bootstrap
    - Agent lifecycle orchestration
    - Message bus coordination
    - Security/runtime initialization
    - Telegram operations console startup
    - Graceful shutdown handling
    """

    def __init__(self) -> None:
        self.running = False
        self.shutdown_event = asyncio.Event()

        # =====================================================
        # TELEGRAM
        # =====================================================
        self.bot = TelegramBot()

        # =====================================================
        # DATABASE REPOSITORIES
        # =====================================================
        self.rclone_repository = RcloneMetaRepository()
        self.reminder_service = ReminderService()

        # =====================================================
        # CORE BUS
        # =====================================================
        self.message_bus = MessageBus()

        # =====================================================
        # SECURITY
        # =====================================================
        self.audit_logger = AuditLogger()
        self.abuse_preventer = AbusePreventer()

        # =====================================================
        # RELIABILITY
        # =====================================================
        self.runtime_manager = RuntimeManager()
        self.crash_recovery = CrashRecoveryManager()
        self.degraded_orchestrator = DegradedModeOrchestrator()
        self.persistent_queue = PersistentQueue()

        # =====================================================
        # AGENTS
        # =====================================================
        self.memory_agent = MemoryAgent(self.message_bus)

        self.monitoring_agent = MonitoringAgent(
            message_bus=self.message_bus
        )

        self.recovery_agent = RecoveryAgent(
            message_bus=self.message_bus
        )

        # =====================================================
        # AUTONOMOUS BRAIN
        # =====================================================
        self.operational_planner = OperationalPlanner(
            message_bus=self.message_bus
        )

        self.predictive_planner = PredictivePlanner(
            message_bus=self.message_bus
        )

        self.adaptive_orchestrator = AdaptiveOrchestrator(
            message_bus=self.message_bus
        )

        self.decision_engine = OperationalDecisionEngine(
            message_bus=self.message_bus
        )

        # =====================================================
        # COORDINATION ENGINE
        # =====================================================
        self.coordination_engine = CoordinationEngine(
            message_bus=self.message_bus,
            agents=[
                self.memory_agent,
                self.monitoring_agent,
                self.recovery_agent,
            ]
        )

        # =====================================================
        # TELEGRAM CONSOLE
        # =====================================================
        self.dashboard = LiveDashboard(
            telegram_application=self.bot.application,
            message_bus=self.message_bus
        )

        self.workflow_controls = WorkflowControls(
            telegram_application=self.bot.application,
            message_bus=self.message_bus
        )

        self.runtime_controls = RuntimeControls(
            telegram_application=self.bot.application,
            message_bus=self.message_bus
        )

        self.memory_inspector = MemoryInspector(
            telegram_application=self.bot.application
        )

        self.abuse_review_console = AbuseReviewConsole(
            telegram_application=self.bot.application
        )

        # =====================================================
        # LIFECYCLE
        # =====================================================
        self.db_maintenance = DatabaseMaintenanceManager()

        self.audit_exporter = AuditExporter()

        self.key_rotator = KeyRotationManager()

    # =========================================================
    # INITIALIZATION
    # =========================================================

    async def initialize(self) -> None:
        logger.info("=================================================")
        logger.info("TeleOps-AI Bootstrap Starting")
        logger.info("=================================================")

        # =====================================================
        # DATABASE
        # =====================================================
        logger.info("Initializing database...")
        await init_db()

        logger.info("Initializing chat memory tables...")
        await chat_memory_repository.initialize_table()

        logger.info("Initializing RClone metadata tables...")
        await self.rclone_repository.initialize_table()

        # =====================================================
        # SECURITY SYSTEMS
        # =====================================================
        logger.info("Initializing audit logger...")
        await self.audit_logger.initialize()

        logger.info("Initializing abuse prevention...")
        await self.abuse_preventer.initialize()

        # =====================================================
        # RUNTIME RELIABILITY
        # =====================================================
        logger.info("Initializing persistent queue...")
        await self.persistent_queue.initialize()

        logger.info("Initializing crash recovery...")
        await self.crash_recovery.initialize()

        logger.info("Restoring latest runtime snapshot...")
        with suppress(Exception):
            await self.crash_recovery.restore_latest_snapshot()

        logger.info("Initializing degraded orchestrator...")
        await self.degraded_orchestrator.initialize()

        logger.info("Initializing runtime manager...")
        await self.runtime_manager.initialize()

        # =====================================================
        # LOAD PLUGINS
        # =====================================================
        logger.info("Loading plugins...")
        plugin_loader.load_all_plugins()

        # =====================================================
        # TELEGRAM APPLICATION
        # =====================================================
        logger.info("Attaching Telegram app to scheduler...")
        await scheduler_service.attach_application(
            self.bot.application
        )

        logger.info("Starting scheduler...")
        await scheduler_service.start()

        # =====================================================
        # RESTORE REMINDERS
        # =====================================================
        logger.info("Restoring reminders...")
        await self.reminder_service.restore_jobs()

        # =====================================================
        # START AGENTS
        # =====================================================
        logger.info("Starting coordination engine...")
        await self.coordination_engine.start()

        logger.info("Starting operational planner...")
        await self.operational_planner.start()

        logger.info("Starting predictive planner...")
        await self.predictive_planner.start()

        logger.info("Starting adaptive orchestrator...")
        await self.adaptive_orchestrator.start()

        # =====================================================
        # START CONSOLE MODULES
        # =====================================================
        logger.info("Starting dashboard...")
        await self.dashboard.start()

        logger.info("Starting workflow controls...")
        await self.workflow_controls.start()

        logger.info("Starting runtime controls...")
        await self.runtime_controls.start()

        logger.info("Starting memory inspector...")
        await self.memory_inspector.start()

        logger.info("Starting abuse review console...")
        await self.abuse_review_console.start()

        # =====================================================
        # BACKGROUND MAINTENANCE
        # =====================================================
        logger.info("Starting DB maintenance...")
        await self.db_maintenance.start()

        logger.info("Starting audit exporter...")
        await self.audit_exporter.start()

        logger.info("Starting key rotator...")
        await self.key_rotator.start()

        logger.info("=================================================")
        logger.info("TeleOps-AI Initialization Completed")
        logger.info("=================================================")

    # =========================================================
    # BOT
    # =========================================================

    async def start_bot(self) -> None:
        logger.info("Starting Telegram bot...")
        await self.bot.run()

    # =========================================================
    # SHUTDOWN
    # =========================================================

    async def shutdown(self) -> None:
        if not self.running:
            return

        logger.info("=================================================")
        logger.info("Shutdown sequence started")
        logger.info("=================================================")

        self.running = False

        # =====================================================
        # SNAPSHOT
        # =====================================================
        try:
            logger.info("Creating final runtime snapshot...")
            await self.crash_recovery.create_snapshot()
        except Exception:
            logger.exception("Snapshot creation failed")

        # =====================================================
        # TELEGRAM CONSOLE
        # =====================================================
        try:
            logger.info("Stopping dashboard...")
            await self.dashboard.shutdown()
        except Exception:
            logger.exception("Dashboard shutdown failed")

        try:
            logger.info("Stopping workflow controls...")
            await self.workflow_controls.shutdown()
        except Exception:
            logger.exception("Workflow controls shutdown failed")

        try:
            logger.info("Stopping runtime controls...")
            await self.runtime_controls.shutdown()
        except Exception:
            logger.exception("Runtime controls shutdown failed")

        try:
            logger.info("Stopping memory inspector...")
            await self.memory_inspector.shutdown()
        except Exception:
            logger.exception("Memory inspector shutdown failed")

        # =====================================================
        # AUTONOMOUS SYSTEMS
        # =====================================================
        try:
            logger.info("Stopping operational planner...")
            await self.operational_planner.shutdown()
        except Exception:
            logger.exception("Operational planner shutdown failed")

        try:
            logger.info("Stopping predictive planner...")
            await self.predictive_planner.shutdown()
        except Exception:
            logger.exception("Predictive planner shutdown failed")

        try:
            logger.info("Stopping adaptive orchestrator...")
            await self.adaptive_orchestrator.shutdown()
        except Exception:
            logger.exception("Adaptive orchestrator shutdown failed")

        # =====================================================
        # COORDINATION ENGINE
        # =====================================================
        try:
            logger.info("Stopping coordination engine...")
            await self.coordination_engine.shutdown()
        except Exception:
            logger.exception("Coordination engine shutdown failed")

        # =====================================================
        # SCHEDULER
        # =====================================================
        try:
            logger.info("Stopping scheduler...")
            await scheduler_service.shutdown()
        except Exception:
            logger.exception("Scheduler shutdown failed")

        # =====================================================
        # TELEGRAM BOT
        # =====================================================
        try:
            logger.info("Stopping Telegram bot...")
            await self.bot.shutdown()
        except Exception:
            logger.exception("Telegram bot shutdown failed")

        # =====================================================
        # DATABASE
        # =====================================================
        try:
            logger.info("Closing database...")
            await close_database()
        except Exception:
            logger.exception("Database shutdown failed")

        self.shutdown_event.set()

        logger.info("=================================================")
        logger.info("TeleOps-AI shutdown completed")
        logger.info("=================================================")

    # =========================================================
    # MAIN RUN LOOP
    # =========================================================

    async def run(self) -> None:
        try:
            self.running = True

            await self.initialize()

            logger.info("TeleOps-AI is fully operational")

            await self.start_bot()

            await self.shutdown_event.wait()

        except asyncio.CancelledError:
            logger.info("Application cancelled")

        except Exception:
            logger.exception("Fatal application error")

        finally:
            await self.shutdown()


# =============================================================
# ENTRYPOINT
# =============================================================

async def main() -> None:
    application = TeleOpsApplication()

    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("Shutdown signal received")
        asyncio.create_task(application.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            logger.warning(
                "Signal handlers unsupported on this platform"
            )

    await application.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted")
