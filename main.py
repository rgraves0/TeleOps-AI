from __future__ import annotations
        logger.info(
            "Stopping scheduler..."
        )

        await scheduler_manager.shutdown()

        logger.info(
            "Disconnecting database..."
        )

        await db.disconnect()

        logger.info(
            "Shutdown completed"
        )

    def register_signal_handlers(
        self
    ) -> None:
        loop = asyncio.get_running_loop()

        for sig in (
            signal.SIGINT,
            signal.SIGTERM
        ):
            loop.add_signal_handler(
                sig,
                self.shutdown_event.set
            )

    async def run(self) -> None:
        self.register_signal_handlers()

        await self.initialize()

        await self.start_bot()

        logger.info(
            "TeleOps-AI is fully operational"
        )

        await self.shutdown_event.wait()

        logger.info(
            "Shutdown signal received"
        )

        await self.shutdown()


async def main() -> None:
    application = (
        TeleOpsApplication()
    )

    try:
        await application.run()

    except KeyboardInterrupt:
        logger.warning(
            "Keyboard interrupt received"
        )

        await application.shutdown()

    except Exception:
        logger.exception(
            "Fatal application error"
        )

        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
