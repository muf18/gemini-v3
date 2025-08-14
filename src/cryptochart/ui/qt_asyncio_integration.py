import asyncio
import sys
from collections.abc import Coroutine
from typing import Any

import qt_asyncio
from loguru import logger
from PySide6.QtWidgets import QApplication


def run_with_asyncio(main_coro: Coroutine[Any, Any, int]) -> int:
    """Sets up and runs the application with an integrated Qt and asyncio event loop.

    This function is the main entry point for the application. It initializes
    the QApplication, installs an event loop that can drive both Qt's GUI events
    and asyncio's tasks, and then runs the main application coroutine.

    Args:
        main_coro: The main asynchronous function (coroutine) of the application.
                   This coroutine should set up the UI and the core logic.

    Returns:
        The exit code of the application.
    """
    # Get or create the QApplication instance. This must be done before
    # any widgets are created.
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)

    # Set up the asyncio event loop.
    # QAsyncioEventLoop integrates seamlessly with QApplication.exec().
    # When app.exec() is called, this loop will run both Qt events and
    # asyncio tasks.
    try:
        loop = qt_asyncio.QAsyncioEventLoop(app)
        asyncio.set_event_loop(loop)
        logger.info("QAsyncioEventLoop installed as the current asyncio event loop.")
    except Exception:
        logger.exception(
            "Failed to set up the QAsyncioEventLoop. The application may not "
            "function correctly."
        )
        # Fallback to a standard loop if qt_asyncio fails, though this is unlikely.
        loop = asyncio.get_event_loop()

    exit_code = 0
    try:
        # The main application logic is started as a task in the asyncio loop.
        main_task = loop.create_task(main_coro)

        # Start the Qt event loop. This is a blocking call that will run until
        # the application is closed. The QAsyncioEventLoop ensures that our
        # main_task and other asyncio tasks run concurrently.
        logger.info("Starting the Qt application event loop.")
        app.exec()
        logger.info("Qt application event loop has finished.")

        # After the Qt loop exits, we check if our main task finished with an error.
        if main_task.done() and not main_task.cancelled():
            exception = main_task.exception()
            if exception:
                logger.error(
                    f"The main application task exited with an exception: {exception}"
                )
                # Propagate the exception for debugging purposes if not handled.
                raise exception
            exit_code = main_task.result()

    finally:
        # --- Graceful Shutdown ---
        logger.info("Closing the asyncio event loop.")
        # Gather and cancel all remaining tasks to prevent "Task was destroyed
        # but it is pending!" warnings.
        tasks = asyncio.all_tasks(loop=loop)
        for task in tasks:
            task.cancel()

        # Wait for all tasks to be cancelled.
        try:
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        except Exception as e:
            logger.warning(f"Exception during task cancellation: {e}")

        # Close the loop itself.
        loop.close()
        asyncio.set_event_loop(None)
        logger.info("Asyncio event loop closed.")

    return exit_code