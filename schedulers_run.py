import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.utils.external_api import start_scheduler


async def start_job():
    scheduler = AsyncIOScheduler()
    if not scheduler.running:
        start_scheduler(scheduler)
        scheduler.start()
        print("Scheduler is starting!")
    try:
        # Run forever
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(start_job())
