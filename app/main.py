# Uncomment this to pass the first stage
import socket
import asyncio

from app.handler.handler import main_with_event_loop


if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.run(main_with_event_loop())
