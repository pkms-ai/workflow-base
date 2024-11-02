# This module implements a RabbitMQ consumer for the Content Processing Service.
# It connects to a RabbitMQ server, listens for messages on the content processing queue,
# and processes incoming content using the `process_content` function from the
# `processors` module.

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Tuple

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractIncomingMessage,
    AbstractQueue,
)

logger = logging.getLogger(__name__)


class RabbitMQConsumer:
    """
    The main class that handles the RabbitMQ connection, channel setup, and message consumption.
    """

    def __init__(
        self,
        rabbitmq_url: str,
        input_queue: str,
        error_queue: str,
        output_queues: List[str],
        process_func: Callable[[Dict[str, Any]], Awaitable[Tuple[str, Any]]],
        process_error_handler: Optional[
            Callable[
                [Exception, Optional[Dict[str, Any]], AbstractIncomingMessage],
                Coroutine[Any, Any, None],
            ]
        ] = None,
        processing_timeout: int = 300,
        max_retries: int = 3,
    ) -> None:
        self.rabbitmq_url = rabbitmq_url
        self.input_queue_name = input_queue
        self.error_queue_name = error_queue
        self.output_queues = output_queues
        self.process_func = process_func
        self.process_error_handler = process_error_handler
        self.processing_timeout = processing_timeout
        self.max_retries = max_retries
        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.input_queue: Optional[AbstractQueue] = None
        self.error_queue: Optional[AbstractQueue] = None

    async def connect(self) -> None:
        """
        Establishes a robust connection to the RabbitMQ server and sets up a channel
        with a prefetch count of 1.
        """
        if not self.connection or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(
                self.rabbitmq_url,
                heartbeat=60,  # Set heartbeat interval to 60 seconds
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(
                prefetch_count=1
            )  # Explicitly set prefetch count
            logger.info("Connected to RabbitMQ")

    async def setup_queues(self) -> None:
        if self.channel is None:
            raise RuntimeError("Channel is not initialized")

        # Declare all necessary queues
        queues_to_declare = [
            self.input_queue_name,
            self.error_queue_name,
        ] + self.output_queues

        for queue_name in queues_to_declare:
            queue = await self.channel.declare_queue(queue_name, durable=True)
            logger.info(f"Declared and bound queue: {queue.name}")

        # Set the content processing queue
        self.input_queue = await self.channel.get_queue(self.input_queue_name)

    async def process_message(self, message: AbstractIncomingMessage) -> None:
        content: Dict[str, Any] = dict()
        try:
            body = message.body.decode()
            content: Dict[str, Any] = json.loads(body)

            logger.info(f"Received message: {content.get('url')}")
            queue_name, processed_content = await asyncio.wait_for(
                self.process_func(content), timeout=self.processing_timeout
            )

            if queue_name == "":
                # in case queue name is not in output queues, we assume the worklow is completed
                logger.info(
                    "Processed content. Workflow completed. No further processing required."
                )
            else:
                logger.info(f"Processed content. Forwarding to queue: {queue_name}")

                if queue_name not in self.output_queues:
                    raise ValueError(f"Queue {queue_name} is not in output queues")

                sending_message = aio_pika.Message(
                    body=json.dumps(processed_content).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                # Publish the processed content to the appropriate next queue
                await self.publish_message(queue_name, sending_message)

            await message.ack()
        except Exception as e:
            # Use the custom handler if available, otherwise default handling
            if self.process_error_handler:
                try:
                    await self.process_error_handler(e, content, message)
                    return
                except Exception as custom_handler_error:
                    logger.error(
                        f"Error in custom error handler: {str(custom_handler_error)}"
                    )
                    e = custom_handler_error  # Assign custom handler error to fall back to default handling

            # Fallback to default handling if no custom handler or custom handler fails
            logger.error(f"Unhandled error: {str(e)}")
            await self.handle_failed_message(message)

    async def handle_failed_message(self, message: AbstractIncomingMessage) -> None:
        """
        Handles failed messages by either requeuing them with a delay or moving them to the error queue.

        This method implements a retry mechanism:
        1. If the retry count is less than MAX_RETRIES, it requeues the message with an incremented retry count.
        2. If the retry count has reached MAX_RETRIES, it moves the message to the error queue.

        Args:
            message (AbstractIncomingMessage): The failed message from RabbitMQ.
        """
        # Safely get the retry count, defaulting to 0 if header or x-retry-count doesn't exist or is invalid
        retry_count = 0
        if hasattr(message, "headers"):
            retry_count_value = message.headers.get("x-retry-count")
            if retry_count_value is not None:
                try:
                    if isinstance(retry_count_value, (int, str)):
                        retry_count = int(retry_count_value)
                    else:
                        logger.warning(
                            f"Unexpected type for retry count: {type(retry_count_value)}. Defaulting to 0."
                        )
                except ValueError:
                    logger.warning(
                        f"Invalid retry count in message headers: {retry_count_value}. Defaulting to 0."
                    )
        retry_count += 1

        body = message.body.decode()
        content: Dict[str, Any] = json.loads(body)

        # Preserve existing headers if they exist, otherwise start with an empty dict
        existing_headers = message.headers if hasattr(message, "headers") else {}
        new_headers = {**existing_headers, "x-retry-count": retry_count}

        new_message = aio_pika.Message(
            body=message.body,
            headers=new_headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        if retry_count < self.max_retries:
            # sleep for 10 seconds before requeuing the message
            await asyncio.sleep(10)
            # Requeue the message
            await self.publish_message(
                self.input_queue_name,
                new_message,
            )
            logger.info(
                f"Requeued message: {content.get('url')}, with retry count: {retry_count}"
            )
        else:
            # Move to error queue
            new_message.headers["x-error-reason"] = "exceeded_max_retries"
            await self.publish_message(
                self.error_queue_name,
                new_message,
            )
            logger.warning(
                f"Message exceeded max retries. Moved to error queue: {content.get('url')}"
            )

        await message.ack()

    async def publish_message(
        self, routing_key: str, message: aio_pika.Message
    ) -> None:
        if self.channel is None:
            raise RuntimeError("Channel is not initialized")

        await self.channel.default_exchange.publish(message, routing_key=routing_key)
        logger.info(f"Published message to queue: {routing_key}")

    async def start_consuming(self) -> None:
        """
        Starts consuming messages from the content processing queue specified in the configuration.
        """
        if self.input_queue is None:
            raise RuntimeError(f"{self.input_queue_name} is not initialized")
        await self.input_queue.consume(self.process_message)
        logger.info(f"Started consuming from {self.input_queue_name}")

    async def run(self) -> None:
        """
        Main loop that connects to RabbitMQ, sets up the queues, and starts consuming messages.
        Implements error handling and reconnection logic.
        """
        backoff_time = 5
        while True:
            try:
                await self.connect()
                await self.setup_queues()
                await asyncio.wait_for(self.start_consuming(), timeout=60)
                # await self.start_consuming()
                # Keep the consumer running, but allow for interruption
                await asyncio.Future()
            except aio_pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"RabbitMQ connection error: {e}. Reconnecting...")
                await asyncio.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)
            except asyncio.CancelledError:
                logger.info("RabbitMQ consumer is shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                await asyncio.sleep(5)
            finally:
                if (
                    self.connection
                    and self.connection is not None
                    and self.connection.is_closed is False
                ):
                    await self.connection.close()
                # reset backoff time after successful connection
                backoff_time = 5

    async def stop(self):
        logger.info("Shutting down consumer")
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                logger.info("Channel closed")
        except Exception as e:
            logger.error(f"Error closing channel: {e}")

        try:
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

        # Optionally, set a flag here if double shutdown protection is needed
        # self._is_shutting_down = True
