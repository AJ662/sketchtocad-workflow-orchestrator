import json
import logging
from typing import AsyncIterator, Optional, Callable, Any, Dict
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

from .base import SagaEvent
from .types import EventType

logger = logging.getLogger(__name__)

# OWASP A04: Max retries before sending to DLQ
MAX_RETRIES = 3
DLQ_TOPIC = "saga-events-dlq"


class KafkaEventBus:
    """Kafka-based event bus for saga events"""

    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        # OWASP A04: Track retry counts per message key to prevent poison pill loops
        self._retry_counts: Dict[str, int] = {}

    async def start_producer(self):
        """Initialize Kafka producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            acks="all",  # Wait for all replicas
        )
        await self.producer.start()
        logger.info("Kafka producer started")

    async def stop_producer(self):
        """Stop Kafka producer"""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

    async def _send_to_dlq(
        self, msg_value: dict, error: str, original_topic: str
    ) -> None:
        """Send failed message to Dead Letter Queue (OWASP A04 - Resilience)"""
        if not self.producer:
            logger.error("Cannot send to DLQ: producer not started")
            return

        dlq_message = {
            "original_message": msg_value,
            "error": str(error),
            "original_topic": original_topic,
            "retry_count": MAX_RETRIES,
        }

        try:
            await self.producer.send_and_wait(
                DLQ_TOPIC,
                dlq_message,
                key=msg_value.get("saga_id", "unknown").encode("utf-8"),
            )
            logger.warning(
                f"Message sent to DLQ after {MAX_RETRIES} failures: "
                f"saga_id={msg_value.get('saga_id')}, error={error}"
            )
        except KafkaError as e:
            logger.error(f"Failed to send message to DLQ: {e}")

    async def publish(self, topic: str, event: SagaEvent) -> None:
        """
        Publish an event to a Kafka topic

        Args:
            topic: Kafka topic name (saga-commands or saga-events)
            event: SagaEvent to publish
        """
        if not self.producer:
            raise RuntimeError("Producer not started. Call start_producer() first")

        try:
            # Serialize event to JSON-compatible dict
            event_dict = event.model_dump(mode="json")

            # Use saga_id as partition key for ordering
            key = event.saga_id.encode("utf-8")

            # Send to Kafka
            await self.producer.send_and_wait(topic, event_dict, key=key)

            logger.info(
                f"Published event: {event.event_type} "
                f"[saga_id={event.saga_id}, correlation_id={event.correlation_id}] "
                f"to topic: {topic}"
            )

        except KafkaError as e:
            logger.error(f"Failed to publish event: {e}")
            raise

    async def subscribe(
        self,
        topics: list[str],
        group_id: str,
        handler: Optional[Callable[[SagaEvent], Any]] = None,
    ) -> AsyncIterator[SagaEvent]:
        """
        Subscribe to Kafka topics and consume events

        Args:
            topics: List of topic names to subscribe to
            group_id: Consumer group ID
            handler: Optional async callback to process each event

        Yields:
            SagaEvent objects
        """
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit for error handling
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000,
            max_poll_records=10,
        )

        await self.consumer.start()
        logger.info(f"Kafka consumer started: group_id={group_id}, topics={topics}")

        try:
            async for msg in self.consumer:
                msg_key = f"{msg.topic}:{msg.partition}:{msg.offset}"

                try:
                    # Deserialize to SagaEvent
                    event_data = msg.value
                    event = SagaEvent(**event_data)

                    logger.debug(
                        f"Consumed event: {event.event_type} "
                        f"[saga_id={event.saga_id}] "
                        f"from topic: {msg.topic}"
                    )

                    # Call handler if provided
                    if handler:
                        await handler(event)

                    # Success - commit offset and clear retry count
                    await self.consumer.commit()
                    self._retry_counts.pop(msg_key, None)

                    # Yield event for external processing
                    yield event

                except Exception as e:
                    # OWASP A04: Track retries and send to DLQ after MAX_RETRIES
                    self._retry_counts[msg_key] = self._retry_counts.get(msg_key, 0) + 1
                    retry_count = self._retry_counts[msg_key]

                    logger.error(
                        f"Error processing message (attempt {retry_count}/{MAX_RETRIES}) "
                        f"from {msg.topic}: {e}",
                        exc_info=True,
                    )

                    if retry_count >= MAX_RETRIES:
                        # Send to DLQ and commit to prevent infinite loop
                        await self._send_to_dlq(msg.value, str(e), msg.topic)
                        await self.consumer.commit()
                        self._retry_counts.pop(msg_key, None)
                        logger.warning(f"Message committed after DLQ send: {msg_key}")
                    # else: Don't commit - message will be reprocessed on restart

        finally:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    async def close(self):
        """Close all connections"""
        await self.stop_producer()
        if self.consumer:
            await self.consumer.stop()
