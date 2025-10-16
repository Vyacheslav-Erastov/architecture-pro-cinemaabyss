from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import json
import logging
from threading import Thread
from kafka import KafkaProducer, KafkaConsumer
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
PORT = int(os.getenv("PORT", "8082"))


class UserEvent(BaseModel):
    user_id: int
    username: str
    action: str
    timestamp: datetime = Field(default_factory=datetime.now)


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime = Field(default_factory=datetime.now)
    method_type: str


class MovieEvent(BaseModel):
    movie_id: int
    user_id: int
    title: str
    action: str


topics = {"user": "user-events", "payment": "payment-events", "movie": "movie-events"}

producer = None
consumer_thread = None
consumer_running = False


def create_producer():
    """Create Kafka producer"""
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            retries=3,
            acks="all",
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def consume_events(topic):
    """Kafka consumer function to process events"""
    global consumer_running

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="events-service-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda x: x.decode("utf-8") if x else None,
        )

        logger.info(f"Kafka consumer started. Listening to topic: {topic}")

        while consumer_running:
            msg_pack = consumer.poll(timeout_ms=1000)

            for tp, messages in msg_pack.items():
                for message in messages:
                    try:
                        event_data = message.value
                        event_key = message.key

                        logger.info(
                            f"Received event - Key: {event_key}, Value: {event_data}"
                        )

                        process_event(event_data, topic)

                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
    finally:
        if "consumer" in locals():
            consumer.close()


def process_event(event_data: dict, topic: str):
    """Process user events"""
    logger.info(f"👤 Topic {topic} Event - Data {str(event_data)}")


def start_consumer(topic):
    """Start Kafka consumer in a separate thread"""
    global consumer_running
    consumer_running = True
    thread = Thread(target=consume_events, args=(topic,), daemon=True)
    thread.start()
    return thread


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Kafka components on startup"""
    global producer, consumer_thread

    logger.info("Starting Events Service...")

    producer = create_producer()

    if producer:
        logger.info("Kafka producer initialized")
    else:
        logger.error("Failed to initialize Kafka producer")

    for topic in topics.values():
        consumer_thread = start_consumer(topic)
    logger.info("Kafka consumers started")

    yield

    """Cleanup on shutdown"""
    global consumer_running

    consumer_running = False

    if producer:
        producer.close()
        logger.info("Kafka producer closed")


app = FastAPI(title="Events Service", lifespan=lifespan)


@app.get("/api/events/health")
async def health_check():
    """Health check endpoint"""
    kafka_status = "connected" if producer else "disconnected"
    return {
        "status": True,
        "service": "events",
        "kafka": kafka_status,
        "topics": ", ".join(topics.values()),
    }


@app.post("/api/events/user", status_code=201)
async def create_user_event(event: UserEvent):
    """Create a user event"""
    try:
        event_data = event.model_dump(mode="json")

        if producer:
            producer.send(topics["user"], key="user", value=event_data)
            producer.flush()

            logger.info(f"User event produced: {event_data}")

            return {
                "status": "success",
                "message": "User event created",
                "event": event_data,
            }
        else:
            raise HTTPException(status_code=500, detail="Kafka producer not available")

    except Exception as e:
        logger.error(f"Error creating user event: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create user event: {str(e)}"
        )


@app.post("/api/events/payment", status_code=201)
async def create_payment_event(event: PaymentEvent):
    """Create a payment event"""
    try:
        event_data = event.model_dump(mode="json")

        if producer:
            producer.send(topics["payment"], key="payment", value=event_data)
            producer.flush()

            logger.info(f"Payment event produced: {event_data}")

            return {
                "status": "success",
                "message": "Payment event created",
                "event": event_data,
            }
        else:
            raise HTTPException(status_code=500, detail="Kafka producer not available")

    except Exception as e:
        logger.error(f"Error creating payment event: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to payment event: {str(e)}"
        )


@app.post("/api/events/movie", status_code=201)
async def create_movie_event(event: MovieEvent):
    """Create a movie event"""
    try:
        event_data = event.model_dump(mode="json")

        if producer:
            producer.send(topics["movie"], key="movie", value=event_data)
            producer.flush()

            logger.info(f"Movie event produced: {event_data}")

            return {
                "status": "success",
                "message": "Movie event created",
                "event": event_data,
            }
        else:
            raise HTTPException(status_code=500, detail="Kafka producer not available")

    except Exception as e:
        logger.error(f"Error creating movie event: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to movie event: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
