export default {
    EXCHANGE: {
        FANOUT: "fanout",
        TOPIC: "topic",
        DIRECT: "direct",
    },
    CONSUMER: {
        EVENTS: "events",
        TACTICAL_EVENTS: "tacticalEvents",
        COMMANDS: "commands",
    },
    RABBITMQ_HEADERS: {
        MESSAGE_ID: "message_id",
        TYPE: "type",
        REDELIVERY_COUNT: "redelivery_count",
        RETRY_ENDPOINT: "retry_endpoint",
        EXCEPTION_DETAILS: "exception_details",
        ENDPOINT: "endpoint",
        SENDER: "sender",
    },
    CONTENT_TYPE: {
        JSON: "application/json",
    },
    DEFAULTS: {
        RECONNECT_DELAY_MS: 5000,
        RETRY_BACKOFF_MS: 2000,
        PUBLISH_ATTEMPTS: 6,
        PUBLISH_RETRY_DELAY_MS: 1000,
        PUBLISH_RETRY_MAX_DELAY_MS: 16000,
    },
};
