import { mock } from "bun:test";
import { fakeAmqp } from "@tests/helpers/fakeAmqp";

// Every test talks to an in-memory broker instead of a real RabbitMQ.
mock.module("amqplib", () => ({
    default: fakeAmqp,
    connect: fakeAmqp.connect,
}));
