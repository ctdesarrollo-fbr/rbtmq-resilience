import type { RbtmqResilienceConfigDto, ResilienceDatabase, EventProcessConfigDto } from "@/index";
import { createFakeTransactions } from "./fakeTransactions";

/** Service-style config (the shape services use in production). */
export function buildServiceConfig(overrides: Partial<RbtmqResilienceConfigDto> = {}): RbtmqResilienceConfigDto {
    return {
        rabbitMQConfigConnect: {
            protocol: "amqp",
            hostname: "rabbit.local",
            port: 5672,
            username: "user@x",
            password: "p/ss",
            vhost: "/",
        },
        queue: "commerce.orders.checkout.events",
        routingKey: "commerce.orders.checkout.events",
        exchange: "app.integration",
        typeExchange: "fanout",
        prefetch: 10,
        directExchange: "app.direct",
        typeDirectExchange: "direct",
        retryQueue: "commerce.orders.checkout.events-retry",
        retryRoutingKey: "commerce.orders.checkout.events-retry",
        retryEndpoint: "checkout-service",
        deadLetterQueue: "commerce.orders.checkout.events-error",
        deadLetterRoutingKey: "commerce.orders.checkout.events-error",
        messageTTL: 15000,
        eventResilienceHandlerConfig: {
            immediateRetryAttempts: 2,
            delayedRetryAttempts: 3,
            delayInMs: 10000,
        },
        eventsToProcess: [],
        mysql: { host: "db", user: "u", password: "p", database: "d" },
        logs: false,
        // publication retries without waiting (the waits are tested in publishRetry.test.ts)
        publishRetry: { delayInMs: 0 },
        // required to consume: the inbox is written in the handler's transaction
        inboxTransaction: createFakeTransactions().config,
        ...overrides,
    } as RbtmqResilienceConfigDto;
}

export function withDb(config: RbtmqResilienceConfigDto, db: ResilienceDatabase): RbtmqResilienceConfigDto {
    return { ...config, db };
}

export function eventProcess(
    eventType: string,
    processName: string,
    processFunction: EventProcessConfigDto["processes"][number]["processFunction"],
): EventProcessConfigDto {
    return { eventType, processes: [{ processName, processFunction }] };
}
