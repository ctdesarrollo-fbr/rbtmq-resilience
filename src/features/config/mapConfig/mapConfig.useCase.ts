import {
    BindingConfigDto,
    ConsumerConfigDto,
    ExchangeConfigDto,
    ExchangesConfigDto,
    EventProcessConfigDto,
    MessageHandlerDto,
    RabbitMQMessageDto,
    RbtmqConsumerConfigDto,
    RbtmqResilienceConfigDto,
    RbtmqResilienceRuntimeConfigDto,
    TacticalBindingConfigDto,
} from "@/shared/dtos";
import { RbtmqNaming } from "@/features/config/naming";
import constant from "@/shared/constant";
import utils from "@/shared/utils";

/** True when the service passed the classic config (the one with rabbitMQConfigConnect). */
export function isServiceConfig(
    config: RbtmqResilienceConfigDto | RbtmqResilienceRuntimeConfigDto,
): config is RbtmqResilienceConfigDto {
    return "rabbitMQConfigConnect" in config;
}

/**
 * Turns the service config into the runtime config the library works with.
 * Only what the service configures is created: the integration (fanout) exchange, the topic
 * exchange, the events queue and the commands queue are all optional; the direct exchange is not.
 */
export function toRuntimeConfig(config: RbtmqResilienceConfigDto): RbtmqResilienceRuntimeConfigDto {
    const exchanges = buildExchanges(config);
    const eventsConsumer = buildEventsConsumer(config);
    const resilience = config.eventResilienceHandlerConfig;

    const consumers: RbtmqConsumerConfigDto[] = [];
    if (eventsConsumer) consumers.push(eventsConsumer);
    if (config.commands) consumers.push(config.commands);
    if (config.tacticalEvents) consumers.push(config.tacticalEvents);
    consumers.push(...(config.additionalConsumers ?? []));

    const bindings: BindingConfigDto[] = [];
    if (config.queue && exchanges.integration) {
        bindings.push({
            queue: config.queue,
            exchange: exchanges.integration.name,
            routingKey: config.bindingRoutingKey ?? config.routingKey,
        });
    }
    bindings.push(...consumerBindings(config.commands, exchanges.direct.name));
    if (exchanges.tactical) {
        bindings.push(...consumerBindings(config.tacticalEvents, exchanges.tactical.name));
        bindings.push(...tacticalBindings(config.tacticalBindings, exchanges.tactical.name));
    }
    bindings.push(...(config.bindings ?? []));

    return {
        serviceName: config.retryEndpoint,
        amqp: {
            url: utils.toAmqpUrl(config.rabbitMQConfigConnect),
        },
        exchanges,
        bindings,
        retry: {
            immediateRetries: resilience.immediateRetryAttempts,
            delayedAttempts: resilience.delayedRetryAttempts,
            delayMs: resilience.delayInMs,
            prefetch: config.prefetch,
            backoffMs: resilience.backoffInMs,
        },
        consumers: consumers.map(toConsumer),
        mysql: config.mysql,
        db: config.db,
        inboxTransaction: config.inboxTransaction,
        logs: config.logs,
        publishRetry: config.publishRetry,
    };
}

/** Declares only the exchanges the service uses. The direct exchange is always there. */
function buildExchanges(config: RbtmqResilienceConfigDto): ExchangesConfigDto {
    const direct = new ExchangeConfigDto(
        config.directExchange,
        utils.toExchangeKind(config.typeDirectExchange, "direct"),
    );

    const integration = config.exchange
        ? new ExchangeConfigDto(config.exchange, utils.toExchangeKind(config.typeExchange ?? "fanout", "fanout"))
        : undefined;

    const usesTopic = Boolean(config.tacticalExchange || config.tacticalBindings?.length || config.tacticalEvents);
    let tactical: ExchangeConfigDto | undefined;
    if (usesTopic) {
        const name = config.tacticalExchange ?? (config.exchange ? `${config.exchange}-tactical` : undefined);
        if (!name) {
            throw new Error("rbtmq-resilience: tacticalExchange is required to consume topic events (tacticalBindings or tacticalEvents).");
        }
        tactical = new ExchangeConfigDto(name, utils.toExchangeKind(config.tacticalTypeExchange ?? "topic", "topic"));
    }

    const declared = [integration, tactical, direct].filter((exchange) => exchange !== undefined);
    for (const [index, first] of declared.entries()) {
        for (const second of declared.slice(index + 1)) {
            assertCompatibleExchange(first.name, first.type!, second.name, second.type!);
        }
    }

    return new ExchangesConfigDto(direct, integration, tactical);
}

/**
 * The events queue (fanout and/or topic) exists only when `queue` is configured.
 * When it is, every name of its retry loop is required.
 */
function buildEventsConsumer(config: RbtmqResilienceConfigDto): RbtmqConsumerConfigDto | undefined {
    if (!config.queue) {
        if (config.eventsToProcess?.length) {
            throw new Error("rbtmq-resilience: eventsToProcess requires queue (the events queue).");
        }
        return undefined;
    }

    const { queue, routingKey, retryQueue, retryRoutingKey, deadLetterQueue, deadLetterRoutingKey, messageTTL } = config;
    const required = { routingKey, retryQueue, retryRoutingKey, deadLetterQueue, deadLetterRoutingKey, messageTTL };
    const missing = Object.entries(required)
        .filter(([, value]) => value === undefined || value === "")
        .map(([field]) => field);
    if (missing.length > 0) {
        throw new Error(`rbtmq-resilience: the events queue "${queue}" also needs ${missing.join(", ")}.`);
    }

    return {
        name: queue,
        kind: "events",
        queue,
        routingKey: routingKey!,
        retryQueue: retryQueue!,
        retryRoutingKey: retryRoutingKey!,
        deadLetterQueue: deadLetterQueue!,
        deadLetterRoutingKey: deadLetterRoutingKey!,
        messageTTL: messageTTL!,
        processes: config.eventsToProcess ?? [],
        prefetch: config.prefetch,
    };
}

/** RabbitMQ does not allow one exchange name with two different types. */
function assertCompatibleExchange(firstName: string, firstType: string, secondName: string, secondType: string): void {
    if (firstName !== secondName || firstType === secondType) return;

    throw new Error(
        `RabbitMQ exchange "${firstName}" is configured with incompatible types: ` +
            `"${firstType}" and "${secondType}". RabbitMQ does not allow one exchange ` +
            "name to be fanout/topic/direct at the same time. Use different exchange " +
            "names or configure both usages with the same type.",
    );
}

function toConsumer(config: RbtmqConsumerConfigDto): ConsumerConfigDto {
    return {
        name: config.name,
        kind: config.kind,
        queue: {
            primary: { name: config.queue, routingKey: config.routingKey },
            retry: { name: config.retryQueue, routingKey: config.retryRoutingKey, messageTtlMs: config.messageTTL },
            error: { name: config.deadLetterQueue, routingKey: config.deadLetterRoutingKey },
        },
        handlers: toMessageHandlers(config.processes),
        prefetch: config.prefetch,
    };
}

function consumerBindings(config: RbtmqConsumerConfigDto | undefined, defaultExchange: string): BindingConfigDto[] {
    if (!config) return [];

    return [{
        queue: config.queue,
        exchange: config.bindingExchange ?? defaultExchange,
        routingKey: config.bindingRoutingKey ?? config.routingKey,
    }];
}

function tacticalBindings(bindings: TacticalBindingConfigDto[] | undefined, tacticalExchange: string): BindingConfigDto[] {
    return (bindings ?? []).map((binding) => ({
        queue: binding.queue,
        exchange: tacticalExchange,
        routingKey: tacticalPattern(binding),
    }));
}

/** domain.# or domain.{subdomain|boundedContext}.# */
function tacticalPattern(binding: TacticalBindingConfigDto): string {
    if (binding.level === "domain") return RbtmqNaming.tacticalBindingPattern(binding.domain);
    if (binding.level === "subdomain") return RbtmqNaming.tacticalBindingPattern(binding.domain, binding.subdomain);
    return RbtmqNaming.tacticalBindingPattern(binding.domain, binding.boundedContext);
}

/** Each process of each event becomes one handler. */
function toMessageHandlers(eventsToProcess: EventProcessConfigDto[]): MessageHandlerDto[] {
    const handlers: MessageHandlerDto[] = [];

    for (const event of eventsToProcess) {
        for (const process of event.processes) {
            handlers.push(new MessageHandlerDto(
                event.eventType,
                process.processName,
                (payload) => payload,
                async (message, context) => {
                    const rabbitMessage = new RabbitMQMessageDto(
                        Buffer.from(JSON.stringify(message.payload)),
                        {
                            consumerTag: "",
                            deliveryTag: 0,
                            exchange: "",
                            routingKey: message.routingKey,
                            redelivered: message.redeliveryCount > 0,
                        },
                        {
                            contentType: constant.CONTENT_TYPE.JSON,
                            contentEncoding: "utf-8",
                            headers: message.headers,
                            deliveryMode: 2,
                            messageId: message.messageId,
                            type: message.type,
                        },
                    );
                    await process.processFunction(rabbitMessage, context);
                },
            ));
        }
    }

    return handlers;
}
