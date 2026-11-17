import { MySql2Database } from "drizzle-orm/mysql2";
import { Options } from "amqplib";
import {
    ConsumerKind,
    ExchangeKind,
    TacticalBindingLevel,
} from "@/shared/primitives";
import { RabbitMQMessageDto } from "./eventManager.dto";
import { MessageHandlerDto, HandlerContextDto } from "./handler.dto";

export class ExchangeConfigDto {
    constructor(
        public name: string,
        public type?: ExchangeKind,
        public durable?: boolean,
    ) {}
}

export class QueueConfigDto {
    constructor(
        public name: string,
        public routingKey: string,
        public durable?: boolean,
    ) {}
}

export class RetryQueueConfigDto extends QueueConfigDto {
    constructor(
        public override name: string,
        public override routingKey: string,
        public messageTtlMs?: number,
        public override durable?: boolean,
    ) {
        super(name, routingKey, durable);
    }
}

export class ResilientQueueConfigDto {
    constructor(
        public primary: QueueConfigDto,
        public retry: RetryQueueConfigDto,
        public error: QueueConfigDto,
    ) {}
}

export class BindingConfigDto {
    constructor(
        public queue: string,
        public exchange: string,
        public routingKey?: string,
    ) {}
}

export class TacticalBindingConfigDto {
    constructor(
        public queue: string,
        public domain: string,
        public level: TacticalBindingLevel,
        public subdomain?: string,
        public boundedContext?: string,
    ) {}
}

/**
 * Recoverability (same model as NServiceBus):
 * total attempts = (immediateRetries + 1) * (delayedAttempts + 1).
 */
export class RetryConfigDto {
    constructor(
        /** Immediate retries after the first attempt (IR). 5 = the handler runs up to 6 times per round. */
        public immediateRetries: number,
        /** Delayed retries through the retry queue (N). When redelivery_count == N the message goes to the error queue. */
        public delayedAttempts: number,
        /** x-message-ttl of the retry queue. */
        public delayMs: number,
        public prefetch: number,
        /**
         * Exponential backoff: expiration = redelivery_count * backoffMs (default 2000).
         * RabbitMQ uses the lower of this and the queue TTL. 0 disables it (queue TTL only).
         */
        public backoffMs?: number,
    ) {}
}

export class ConsumerConfigDto {
    constructor(
        public name: string,
        public kind: ConsumerKind,
        public queue: ResilientQueueConfigDto,
        public handlers: MessageHandlerDto[],
        public prefetch?: number,
    ) {}
}

export class MySqlConnectionConfigDto {
    constructor(
        public host: string,
        public user: string,
        public password: string,
        public database: string,
        public port?: number,
    ) {}
}

export class ExchangesConfigDto {
    constructor(
        /** Always required: retry and error queues of every consumer are bound to it. */
        public direct: ExchangeConfigDto,
        /** Only when the service consumes or publishes integration (fanout) events. */
        public integration?: ExchangeConfigDto,
        /** Only when the service consumes or publishes tactical (topic) events. */
        public tactical?: ExchangeConfigDto,
    ) {}
}

export class AmqpConfigDto {
    constructor(
        public url: string,
        public reconnectDelayMs?: number,
    ) {}
}

/**
 * Required when the service consumes messages: the library writes the inbox inside the
 * service's own transaction (any ORM), so the handler's writes and the inbox record commit or roll back together.
 * Example with Sequelize:
 *   run: (work) => sequelize.transaction((transaction) => work(transaction))
 *   execute: (transaction, sql, params) =>
 *       sequelize.query(sql, { replacements: params, transaction: transaction as Transaction })
 * The inbox table must live in the same database the service's transaction writes to.
 */
export class InboxTransactionConfigDto {
    constructor(
        public run: <T>(work: (transaction: unknown) => Promise<T>) => Promise<T>,
        public execute: (transaction: unknown, sql: string, params: unknown[]) => Promise<unknown>,
    ) {}
}

export class RbtmqResilienceRuntimeConfigDto {
    constructor(
        public serviceName: string,
        public amqp: AmqpConfigDto,
        public exchanges: ExchangesConfigDto,
        public retry: RetryConfigDto,
        public mysql?: MySqlConnectionConfigDto,
        public db?: MySql2Database<Record<string, never>>,
        public bindings?: BindingConfigDto[],
        public consumers?: ConsumerConfigDto[],
        public inboxTransaction?: InboxTransactionConfigDto,
        /** One line per consumed/published message with its id and duration. Default true; false turns them off. */
        public logs?: boolean,
        /** Publication retries (default 6 attempts, 1 s → 16 s). */
        public publishRetry?: PublishRetryConfigDto,
    ) {}
}

export class EventProcessHandlerConfigDto {
    constructor(
        public processName: string,
        public processFunction: (event: RabbitMQMessageDto, context?: HandlerContextDto) => Promise<void>,
    ) {}
}

export class EventProcessConfigDto {
    constructor(
        public eventType: string,
        public processes: EventProcessHandlerConfigDto[],
    ) {}
}

/** Retries of publishEvent / publishTacticalEvent / sendCommand when the broker does not confirm them. */
export class PublishRetryConfigDto {
    constructor(
        /** Total attempts, the first one included. Default 6. 1 turns the retries off. */
        public attempts?: number,
        /** Wait before the first retry; it doubles on every retry. Default 1000. */
        public delayInMs?: number,
        /** Upper limit for the wait between attempts. Default 16000. */
        public maxDelayInMs?: number,
    ) {}
}

export class EventResilienceHandlerConfigDto {
    constructor(
        /** Immediate retries after the first attempt. 5 = the handler runs up to 6 times per round. */
        public immediateRetryAttempts: number,
        /** Delayed retries through the retry queue before the message goes to the error queue. */
        public delayedRetryAttempts: number,
        /** x-message-ttl of the retry queue. */
        public delayInMs: number,
        /** expiration = redelivery_count * backoffInMs (default 2000). 0 disables it (queue TTL only). */
        public backoffInMs?: number,
    ) {}
}

export class RbtmqConsumerConfigDto {
    constructor(
        public name: string,
        public kind: ConsumerKind,
        public queue: string,
        public routingKey: string,
        public retryQueue: string,
        public retryRoutingKey: string,
        public deadLetterQueue: string,
        public deadLetterRoutingKey: string,
        public messageTTL: number,
        public processes: EventProcessConfigDto[],
        public prefetch?: number,
        public bindingExchange?: string,
        public bindingRoutingKey?: string,
    ) {}
}

/**
 * Service config. A service consumes any combination of:
 * - fanout events: `queue` + `exchange` (fanout)
 * - topic events:  `queue` + `tacticalExchange` + `tacticalBindings` on that queue, or a `tacticalEvents` consumer
 * - commands:      `commands` (direct exchange)
 * Fanout and topic events can share the same `queue` (one retry loop).
 * The direct exchange is always required: every consumer's retry and error queues live on it.
 */
export class RbtmqResilienceConfigDto {
    constructor(
        public rabbitMQConfigConnect: Options.Connect | string,
        /** Service name: appId of published messages and retry_endpoint of retries. */
        public retryEndpoint: string,
        public prefetch: number,
        public directExchange: string,
        public typeDirectExchange: string,
        public eventResilienceHandlerConfig: EventResilienceHandlerConfigDto,
        public mysql: MySqlConnectionConfigDto,

        /** Integration exchange (usually fanout). Leave it out if the service neither consumes nor publishes integration events. */
        public exchange?: string,
        public typeExchange?: string,

        /** Events queue (fanout and/or topic). Leave it out if the service does not consume events. */
        public queue?: string,
        /** Key the events queue is bound with on the direct exchange (retries come back with it). */
        public routingKey?: string,
        /** Key the events queue is bound with on `exchange`. Defaults to routingKey (ignored by fanout). */
        public bindingRoutingKey?: string,
        public retryQueue?: string,
        public retryRoutingKey?: string,
        public deadLetterQueue?: string,
        public deadLetterRoutingKey?: string,
        public messageTTL?: number,
        public eventsToProcess?: EventProcessConfigDto[],

        public tacticalExchange?: string,
        public tacticalTypeExchange?: string,
        public tacticalBindings?: TacticalBindingConfigDto[],
        public tacticalEvents?: RbtmqConsumerConfigDto,
        public commands?: RbtmqConsumerConfigDto,
        public additionalConsumers?: RbtmqConsumerConfigDto[],
        public bindings?: BindingConfigDto[],

        public db?: MySql2Database<Record<string, never>>,
        public inboxTransaction?: InboxTransactionConfigDto,
        /** One line per consumed/published message with its id and duration. Default true; false turns them off. */
        public logs?: boolean,
        /** Publication retries (default 6 attempts, 1 s → 16 s). */
        public publishRetry?: PublishRetryConfigDto,
    ) {}
}
