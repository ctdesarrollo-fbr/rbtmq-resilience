# rbtmq-resilience

RabbitMQ resilience library for TypeScript microservices with resilient message handling:

- Publishes integration events (fanout), tactical events (topic) and commands (direct), with publisher confirms and persistent messages.
- Consumes with a transactional inbox: each handler runs at most once per message.
- Immediate retries, delayed retries (retry queue + backoff), and an error queue `domain.dead-letter` read by Failed Messages Monitoring.
- Reconnects on its own when RabbitMQ goes down.

## Installation

```bash
pnpm add @ctdesarrollo-pkgs/rbtmq-resilience
```

## Usage

### 1. Handlers

```ts
import { Transaction } from "sequelize";
import { EventProcessConfigDto } from "@ctdesarrollo-pkgs/rbtmq-resilience";

export const eventsToProcess: EventProcessConfigDto[] = [
    {
        eventType: "commerce.orders.order_created",
        processes: [
            {
                processName: "create-local-order", // stored in the inbox: keep it stable
                processFunction: async (event, context) => {
                    const payload = JSON.parse(event.content.toString("utf8"));
                    const transaction = context?.transaction as Transaction; // use it for your writes
                    await Order.create({ uuid: payload.orderId }, { transaction });
                },
            },
        ],
    },
];
```

Throw to send the message through the retry flow.

### 2. Config

```ts
import { QueueNamePartsDto, RabbitMQResilience, RbtmqNaming, RbtmqResilienceConfigDto } from "@ctdesarrollo-pkgs/rbtmq-resilience";

const parts = new QueueNamePartsDto(env.RABBIT_DOMAIN, env.RABBIT_SUBDOMAIN);
const events = RbtmqNaming.consumerQueues(parts, "events");
const commands = RbtmqNaming.consumerQueues(parts, "commands");

const config: RbtmqResilienceConfigDto = {
    rabbitMQConfigConnect: { protocol: "amqp", hostname: "localhost", port: 5672, username: "guest", password: "guest", vhost: "/" },
    retryEndpoint: "checkout-service", // this service's name
    prefetch: 1,
    directExchange: "app.direct",      // always required (retries and errors)
    typeDirectExchange: "direct",

    // events (optional): fanout and/or topic, same queue
    exchange: "app.integration",
    typeExchange: "fanout",
    tacticalExchange: "app.tactical",
    tacticalTypeExchange: "topic",
    queue: events.queue,                               // commerce.orders.events
    routingKey: events.routingKey,                     // commerce.orders.retry
    retryQueue: events.retryQueue,                     // commerce.orders.delayed-retry
    retryRoutingKey: events.retryRoutingKey,           // commerce.orders.delayed-retry
    deadLetterQueue: events.deadLetterQueue,           // commerce.dead-letter
    deadLetterRoutingKey: events.deadLetterRoutingKey, // commerce.dead-letter
    messageTTL: 10000,
    tacticalBindings: [{ queue: events.queue, domain: env.RABBIT_DOMAIN, level: "domain" }],
    eventsToProcess,

    // commands (optional)
    commands: {
        name: commands.queue,
        kind: "commands",
        queue: commands.queue,                               // commerce.orders.commands
        routingKey: commands.routingKey,                     // commerce.orders.commands-retry
        bindingRoutingKey: commands.bindingRoutingKey,       // commerce.orders.commands (used by sendCommand)
        retryQueue: commands.retryQueue,                     // commerce.orders.commands-delayed-retry
        retryRoutingKey: commands.retryRoutingKey,           // commerce.orders.commands-delayed-retry
        deadLetterQueue: commands.deadLetterQueue,           // commerce.dead-letter
        deadLetterRoutingKey: commands.deadLetterRoutingKey, // commerce.dead-letter
        messageTTL: 10000,
        processes: commandsToProcess,
    },

    eventResilienceHandlerConfig: {
        immediateRetryAttempts: 5, // retries after the first attempt
        delayedRetryAttempts: 3,   // then the error queue
        delayInMs: 10000,
        // backoffInMs: 2000 -> expiration = redelivery_count * 2000
    },

    // required when the service consumes
    inboxTransaction: {
        run: (work) => sequelize.transaction((transaction) => work(transaction)),
        execute: (transaction, sql, params) => sequelize.query(sql, { replacements: params, transaction: transaction as Transaction }),
    },

    mysql: { host: "localhost", port: 3306, user: "root", password: "secret", database: "service_db" },
};

export const RabbitMQR = RabbitMQResilience.initialize(config);
```

Configure only what the service consumes: fanout (`exchange`), topic (`tacticalExchange` + `tacticalBindings`), commands (`commands`), in any combination. A service that only publishes needs no consumers and no `inboxTransaction`.

### 3. Start and stop

```ts
await RabbitMQR.start(); // MySQL tables, connection, topology, consumers
await RabbitMQR.stop();  // on shutdown
```

### 4. Publish

```ts
await RabbitMQR.publishEvent({ type: "commerce.orders.order_created", payload });

await RabbitMQR.publishTacticalEvent({
    type: "commerce.orders.payment_authorized",
    routingKey: "commerce.orders.payment-authorized", // domain.{subdomain|boundedContext}.messageType
    payload,
});

await RabbitMQR.sendCommand({ type: "commerce.orders.reserve_stock", routingKey: "commerce.orders.commands", payload });
```

Each one returns the message id once the broker confirms it. If it does not (nack, closed channel, RabbitMQ restarting), the library retries it with backoff and the same message id, so the service does nothing. It throws only when the attempts run out. `sendCommand` also fails, without retrying, if no queue is bound to the routing key.

```ts
// optional, these are the defaults: 6 attempts, waiting 1 s, 2 s, 4 s, 8 s and 16 s
publishRetry: { attempts: 6, delayInMs: 1000, maxDelayInMs: 16000 },
```

### Health check

```ts
const ready = await RabbitMQR.isReady(); // RabbitMQ and MySQL reachable
```

## Naming

`X` = `domain.{subdomain|boundedContext}`, generated with `RbtmqNaming.consumerQueues(parts, kind)`:

| kind | queue | retry queue | error queue |
|---|---|---|---|
| events | `X.events` | `X.delayed-retry` | `domain.dead-letter` |
| commands | `X.commands` | `X.commands-delayed-retry` | `domain.dead-letter` |

- Commands are sent with routing key `X.commands`.
- Tactical keys: `RbtmqNaming.tacticalRoutingKey(parts)` → `commerce.orders.payment-authorized`.
- Topic bindings: `level: "domain"` → `commerce.#`; `"subdomain"` / `"boundedContext"` → `commerce.orders.#`.

## How a message is processed

1. **Already handled** (inbox has every handler) → rejected.
2. Each pending handler runs in **one transaction together with its inbox row**, with immediate retries.
3. All succeeded → **ack**.
4. Failed, retries left → copy to the retry queue (`redelivery_count + 1`, `expiration = redelivery_count * backoffInMs`, same body byte for byte); it comes back after the TTL, only to this queue.
5. Redelivered N times → copy to `domain.dead-letter`, then ack.
6. Could not publish the copy → the original is requeued.

Every failure takes this path, whatever its cause: a handler error, a validation error or a body that is not valid JSON.

The error queue message (what Failed Messages Monitoring stores):

```jsonc
// headers
"exception_details": { "exception_type": "MailServerUnavailableError", "stack_trace": "MailServerUnavailableError: Mail server is not available.\n    at ...", "failed_at": "2026-11-06T16:24:39.888Z" },
"endpoint": {
  "name": "financial.orders-billing",                          // retryEndpoint
  "delivery_metadata": {
    "message_type": "financial.orders-billing.order_billed",
    "exchange": "sagittarius-a-direct",                        // replay goes back only to this queue
    "routing_key": "financial.orders-billing.retry",
    "sender": "sales.orders-booking"                           // when known
  }
}
// body
{ "payload": { ... } }
```

Total runs before the error queue: `(immediateRetryAttempts + 1) * (delayedRetryAttempts + 1)`.

If two copies of a message are processed at the same time, the one that loses the inbox insert rolls back and is rejected as already handled.

## Broker failures

- **Publisher confirms:** a publication (and an ack after a retry copy) only counts once the broker confirmed it.
- **Publication retries:** an unconfirmed publication is retried with backoff (`publishRetry`), keeping its message id; invalid messages and unroutable commands are not retried.
- **Persistent messages:** every message (events, commands, retry copies, poison messages) is published as persistent (delivery mode 2), so the durable queues keep it across a RabbitMQ restart.
- **Mandatory:** retry, error and command messages fail if no queue receives them; events do not.
- **Reconnection:** on a lost connection it reconnects, declares the topology and registers the consumers again.

## Outbox

`outbox` is a history of published messages (`status` `sent` / `failed`, `attempts`, `error`). It does not publish anything; messages go straight to the exchange.

## Logs

```txt
[rbtmq-resilience] handled commerce.orders.order_created 3fa85f64-… (12 ms)
[rbtmq-resilience] retry 1/3 commerce.orders.order_created 3fa85f64-… (35 ms): Error: db down
[rbtmq-resilience] rejected commerce.orders.order_created 3fa85f64-… (40 ms) -> commerce.dead-letter: Error: db down
[rbtmq-resilience] published commerce.orders.order_created 3fa85f64-… (3 ms) -> app.integration
```

`logs: false` turns them off (`requeued` and `publish failed` are always logged).

## Development

```bash
bun install
bun run typecheck
bun test
bun run build
```
## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
