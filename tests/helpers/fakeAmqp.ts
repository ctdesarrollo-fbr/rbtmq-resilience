import type { ConsumeMessage } from "amqplib";

export type BrokerCall = { method: string; args: unknown[] };
type ConsumeCallback = (message: ConsumeMessage | null) => Promise<void> | void;
type Listener = (...args: unknown[]) => void;
type ConfirmCallback = (error: Error | null) => void;

/**
 * Stand-in for an amqplib ConfirmChannel: every publish is confirmed asynchronously, like the broker
 * does. `unroutableKeys` makes mandatory publications come back as basic.return; `close()` emits
 * "close" like a lost connection, after which ack/reject/nack throw.
 */
export class FakeChannel {
    public readonly calls: BrokerCall[] = [];
    public readonly consumers = new Map<string, ConsumeCallback>();
    public failAssertExchange = false;
    /** publish throws synchronously (e.g. the channel buffer is gone). */
    public failPublish = false;
    /** the broker nacks every publication. */
    public nackPublish = false;
    /** the broker nacks only the next N publications (they are still recorded). */
    public nackNextPublishes = 0;
    /** routing keys no queue is bound to: mandatory publications to them are returned. */
    public readonly unroutableKeys = new Set<string>();
    public closed = false;
    private readonly listeners = new Map<string, Listener[]>();

    private record(method: string, args: unknown[]): void {
        this.calls.push({ method, args });
    }

    public callsOf(method: string): unknown[][] {
        return this.calls.filter((call) => call.method === method).map((call) => call.args);
    }

    on(event: string, listener: Listener) {
        this.listeners.set(event, [...(this.listeners.get(event) ?? []), listener]);
        return this;
    }
    once(event: string, listener: Listener) {
        return this.on(event, listener);
    }
    emit(event: string, ...args: unknown[]) {
        for (const listener of this.listeners.get(event) ?? []) listener(...args);
    }

    async assertExchange(...args: unknown[]) {
        this.record("assertExchange", args);
        if (this.failAssertExchange) throw new Error("PRECONDITION_FAILED");
    }
    async assertQueue(...args: unknown[]) {
        this.record("assertQueue", args);
    }
    async bindQueue(...args: unknown[]) {
        this.record("bindQueue", args);
    }
    async prefetch(...args: unknown[]) {
        this.record("prefetch", args);
    }
    async consume(queue: string, callback: ConsumeCallback) {
        this.record("consume", [queue]);
        this.consumers.set(queue, callback);
    }
    publish(exchange: string, routingKey: string, content: Buffer, options: Record<string, unknown>, confirm?: ConfirmCallback): boolean {
        if (this.failPublish) throw new Error("publish failed");
        if (this.closed) throw new Error("Channel closed");
        this.record("publish", [exchange, routingKey, content, options]);

        const nack = this.nackPublish || this.nackNextPublishes > 0;
        if (this.nackNextPublishes > 0) this.nackNextPublishes -= 1;
        queueMicrotask(() => {
            if (nack) return confirm?.(new Error("message nacked"));
            if (options.mandatory && this.unroutableKeys.has(routingKey)) {
                this.emit("return", { properties: { messageId: options.messageId } });
            }
            confirm?.(null);
        });
        return true;
    }
    ack(message: ConsumeMessage) {
        this.assertOpen();
        this.record("ack", [message.properties.messageId]);
    }
    nack(message: ConsumeMessage, allUpTo?: boolean, requeue?: boolean) {
        this.assertOpen();
        this.record("nack", [message.properties.messageId, allUpTo, requeue]);
    }
    reject(message: ConsumeMessage, requeue?: boolean) {
        this.assertOpen();
        this.record("reject", [message.properties.messageId, requeue]);
    }
    async close() {
        this.record("close", []);
        this.simulateClose();
    }

    /** The channel closes as when RabbitMQ goes down: "close" is emitted and settling throws. */
    simulateClose(): void {
        if (this.closed) return;
        this.closed = true;
        this.emit("close");
    }

    /** Delivers a message to a queue's consumer as RabbitMQ would. */
    async deliver(queue: string, message: ConsumeMessage): Promise<void> {
        const callback = this.consumers.get(queue);
        if (!callback) throw new Error(`No consumer registered for ${queue}`);
        await callback(message);
    }

    private assertOpen(): void {
        if (this.closed) throw new Error("Channel closed");
    }
}

export const fakeAmqp = {
    channel: new FakeChannel(),
    connectedUrls: [] as string[],
    reset(): FakeChannel {
        this.channel = new FakeChannel();
        this.connectedUrls = [];
        return this.channel;
    },
    /** The next connection gets a new channel (as after a reconnection). */
    nextChannel(): FakeChannel {
        this.channel = new FakeChannel();
        return this.channel;
    },
    async connect(url: string) {
        fakeAmqp.connectedUrls.push(url);
        const channel = fakeAmqp.channel;
        return {
            on() {},
            createChannel: async () => channel,
            createConfirmChannel: async () => channel,
            close: async () => {},
        };
    },
};

export function buildConsumeMessage(input: {
    messageId?: string;
    type?: string;
    payload?: unknown;
    rawContent?: string;
    headers?: Record<string, unknown>;
    routingKey?: string;
    exchange?: string;
    appId?: string;
}): ConsumeMessage {
    const content =
        input.rawContent !== undefined
            ? Buffer.from(input.rawContent)
            : Buffer.from(JSON.stringify(input.payload ?? {}));
    return {
        content,
        fields: {
            consumerTag: "ctag",
            deliveryTag: 1,
            redelivered: false,
            exchange: input.exchange ?? "app.integration",
            routingKey: input.routingKey ?? "",
        },
        properties: {
            contentType: "application/json",
            contentEncoding: undefined,
            headers: input.headers,
            deliveryMode: undefined,
            priority: undefined,
            correlationId: undefined,
            replyTo: undefined,
            expiration: undefined,
            messageId: input.messageId,
            timestamp: undefined,
            type: input.type,
            userId: undefined,
            appId: input.appId,
            clusterId: undefined,
        },
    } as ConsumeMessage;
}
