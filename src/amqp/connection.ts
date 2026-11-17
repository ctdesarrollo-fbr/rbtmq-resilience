import amqp, { ChannelModel, ConfirmChannel } from "amqplib";
import constant from "@/shared/constant";
import { AmqpConfigDto } from "@/shared/dtos";
import utils from "@/shared/utils";

/**
 * Single AMQP connection and confirm channel shared by every feature.
 * - Confirm channel: every publication waits for the broker's ack (publisher confirms).
 * - Reconnection: when the channel or the connection closes without stop(), it connects again
 *   and runs the reconnect handlers (declare the topology, register the consumers).
 */
export class AmqpConnection {
    private connection?: ChannelModel;
    private channel?: ConfirmChannel;
    private connecting?: Promise<ConfirmChannel>;
    private reconnecting = false;
    private closing = false;
    private readonly reconnectHandlers: Array<() => Promise<void>> = [];
    /** Message ids the broker returned as unroutable (publications with `mandatory`). */
    private readonly returnedMessageIds = new Set<string>();

    constructor(
        private readonly config: AmqpConfigDto,
    ) {}

    public async ensureChannel(): Promise<ConfirmChannel> {
        if (this.channel) return this.channel;
        this.connecting ??= this.connect().finally(() => {
            this.connecting = undefined;
        });
        return this.connecting;
    }

    /** Runs after every reconnection, in order. */
    public onReconnect(handler: () => Promise<void>): void {
        this.reconnectHandlers.push(handler);
    }

    /** True (once) when the broker returned this message because no queue matched its routing key. */
    public wasReturned(messageId: string): boolean {
        return this.returnedMessageIds.delete(messageId);
    }

    public async close(): Promise<void> {
        this.closing = true;
        await this.channel?.close();
        await this.connection?.close();
    }

    public reset(): void {
        this.channel = undefined;
        this.connection = undefined;
        this.closing = false;
    }

    private async connect(): Promise<ConfirmChannel> {
        const reconnectDelayMs = this.config.reconnectDelayMs ?? constant.DEFAULTS.RECONNECT_DELAY_MS;

        for (;;) {
            try {
                const connection = await amqp.connect(this.config.url);
                // without an "error" listener Node would crash the process on a connection error
                connection.on("error", () => {});
                connection.on("close", () => this.handleClose(connection));

                const channel = await connection.createConfirmChannel();
                channel.on("error", () => {});
                channel.on("close", () => this.handleClose(connection));
                channel.on("return", (message: { properties: { messageId?: string } }) => {
                    if (message.properties.messageId) this.returnedMessageIds.add(message.properties.messageId);
                });

                this.connection = connection;
                this.channel = channel;
                return channel;
            } catch {
                await utils.sleep(reconnectDelayMs);
            }
        }
    }

    /** Channel and connection both emit "close": only the first one for the current connection counts. */
    private handleClose(closed: ChannelModel): void {
        if (closed !== this.connection) return;
        this.channel = undefined;
        this.connection = undefined;
        if (this.closing) return;
        void this.reconnect();
    }

    private async reconnect(): Promise<void> {
        if (this.reconnecting) return;
        this.reconnecting = true;
        const reconnectDelayMs = this.config.reconnectDelayMs ?? constant.DEFAULTS.RECONNECT_DELAY_MS;
        console.error("[rbtmq-resilience] RabbitMQ connection lost; reconnecting.");

        try {
            while (!this.closing) {
                try {
                    await this.ensureChannel();
                    for (const handler of this.reconnectHandlers) await handler();
                    console.log("[rbtmq-resilience] reconnected to RabbitMQ: topology declared and consumers registered again.");
                    return;
                } catch (error) {
                    const reason = error instanceof Error ? error.message : String(error);
                    console.error(`[rbtmq-resilience] reconnection failed, retrying: ${reason}`);
                    await utils.sleep(reconnectDelayMs);
                }
            }
        } finally {
            this.reconnecting = false;
        }
    }
}
