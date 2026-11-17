import { Options } from "amqplib";
import { ExchangeKind, JsonObject } from "./primitives";
import { HandlerFailureDto } from "@/shared/dtos";

const utils = {
    normalizeHeaders(headers: unknown): JsonObject {
        return headers && typeof headers === "object" ? { ...(headers as JsonObject) } : {};
    },

    stringHeader(value: unknown): string | undefined {
        if (typeof value === "string") return value;
        if (typeof value === "number") return String(value);
        if (Buffer.isBuffer(value)) return value.toString("utf8");
        return undefined;
    },

    numberHeader(value: unknown): number {
        const parsed = Number(utils.stringHeader(value) ?? value ?? 0);
        return Number.isFinite(parsed) ? parsed : 0;
    },

    /**
     * RabbitMQ adds `x-death` when a retry-queue message expires and is dead-lettered
     * back to its primary queue. It provides a fallback counter for messages that do
     * not yet contain the application-managed `redelivery_count` header.
     */
    xDeathCount(headers: JsonObject): number {
        const xDeath = headers["x-death"];
        if (!Array.isArray(xDeath)) return 0;

        return xDeath.reduce<number>((total, entry) => {
            if (!entry || typeof entry !== "object") return total;
            return total + utils.numberHeader((entry as JsonObject).count);
        }, 0);
    },

    toHandlerFailure(handlerName: string, error: unknown): HandlerFailureDto {
        const exception = error instanceof Error ? error : new Error(String(error));
        return new HandlerFailureDto(
            handlerName,
            exception.name,
            exception.message,
            new Date().toISOString(),
            exception.stack,
        );
    },

    toAmqpUrl(connect: Options.Connect | string): string {
        if (typeof connect === "string") return connect;

        const protocol = String(connect.protocol ?? "amqp").replace(/:$/, "");
        const username = encodeURIComponent(connect.username ?? "guest");
        const password = encodeURIComponent(connect.password ?? "guest");
        const hostname = connect.hostname ?? "localhost";
        const port = connect.port ?? 5672;
        const vhost = encodeURIComponent(connect.vhost ?? "/");

        return `${protocol}://${username}:${password}@${hostname}:${port}/${vhost}`;
    },

    toExchangeKind(value: string, fallback: ExchangeKind): ExchangeKind {
        return value === "fanout" || value === "topic" || value === "direct"
            ? value
            : fallback;
    },

    /** domain.{subdomain|boundedContext}.messageType — three non-empty words, no wildcards. */
    isTacticalRoutingKey(routingKey: string): boolean {
        return /^[^.#*\s]+\.[^.#*\s]+\.[^.#*\s]+$/.test(routingKey);
    },

    /** 850 → "850 ms", 1234 → "1.2 s". */
    formatDuration(ms: number): string {
        if (ms < 1000) return `${Math.round(ms)} ms`;
        return `${(ms / 1000).toFixed(1)} s`;
    },

    /** Short reason for a log line, e.g. "TypeError: volume not found" (at most 120 characters). */
    describeFailure(failure: HandlerFailureDto): string {
        const reason = `${failure.exception_type}: ${failure.message}`.replace(/\s+/g, " ");
        return reason.length > 120 ? `${reason.slice(0, 117)}...` : reason;
    },

    async sleep(ms: number): Promise<void> {
        await new Promise((resolve) => setTimeout(resolve, ms));
    },
};

export default utils;
