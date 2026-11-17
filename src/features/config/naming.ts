import { ConsumerKind } from "@/shared/primitives";
import utils from "@/shared/utils";
import { ConsumerQueueNamesDto, QueueNamePartsDto } from "./naming.dto";

export class RbtmqNaming {
    /**
     * Every name of a consumer (X = domain.{subdomain|boundedContext}):
     *
     * | kind                    | primary queue | primary key on direct | retry key                | error queue        |
     * |-------------------------|---------------|-----------------------|--------------------------|--------------------|
     * | events / tacticalEvents | X.events      | X.retry               | X.delayed-retry          | domain.dead-letter |
     * | commands                | X.commands    | X.commands-retry      | X.commands-delayed-retry | domain.dead-letter |
     *
     * Fanout and topic events share one primary queue and one retry loop.
     * A service that needs both binds X.events to the fanout exchange and to the topic exchange
     * (tacticalBindings with queue X.events and domain.# or domain.subdomain.#).
     * Commands are also bound to the direct exchange with X.commands (bindingRoutingKey).
     */
    public static consumerQueues(parts: QueueNamePartsDto, kind: ConsumerKind): ConsumerQueueNamesDto {
        const base = this.base(parts);
        const deadLetter = this.deadLetter(parts.domain);

        if (kind === "commands") {
            return new ConsumerQueueNamesDto(
                `${base}.commands`,
                `${base}.commands-retry`,
                `${base}.commands-delayed-retry`,
                `${base}.commands-delayed-retry`,
                deadLetter,
                deadLetter,
                `${base}.commands`,
            );
        }

        return new ConsumerQueueNamesDto(
            `${base}.events`,
            `${base}.retry`,
            `${base}.delayed-retry`,
            `${base}.delayed-retry`,
            deadLetter,
            deadLetter,
        );
    }

    /** Error queue (and its routing key) of a domain: the one Failed Messages Monitoring consumes. */
    public static deadLetter(domain: string): string {
        return `${domain.trim()}.dead-letter`;
    }

    /** Routing key of a tactical event: domain.{subdomain|boundedContext}.messageType */
    public static tacticalRoutingKey(parts: QueueNamePartsDto): string {
        const routingKey = this.join([this.base(parts), parts.messageType]);
        if (!this.isTacticalRoutingKey(routingKey)) {
            throw new Error(
                `rbtmq-resilience: a tactical routing key needs domain, subdomain or boundedContext, and messageType (got "${routingKey}").`,
            );
        }
        return routingKey;
    }

    /** True for domain.{subdomain|boundedContext}.messageType: three non-empty words, no wildcards. */
    public static isTacticalRoutingKey(routingKey: string): boolean {
        return utils.isTacticalRoutingKey(routingKey);
    }

    /**
     * Binding key of a tactical queue:
     * domain.# (whole domain) or domain.{subdomain|boundedContext}.# (one subdomain or bounded context).
     */
    public static tacticalBindingPattern(domain: string, subdomainOrBoundedContext?: string): string {
        return `${this.join([domain, subdomainOrBoundedContext])}.#`;
    }

    /** X: domain.{subdomain|boundedContext}. The subdomain wins when both are given. */
    private static base(parts: QueueNamePartsDto): string {
        const subdomain = parts.subdomain?.trim() ? parts.subdomain : undefined;
        return this.join([parts.domain, subdomain ?? parts.boundedContext]);
    }

    private static join(parts: Array<string | undefined>): string {
        return parts.filter((part) => part && part.trim().length > 0).join(".");
    }
}
