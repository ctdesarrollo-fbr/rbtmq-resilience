import { RbtmqResilienceRuntimeConfigDto } from "@/shared/dtos";

const DEAD_LETTER_SUFFIX = ".dead-letter";

/**
 * Throws when two consumers share a primary queue or a retry key: a retry for one of them
 * would be delivered to the other too.
 */
export function assertUniqueConsumerKeys(config: RbtmqResilienceRuntimeConfigDto): void {
    const errors: string[] = [];
    const seen = new Map<string, string>();

    for (const consumer of config.consumers ?? []) {
        const keys = [
            `primary queue "${consumer.queue.primary.name}"`,
            `primary routing key "${consumer.queue.primary.routingKey}"`,
            `retry routing key "${consumer.queue.retry.routingKey}"`,
        ];
        for (const key of keys) {
            const owner = seen.get(key);
            if (owner) errors.push(`${key} is used by consumers "${owner}" and "${consumer.name}"`);
            else seen.set(key, consumer.name);
        }
    }

    if (errors.length > 0) {
        throw new Error(
            `rbtmq-resilience: invalid consumers. ${errors.join("; ")}. ` +
                "To consume fanout and topic events at the same time, use one events queue bound to both " +
                "exchanges (tacticalBindings with that queue) instead of a second consumer.",
        );
    }
}

/**
 * The inbox record must be written in the same transaction as the handler, so a service that
 * consumes messages must hand the library its own transaction.
 */
export function assertInboxTransaction(config: RbtmqResilienceRuntimeConfigDto): void {
    if ((config.consumers ?? []).length === 0 || config.inboxTransaction) return;

    throw new Error(
        "rbtmq-resilience: inboxTransaction is required to consume messages. The handler's writes and the " +
            "inbox record (message id + handler name) are stored in the same transaction, e.g. with Sequelize: " +
            "{ run: (work) => sequelize.transaction(work), execute: (transaction, sql, params) => " +
            "sequelize.query(sql, { replacements: params, transaction }) }.",
    );
}

/** Settings that work but send poison messages where Failed Messages Monitoring does not read them. */
export function configWarnings(config: RbtmqResilienceRuntimeConfigDto): string[] {
    const warnings: string[] = [];
    const consumers = config.consumers ?? [];

    for (const consumer of consumers) {
        const errorKey = consumer.queue.error.routingKey;
        if (!errorKey.endsWith(DEAD_LETTER_SUFFIX)) {
            warnings.push(
                `consumer "${consumer.name}" sends poison messages to "${errorKey}". Failed Messages Monitoring ` +
                    `consumes "<domain>${DEAD_LETTER_SUFFIX}" (see RbtmqNaming.deadLetter()).`,
            );
        }
    }

    return warnings;
}
