import { AmqpPublisher, PublishMessageDto, UnroutableMessageError } from "@/amqp";
import { PublishRetryPolicyDto } from "./publishRetry.dto";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Publishes a message and, if the broker does not confirm it (nack, closed channel, lost
 * connection), publishes it again with the same message id until it does or the attempts run out.
 * An unroutable command is not retried: retrying cannot fix it.
 */
export class PublishRetry {
    constructor(
        private readonly publisher: AmqpPublisher,
        private readonly policy: PublishRetryPolicyDto,
        private readonly wait: (ms: number) => Promise<void> = sleep,
    ) {}

    public async publish(message: PublishMessageDto, mandatory: boolean): Promise<string> {
        for (let attempt = 1; ; attempt += 1) {
            try {
                return await this.publisher.publishRaw(message, mandatory);
            } catch (error) {
                if (!this.canRetry(error, attempt)) throw error;

                const delayInMs = this.policy.delayBeforeRetry(attempt);
                console.warn(
                    `[rbtmq-resilience] publish retry ${attempt}/${this.policy.attempts - 1} ${message.type} ${message.messageId} in ${delayInMs} ms`,
                );
                await this.wait(delayInMs);
            }
        }
    }

    private canRetry(error: unknown, attempt: number): boolean {
        if (error instanceof UnroutableMessageError) return false;
        return attempt < this.policy.attempts;
    }
}
