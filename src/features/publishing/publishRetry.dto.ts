import constant from "@/shared/constant";
import { PublishRetryConfigDto } from "@/shared/dtos";

/** How a publication the broker did not confirm is retried: the service's publishRetry plus the defaults. */
export class PublishRetryPolicyDto {
    constructor(
        public attempts: number,
        public delayInMs: number,
        public maxDelayInMs: number,
    ) {}

    /** Default: 6 attempts waiting 1 s, 2 s, 4 s, 8 s and 16 s. */
    static create(config?: PublishRetryConfigDto): PublishRetryPolicyDto {
        const { attempts, delayInMs, maxDelayInMs } = config ?? {};

        return new PublishRetryPolicyDto(
            Math.max(1, attempts ?? constant.DEFAULTS.PUBLISH_ATTEMPTS),
            delayInMs ?? constant.DEFAULTS.PUBLISH_RETRY_DELAY_MS,
            maxDelayInMs ?? constant.DEFAULTS.PUBLISH_RETRY_MAX_DELAY_MS,
        );
    }

    /** Wait before retry n (1, 2, 3…): it doubles every time, never more than maxDelayInMs. */
    public delayBeforeRetry(retry: number): number {
        return Math.min(this.delayInMs * 2 ** (retry - 1), this.maxDelayInMs);
    }
}
