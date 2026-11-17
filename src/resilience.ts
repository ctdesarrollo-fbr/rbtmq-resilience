import { AmqpConnection, AmqpPublisher } from "@/amqp";
import { DatabaseService } from "@/database";
import {
    RabbitMQMessageDto,
    RbtmqResilienceConfigDto,
    RbtmqResilienceRuntimeConfigDto,
} from "@/shared/dtos";
import { assertInboxTransaction, assertUniqueConsumerKeys, configWarnings, isServiceConfig, toRuntimeConfig } from "@/features/config";
import { DeclareTopologyUseCase } from "@/features/topology/declareTopology.useCase";
import {
    PublishEnvelopeDto,
    PublishEventUseCase,
    PublishTacticalEventUseCase,
    SendCommandUseCase,
} from "@/features/publishing";
import { InboxRepository, ConsumeMessagesUseCase } from "@/features/consuming";
import { OutboxRepository } from "@/features/outbox";

/**
 * Public entry point. Creates the shared pieces (AMQP connection and database)
 * and hands them to every feature. Features never import each other.
 */
export class RabbitMQResilience {
    private readonly config: RbtmqResilienceRuntimeConfigDto;

    private readonly amqpConnection: AmqpConnection;
    private readonly databaseService: DatabaseService;

    private readonly declareTopologyUseCase: DeclareTopologyUseCase;
    private readonly publishEventUseCase: PublishEventUseCase;
    private readonly publishTacticalEventUseCase: PublishTacticalEventUseCase;
    private readonly sendCommandUseCase: SendCommandUseCase;
    private readonly consumeMessagesUseCase: ConsumeMessagesUseCase;
    private reconnectRegistered = false;

    constructor(config: RbtmqResilienceConfigDto | RbtmqResilienceRuntimeConfigDto) {
        this.config = isServiceConfig(config) ? toRuntimeConfig(config) : config;
        assertUniqueConsumerKeys(this.config);
        assertInboxTransaction(this.config);

        this.amqpConnection = new AmqpConnection(this.config.amqp);
        this.databaseService = new DatabaseService(this.config);

        const publisher = new AmqpPublisher(this.config, this.amqpConnection);
        const inboxRepository = new InboxRepository(this.databaseService);
        const outboxRepository = new OutboxRepository(this.databaseService);

        this.declareTopologyUseCase = new DeclareTopologyUseCase(this.config, this.amqpConnection);
        // every publication goes straight to its exchange; the outbox only keeps the history
        this.publishEventUseCase = new PublishEventUseCase(this.config, publisher, outboxRepository);
        this.publishTacticalEventUseCase = new PublishTacticalEventUseCase(this.config, publisher, outboxRepository);
        this.sendCommandUseCase = new SendCommandUseCase(this.config, publisher, outboxRepository);
        this.consumeMessagesUseCase = new ConsumeMessagesUseCase(this.config, this.amqpConnection, inboxRepository, publisher);
    }

    public static initialize(config: RbtmqResilienceConfigDto | RbtmqResilienceRuntimeConfigDto): RabbitMQResilience {
        return new RabbitMQResilience(config);
    }

    public async init(): Promise<void> {
        await this.start();
    }

    public async start(): Promise<void> {
        for (const warning of this.getConfigWarnings()) {
            console.warn(`[rbtmq-resilience] ${warning}`);
        }

        await this.databaseService.init();
        await this.amqpConnection.ensureChannel();
        await this.declareTopologyUseCase.execute();
        await this.consumeMessagesUseCase.execute();

        // after RabbitMQ comes back, the new channel needs the topology and the consumers again
        if (!this.reconnectRegistered) {
            this.reconnectRegistered = true;
            this.amqpConnection.onReconnect(async () => {
                await this.declareTopologyUseCase.execute();
                await this.consumeMessagesUseCase.execute();
            });
        }
    }

    public async stop(): Promise<void> {
        await this.amqpConnection.close();
        await this.databaseService.close();
        this.amqpConnection.reset();
    }

    public async declareTopology(): Promise<void> {
        await this.declareTopologyUseCase.execute();
    }

    public async publishEvent(envelope: PublishEnvelopeDto | RabbitMQMessageDto): Promise<string> {
        return this.publishEventUseCase.execute(envelope);
    }

    public async publishTacticalEvent(envelope: PublishEnvelopeDto): Promise<string> {
        return this.publishTacticalEventUseCase.execute(envelope);
    }

    public async sendCommand(envelope: PublishEnvelopeDto): Promise<string> {
        return this.sendCommandUseCase.execute(envelope);
    }

    /**
     * Settings that work but send poison messages where Failed Messages Monitoring does not read them.
     * Logged on start().
     */
    public getConfigWarnings(): string[] {
        return configWarnings(this.config);
    }

    /** True when RabbitMQ and the database are reachable (for the service's own health check). */
    public async isReady(): Promise<boolean> {
        try {
            await this.amqpConnection.ensureChannel();
            await this.databaseService.init();
            return true;
        } catch {
            return false;
        }
    }
}

export function createRabbitMqResilience(config: RbtmqResilienceConfigDto | RbtmqResilienceRuntimeConfigDto): RabbitMQResilience {
    return new RabbitMQResilience(config);
}

export const RabbitMqResilience = RabbitMQResilience;
