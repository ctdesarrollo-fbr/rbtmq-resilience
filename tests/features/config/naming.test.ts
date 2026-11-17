import { describe, expect, test } from "bun:test";
import { ConsumerQueueNamesDto, QueueNamePartsDto, RbtmqNaming } from "@/index";

describe("RbtmqNaming", () => {
    test("tactical routing key: domain.{subdomain|boundedContext}.messageType", () => {
        expect(RbtmqNaming.tacticalRoutingKey(new QueueNamePartsDto("trade-logistics", "shipment", undefined, "manifest-finalized"))).toBe(
            "trade-logistics.shipment.manifest-finalized",
        );
        expect(RbtmqNaming.tacticalRoutingKey(new QueueNamePartsDto("trade-logistics", undefined, "customs", "document-verified"))).toBe(
            "trade-logistics.customs.document-verified",
        );
        // one of the two: the subdomain wins when both are given
        expect(RbtmqNaming.tacticalRoutingKey(new QueueNamePartsDto("commerce", "orders", "checkout", "created"))).toBe(
            "commerce.orders.created",
        );
    });

    test("a tactical routing key needs the three parts", () => {
        expect(() => RbtmqNaming.tacticalRoutingKey(new QueueNamePartsDto("commerce", " ", undefined, "created"))).toThrow(
            'a tactical routing key needs domain, subdomain or boundedContext, and messageType (got "commerce.created")',
        );
        expect(() => RbtmqNaming.tacticalRoutingKey(new QueueNamePartsDto("commerce", "orders"))).toThrow();
    });

    test("recognizes tactical routing keys: domain.{subdomain|boundedContext}.messageType", () => {
        expect(RbtmqNaming.isTacticalRoutingKey("trade-logistics.customs.document-verification-completed")).toBe(true);
        for (const invalid of ["a.b", "a.b.c.d", "a..c", "a.b.#", "a.*.c", "a.b c.d", ""]) {
            expect(RbtmqNaming.isTacticalRoutingKey(invalid)).toBe(false);
        }
    });

    test("builds tactical binding patterns: domain.# or domain.{subdomain|boundedContext}.#", () => {
        expect(RbtmqNaming.tacticalBindingPattern("trade-logistics")).toBe("trade-logistics.#");
        expect(RbtmqNaming.tacticalBindingPattern("trade-logistics", "customs")).toBe("trade-logistics.customs.#");
    });

    describe("consumerQueues", () => {
        const service = new QueueNamePartsDto("logistics", "volume-reception");

        test("events: X.retry on the primary queue, X.delayed-retry on the retry queue", () => {
            expect(RbtmqNaming.consumerQueues(service, "events")).toEqual(
                new ConsumerQueueNamesDto(
                    "logistics.volume-reception.events",
                    "logistics.volume-reception.retry",
                    "logistics.volume-reception.delayed-retry",
                    "logistics.volume-reception.delayed-retry",
                    "logistics.dead-letter",
                    "logistics.dead-letter",
                ),
            );
        });

        test("commands: X.commands, X.commands-retry and X.commands-delayed-retry", () => {
            expect(RbtmqNaming.consumerQueues(service, "commands")).toEqual(
                new ConsumerQueueNamesDto(
                    "logistics.volume-reception.commands",
                    "logistics.volume-reception.commands-retry",
                    "logistics.volume-reception.commands-delayed-retry",
                    "logistics.volume-reception.commands-delayed-retry",
                    "logistics.dead-letter",
                    "logistics.dead-letter",
                    "logistics.volume-reception.commands",
                ),
            );
        });

        test("tactical (topic) events share the events queue and retry loop", () => {
            expect(RbtmqNaming.consumerQueues(service, "tacticalEvents")).toEqual(
                RbtmqNaming.consumerQueues(service, "events"),
            );
        });

        test("uses the bounded context when there is no subdomain", () => {
            const names = RbtmqNaming.consumerQueues(new QueueNamePartsDto("logistics", undefined, "shipment"), "events");
            expect(names.routingKey).toBe("logistics.shipment.retry");
        });

        test("the error queue is shared by the whole domain", () => {
            expect(RbtmqNaming.deadLetter("logistics")).toBe("logistics.dead-letter");
        });
    });
});
