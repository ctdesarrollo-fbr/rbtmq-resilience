import { describe, expect, test } from "bun:test";
import utils from "@/shared/utils";
import { rabbitMQResilienceConstants } from "@/index";

describe("shared utils", () => {
    test("header readers", () => {
        expect(utils.stringHeader("a")).toBe("a");
        expect(utils.stringHeader(3)).toBe("3");
        expect(utils.stringHeader(Buffer.from("b"))).toBe("b");
        expect(utils.stringHeader({})).toBeUndefined();
        expect(utils.numberHeader("4")).toBe(4);
        expect(utils.numberHeader("x")).toBe(0);
        expect(utils.numberHeader(undefined)).toBe(0);
        expect(utils.normalizeHeaders(null)).toEqual({});
    });

    test("xDeathCount sums x-death counts", () => {
        expect(utils.xDeathCount({})).toBe(0);
        expect(utils.xDeathCount({ "x-death": [{ count: 2 }, { count: "3" }, "junk"] })).toBe(5);
    });

    test("conversions", () => {
        expect(utils.toExchangeKind("topic", "fanout")).toBe("topic");
        expect(utils.toExchangeKind("bad", "direct")).toBe("direct");
        expect(utils.toAmqpUrl({})).toBe("amqp://guest:guest@localhost:5672/%2F");
        expect(utils.toAmqpUrl({ protocol: "amqps:" })).toBe("amqps://guest:guest@localhost:5672/%2F");
    });

    test("public constants", () => {
        expect(rabbitMQResilienceConstants).toMatchSnapshot();
    });
});
