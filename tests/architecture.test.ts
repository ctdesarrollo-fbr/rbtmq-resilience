import { describe, expect, test } from "bun:test";
import { Glob } from "bun";
import fs from "node:fs";
import path from "node:path";

const SRC = path.resolve(import.meta.dir, "../src");

type Import = { from: string; to: string };

/** Resolves "@/x" and "./x" specifiers to a file path relative to src/ ("x.ts" or "x/index.ts"). */
function resolveSpecifier(fromFile: string, specifier: string): string | undefined {
    let absolute: string;
    if (specifier.startsWith("@/")) absolute = path.join(SRC, specifier.slice(2));
    else if (specifier.startsWith(".")) absolute = path.resolve(path.dirname(path.join(SRC, fromFile)), specifier);
    else return undefined;

    const relative = (file: string) => path.relative(SRC, file).split(path.sep).join("/");
    if (fs.existsSync(`${absolute}.ts`)) return relative(`${absolute}.ts`);
    if (fs.existsSync(path.join(absolute, "index.ts"))) return relative(path.join(absolute, "index.ts"));
    throw new Error(`Cannot resolve "${specifier}" from ${fromFile}`);
}

function collectImports(): Import[] {
    const imports: Import[] = [];
    for (const file of new Glob("**/*.ts").scanSync(SRC)) {
        const source = fs.readFileSync(path.join(SRC, file), "utf8");
        for (const match of source.matchAll(/from\s+"([^"]+)"/g)) {
            const target = resolveSpecifier(file, match[1]!);
            if (target) imports.push({ from: file.split(path.sep).join("/"), to: target });
        }
    }
    return imports;
}

const featureOf = (file: string) => file.match(/^features\/([^/]+)\//)?.[1];
/** A slice is a folder directly under a feature, except the feature's shared `database/` folder. */
const sliceOf = (file: string) => {
    const match = file.match(/^features\/([^/]+)\/([^/]+)\/.+/);
    if (!match || match[2] === "database") return undefined;
    return `${match[1]}/${match[2]}`;
};

describe("vertical slice rules", () => {
    const imports = collectImports();

    test("collects the internal imports (guards against a vacuous check)", () => {
        expect(imports.length).toBeGreaterThan(50);
        expect(imports).toContainEqual({ from: "resilience.ts", to: "features/outbox/index.ts" });
    });

    test("features do not import other features", () => {
        const violations = imports.filter(({ from, to }) => {
            const source = featureOf(from);
            const target = featureOf(to);
            return source && target && source !== target;
        });
        expect(violations).toEqual([]);
    });

    test("slices do not import other slices", () => {
        const violations = imports.filter(({ from, to }) => {
            const source = sliceOf(from);
            const target = sliceOf(to);
            return source && target && source !== target;
        });
        expect(violations).toEqual([]);
    });

    test("files inside a feature do not import their own feature barrel (avoids import cycles)", () => {
        const violations = imports.filter(
            ({ from, to }) => featureOf(from) !== undefined && to === `features/${featureOf(from)}/index.ts`,
        );
        expect(violations).toEqual([]);
    });

    test("shared/, amqp/ and database/ internals do not depend on features", () => {
        const registries = new Set(["database/registry.ts"]);
        const violations = imports.filter(
            ({ from, to }) =>
                /^(shared|amqp|database)\//.test(from) && !registries.has(from) && to.startsWith("features/"),
        );
        expect(violations).toEqual([]);
    });

    test("there are no layered (hexagonal) folders left", () => {
        const layered = [...new Glob("{application,domain,infrastructure,presentation}/**").scanSync(SRC)];
        expect(layered).toEqual([]);
    });
});
