import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  ModelCapabilities,
  ProviderOptionDescriptor,
  ProviderOptionSelection,
  ProviderOptionSelections,
} from "./model.ts";

describe("explicit provider defaults", () => {
  it.each([[{ id: "agent", value: "" }], { agent: "" }])(
    "round-trips an explicit empty selection from %j",
    (options) => {
      const codec = Schema.fromJsonString(ProviderOptionSelections);
      const decoded = Schema.decodeSync(codec)(JSON.stringify(options));
      expect(decoded).toEqual([{ id: "agent", value: "" }]);
      expect(JSON.parse(Schema.encodeSync(codec)(decoded))).toEqual([{ id: "agent", value: "" }]);
    },
  );

  it("round-trips the advertised default choice and current value", () => {
    const caps = {
      optionDescriptors: [
        {
          id: "agent",
          label: "Agent",
          type: "select",
          currentValue: "",
          options: [
            { id: "", label: "Copilot" },
            { id: "researcher", label: "Researcher" },
          ],
        },
      ],
    };
    const codec = Schema.fromJsonString(ModelCapabilities);
    const decoded = Schema.decodeSync(codec)(JSON.stringify(caps));
    expect(JSON.parse(Schema.encodeSync(codec)(decoded))).toEqual(caps);
  });

  it("keeps empty option IDs invalid and omission distinct from default", () => {
    expect(() => Schema.decodeSync(ProviderOptionSelection)({ id: "", value: "" })).toThrow();
    expect(() =>
      Schema.decodeSync(ProviderOptionDescriptor)({
        id: "",
        label: "Agent",
        type: "select",
        options: [],
      }),
    ).toThrow();
    expect(Schema.decodeSync(ProviderOptionSelections)([])).toEqual([]);
    expect(Schema.decodeSync(ProviderOptionSelections)({})).toEqual([]);
    expect(Schema.decodeSync(ProviderOptionSelections)({ agent: "   " })).toEqual([]);
  });
});
