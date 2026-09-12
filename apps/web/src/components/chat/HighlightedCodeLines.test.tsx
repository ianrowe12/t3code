import { getSharedHighlighter } from "@pierre/diffs";
import { toHtml } from "hast-util-to-html";
import { act } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { createIncrementalHighlightedDocument } from "../../lib/incrementalHighlighting";
import { HighlightedCodeLines } from "./HighlightedCodeLines";

class TestNode {
  parentNode: TestNode | null = null;
  childNodes: TestNode[] = [];
  readonly nodeName: string;
  readonly tagName: string;
  readonly namespaceURI = "http://www.w3.org/1999/xhtml";
  readonly style: Record<string, string> = {};
  readonly attributes = new Map<string, string>();
  innerHTML = "";
  nodeValue: string | null = null;

  constructor(
    name: string,
    readonly ownerDocument: TestNode | null = null,
    readonly nodeType = 1,
  ) {
    this.nodeName = name.toUpperCase();
    this.tagName = this.nodeName;
  }

  get firstChild() {
    return this.childNodes[0] ?? null;
  }

  get nextSibling() {
    if (this.parentNode === null) return null;
    const index = this.parentNode.childNodes.indexOf(this);
    return this.parentNode.childNodes[index + 1] ?? null;
  }

  set textContent(value: string) {
    this.childNodes = [];
    this.nodeValue = value;
  }

  get textContent() {
    return this.nodeValue ?? this.childNodes.map((child) => child.textContent).join("");
  }

  appendChild(child: TestNode) {
    child.parentNode?.removeChild(child);
    child.parentNode = this;
    this.childNodes.push(child);
    return child;
  }

  insertBefore(child: TestNode, before: TestNode | null) {
    if (before === null) return this.appendChild(child);
    child.parentNode?.removeChild(child);
    const index = this.childNodes.indexOf(before);
    child.parentNode = this;
    this.childNodes.splice(index, 0, child);
    return child;
  }

  removeChild(child: TestNode) {
    this.childNodes.splice(this.childNodes.indexOf(child), 1);
    child.parentNode = null;
    return child;
  }

  contains(candidate: TestNode | null): boolean {
    return candidate === this || this.childNodes.some((child) => child.contains(candidate));
  }

  createElement(name: string) {
    return new TestNode(name, this);
  }

  createElementNS(_namespace: string, name: string) {
    return this.createElement(name);
  }

  createTextNode(value: string) {
    const node = new TestNode("#text", this, 3);
    node.nodeValue = value;
    return node;
  }

  setAttribute(name: string, value: string) {
    this.attributes.set(name, value);
  }

  removeAttribute(name: string) {
    this.attributes.delete(name);
  }

  getRootNode() {
    let current: TestNode = this;
    while (current.parentNode !== null) current = current.parentNode;
    return current;
  }

  addEventListener() {}
  removeEventListener() {}
}

function installTestDom() {
  const document = new TestNode("#document", null, 9);
  const window = {
    document,
    HTMLIFrameElement: TestNode,
    setTimeout: globalThis.setTimeout,
    clearTimeout: globalThis.clearTimeout,
    addEventListener() {},
    removeEventListener() {},
  };
  vi.stubGlobal("document", document);
  vi.stubGlobal("window", window);
  vi.stubGlobal("Node", TestNode);
  vi.stubGlobal("Element", TestNode);
  vi.stubGlobal("HTMLElement", TestNode);
  vi.stubGlobal("HTMLIFrameElement", TestNode);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  return document;
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("highlighted code lines", () => {
  it("preserves Shiki HTML, including colors, escaping, whitespace, and blank lines", async () => {
    const highlighter = await getSharedHighlighter({
      langs: ["typescript"],
      themes: ["pierre-dark", "pierre-light"],
      preferredHighlighter: "shiki-wasm",
    });
    for (const theme of ["pierre-dark", "pierre-light"] as const) {
      const highlight = createIncrementalHighlightedDocument(highlighter, "typescript", theme);
      const code =
        'const html = "<img src=x onerror=alert(1)>";\n\n/* multi\nline */\n\tconst x = 1;\n';
      for (let end = 0; end <= code.length; end++) {
        const root = highlight(code.slice(0, end));
        expect(renderToStaticMarkup(<HighlightedCodeLines root={root} />)).toBe(toHtml(root));
      }
    }
  });

  it("keeps completed line DOM mounted while highlighting only the streamed suffix", async () => {
    const highlighter = await getSharedHighlighter({
      langs: ["typescript"],
      themes: ["pierre-dark"],
      preferredHighlighter: "shiki-wasm",
    });
    const highlightCall = vi.spyOn(highlighter, "codeToHast");
    const highlight = createIncrementalHighlightedDocument(
      highlighter,
      "typescript",
      "pierre-dark",
    );
    const document = installTestDom();
    const { createRoot } = await import("react-dom/client");
    const container = document.createElement("div");
    const reactRoot = createRoot(container as unknown as HTMLElement);

    try {
      const firstRoot = highlight("const completed = 1;\nconst streaming =");
      await act(() => reactRoot.render(<HighlightedCodeLines root={firstRoot} />));
      const completedLine = container.firstChild?.firstChild?.firstChild;
      expect(completedLine?.nodeName).toBe("SPAN");

      highlightCall.mockClear();
      const nextRoot = highlight("const completed = 1;\nconst streaming = 2;\n");
      expect(highlightCall).toHaveBeenCalled();
      expect(
        highlightCall.mock.calls.every(([source]) => !String(source).includes("completed = 1")),
      ).toBe(true);

      await act(() => reactRoot.render(<HighlightedCodeLines root={nextRoot} />));
      expect(container.firstChild?.firstChild?.firstChild).toBe(completedLine);
    } finally {
      await act(() => reactRoot.unmount());
    }
  });
});
