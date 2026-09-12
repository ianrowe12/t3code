const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const test = require("node:test");

const root = join(__dirname, "../..");
const workflow = readFileSync(join(root, ".github/workflows/independent-review-gate.yml"), "utf8");
const relay = readFileSync(join(root, ".github/workflows/independent-review-events.yml"), "utf8");
const scripts = [...workflow.matchAll(/^          script: \|\n((?:^            .*\n|^\n)+)/gm)]
  .map((match) => match[1].replace(/^            /gm, ""));
assert.equal(scripts.length, 2, "extract the actual target-selection and gate scripts");
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const execute = (index, options) => new AsyncFunction(
  "github", "context", "core", "process", "setTimeout", scripts[index],
)(options.github, options.context, options.core, options.process, options.setTimeout);

const HEAD = "a17d08e" + "0".repeat(33);
const OLD = "0423f73" + "0".repeat(33);
const TIME = "2026-09-12T13:00:00Z";
const LATER = "2026-09-12T13:01:00Z";
const LAST = "2026-09-12T13:02:00Z";
const verdict = (direction = "APPROVE", sha = HEAD) =>
  `Reviewed head: ${sha}\nReviewed by: unknown\nIndependent review verdict: ${direction}`;
const comment = (body = verdict(), overrides = {}) => ({
  id: 1, body, created_at: TIME, updated_at: TIME, ...overrides,
});
const review = (body = verdict(), overrides = {}) => ({
  databaseId: 1, body, submittedAt: TIME, updatedAt: TIME, state: "COMMENTED", ...overrides,
});

function fixture({
  comments = [],
  reviews = [],
  labels = [],
  sha = HEAD,
  state = "open",
  before,
  failures = {},
  postFailure,
} = {}) {
  const current = {
    pr: { state, head: { sha }, labels: labels.map((name) => ({ name })) },
    comments: structuredClone(comments),
    reviews: structuredClone(reviews),
  };
  const calls = [];
  const counts = {};
  const statuses = [];
  const sleeps = [];
  const logs = [];
  function call(name, params) {
    calls.push({ name, params });
    counts[name] = (counts[name] ?? 0) + 1;
    before?.({ name, count: counts[name], current });
    if (failures[name]?.length) throw failures[name].shift();
  }
  const github = {
    rest: {
      pulls: {
        async get(params) {
          call("pull", params);
          return { data: structuredClone(current.pr) };
        },
      },
      issues: { listComments() {} },
      repos: {
        async createCommitStatus(params) {
          call("post", params);
          statuses.push(params);
          if (postFailure) throw postFailure;
          return { data: {} };
        },
      },
    },
    async paginate(endpoint, params) {
      assert.equal(endpoint, github.rest.issues.listComments);
      assert.equal(params.per_page, 100);
      call("comments", params);
      return structuredClone(current.comments);
    },
    async graphql(query, params) {
      assert.match(query, /reviews\(first: 100, after: \$cursor\)/);
      assert.match(query, /nodes \{ databaseId body state updatedAt \}/);
      assert.equal(params.owner, "example");
      assert.equal(params.repo, "t3");
      assert.equal(params.number, 42);
      call("reviews", params);
      const start = Number(params.cursor ?? 0);
      const end = start + 100;
      return {
        repository: { pullRequest: { reviews: {
          nodes: structuredClone(current.reviews.slice(start, end)),
          pageInfo: {
            hasNextPage: end < current.reviews.length,
            endCursor: String(end),
          },
        } } },
      };
    },
  };
  return {
    current, calls, counts, statuses, sleeps, logs,
    options: {
      github,
      context: { repo: { owner: "example", repo: "t3" }, serverUrl: "https://github.com" },
      process: { env: { PR_NUMBER: "42" } },
      core: { info(message) { logs.push(message); } },
      setTimeout(resolve, delay) { sleeps.push(delay); resolve(); },
    },
  };
}

async function evaluate(input) {
  const result = fixture(input);
  await execute(1, result.options);
  return result;
}

function assertStatus(result, state, description, sha = HEAD) {
  assert.equal(result.statuses.length, 1);
  const status = result.statuses[0];
  assert.equal(status.sha, sha);
  assert.equal(status.state, state);
  assert.equal(status.context, "Independent Review");
  assert.equal(status.target_url, "https://github.com/example/t3/blob/main/docs/agents/independent-review.md");
  assert.ok(status.description.length <= 140);
  if (description) assert.match(status.description, description);
}

test("the trusted workflow handles every comment mutation and serializes per PR", () => {
  assert.match(workflow, /pull_request_target:\n    types: \[opened, reopened, synchronize, labeled, unlabeled, ready_for_review, edited\]/);
  assert.match(workflow, /issue_comment:\n    types: \[created, edited, deleted\]/);
  assert.match(workflow, /github\.event\.issue\.pull_request/);
  assert.doesNotMatch(workflow, /contains\(/);
  assert.match(workflow, /group: independent-review-\$\{\{ matrix\.number \}\}/);
  assert.match(workflow, /cancel-in-progress: false\n      queue: max/);
  assert.match(workflow, /statuses: write/);
  assert.match(workflow, /retries: 0/);
});

test("review mutations wake trusted base code through an unprivileged relay", () => {
  assert.match(relay, /pull_request_review:\n    types: \[submitted, edited, dismissed\]/);
  assert.match(relay, /permissions: \{\}/);
  assert.match(workflow, /workflow_run:\n    workflows: \[Independent Review Events\]\n    types: \[completed\]/);
  assert.match(workflow, /github\.event\.workflow_run\.event == 'pull_request_review'/);
  assert.doesNotMatch(workflow, /^  pull_request(?:_review)?:/m);
  for (const source of [workflow, relay]) {
    assert.doesNotMatch(source, /uses:.*(?:checkout|artifact|cache|setup)/);
    assert.doesNotMatch(source, /\$\{\{[^}]*\.(?:body|title|head_ref)/);
    assert.doesNotMatch(source, /secrets\./);
    assert.match(source, /runs-on: blacksmith-8vcpu-ubuntu-2404/);
  }
  const ci = readFileSync(join(root, ".github/workflows/ci.yml"), "utf8");
  assert.match(ci, /node --test \.github\/scripts\/independent-review-gate\.test\.cjs/);
});

test("target selection uses PR numbers, deduplicates reviews, and reconciles forks without artifacts", async () => {
  for (const [eventName, payload, expected] of [
    ["pull_request_target", { pull_request: { number: 42 } }, [42]],
    ["issue_comment", { issue: { number: 43 } }, [43]],
    ["workflow_run", { workflow_run: { pull_requests: [{ number: 42 }, { number: 42 }, { number: 43 }] } }, [42, 43]],
    ["workflow_run", { workflow_run: { pull_requests: [] } }, [44, 45]],
    ["workflow_run", { workflow_run: {} }, [44, 45]],
  ]) {
    const { options } = fixture();
    options.context.eventName = eventName;
    options.context.payload = payload;
    options.github.rest.pulls.list = () => {};
    let listed = false;
    options.github.paginate = async (endpoint, params) => {
      listed = true;
      assert.equal(endpoint, options.github.rest.pulls.list);
      assert.equal(params.state, "open");
      assert.equal(params.per_page, 100);
      return [{ number: 44 }, { number: 45 }];
    };
    assert.deepEqual(await execute(0, options), expected);
    assert.equal(listed, expected[0] === 44);
  }
});

for (const source of ["comments", "reviews"]) {
  const record = source === "comments" ? comment : review;
  test(`${source}: exact current head approves without consulting commit timestamps`, async () => {
    assertStatus(await evaluate({ [source]: [record()] }), "success", /approved for current head/);
    assertStatus(await evaluate({ [source]: [record(verdict("APPROVE", OLD), {
      updated_at: LAST, updatedAt: LAST,
    })] }), "failure", /No verdict names the current head/);
    assertStatus(await evaluate({ [source]: [record(
      verdict("APPROVE", HEAD.toUpperCase()).replaceAll("\n", "\r\n") + "  \r\n",
    )] }), "success");
  });

  test(`${source}: missing, abbreviated, quoted, duplicate, and ambiguous head lines cannot approve`, async () => {
    for (const head of [
      "",
      `I reviewed ${HEAD}.\n`,
      `Reviewed head: ${HEAD.slice(0, 7)}\n`,
      `Reviewed head: ${HEAD}0\n`,
      `Reviewed head: ${"g".repeat(40)}\n`,
      `> Reviewed head: ${HEAD}\n`,
      `Previous Reviewed head: ${HEAD}\n`,
      `Reviewed head: ${HEAD} extra text\n`,
      `Reviewed head: ${HEAD}\nReviewed head: ${OLD}\n`,
      `Reviewed head: ${HEAD}\nReviewed head: ${HEAD}\n`,
      `Reviewed head: ${HEAD}\nReviewed head: ${HEAD.slice(0, 7)}\n`,
      `Reviewed head: ${HEAD}\nReviewed head: unknown\n`,
      `    Reviewed head: ${HEAD}\n`,
    ]) {
      const result = await evaluate({
        [source]: [record(head + "Independent review verdict: APPROVE")],
      });
      assertStatus(result, "failure", /exactly one full Reviewed head SHA/);
    }
  });

  test(`${source}: request changes stays red and only a later approval clears it`, async () => {
    assertStatus(await evaluate({ [source]: [record(verdict("REQUEST CHANGES"))] }),
      "failure", /requested changes/);
    assertStatus(await evaluate({ [source]: [
      record(verdict("REQUEST CHANGES")),
      record(verdict(), { id: 2, databaseId: 2, updated_at: LATER, updatedAt: LATER }),
    ] }), "success");
  });
}

test("both documented closing blocks execute for comments and reviews", async () => {
  const protocol = readFileSync(join(root, "docs/agents/independent-review.md"), "utf8");
  const blocks = [...protocol.matchAll(/^```text\n(.*?)^```/gms)].map((match) => match[1]);
  assert.equal(blocks.length, 2);
  for (const [index, block] of blocks.entries()) {
    const body = block.replace("<full 40-character SHA>", HEAD);
    for (const [source, record] of [["comments", comment], ["reviews", review]]) {
      assertStatus(await evaluate({ [source]: [record(body)] }), index === 0 ? "success" : "failure");
    }
  }
});

test("missing markers, fenced examples, and quoted protocol text never approve", async () => {
  for (const body of [
    "",
    "Looks good",
    "```\n" + verdict() + "\n```",
    "~~~text\n" + verdict() + "\n~~~",
    "````text\n```\n" + verdict() + "\n```\n````",
    verdict().split("\n").map((line) => `> ${line}`).join("\n"),
    `I said ${verdict()}`.replaceAll("\n", " "),
  ]) {
    assertStatus(await evaluate({ comments: [comment(body)] }), "failure", /No independent review verdict/);
  }
});

test("approval must be a single exact final marker, not a prefix or duplicate", async () => {
  for (const body of [
    verdict() + "D",
    verdict() + " if CI passes",
    verdict() + "\nMore text",
    verdict() + "\nIndependent review verdict: REQUEST CHANGES",
    verdict() + "\nIndependent review verdict: APPROVE",
    verdict().replace("Independent review verdict:", "Verdict:"),
    verdict().replace("Independent review verdict:", "    Independent review verdict:"),
    verdict().replace("APPROVE", "REQUEST-CHANGES"),
    verdict() + "\n```\nIndependent review verdict: APPROVE",
  ]) {
    assertStatus(await evaluate({ comments: [comment(body)] }), "failure", /exactly one final/);
  }
});

test("latest conflicts fail closed across both sources and regardless of order", async () => {
  for (const [first, second] of [["APPROVE", "REQUEST CHANGES"], ["REQUEST CHANGES", "APPROVE"]]) {
    assertStatus(await evaluate({ comments: [
      comment(verdict(first)),
      comment(verdict(second), { id: 2 }),
    ] }), "failure", /Conflicting/);
    assertStatus(await evaluate({
      comments: [comment(verdict(first))],
      reviews: [review(verdict(second))],
    }), "failure", /Conflicting/);
  }
  assertStatus(await evaluate({
    comments: [comment()],
    reviews: [review(verdict(), { updatedAt: "2026-09-12T13:00:00.000Z" })],
  }), "success");
});

test("a later other-head verdict cannot override current-head direction", async () => {
  for (const [direction, other, expected] of [
    ["APPROVE", "REQUEST CHANGES", "success"],
    ["REQUEST CHANGES", "APPROVE", "failure"],
  ]) {
    assertStatus(await evaluate({
      comments: [comment(verdict(direction))],
      reviews: [review(verdict(other, OLD), { updatedAt: LAST })],
    }), expected);
  }
});

test("edits reorder comments and reviews by update time, not submission time", async () => {
  assertStatus(await evaluate({
    comments: [comment(verdict(), { created_at: LATER, updated_at: LATER })],
    reviews: [review(verdict("REQUEST CHANGES"), { submittedAt: TIME, updatedAt: LAST })],
  }), "failure", /requested changes/);
  assertStatus(await evaluate({
    comments: [comment(verdict(), { created_at: TIME, updated_at: LAST })],
    reviews: [review(verdict("REQUEST CHANGES"), { updatedAt: LATER })],
  }), "success");
});

test("latest unbound or invalid verdict fails closed until a newer bound approval", async () => {
  for (const body of ["Independent review verdict: REQUEST CHANGES", verdict() + " maybe"]) {
    assertStatus(await evaluate({
      comments: [comment(), comment(body, { id: 2, updated_at: LATER })],
    }), "failure");
    assertStatus(await evaluate({
      comments: [comment(body), comment(verdict(), { id: 2, updated_at: LATER })],
    }), "success");
  }
  for (const timestamp of ["", "not-a-date", undefined]) {
    assertStatus(await evaluate({ reviews: [review(verdict(), { updatedAt: timestamp })] }),
      "failure", /timestamp unavailable/);
  }
});

test("dismissed and pending reviews are ignored but a submitted COMMENTED verdict counts", async () => {
  for (const state of ["DISMISSED", "PENDING"]) {
    assertStatus(await evaluate({ reviews: [review(verdict(), { state })] }), "failure");
    assertStatus(await evaluate({
      comments: [comment()],
      reviews: [review(verdict("REQUEST CHANGES"), { state, updatedAt: LATER })],
    }), "success");
  }
  assertStatus(await evaluate({ reviews: [review()] }), "success");
});

test("comment deletion, marker removal, review dismissal, and edits are reread before publishing", async () => {
  for (const mutate of [
    (current) => { current.comments = []; },
    (current) => { current.comments[0].body = "Review withdrawn"; },
    (current) => { current.comments[0].body = verdict("REQUEST CHANGES"); },
  ]) {
    assertStatus(await evaluate({
      comments: [comment()],
      before({ name, count, current }) {
        if (name === "comments" && count === 2) mutate(current);
      },
    }), "failure");
  }
  for (const mutate of [
    (current) => { current.reviews[0].state = "DISMISSED"; },
    (current) => { current.reviews[0].body = verdict("REQUEST CHANGES"); },
    (current) => { current.reviews = []; },
  ]) {
    assertStatus(await evaluate({
      reviews: [review()],
      before({ name, count, current }) {
        if (name === "reviews" && count === 2) mutate(current);
      },
    }), "failure");
  }
});

test("a new blocker or head observed before the write never publishes stale green", async () => {
  assertStatus(await evaluate({
    comments: [comment()],
    before({ name, count, current }) {
      if (name === "comments" && count === 2) {
        current.comments.push(comment(verdict("REQUEST CHANGES"), { id: 2, updated_at: LATER }));
      }
    },
  }), "failure", /requested changes/);
  assertStatus(await evaluate({
    comments: [comment()],
    before({ name, count, current }) {
      if (name === "pull" && count === 2) current.pr.head.sha = OLD;
    },
  }), "failure", /No verdict names the current head/, OLD);
});

test("closed PRs do not receive status, including closure while evaluating", async () => {
  for (const closeOnRead of [1, 2]) {
    const result = await evaluate({
      comments: [comment()],
      before({ name, count, current }) {
        if (name === "pull" && count === closeOnRead) current.pr.state = "closed";
      },
    });
    assert.equal(result.statuses.length, 0);
  }
});

test("the explicit exemption label is reevaluated and removal restores failure", async () => {
  assertStatus(await evaluate({ labels: ["independent-review-exempt"] }), "success", /Exempt via/);
  assertStatus(await evaluate({
    labels: ["independent-review-exempt"],
    before({ name, count, current }) {
      if (name === "pull" && count === 2) current.pr.labels = [];
    },
  }), "failure");
  assertStatus(await evaluate({
    before({ name, count, current }) {
      if (name === "pull" && count === 2) {
        current.pr.labels = [{ name: "independent-review-exempt" }];
      }
    },
  }), "success");
});

test("continuous verdict changes hit the retry bound and publish failure on the reread head", async () => {
  const result = await evaluate({
    comments: [comment()],
    before({ name, count, current }) {
      if (name === "comments") current.comments[0].id = count;
      if (name === "pull" && count === 7) current.pr.head.sha = OLD;
    },
  });
  assert.equal(result.counts.pull, 7);
  assert.equal(result.counts.comments, 6);
  assertStatus(result, "failure", /changed during evaluation/, OLD);
});

test("all paginated comments and reviews participate in the decision", async () => {
  for (const source of ["comments", "reviews"]) {
    const record = source === "comments" ? comment : review;
    const records = Array.from({ length: 101 }, (_, index) => record(
      index === 100 ? verdict("REQUEST CHANGES") : verdict(),
      { id: index, databaseId: index, updated_at: index === 100 ? LAST : TIME, updatedAt: index === 100 ? LAST : TIME },
    ));
    const result = await evaluate({ [source]: records });
    assertStatus(result, "failure", /requested changes/);
    if (source === "reviews") {
      assert.deepEqual(result.calls.filter((call) => call.name === "reviews").map((call) => call.params.cursor),
        [null, "100", null, "100"]);
    }
  }
});

test("malformed review pagination and API results surface errors instead of approving", async () => {
  for (const response of [
    { repository: null },
    { repository: { pullRequest: { reviews: {
      nodes: [], pageInfo: { hasNextPage: true, endCursor: null },
    } } } },
  ]) {
    const result = fixture({ comments: [comment()] });
    result.options.github.graphql = async () => response;
    await assert.rejects(execute(1, result.options));
    assert.equal(result.statuses.length, 0);
  }
});

test("transient HTTP and connection failures retry reads with bounded backoff", async () => {
  for (const name of ["pull", "comments", "reviews"]) {
    for (const errors of [
      [502, 503, 504].map((status) => Object.assign(new Error("HTTP failure"), { status })),
      ["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN"].map((code) => Object.assign(new Error("connection failure"), { code })),
      ["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN"].map((code) => new Error("fetch failed", { cause: { code } })),
    ]) {
      const result = await evaluate({ comments: [comment()], failures: { [name]: errors } });
      assertStatus(result, "success");
      assert.equal(result.counts[name], 5);
      assert.deepEqual(result.sleeps, [500, 1000, 2000]);
      assert.ok(result.calls.every((call) => call.params.request.timeout === 30000));
    }
  }
});

test("four transient failures stop; permanent HTTP, DNS, and unrelated errors do not retry", async () => {
  for (const [error, expectedAttempts] of [
    [Object.assign(new Error("gateway timeout"), { status: 504 }), 4],
    [Object.assign(new Error("validation"), { status: 422 }), 1],
    [Object.assign(new Error("unauthorized"), { status: 401 }), 1],
    [Object.assign(new Error("permanent DNS failure"), { code: "ENOTFOUND" }), 1],
    [new Error("unsupported URL scheme"), 1],
  ]) {
    const result = fixture({ failures: { pull: Array(4).fill(error) } });
    await assert.rejects(execute(1, result.options), (actual) => actual === error);
    assert.equal(result.counts.pull, expectedAttempts);
    assert.equal(result.statuses.length, 0);
  }
});

test("ambiguous status write failure is never retried", async () => {
  for (const error of [
    Object.assign(new Error("response timed out"), { code: "ETIMEDOUT" }),
    Object.assign(new Error("gateway error after accepting write"), { status: 502 }),
  ]) {
    const result = fixture({ comments: [comment()], postFailure: error });
    await assert.rejects(execute(1, result.options), (actual) => actual === error);
    assert.equal(result.counts.post, 1);
    assert.equal(result.statuses.length, 1);
    assert.deepEqual(result.sleeps, []);
  }
});

test("invalid PR identifiers and malformed current heads fail explicitly", async () => {
  for (const value of ["", "NaN", "-1", "1.5", "9007199254740992"]) {
    const result = fixture();
    result.options.process.env.PR_NUMBER = value;
    await assert.rejects(execute(1, result.options), /PR_NUMBER/);
    assert.equal(result.calls.length, 0);
  }
  const result = fixture({ sha: HEAD.slice(0, 7) });
  await assert.rejects(execute(1, result.options), /Invalid PR head SHA/);
  assert.equal(result.statuses.length, 0);
});
