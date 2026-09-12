# Independent review protocol

Every PR needs one fresh reviewer with no involvement in writing the change. An implementing session may spawn that reviewer in a clean context, but cannot review its own work. The reviewer reads the diff cold and never pushes fixes.

## Review and merge

1. Finish the scope, resolve conflicts, and pin the full PR head SHA. Start independent review alongside CI, not after it finishes. Queued or running CI is fine. If expected CI is absent, or the PR is conflicting or incomplete, report that blocker without a verdict and resume when it is reviewable.
2. Use Claude Opus 5 by default. Request GPT-6 Astra for security-sensitive changes or a stuck review loop. If the preferred model is unavailable, use another available model rather than skipping review. Select models through the host's supported controls.
3. Give the reviewer the PR, pinned SHA, linked issue/spec, and this protocol. The reviewer re-derives requirements from the spec and reads the complete diff and relevant surrounding code. Review the dimensions below, not only the path the author exercised.
4. Immediately before posting, fetch the current PR head again. If it differs, review the new diff before posting any findings or verdict. Publish the reviewer's findings verbatim, with file and line for each, each prefixed `BLOCKING -` or `ADVISORY -`. An author relaying a review must preserve the text and identify the independent reviewer; the author's account is not evidence of independence.
5. End the verdict comment or review body with one of the plain-text blocks below, without code fences. Include exactly one `Reviewed head:` line with the full 40-character SHA in that same body. Keep the verdict marker as the final line.

```text
Reviewed head: <full 40-character SHA>
Reviewed by: unknown
Independent review verdict: APPROVE
```

```text
Reviewed head: <full 40-character SHA>
Reviewed by: unknown
Independent review verdict: REQUEST CHANGES
```

Use the actual host-known model in `Reviewed by:` and include reasoning effort only when the runtime exposes it. Use `unknown` when identity is unavailable. A requested model is not proof of which model ran. Host invocation metadata is authoritative; self-reported model identity is advisory. A host that can attest the invocation may add a provenance line immediately before the verdict. Do not invent host stamps or claim this gate authenticates them.

6. The author fixes only blockers in this PR, or explains why a blocker is wrong. Only the independent reviewer can accept that answer through a fresh approving pass. An author reply or resolved thread does not clear a blocker. The reviewer never takes over authorship to fix it.
7. Merge only with approval of the current head and all required/triggered CI passing. Any new commit invalidates approval, regardless of commit dates. Re-review may use the same independent reviewer thread, provided it reads the new diff. Follow the existing requirement for explicit permission to create PRs or merge.

## Findings and dispositions

`BLOCKING` means this head must not ship. Any unresolved blocker requires `REQUEST CHANGES`. Fix it here or obtain the reviewer's acceptance of an answer. A demonstrated defect is not downgraded to end a loop; an advisory is not promoted merely to get work done.

`ADVISORY` means no code change is required in this PR. Record it in the review and reply with a disposition, even on a `REQUEST CHANGES` round. Advisory fixes never ride along with blocker fixes. `APPROVE` means "merge this head as it stands", not a soft request for polishing.

File an advisory only for a user-visible defect; a security, privacy, or data-loss risk; a CI, release, or production failure; a violation of a documented decision or contract ownership rule; or an accessibility/internationalization defect on a shipped client. Tooling nits, naming, prose preferences, and speculative hardening remain review notes. Repeated mechanically decidable findings should lead to a proposed guard and one backfill, not an issue per occurrence.

Reply to every advisory with its issue link, a reason it is below that bar, or a false-positive explanation. Quote filed findings verbatim with file/line and links to the PR and review. Batch only coherent findings. Filing is a work commitment, not a way to discard a finding; it does not require another review round or code changes before this PR merges.

On an original PR, work qualifying advisory follow-ups after merge when authorized, or hand off their issue numbers explicitly. On a PR closing an issue filed from an advisory, new qualifying advisories go to the backlog without another same-session chain of PRs. Declare `Review lineage` in the PR body; the originating issue and review link determine lineage if the declaration is absent or wrong. Missing lineage is advisory, not blocking. This one-generation limit does not limit blocker fixes.

### Test findings require mutation proof

"This test could be stronger" is not a finding. To challenge a test's protection, make a concrete source mutation that reinstates the defect or breaks the behavior the test claims to protect, then demonstrate that the relevant suite still passes. Report the mutation, command, and result. Work in an isolated disposable copy and remove the mutation afterward, never in the author's dirty worktree or a live install.

A demonstrated false green is `BLOCKING`, not advisory. Without the demonstration, do not raise or file speculative test hardening. Reviewers should also check that changed behavior has focused coverage consistent with AGENTS.md; this does not require speculative tests for unrelated behavior.

## Convergence and the stop rule

Approval ends polishing. After approval, push only to fix a current-head CI failure, resolve a conflict/required branch update, or fix a newly discovered genuine blocker. Each push requires approval and CI for the new head.

There is no automatic cap on blockers. After three consecutive re-reviews introduce new blockers, explain on the PR what keeps breaking and flag it to the owner. Revisit the specification, split the change, or change approach rather than silently dismissing findings.

Distinguish missing coverage from hardening. A realistic mutation that could hide a production regression is coverage. Hardening means the behavior is exercised and asserted, but an oracle admits a corrupted variant for which the reviewer can name no plausible production effect. The reviewer makes that distinction in the finding; unclassified findings count as coverage.

If consecutive rounds only harden tests over an unchanged runtime diff, escalate with the heads, proofs, and unresolved findings. If the loop instead keeps expanding guard code introduced during review, re-derive all its coverage claims, retain proof pointers for claims actually demonstrated, and narrow unsupported claims rather than extending analysis indefinitely. A live defect in anything still claimed remains blocking, and narrowing still needs fresh approval.

This port does not inherit Aria's CTO authorization, standing test-hardening exemption, or automatic merge caps. Any exception here requires explicit per-PR repository-owner authorization, a recorded reason, the exact head and findings covered, and green CI. Round count or an unchanged runtime diff alone grants no exception.

## Review dimensions

- Logic and failure paths: races, cancellation, ordering, partial failures, persistence, and state transitions.
- Specification and contracts: requested behavior, typed wire schemas in `packages/contracts`, and matching server, web, desktop, and mobile implementations.
- Clients and connection modes: loading, empty, error, offline, reconnect, local, remote/relay, tunnel, multi-device, and multi-environment behavior. Inspect provided UI evidence; browser/computer use still requires permission under AGENTS.md.
- Provider boundaries: adapter differences, capability declarations, session ownership, credentials, and unsupported-provider behavior.
- Intentional decisions: architectural constraints in AGENTS.md and `docs/internals/`, documented ownership boundaries, and live-install safety. Working code that contradicts a documented decision is still a finding.
- User impact: responsiveness and transfer volume, accessibility, applicable localization, privacy, and data-loss risks.

## Enforcement and its limits

[The gate workflow](../../.github/workflows/independent-review-gate.yml) publishes the `Independent Review` commit status. It considers verdicts for the exact current head, orders them by comment/review update time, and fails closed on conflicting latest verdicts. Commit timestamps never establish coverage. Missing, abbreviated, duplicate, or ambiguous head lines cannot approve. Quoted/fenced examples and non-final approval markers cannot approve either.

Comments and submitted reviews are supported. Editing/deleting comments, editing/dismissing reviews, pushes, and exemption-label changes trigger a fresh evaluation. Dismissed reviews are excluded. Unbound or malformed verdicts cannot override a newer valid approval, but block if they are the latest relevant verdict. A later verdict explicitly naming a different head does not change the current head's decision.

The privileged workflow uses trusted base/default-branch code and GitHub API data only. A read-only [review-event relay](../../.github/workflows/independent-review-events.yml) wakes it for review changes, including fork PRs. Neither workflow checks out or executes PR code, installs dependencies, or consumes PR-produced artifacts. Gate jobs queue per PR and re-read before writing. Transient reads retry within a bound; ambiguous status writes are not retried.

The gate checks syntax and SHA binding, not reviewer independence, finding severity, model provenance, or the substance of a review. A self-authored approval could satisfy the machine and would violate this protocol. The CI service token publishes status only; it is not an independent reviewer.

The `independent-review-exempt` label makes the status green, but is permitted only after the explicit owner authorization above. Record the authorization on the PR and remove the label before any head outside that authorization is considered. The gate cannot authenticate that authorization and grants no standing exemption.

The workflow is not active from an unmerged branch. The trusted workflow and relay must first reach the base/default branch; existing PRs need a subsequent supported event to acquire a status. This port does not change repository rulesets. An owner must separately require `Independent Review` in branch protection/rulesets for a platform-enforced merge block. Until then it is a protocol requirement, not a configured GitHub merge restriction.

Events and status publication are asynchronous, and GitHub does not offer an atomic snapshot of PR head, verdicts, and status writes. Re-reads prevent observed stale decisions, not every possible concurrent mutation. API failures fail the job but may leave the previous commit status visible. Before merging, confirm the gate's latest relevant run completed, approval still names the current head, and CI is green; never rely on an old green badge alone.

## Reviewer handoff

Give a fresh reviewer the repository, PR number, pinned full head SHA, and linked specification, then ask:

> Read AGENTS.md and docs/agents/independent-review.md. You did not author this change. Review the complete pinned diff against the spec and every applicable review dimension. Keep findings verbatim with file/line and BLOCKING or ADVISORY severity. Test-strength findings need a demonstrated mutation proof. Recheck the PR head immediately before posting and use the protocol's exact closing block with actual host-known model identity or unknown. Report blockers to reviewability without a verdict. Do not edit the author's files, push fixes, merge, or issue your own authorship-based approval.
