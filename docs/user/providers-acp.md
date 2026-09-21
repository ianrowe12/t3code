# ACP Registry

T3 Code can run coding agents from the official
[ACP Registry](https://agentclientprotocol.com/get-started/registry). Registry agents bring their
own models, tools, and sign-in, while T3 Code provides projects, threads, checkpoints, and task
delegation.

Google Antigravity is available through its official `antigravity-acp` Registry entry. Add it like
any other Registry agent; it does not need an Antigravity-specific T3 Code provider.

T3 Code prefers the current ACP v2 preview protocol and uses its richer messages, usage, plans,
configuration, compaction, and agent-terminal updates when the agent supports them. It also
negotiates ACP v1 for Registry agents that have not migrated yet, so agents such as Pi continue to
work through the same generic integration.

## Add an agent

1. Open **Settings → Providers**.
2. Select **Add provider instance**, then **ACP Registry**.
3. Search for the agent and select **Add** on its result.
4. Confirm the name and instance ID, then select **Add instance**.

Search only shows agents that can run on the connected server. Registry agents are third-party
code; review an agent's source and license before adding it.

## Where agents run

Registry agents always run on the machine that hosts your T3 Code server. That stays true when you
connect through `app.t3.codes`, T3 Connect, or a relay.

Binary agents download into a managed cache. T3 Code verifies SHA-256 when the Registry entry
provides one; entries without a checksum retain the Registry's HTTPS distribution guarantee.
Registry `npx` packages install globally through `npm`, and `uvx` packages install globally through
`uv tool`, at the exact version published by the Registry. Their normal CLI command is therefore
available in a new server terminal for sign-in and direct use. Removing an agent's last provider
instance removes T3-managed binary files but leaves globally installed package commands intact.

## Signing in

T3 Code never collects or stores credentials for registry agents. You sign in with the agent's own
method, on the server machine, under the account that runs T3 Code.

The provider card shows what the agent needs. For a browser flow, it displays the exact URL and
waits for you to select **Continue authentication** before telling the agent to proceed. T3 Code
does not open agent-provided URLs automatically. Other agents show a terminal command to run or
take API keys through the instance's environment settings. After you sign in, T3 Code picks it up
on the next automatic provider check. If the agent supports ACP logout, the expanded provider card
also offers **Log out** and stops that instance's active sessions before clearing its credentials.

For Codex, credentials belong to the Codex CLI on the server. Run `codex login status` to check
them, or `codex login --device-auth` to sign in with a ChatGPT subscription.

For Grok Build on a remote or headless server, run `grok login --device-auth`. See the
[Grok Build authentication guide](https://github.com/xai-org/grok-build/blob/main/crates/codegen/xai-grok-pager/docs/user-guide/02-authentication.md).

## Models and options

The model picker lists the models the agent reports. If the agent reports none, or hides some, add
your own model IDs under **Custom models** in the instance settings.

Other settings the agent offers, such as reasoning effort or approval mode, appear in the
composer's model options menu like they do for built-in providers. Agents with their own plan and
build modes follow T3 Code's Plan and Build toggle. Models and options that change while an agent
is running update the picker without waiting for another provider probe.

In this Copilot build, threads use the standard coding assistant and the custom Agent picker is
not shown. Resuming an older custom-agent session restores the standard assistant without
changing the thread's model, reasoning effort, permissions, or conversation. Installed skills,
custom-agent files, and built-in subagents remain available.

## Commands and skills

Slash commands the agent provides appear under **Provider** in the `/` menu while a session is
running. Commands the agent names with a `$` prefix appear in T3 Code's `$` skill menu instead.

## Native sessions

When an agent advertises ACP session listing and import support, expand its provider card,
choose a project, and select **List sessions**. Importing one creates a deterministic T3 thread
backed by that native session. Importing the same agent session again returns the existing thread,
including when another client performs the import.

The thread keeps the native session's title and last-update value as provider metadata without
overwriting a title you set in T3 Code. Agents that report ACP context usage drive the composer's
context meter and cumulative cost metadata. Text resources and links render as assistant output;
binary resources, images, and audio that T3 Code cannot render yet appear as explicit placeholders
instead of disappearing.

When Copilot finishes the main task with a summary but no later reply, that summary appears in
chat. Delegated-task summaries and other tool results stay in their tool entries.

If Copilot reports that its session host is unavailable, T3 shows the error with sensitive details
removed. Your next explicit retry starts a replacement Copilot process while keeping the saved
conversation. T3 does not automatically resend the failed request.

## Permissions and terminals

Registry agents follow the thread's approval mode at the T3 client boundary: full-access threads
approve mediated permission requests automatically, while approval-required threads keep asking.
For ACP v1 agents, T3 can mediate the file and terminal requests they send through the client. ACP
v2 terminals are instead owned by the agent; T3 displays their command, output, and exit state when
the agent publishes them, but does not execute or control those terminals.

ACP does not let T3 Code confine tools the agent executes inside its own process. An agent may run
provider-owned commands or file operations without passing through T3's handlers, so an ACP thread
does not provide the same native sandbox guarantee as Codex. Use the agent's own sandbox and
permission controls when that distinction matters.

Registry agents can schedule work and use T3's MCP tools. Child-task presentation depends on what
the agent exposes: ACP has no portable native subagent-lineage contract, so richer delegation views
remain agent-specific.

## Checkpoints

Checkpoint rollback restores your files as usual. ACP agents cannot rewind their own conversation,
so the next turn after a rollback starts a fresh agent session that no longer remembers the
conversation from before the checkpoint.

GitHub Copilot CLI supports app-owned text generation, including thread titles, commit messages,
branch names, and pull request descriptions. Select your Copilot instance under **Settings → Text
generation** on the environment running the thread. This is separate from the conversation model.
If the selected provider is disabled or unsupported, T3 chooses another enabled text-generation
provider, including Copilot when no built-in provider is enabled. A valid selection stays unchanged.

Use an authenticated Copilot CLI 1.0.83 or newer on that host, or set an authentication token on the
Copilot provider instance. The helper uses the instance's executable and authentication without
workspace access, tools, hooks, plugins, MCP servers, or profile model overrides. Its private session
files are removed afterward. **Default** uses the CLI's default model. Other ACP Registry agents do
not support these helpers. T3 does not switch providers when a generation request fails. Check the
selected host's executable and sign-in if titles are not generated.

## Advanced configuration

- **Executable override** runs an existing local executable instead of the managed distribution,
  keeping the registry-declared arguments and environment.
- **Authentication method** picks a specific method when the agent advertises more than one.
- **Custom models** adds model IDs the agent does not report.
