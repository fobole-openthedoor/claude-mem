// IO discipline (see src/shared/hook-io.ts):
// - hookSpecificOutput.additionalContext → MODEL_CONTEXT (model consumes; via stdout JSON)
// - systemMessage                        → USER_HINT (user-visible; via stdout JSON systemMessage)
// This handler is PURE: it returns a HookResult and MUST NOT call
// process.stderr.write / process.stdout.write / console.* / process.exit.
// logger.* calls are DIAGNOSTIC and route through hook-io's stderr path.
import type { EventHandler, NormalizedHookInput, HookResult } from '../types.js';
import {
  executeWithWorkerFallback,
  isWorkerFallback,
  getWorkerPort,
} from '../../shared/worker-utils.js';
import { getProjectContext } from '../../utils/project-name.js';
import { HOOK_EXIT_CODES } from '../../shared/hook-constants.js';
import { logger } from '../../utils/logger.js';
import { loadFromFileOnce } from '../../shared/hook-settings.js';
import { readStaleMarker } from '../../shared/oauth-token.js';
import { resolveRuntimeContext, logServerBetaFallback } from '../../services/hooks/runtime-selector.js';

export const contextHandler: EventHandler = {
  async execute(input: NormalizedHookInput): Promise<HookResult> {
    const cwd = input.cwd ?? process.cwd();
    const projectContext = getProjectContext(cwd);
    const port = getWorkerPort();

    const settings = loadFromFileOnce();
    const showTerminalOutput = settings.CLAUDE_MEM_CONTEXT_SHOW_TERMINAL_OUTPUT === 'true';

    const runtime = resolveRuntimeContext();
    if (runtime.runtime === 'server-beta') {
      let additionalContext = '';
      try {
        additionalContext = await buildServerBetaContext(runtime, projectContext, cwd);
      } catch (error) {
        logServerBetaFallback('context_injection_failed', {
          error: error instanceof Error ? error.message : String(error),
        });
      }

      const staleReason = readStaleMarker();
      if (staleReason) {
        const hint = `[claude-mem] Claude Desktop OAuth token is stale: ${staleReason}\nPlease re-login via Claude Desktop to refresh the token.`;
        additionalContext = additionalContext
          ? `${hint}\n\n${additionalContext}`
          : hint;
      }

      const systemMessage = showTerminalOutput && additionalContext
        ? `${additionalContext}\n\nView Observations Live @ ${runtime.serverBaseUrl}`
        : undefined;

      return {
        hookSpecificOutput: {
          hookEventName: 'SessionStart',
          additionalContext,
        },
        systemMessage,
      };
    }

    const projectsParam = projectContext.allProjects.join(',');
    const apiPath = `/api/context/inject?projects=${encodeURIComponent(projectsParam)}`;
    const colorApiPath = input.platform === 'claude-code' ? `${apiPath}&colors=true` : apiPath;

    const emptyResult: HookResult = {
      hookSpecificOutput: { hookEventName: 'SessionStart', additionalContext: '' },
      exitCode: HOOK_EXIT_CODES.SUCCESS,
    };

    const contextResult = await executeWithWorkerFallback<string>(apiPath, 'GET');
    if (isWorkerFallback(contextResult)) {
      return emptyResult;
    }

    let additionalContext: string;
    if (typeof contextResult === 'string') {
      additionalContext = contextResult.trim();
    } else if (contextResult === undefined) {
      additionalContext = '';
    } else {
      logger.warn('HOOK', 'Context response was not a string', { type: typeof contextResult });
      return emptyResult;
    }

    // Issue #2215: surface stale OAuth token marker as a session-start hint.
    // Marker is written by EnvManager.buildIsolatedEnvWithFreshOAuth() when
    // a previous worker spawn detected an expired keychain entry.
    const staleReason = readStaleMarker();
    if (staleReason) {
      const hint = `[claude-mem] Claude Desktop OAuth token is stale: ${staleReason}\nPlease re-login via Claude Desktop to refresh the token.`;
      additionalContext = additionalContext
        ? `${hint}\n\n${additionalContext}`
        : hint;
    }

    let coloredTimeline = '';
    if (showTerminalOutput) {
      const colorResult = await executeWithWorkerFallback<string>(colorApiPath, 'GET');
      if (!isWorkerFallback(colorResult) && typeof colorResult === 'string') {
        coloredTimeline = colorResult.trim();
      }
    }

    const platform = input.platform;

    const displayContent = coloredTimeline || (platform === 'gemini-cli' || platform === 'gemini' ? additionalContext : '');

    const systemMessage = showTerminalOutput && displayContent
      ? `${displayContent}\n\nView Observations Live @ http://localhost:${port}`
      : undefined;

    return {
      hookSpecificOutput: {
        hookEventName: 'SessionStart',
        additionalContext
      },
      systemMessage
    };
  }
};

async function buildServerBetaContext(
  runtime: Extract<ReturnType<typeof resolveRuntimeContext>, { runtime: 'server-beta' }>,
  projectContext: ReturnType<typeof getProjectContext>,
  _cwd: string,
): Promise<string> {
  const url = new URL('/api/context/preview', runtime.serverBaseUrl);
  url.searchParams.set('projectId', runtime.projectId);
  const response = await fetch(url, {
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok) {
    throw new Error(`server-beta context preview failed with ${response.status}`);
  }
  const context = (await response.text()).trim();
  if (!context) return '';
  if (context.startsWith('No observations found for ')) return '';

  return [
    `# [${projectContext.primary}] recent context from claude-mem server-beta`,
    context,
  ].join('\n\n');
}
