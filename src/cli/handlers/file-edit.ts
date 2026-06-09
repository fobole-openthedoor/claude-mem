
import type { EventHandler, NormalizedHookInput, HookResult } from '../types.js';
import { executeWithWorkerFallback, isWorkerFallback } from '../../shared/worker-utils.js';
import { logger } from '../../utils/logger.js';
import { HOOK_EXIT_CODES } from '../../shared/hook-constants.js';
import { normalizePlatformSource } from '../../shared/platform-source.js';
import { shouldTrackProject } from '../../shared/should-track-project.js';
import { getProjectContext } from '../../utils/project-name.js';
import { resolveRuntimeContext, logServerBetaFallback, resolveServerBetaProjectId } from '../../services/hooks/runtime-selector.js';
import { isServerBetaClientError } from '../../services/hooks/server-beta-client.js';

export const fileEditHandler: EventHandler = {
  async execute(input: NormalizedHookInput): Promise<HookResult> {
    const { sessionId, cwd, filePath, edits } = input;
    const platformSource = normalizePlatformSource(input.platform);

    if (!filePath) {
      throw new Error('fileEditHandler requires filePath');
    }

    logger.dataIn('HOOK', `FileEdit: ${filePath}`, {
      editCount: edits?.length ?? 0
    });

    if (!cwd) {
      throw new Error(`Missing cwd in FileEdit hook input for session ${sessionId}, file ${filePath}`);
    }

    if (!shouldTrackProject(cwd)) {
      logger.debug('HOOK', 'Project excluded from tracking, skipping file edit observation', { cwd, filePath });
      return { continue: true, suppressOutput: true, exitCode: HOOK_EXIT_CODES.SUCCESS };
    }

    const runtime = resolveRuntimeContext();
    if (runtime.runtime === 'server-beta') {
      try {
        const projectContext = getProjectContext(cwd);
        const projectId = await resolveServerBetaProjectId(runtime, {
          projectName: projectContext.primary,
          rootPath: cwd,
          metadata: { projectContext },
        });
        await runtime.client.recordEvent({
          projectId,
          contentSessionId: sessionId,
          sourceType: 'hook',
          eventType: 'tool_use',
          occurredAtEpoch: Date.now(),
          payload: {
            tool_name: 'write_file',
            tool_input: { filePath, edits },
            tool_response: { success: true },
            cwd,
            platformSource,
          },
        });
        logger.debug('HOOK', 'File edit observation sent successfully via server-beta', { filePath });
        return { continue: true, suppressOutput: true };
      } catch (error: unknown) {
        if (isServerBetaClientError(error) && error.isFallbackEligible()) {
          logServerBetaFallback(error.kind, { status: error.status, message: error.message, route: '/v1/events' });
          // fall through to worker fallback
        } else {
          logger.error('HOOK', 'Server beta file edit event failed (non-recoverable)', {
            error: error instanceof Error ? error.message : String(error),
          });
          return { continue: true, suppressOutput: true, exitCode: HOOK_EXIT_CODES.SUCCESS };
        }
      }
    }

    const result = await executeWithWorkerFallback<{ status?: string }>(
      '/api/sessions/observations',
      'POST',
      {
        contentSessionId: sessionId,
        platformSource,
        tool_name: 'write_file',
        tool_input: { filePath, edits },
        tool_response: { success: true },
        cwd,
      },
    );

    if (isWorkerFallback(result)) {
      return { continue: true, suppressOutput: true, exitCode: HOOK_EXIT_CODES.SUCCESS };
    }

    logger.debug('HOOK', 'File edit observation sent successfully', { filePath });
    return { continue: true, suppressOutput: true };
  },
};
