// SPDX-License-Identifier: Apache-2.0
//
// Viewer compatibility API for the server-beta runtime.
//
// The React viewer still speaks the legacy worker `/api/*` shape while
// server-beta stores canonical data in Postgres and exposes `/v1/*`. These
// routes adapt the viewer's read-only/feed endpoints to Postgres so the latest
// server-beta runtime can serve the bundled viewer from the same origin.

import type { Application, Request, Response } from 'express';
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'fs';
import path from 'path';
import type { RouteHandler } from '../../services/server/Server.js';
import { getPackageRoot, paths } from '../../shared/paths.js';
import { SettingsDefaultsManager } from '../../shared/SettingsDefaultsManager.js';
import { normalizePlatformSource, sortPlatformSources } from '../../shared/platform-source.js';
import { getUptimeSeconds } from '../../shared/uptime.js';
import { logger } from '../../utils/logger.js';
import type { ServerBetaServiceGraph } from './types.js';
import { ActiveServerBetaQueueManager } from './ActiveServerBetaQueueManager.js';

const SETTINGS_WRITE_ALLOWLIST = [
  'CLAUDE_MEM_MODEL',
  'CLAUDE_MEM_CONTEXT_OBSERVATIONS',
  'CLAUDE_MEM_WORKER_PORT',
  'CLAUDE_MEM_WORKER_HOST',
  'CLAUDE_MEM_PROVIDER',
  'CLAUDE_MEM_CONTEXT_SHOW_READ_TOKENS',
  'CLAUDE_MEM_CONTEXT_SHOW_WORK_TOKENS',
  'CLAUDE_MEM_CONTEXT_SHOW_SAVINGS_AMOUNT',
  'CLAUDE_MEM_CONTEXT_SHOW_SAVINGS_PERCENT',
  'CLAUDE_MEM_CONTEXT_FULL_COUNT',
  'CLAUDE_MEM_CONTEXT_FULL_FIELD',
  'CLAUDE_MEM_CONTEXT_SESSION_COUNT',
  'CLAUDE_MEM_CONTEXT_SHOW_LAST_SUMMARY',
  'CLAUDE_MEM_CONTEXT_SHOW_LAST_MESSAGE',
] as const;

const SECRET_SETTING_KEYS = [
  'CLAUDE_MEM_GEMINI_API_KEY',
  'CLAUDE_MEM_OPENROUTER_API_KEY',
  'CLAUDE_MEM_SERVER_BETA_API_KEY',
  'ANTHROPIC_API_KEY',
  'OPENROUTER_API_KEY',
] as const;

const STREAM_HEARTBEAT_INTERVAL_MS = 25_000;
const STREAM_POLL_INTERVAL_MS = 2_000;
const STREAM_OBSERVATION_BATCH_LIMIT = 100;

const ONBOARDING_EXPLAINER_CANDIDATES: readonly string[] = (() => {
  const packageRoot = getPackageRoot();
  return [
    path.join(packageRoot, 'skills', 'how-it-works', 'onboarding-explainer.md'),
    path.join(packageRoot, 'plugin', 'skills', 'how-it-works', 'onboarding-explainer.md'),
    path.join(packageRoot, 'skills', 'how-it-works', 'SKILL.md'),
    path.join(packageRoot, 'plugin', 'skills', 'how-it-works', 'SKILL.md'),
  ];
})();

const onboardingExplainer: string | null = (() => {
  const candidate = ONBOARDING_EXPLAINER_CANDIDATES.find(file => existsSync(file));
  if (!candidate) return null;
  try {
    return readFileSync(candidate, 'utf-8');
  } catch {
    return null;
  }
})();

interface ProjectSourceRow {
  project: string;
  source: string | null;
}

interface ObservationFeedRow {
  id: string;
  project_id: string;
  memory_session_id: string | null;
  project: string;
  platform_source: string | null;
  kind: string;
  content: string;
  metadata: unknown;
  created_at: Date;
  created_at_epoch: string | number;
}

interface ObservationCursor {
  createdAtEpoch: string;
  id: string;
}

interface PromptFeedRow {
  id: string;
  content_session_id: string | null;
  project: string;
  platform_source: string | null;
  payload: unknown;
  occurred_at_epoch: string | number;
}

type JsonRecord = Record<string, unknown>;

export class ServerBetaViewerApiRoutes implements RouteHandler {
  private readonly startTime = Date.now();

  constructor(private readonly graph: ServerBetaServiceGraph) {}

  setupRoutes(app: Application): void {
    app.get('/stream', this.asyncHandler(this.handleStream.bind(this)));
    app.get('/api/projects', this.asyncHandler(this.handleProjects.bind(this)));
    app.get('/api/observations', this.asyncHandler(this.handleObservations.bind(this)));
    app.get('/api/summaries', this.handleEmptyPage);
    app.get('/api/prompts', this.asyncHandler(this.handlePrompts.bind(this)));
    app.get('/api/stats', this.asyncHandler(this.handleStats.bind(this)));
    app.get('/api/settings', this.handleGetSettings.bind(this));
    app.post('/api/settings', this.handleUpdateSettings.bind(this));
    app.get('/api/processing-status', this.asyncHandler(this.handleProcessingStatus.bind(this)));
    app.post('/api/processing', this.asyncHandler(this.handleProcessingStatus.bind(this)));
    app.get('/api/context/preview', this.asyncHandler(this.handleContextPreview.bind(this)));
    app.get('/api/logs', this.handleGetLogs.bind(this));
    app.post('/api/logs/clear', this.handleClearLogs.bind(this));
    app.get('/api/onboarding/explainer', this.handleOnboardingExplainer.bind(this));
  }

  private async handleStream(_req: Request, res: Response): Promise<void> {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    let isClosed = false;
    let isPolling = false;
    let cursor = await this.getLatestObservationCursor();
    const catalog = await this.getProjectCatalog();
    const sendEvent = (event: JsonRecord): void => {
      if (isClosed || res.writableEnded) return;
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    };
    const sendComment = (comment: string): void => {
      if (isClosed || res.writableEnded) return;
      res.write(`: ${comment}\n\n`);
    };

    sendEvent({
      type: 'initial_load',
      projects: catalog.projects,
      sources: catalog.sources,
      projectsBySource: catalog.projectsBySource,
      timestamp: Date.now(),
    });

    let status = await this.getProcessingStatus();
    sendEvent({
      type: 'processing_status',
      isProcessing: status.isProcessing,
      queueDepth: status.queueDepth,
    });

    const poll = async (): Promise<void> => {
      if (isClosed || isPolling) return;
      isPolling = true;
      try {
        const rows = await this.getObservationsAfterCursor(cursor, STREAM_OBSERVATION_BATCH_LIMIT);
        for (const row of rows) {
          sendEvent({
            type: 'new_observation',
            observation: serializeObservation(row),
            timestamp: Date.now(),
          });
          cursor = { createdAtEpoch: String(row.created_at_epoch), id: row.id };
        }

        const nextStatus = await this.getProcessingStatus();
        if (nextStatus.isProcessing !== status.isProcessing || nextStatus.queueDepth !== status.queueDepth) {
          status = nextStatus;
          sendEvent({
            type: 'processing_status',
            isProcessing: status.isProcessing,
            queueDepth: status.queueDepth,
          });
        }
      } catch (error) {
        logger.warn('HTTP', 'Failed to poll server-beta observations for viewer stream', {
          error: error instanceof Error ? error.message : String(error),
        });
        sendComment(`poll_error ${Date.now()}`);
      } finally {
        isPolling = false;
      }
    };

    const heartbeat = setInterval(() => {
      sendComment(`heartbeat ${Date.now()}`);
    }, STREAM_HEARTBEAT_INTERVAL_MS);
    const polling = setInterval(() => {
      void poll();
    }, STREAM_POLL_INTERVAL_MS);

    res.on('close', () => {
      isClosed = true;
      clearInterval(heartbeat);
      clearInterval(polling);
    });
  }

  private async handleProjects(req: Request, res: Response): Promise<void> {
    const rawSource = typeof req.query.platformSource === 'string' ? req.query.platformSource : undefined;
    const catalog = await this.getProjectCatalog(rawSource);
    res.json(catalog);
  }

  private async handleObservations(req: Request, res: Response): Promise<void> {
    const { offset, limit, project, platformSource } = this.parsePageQuery(req);
    const rows = await this.graph.postgres.pool.query<ObservationFeedRow>(
      `
        WITH observation_feed AS (
          SELECT
            o.id,
            o.project_id,
            o.server_session_id AS memory_session_id,
            p.name AS project,
            COALESCE(
              NULLIF(source_event.platform_source, ''),
              NULLIF(source_event.payload->>'platformSource', ''),
              NULLIF(ss.platform_source, ''),
              'claude'
            ) AS platform_source,
            o.kind,
            o.content,
            o.metadata,
            o.created_at,
            EXTRACT(EPOCH FROM o.created_at) * 1000 AS created_at_epoch
          FROM observations o
          INNER JOIN projects p ON p.id = o.project_id
          LEFT JOIN server_sessions ss ON ss.id = o.server_session_id
          LEFT JOIN LATERAL (
            SELECT ae.platform_source, ae.payload
            FROM observation_sources os
            INNER JOIN agent_events ae ON ae.id = os.source_id
            WHERE os.observation_id = o.id
              AND os.source_type = 'agent_event'
            ORDER BY ae.occurred_at DESC
            LIMIT 1
          ) source_event ON true
        )
        SELECT *
        FROM observation_feed
        WHERE ($1::text IS NULL OR project = $1)
          AND ($2::text IS NULL OR lower(platform_source) LIKE '%' || lower($2) || '%')
        ORDER BY created_at DESC
        LIMIT $3 OFFSET $4
      `,
      [project ?? null, platformSource ?? null, limit + 1, offset],
    );

    const items = rows.rows.slice(0, limit).map(row => serializeObservation(row));
    res.json({ items, hasMore: rows.rows.length > limit });
  }

  private async handlePrompts(req: Request, res: Response): Promise<void> {
    const { offset, limit, project, platformSource } = this.parsePageQuery(req);
    const rows = await this.graph.postgres.pool.query<PromptFeedRow>(
      `
        WITH prompt_feed AS (
          SELECT
            ae.id,
            COALESCE(ss.content_session_id, ae.payload->>'contentSessionId') AS content_session_id,
            p.name AS project,
            COALESCE(
              NULLIF(ae.platform_source, ''),
              NULLIF(ae.payload->>'platformSource', ''),
              NULLIF(ss.platform_source, ''),
              'claude'
            ) AS platform_source,
            ae.payload,
            EXTRACT(EPOCH FROM ae.occurred_at) * 1000 AS occurred_at_epoch
          FROM agent_events ae
          INNER JOIN projects p ON p.id = ae.project_id
          LEFT JOIN server_sessions ss ON ss.id = ae.server_session_id
          WHERE ae.event_type IN ('UserPromptSubmit', 'user_prompt', 'session_init')
            AND (
              ae.payload ? 'prompt'
              OR ae.payload ? 'promptText'
              OR ae.payload ? 'prompt_text'
              OR ae.payload ? 'message'
            )
        )
        SELECT *
        FROM prompt_feed
        WHERE ($1::text IS NULL OR project = $1)
          AND ($2::text IS NULL OR lower(platform_source) LIKE '%' || lower($2) || '%')
        ORDER BY occurred_at_epoch DESC
        LIMIT $3 OFFSET $4
      `,
      [project ?? null, platformSource ?? null, limit + 1, offset],
    );

    const items = rows.rows.slice(0, limit).map((row, index) => serializePrompt(row, offset + index + 1));
    res.json({ items, hasMore: rows.rows.length > limit });
  }

  private handleEmptyPage(_req: Request, res: Response): void {
    res.json({ items: [], hasMore: false });
  }

  private async handleStats(_req: Request, res: Response): Promise<void> {
    const packageJsonPath = path.join(getPackageRoot(), 'package.json');
    const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf-8')) as { version?: string };
    const [observationCount, sessionCount, firstObservation] = await Promise.all([
      this.graph.postgres.pool.query<{ count: string }>('SELECT COUNT(*)::text AS count FROM observations'),
      this.graph.postgres.pool.query<{ count: string }>('SELECT COUNT(*)::text AS count FROM server_sessions'),
      this.graph.postgres.pool.query<{ first_observation_at: Date | null }>(
        'SELECT MIN(created_at) AS first_observation_at FROM observations',
      ),
    ]);

    res.json({
      worker: {
        version: packageJson.version ?? 'development',
        uptime: getUptimeSeconds(this.startTime),
        activeSessions: 0,
        sseClients: 0,
        port: Number(process.env.CLAUDE_MEM_WORKER_PORT ?? process.env.CLAUDE_MEM_SERVER_PORT ?? 37877),
      },
      database: {
        path: 'postgres',
        size: 0,
        observations: Number(observationCount.rows[0]?.count ?? 0),
        sessions: Number(sessionCount.rows[0]?.count ?? 0),
        summaries: 0,
        firstObservationAt: firstObservation.rows[0]?.first_observation_at?.toISOString() ?? null,
      },
    });
  }

  private handleGetSettings(_req: Request, res: Response): void {
    const settings = SettingsDefaultsManager.loadFromFile(paths.settings()) as unknown as JsonRecord;
    const sanitized: JsonRecord = { ...settings };
    for (const key of SECRET_SETTING_KEYS) {
      if (sanitized[key] !== undefined) sanitized[key] = '';
    }
    res.json(sanitized);
  }

  private handleUpdateSettings(req: Request, res: Response): void {
    const settingsPath = paths.settings();
    const dir = path.dirname(settingsPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    let existing: JsonRecord = {};
    if (existsSync(settingsPath)) {
      try {
        existing = JSON.parse(readFileSync(settingsPath, 'utf-8')) as JsonRecord;
      } catch {
        res.status(500).json({ success: false, error: `Settings file is corrupted: ${settingsPath}` });
        return;
      }
    }
    const flat = existing.env && isRecord(existing.env) ? existing.env : existing;

    const body = isRecord(req.body) ? req.body : {};
    for (const key of SETTINGS_WRITE_ALLOWLIST) {
      if (body[key] !== undefined) {
        flat[key] = String(body[key]);
      }
    }

    writeFileSync(settingsPath, JSON.stringify(flat, null, 2), 'utf-8');
    res.json({ success: true, message: 'Settings updated successfully' });
  }

  private async handleProcessingStatus(_req: Request, res: Response): Promise<void> {
    res.json(await this.getProcessingStatus());
  }

  private async handleContextPreview(req: Request, res: Response): Promise<void> {
    const project = typeof req.query.project === 'string' ? req.query.project : null;
    const projectId = typeof req.query.projectId === 'string' ? req.query.projectId : null;
    if (!project && !projectId) {
      res.status(400).send('Project or projectId parameter is required');
      return;
    }
    const rawPlatformSource = typeof req.query.platformSource === 'string'
      ? normalizePlatformSource(req.query.platformSource)
      : null;

    const rows = await this.graph.postgres.pool.query<ObservationFeedRow>(
      `
        WITH observation_feed AS (
          SELECT
            o.id,
            o.project_id,
            o.server_session_id AS memory_session_id,
            p.name AS project,
            COALESCE(
              NULLIF(source_event.platform_source, ''),
              NULLIF(source_event.payload->>'platformSource', ''),
              NULLIF(ss.platform_source, ''),
              'claude'
            ) AS platform_source,
            o.kind,
            o.content,
            o.metadata,
            o.created_at,
            EXTRACT(EPOCH FROM o.created_at) * 1000 AS created_at_epoch
          FROM observations o
          INNER JOIN projects p ON p.id = o.project_id
          LEFT JOIN server_sessions ss ON ss.id = o.server_session_id
          LEFT JOIN LATERAL (
            SELECT ae.platform_source, ae.payload
            FROM observation_sources os
            INNER JOIN agent_events ae ON ae.id = os.source_id
            WHERE os.observation_id = o.id
              AND os.source_type = 'agent_event'
            ORDER BY ae.occurred_at DESC
            LIMIT 1
          ) source_event ON true
        )
        SELECT *
        FROM observation_feed
        WHERE ($1::text IS NULL OR project = $1)
          AND ($2::text IS NULL OR project_id = $2)
          AND ($3::text IS NULL OR lower(platform_source) LIKE '%' || lower($3) || '%')
        ORDER BY created_at DESC
        LIMIT 20
      `,
      [project, projectId, rawPlatformSource],
    );

    const lines = rows.rows.map(row => {
      const obs = serializeObservation(row);
      const parts = [
        `## ${obs.title ?? obs.type}`,
        obs.subtitle ?? '',
        obs.narrative ?? obs.text ?? '',
      ].filter(Boolean);
      return parts.join('\n\n');
    });

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(lines.length > 0 ? lines.join('\n\n---\n\n') : `No observations found for ${project ?? projectId}`);
  }

  private handleGetLogs(req: Request, res: Response): void {
    const logFilePath = this.getLogFilePath();
    if (!existsSync(logFilePath)) {
      res.json({ logs: '', path: logFilePath, exists: false });
      return;
    }

    const requestedLines = parseInt(String(req.query.lines ?? '1000'), 10);
    const maxLines = Math.min(Number.isFinite(requestedLines) ? requestedLines : 1000, 10_000);
    const { lines, totalEstimate } = readLastLines(logFilePath, maxLines);
    res.json({
      logs: lines,
      path: logFilePath,
      exists: true,
      totalLines: totalEstimate,
      returnedLines: lines ? lines.split('\n').length : 0,
    });
  }

  private handleClearLogs(_req: Request, res: Response): void {
    const logFilePath = this.getLogFilePath();
    if (!existsSync(logFilePath)) {
      res.json({ success: true, message: 'Log file does not exist', path: logFilePath });
      return;
    }
    writeFileSync(logFilePath, '', 'utf-8');
    res.json({ success: true, message: 'Log file cleared', path: logFilePath });
  }

  private handleOnboardingExplainer(_req: Request, res: Response): void {
    if (!onboardingExplainer) {
      res.status(404).json({ error: 'NotFound', message: 'Onboarding explainer not found' });
      return;
    }
    res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
    res.send(onboardingExplainer);
  }

  private async getProjectCatalog(sourceFilter?: string): Promise<{
    projects: string[];
    sources: string[];
    projectsBySource: Record<string, string[]>;
  }> {
    const result = await this.graph.postgres.pool.query<ProjectSourceRow>(
      `
        WITH project_sources AS (
          SELECT p.name AS project, NULL::text AS source
          FROM projects p
          UNION
          SELECT p.name AS project, ss.platform_source AS source
          FROM projects p
          INNER JOIN server_sessions ss ON ss.project_id = p.id
          UNION
          SELECT p.name AS project, COALESCE(ae.platform_source, ae.payload->>'platformSource') AS source
          FROM projects p
          INNER JOIN agent_events ae ON ae.project_id = p.id
          UNION
          SELECT p.name AS project, COALESCE(ae.platform_source, ae.payload->>'platformSource', ss.platform_source) AS source
          FROM projects p
          INNER JOIN observations o ON o.project_id = p.id
          LEFT JOIN server_sessions ss ON ss.id = o.server_session_id
          LEFT JOIN observation_sources os ON os.observation_id = o.id AND os.source_type = 'agent_event'
          LEFT JOIN agent_events ae ON ae.id = os.source_id
        )
        SELECT project, source
        FROM project_sources
        ORDER BY project ASC
      `,
    );

    const projects = new Set<string>();
    const projectsBySource: Record<string, string[]> = {};
    const normalizedFilter = sourceFilter ? normalizePlatformSource(sourceFilter) : null;

    for (const row of result.rows) {
      const project = row.project;
      if (!project) continue;
      const source = normalizePlatformSource(row.source);
      if (normalizedFilter && source !== normalizedFilter) continue;
      projects.add(project);
      if (!projectsBySource[source]) {
        projectsBySource[source] = [];
      }
      if (!projectsBySource[source].includes(project)) {
        projectsBySource[source].push(project);
      }
    }

    const sources = sortPlatformSources(Object.keys(projectsBySource));
    return {
      projects: Array.from(projects).sort((a, b) => a.localeCompare(b)),
      sources,
      projectsBySource: Object.fromEntries(
        sources.map(source => [source, projectsBySource[source]!.sort((a, b) => a.localeCompare(b))]),
      ),
    };
  }

  private async getProcessingStatus(): Promise<{ isProcessing: boolean; queueDepth: number }> {
    if (!(this.graph.queueManager instanceof ActiveServerBetaQueueManager)) {
      return { isProcessing: false, queueDepth: 0 };
    }
    try {
      const lanes = await this.graph.queueManager.getLaneMetrics();
      const queueDepth = lanes.reduce((sum, lane) => sum + lane.waiting + lane.active + lane.delayed, 0);
      const isProcessing = lanes.some(lane => lane.active > 0 || lane.waiting > 0 || lane.delayed > 0);
      return { isProcessing, queueDepth };
    } catch (error) {
      logger.warn('HTTP', 'Failed to read server-beta queue status for viewer', {
        error: error instanceof Error ? error.message : String(error),
      });
      return { isProcessing: false, queueDepth: 0 };
    }
  }

  private async getLatestObservationCursor(): Promise<ObservationCursor | null> {
    const result = await this.graph.postgres.pool.query<{ id: string; created_at_epoch: string | number }>(
      `
        SELECT id, EXTRACT(EPOCH FROM created_at) * 1000 AS created_at_epoch
        FROM observations
        ORDER BY created_at DESC, id DESC
        LIMIT 1
      `,
    );
    const row = result.rows[0];
    return row ? { createdAtEpoch: String(row.created_at_epoch), id: row.id } : null;
  }

  private async getObservationsAfterCursor(
    cursor: ObservationCursor | null,
    limit: number,
  ): Promise<ObservationFeedRow[]> {
    const rows = await this.graph.postgres.pool.query<ObservationFeedRow>(
      `
        WITH observation_feed AS (
          SELECT
            o.id,
            o.project_id,
            o.server_session_id AS memory_session_id,
            p.name AS project,
            COALESCE(
              NULLIF(source_event.platform_source, ''),
              NULLIF(source_event.payload->>'platformSource', ''),
              NULLIF(ss.platform_source, ''),
              'claude'
            ) AS platform_source,
            o.kind,
            o.content,
            o.metadata,
            o.created_at,
            EXTRACT(EPOCH FROM o.created_at) * 1000 AS created_at_epoch
          FROM observations o
          INNER JOIN projects p ON p.id = o.project_id
          LEFT JOIN server_sessions ss ON ss.id = o.server_session_id
          LEFT JOIN LATERAL (
            SELECT ae.platform_source, ae.payload
            FROM observation_sources os
            INNER JOIN agent_events ae ON ae.id = os.source_id
            WHERE os.observation_id = o.id
              AND os.source_type = 'agent_event'
            ORDER BY ae.occurred_at DESC
            LIMIT 1
          ) source_event ON true
        )
        SELECT *
        FROM observation_feed
        WHERE (
          $1::numeric IS NULL
          OR created_at_epoch > $1::numeric
          OR (created_at_epoch = $1::numeric AND id > $2::text)
        )
        ORDER BY created_at ASC, id ASC
        LIMIT $3
      `,
      [cursor?.createdAtEpoch ?? null, cursor?.id ?? null, limit],
    );
    return rows.rows;
  }

  private parsePageQuery(req: Request): {
    offset: number;
    limit: number;
    project?: string;
    platformSource?: string;
  } {
    const offset = clampPositiveInteger(req.query.offset, 0, 1_000_000);
    const limit = clampPositiveInteger(req.query.limit, 20, 100);
    const project = typeof req.query.project === 'string' && req.query.project.trim()
      ? req.query.project.trim()
      : undefined;
    const platformSource = typeof req.query.platformSource === 'string' && req.query.platformSource.trim()
      ? normalizePlatformSource(req.query.platformSource)
      : undefined;
    return { offset, limit, project, platformSource };
  }

  private getLogFilePath(): string {
    const logsDir = paths.logsDir();
    const date = new Date().toISOString().split('T')[0];
    return path.join(logsDir, `claude-mem-${date}.log`);
  }

  private asyncHandler(
    handler: (req: Request, res: Response) => Promise<void>,
  ): (req: Request, res: Response) => void {
    return (req, res) => {
      handler(req, res).catch(error => {
        logger.error('HTTP', 'Server beta viewer API request failed', {
          path: req.path,
          error: error instanceof Error ? error.message : String(error),
        }, error instanceof Error ? error : new Error(String(error)));
        if (!res.headersSent) {
          res.status(500).json({ error: 'InternalServerError', message: 'Viewer API request failed' });
        }
      });
    };
  }
}

function serializeObservation(row: ObservationFeedRow): JsonRecord {
  const metadata = isRecord(row.metadata) ? row.metadata : {};
  const createdAtEpoch = Number(row.created_at_epoch);
  const narrative = stringFrom(metadata.narrative) ?? row.content;
  const source = normalizePlatformSource(row.platform_source);

  return {
    id: row.id,
    memory_session_id: row.memory_session_id ?? '',
    project: row.project,
    merged_into_project: null,
    platform_source: source,
    type: row.kind,
    title: stringFrom(metadata.title) ?? firstLine(row.content),
    subtitle: stringFrom(metadata.subtitle),
    narrative,
    text: row.content,
    facts: jsonArrayString(metadata.facts),
    concepts: jsonArrayString(metadata.concepts),
    files_read: jsonArrayString(metadata.files_read),
    files_modified: jsonArrayString(metadata.files_modified),
    prompt_number: numberFrom(metadata.prompt_number),
    created_at: row.created_at.toISOString(),
    created_at_epoch: Number.isFinite(createdAtEpoch) ? createdAtEpoch : row.created_at.getTime(),
  };
}

function serializePrompt(row: PromptFeedRow, fallbackPromptNumber: number): JsonRecord {
  const payload = isRecord(row.payload) ? row.payload : {};
  const promptText = stringFrom(payload.prompt)
    ?? stringFrom(payload.promptText)
    ?? stringFrom(payload.prompt_text)
    ?? stringFrom(payload.message)
    ?? '';
  const createdAtEpoch = Number(row.occurred_at_epoch);

  return {
    id: row.id,
    content_session_id: row.content_session_id ?? '',
    project: row.project,
    platform_source: normalizePlatformSource(row.platform_source),
    prompt_number: numberFrom(payload.promptNumber) ?? numberFrom(payload.prompt_number) ?? fallbackPromptNumber,
    prompt_text: promptText,
    created_at_epoch: Number.isFinite(createdAtEpoch) ? createdAtEpoch : Date.now(),
  };
}

function clampPositiveInteger(value: unknown, fallback: number, max: number): number {
  const parsed = parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return Math.min(parsed, max);
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stringFrom(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

function numberFrom(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function jsonArrayString(value: unknown): string {
  if (Array.isArray(value)) return JSON.stringify(value);
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return JSON.stringify(Array.isArray(parsed) ? parsed : [value]);
    } catch {
      return JSON.stringify([value]);
    }
  }
  return '[]';
}

function firstLine(value: string): string | null {
  const line = value.split(/\r?\n/, 1)[0]?.trim();
  return line || null;
}

function readLastLines(filePath: string, lineCount: number): { lines: string; totalEstimate: number } {
  const stat = statSync(filePath);
  if (stat.size === 0) {
    return { lines: '', totalEstimate: 0 };
  }

  const bytesToRead = Math.min(stat.size, 1024 * 1024);
  const fd = readFileSync(filePath);
  const content = fd.subarray(Math.max(0, fd.byteLength - bytesToRead)).toString('utf-8');
  const lines = content.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') {
    lines.pop();
  }
  const selected = lines.slice(Math.max(0, lines.length - lineCount));
  return {
    lines: selected.join('\n'),
    totalEstimate: lines.length,
  };
}
