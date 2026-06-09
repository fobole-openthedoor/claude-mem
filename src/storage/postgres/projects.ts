// SPDX-License-Identifier: Apache-2.0

import type { JsonObject, PostgresQueryable } from './utils.js';
import { newId, queryOne, toEpoch, toJsonObject } from './utils.js';

export interface PostgresProject {
  id: string;
  teamId: string;
  name: string;
  metadata: JsonObject;
  createdAtEpoch: number;
  updatedAtEpoch: number;
}

interface ProjectRow {
  id: string;
  team_id: string;
  name: string;
  metadata: unknown;
  created_at: Date;
  updated_at: Date;
}

export class PostgresProjectsRepository {
  constructor(private client: PostgresQueryable) {}

  async create(input: {
    id?: string;
    teamId: string;
    name: string;
    metadata?: JsonObject;
  }): Promise<PostgresProject> {
    const id = input.id ?? newId();
    const row = await queryOne<ProjectRow>(
      this.client,
      `
        INSERT INTO projects (id, team_id, name, metadata)
        VALUES ($1, $2, $3, $4::jsonb)
        RETURNING *
      `,
      [id, input.teamId, input.name, JSON.stringify(input.metadata ?? {})]
    );
    return mapProjectRow(row!);
  }

  async getByIdForTeam(id: string, teamId: string): Promise<PostgresProject | null> {
    const row = await queryOne<ProjectRow>(
      this.client,
      'SELECT * FROM projects WHERE id = $1 AND team_id = $2',
      [id, teamId]
    );
    return row ? mapProjectRow(row) : null;
  }

  async getByNameForTeam(name: string, teamId: string): Promise<PostgresProject | null> {
    const row = await queryOne<ProjectRow>(
      this.client,
      `
        SELECT *
        FROM projects
        WHERE name = $1 AND team_id = $2
        ORDER BY created_at ASC
        LIMIT 1
      `,
      [name, teamId]
    );
    return row ? mapProjectRow(row) : null;
  }

  async listByTeam(teamId: string): Promise<PostgresProject[]> {
    const result = await this.client.query<ProjectRow>(
      `
        SELECT *
        FROM projects
        WHERE team_id = $1
        ORDER BY name ASC, created_at ASC
      `,
      [teamId]
    );
    return result.rows.map(mapProjectRow);
  }
}

function mapProjectRow(row: ProjectRow): PostgresProject {
  return {
    id: row.id,
    teamId: row.team_id,
    name: row.name,
    metadata: toJsonObject(row.metadata),
    createdAtEpoch: toEpoch(row.created_at),
    updatedAtEpoch: toEpoch(row.updated_at)
  };
}
