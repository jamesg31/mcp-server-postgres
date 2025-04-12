#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

const databaseUrl = args[0];
const env = args[1];

const server = new Server(
  {
    name: `postgres/${env}`,
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  }
);

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

const pool = new pg.Pool({
  connectionString: databaseUrl,
  ssl: {
    rejectUnauthorized: false,
  },
});

const SCHEMA_PATH = "schema";
const ALL_SCHEMAS_PATH = "all-schemas";

interface TableRow {
  table_name: string;
}

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    );
    return {
      resources: [
        {
          uri: new URL(ALL_SCHEMAS_PATH, resourceBaseUrl).href,
          mimeType: "application/json",
          name: "All database schemas",
        },
        ...result.rows.map((row: TableRow) => ({
          uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl)
            .href,
          mimeType: "application/json",
          name: `"${row.table_name}" database schema`,
        })),
      ],
    };
  } finally {
    client.release();
  }
});

interface ResourceRequest {
  params: {
    uri: string;
  };
}

server.setRequestHandler(
  ReadResourceRequestSchema,
  async (request: ResourceRequest) => {
    const resourceUrl = new URL(request.params.uri);
    const pathComponents = resourceUrl.pathname.split("/");

    if (pathComponents[pathComponents.length - 1] === ALL_SCHEMAS_PATH) {
      const client = await pool.connect();
      try {
        const result = await client.query(
          `SELECT 
          t.table_name,
          json_agg(json_build_object(
            'column_name', c.column_name,
            'data_type', c.data_type
          )) as columns
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name
        WHERE t.table_schema = 'public'
        GROUP BY t.table_name`
        );

        return {
          contents: [
            {
              uri: request.params.uri,
              mimeType: "application/json",
              text: JSON.stringify(result.rows, null, 2),
            },
          ],
        };
      } finally {
        client.release();
      }
    }

    // Handle individual table schema case
    const schema = pathComponents.pop();
    const tableName = pathComponents.pop();

    if (schema !== SCHEMA_PATH) {
      throw new Error("Invalid resource URI");
    }

    const client = await pool.connect();
    try {
      const result = await client.query(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
        [tableName]
      );

      return {
        contents: [
          {
            uri: request.params.uri,
            mimeType: "application/json",
            text: JSON.stringify(result.rows, null, 2),
          },
        ],
      };
    } finally {
      client.release();
    }
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: `query-${env}`,
        description: `Run a read-only SQL query on the ${env} database.`,
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

interface ToolRequest {
  params: {
    name: string;
    arguments?: Record<string, unknown>;
  };
  method: string;
}

server.setRequestHandler(
  CallToolRequestSchema,
  async (request: ToolRequest) => {
    if (request.params.name === `query-${env}`) {
      const sql = request.params.arguments?.sql as string;

      const client = await pool.connect();
      try {
        await client.query("BEGIN TRANSACTION READ ONLY");
        const result = await client.query(sql);
        return {
          content: [
            { type: "text", text: JSON.stringify(result.rows, null, 2) },
          ],
          isError: false,
        };
      } catch (error) {
        throw error;
      } finally {
        client
          .query("ROLLBACK")
          .catch((error: Error) =>
            console.warn("Could not roll back transaction:", error)
          );

        client.release();
      }
    }
    throw new Error(`Unknown tool: ${request.params.name}`);
  }
);

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch(console.error);
