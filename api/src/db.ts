import pg from "pg";

const { Pool } = pg;

export const pool = new Pool({
  host: process.env.DB_HOST || "localhost",
  port: 5432,
  user: "devroom",
  password: "devroom123",
  database: "devroomdb",
});

pool.on("connect", () => {
  console.log("[DB] Connected to Postgres");
});

pool.on("error", (err) => {
  console.error("[DB] Pool error:", err);
});

// Test connection on startup
export async function testConnection() {
  try {
    const result = await pool.query("SELECT NOW()");
    console.log("[DB] Connection OK. Server time:", result.rows[0].now);
  } catch (err) {
    console.error("[DB] Connection failed:", err);
    throw err;
  }
}
