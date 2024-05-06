const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

const defaultPageSize = 250;

// Helper function to calculate the offset based on page number and page size
const getOffset = (page, pageSize) => (page - 1) * pageSize;

// Define your API routes here
app.get("/api/time-series", (req, res) => {
  const {
    marketType,
    startDate,
    endDate,
    page = 1,
    pageSize = defaultPageSize,
    usageId,
  } = req.query;
  const offset = getOffset(page, pageSize);
  const query = `
    SELECT DATE_TRUNC('day', accepted_datetime_utc) AS date,
           SUM(book_risk_component) AS bet_handle
    FROM bet_transactions
    WHERE market_type = $1 AND accepted_datetime_utc BETWEEN $2 AND $3 AND client_id = $6
    GROUP BY DATE_TRUNC('day', accepted_datetime_utc)
    ORDER BY date
    LIMIT $4 OFFSET $5;
  `;
  const values = [marketType, startDate, endDate, pageSize, offset];

  pool.query(query, values, (err, result) => {
    if (err) {
      console.error("Error executing query:", err);
      res.status(500).json({ error: "Internal server error" });
    } else {
      res.json(result.rows);
    }
  });
});

app.get("/api/sports", (req, res) => {
  const { page = 1, pageSize = defaultPageSize } = req.query;
  const offset = getOffset(page, pageSize);
  const query = `
    SELECT DISTINCT sport
    FROM bet_transactions
    WHERE usage_id = $3
    LIMIT $1 OFFSET $2;
  `;
  const values = [pageSize, offset];

  pool.query(query, values, (err, result) => {
    if (err) {
      console.error("Error executing sports query:", err);
      res.status(500).json({ error: "Internal server error" });
    } else {
      const sports = result.rows.map((row) => row.sport);
      res.json(sports);
    }
  });
});

app.get("/api/stat-types", (req, res) => {
  const { page = 1, pageSize = defaultPageSize } = req.query;
  const offset = getOffset(page, pageSize);
  const query = `
    SELECT DISTINCT stat_type
    FROM bet_transactions
    WHERE usage_id = $3
    LIMIT $1 OFFSET $2;
  `;
  const values = [pageSize, offset];

  pool.query(query, values, (err, result) => {
    if (err) {
      console.error("Error executing stat types query:", err);
      res.status(500).json({ error: "Internal server error" });
    } else {
      const statTypes = result.rows.map((row) => row.stat_type);
      res.json(statTypes);
    }
  });
});

app.get("/api/dimensional-analysis", (req, res) => {
  const { dimension, page = 1, pageSize = defaultPageSize } = req.query;
  const offset = getOffset(page, pageSize);
  const query = `
    SELECT ${dimension}, SUM(book_risk_component) AS bet_handle
    FROM bet_transactions
    WHERE usage_id = $3
    GROUP BY ${dimension}
    ORDER BY bet_handle DESC
    LIMIT $1 OFFSET $2;
  `;
  const values = [pageSize, offset];

  pool.query(query, values, (err, result) => {
    if (err) {
      console.error("Error executing dimensional analysis query:", err);
      res.status(500).json({ error: "Internal server error" });
    } else {
      res.json(result.rows);
    }
  });
});

app.get("/api/sports", (req, res) => {
  const query = `
    SELECT sport_id, sport
    FROM bet_transactions
    GROUP BY sport_id, sport;
  `;

  pool.query(query, (err, result) => {
    if (err) {
      console.error("Error executing sports query:", err);
      res.status(500).json({ error: "Internal server error" });
    } else {
      const sports = result.rows.map((row) => ({
        sportId: row.sport_id,
        sportName: row.sport,
      }));
      res.json(sports);
    }
  });
});

// Implement other API routes similarly

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
