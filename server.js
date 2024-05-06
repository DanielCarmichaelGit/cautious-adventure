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
  } = req.query;
  const offset = getOffset(page, pageSize);
  const query = `
    SELECT DATE_TRUNC('day', accepted_datetime_utc) AS date,
           SUM(book_risk_component) AS bet_handle
    FROM bet_transactions
    WHERE market_type = $1 AND accepted_datetime_utc BETWEEN $2 AND $3
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

app.get("/api/client-id", (req, res) => {
  const query = `
    SELECT client_id
    FROM bet_transactions
    ORDER BY RANDOM()
    LIMIT 1;
  `;

  pool.query(query, (err, result) => {
    if (err) {
      console.error("Error executing client ID query:", err);
      res.status(500).json({ error: "Internal server error" });
    } else {
      if (result.rows.length > 0) {
        const clientId = result.rows[0].client_id;
        res.json({ clientId });
      } else {
        res.status(404).json({ error: "No client IDs found" });
      }
    }
  });
});

// Implement other API routes similarly

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
