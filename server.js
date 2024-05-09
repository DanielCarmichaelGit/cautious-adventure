const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

const authenticatePassword = (req, res, next) => {
  let providedPassword = req.header("Authorization");

  if (providedPassword === process.env.PASSWORD) {
    next();
  } else {
    res.status(401).json({ error: "Invalid password" });
  }
};

const defaultPageSize = 250;

// Helper function to calculate the offset based on page number and page size
const getOffset = (page, pageSize) => (page - 1) * pageSize;

app.get("/api/clients", authenticatePassword, (req, res) => {
  const query = `
    SELECT DISTINCT client_id, client_name
    FROM bet_transactions
    WHERE client_id IS NOT NULL
    ORDER BY client_name
    LIMIT 100;
  `;

  pool
    .query(query)
    .then((result) => {
      const clients = result.rows.map((row) => ({
        clientId: row.client_id,
        clientName: row.client_name,
      }));
      res.status(200).json(clients);
    })
    .catch((err) => {
      console.error("Error executing clients query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get(
  "/api/inplay-vs-pregame-performance",
  authenticatePassword,
  (req, res) => {
    const query = `
    SELECT
      CASE WHEN is_inplay = 1 THEN 'In-Play' ELSE 'Pre-Game' END AS bet_timing,
      COUNT(*) AS total_bets,
      COUNT(CASE WHEN book_profit_gross > 0 THEN 1 END) * 100.0 / COUNT(*) AS win_rate,
      AVG(bet_price) AS avg_odds,
      SUM(book_profit_gross) * 100.0 / SUM(book_risk) AS roi
    FROM bet_transactions
    GROUP BY bet_timing
    LIMIT 250;
  `;

    pool
      .query(query)
      .then((result) => {
        const betTimingPerformance = result.rows;
        res.json(betTimingPerformance);
      })
      .catch((err) => {
        console.error(
          "Error executing in-play vs pre-game performance query:",
          err
        );
        res.status(500).json({ error: "Internal server error" });
      });
  }
);

app.get("/api/inplay-vs-pregame-odds", authenticatePassword, (req, res) => {
  const query = `
    SELECT 
      CASE WHEN is_inplay = 1 THEN 'In-Play' ELSE 'Pre-Game' END AS bet_timing,
      ROUND(AVG(bet_price), 2) AS avg_odds
    FROM 
      bet_transactions
    GROUP BY 
      is_inplay
    LIMIT 250;
  `;

  pool
    .query(query)
    .then((result) => {
      const betTimingOdds = result.rows;
      res.json(betTimingOdds);
    })
    .catch((err) => {
      console.error("Error executing in-play vs pre-game odds query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/bet-success-rate-by-team", authenticatePassword, (req, res) => {
  const query = `
    SELECT 
      team_abbr,
      COUNT(*) AS total_bets,
      COUNT(CASE WHEN book_profit_gross > 0 THEN 1 END) AS successful_bets,
      COUNT(CASE WHEN book_profit_gross > 0 THEN 1 END) * 100.0 / COUNT(*) AS success_rate
    FROM bet_transactions
    GROUP BY team_abbr
    ORDER BY success_rate DESC;
  `;

  pool
    .query(query)
    .then((result) => {
      const teamSuccessRates = result.rows;
      res.json(teamSuccessRates);
    })
    .catch((err) => {
      console.error("Error executing bet success rate by team query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get(
  "/api/bet-performance-by-line-movement",
  authenticatePassword,
  (req, res) => {
    const query = `
    SELECT
      CASE
        WHEN line_diff_at_bet < -5 THEN 'Large Decrease'
        WHEN line_diff_at_bet >= -5 AND line_diff_at_bet < -1 THEN 'Small Decrease'
        WHEN line_diff_at_bet >= -1 AND line_diff_at_bet <= 1 THEN 'No Significant Change'
        WHEN line_diff_at_bet > 1 AND line_diff_at_bet <= 5 THEN 'Small Increase'
        ELSE 'Large Increase'
      END AS line_movement,
      COUNT(*) AS total_bets,
      COUNT(CASE WHEN book_profit_gross > 0 THEN 1 END) * 100.0 / COUNT(*) AS win_rate,
      AVG(book_profit_gross) AS avg_profit
    FROM bet_transactions
    GROUP BY line_movement;
  `;

    pool
      .query(query)
      .then((result) => {
        const lineMovementPerformance = result.rows;
        res.json(lineMovementPerformance);
      })
      .catch((err) => {
        console.error(
          "Error executing bet performance by line movement query:",
          err
        );
        res.status(500).json({ error: "Internal server error" });
      });
  }
);

app.get(
  "/api/bet-profitability-by-stat-type",
  authenticatePassword,
  (req, res) => {
    const query = `
    SELECT
      stat_type,
      SUM(book_risk) AS total_handle,
      SUM(book_profit_gross) AS total_profit,
      SUM(book_profit_gross) * 100.0 / SUM(book_risk) AS roi
    FROM bet_transactions
    GROUP BY stat_type
    ORDER BY roi DESC;
  `;

    pool
      .query(query)
      .then((result) => {
        const statTypeProfitability = result.rows;
        res.json(statTypeProfitability);
      })
      .catch((err) => {
        console.error(
          "Error executing bet profitability by stat type query:",
          err
        );
        res.status(500).json({ error: "Internal server error" });
      });
  }
);

app.get("/api/profitability-by-position", authenticatePassword, (req, res) => {
  const query = `
    SELECT 
      pos_abbr AS position,
      SUM(book_risk) AS total_handle,
      SUM(book_profit_gross) AS total_profit,
      SUM(book_profit_gross) * 100.0 / SUM(book_risk) AS roi
    FROM bet_transactions
    GROUP BY pos_abbr
    ORDER BY roi DESC;
  `;

  pool
    .query(query)
    .then((result) => {
      const profitabilityByPosition = result.rows;
      res.json(profitabilityByPosition);
    })
    .catch((err) => {
      console.error("Error executing profitability by position query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.post("/api/authenticate", (req, res) => {
  console.log(req.body);
  const password = req.body.password;
  console.log(req.body); // Log the entire request body

  if (password && typeof password === "string") {
    let validation = password === process.env.PASSWORD;
    if (validation) {
      res.status(202).json({ message: "authorized" });
    } else {
      res.status(403).json({ message: "unauthorized" });
    }
  } else {
    res.status(403).json({ message: "unauthorized" });
  }
});

// Implement other API routes similarly

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
