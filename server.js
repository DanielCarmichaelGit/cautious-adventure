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

// Define your API routes here
app.get("/api/time-series", authenticatePassword, (req, res) => {
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

app.get("/api/stat-types", authenticatePassword, (req, res) => {
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

app.get("/api/dimensional-analysis", authenticatePassword, (req, res) => {
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

app.get("/api/sports", authenticatePassword, (req, res) => {
  const query = `
    SELECT sport_id, sport
    FROM bet_transactions
    GROUP BY sport_id, sport;
  `;

  pool
    .query(query)
    .then((result) => {
      const sports = result.rows.map((row) => ({
        sportId: row.sport_id,
        sportName: row.sport,
      }));
      res.json(sports);
    })
    .catch((err) => {
      console.error("Error executing sports query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/sports-bets-overview", authenticatePassword, (req, res) => {
  const query = `
    SELECT 
      sport_id, 
      sport,
      COUNT(*) AS total_bets,
      SUM(book_risk) AS total_book_risk,
      SUM(book_profit_gross) AS total_book_profit_gross
    FROM 
      bet_transactions
    GROUP BY 
      sport_id, 
      sport;
  `;

  pool
    .query(query)
    .then((result) => {
      const sportsBets = result.rows.map((row) => ({
        sportId: row.sport_id,
        sportName: row.sport,
        totalBets: row.total_bets,
        totalBookRisk: row.total_book_risk,
        totalBookProfitGross: row.total_book_profit_gross,
      }));
      res.json(sportsBets);
    })
    .catch((err) => {
      console.error("Error executing sports bets overview query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/bets-by-datetime-range", authenticatePassword, (req, res) => {
  const { startDate, endDate, startTime, endTime } = req.query;

  let query = `
    SELECT 
      bet_id_swish AS bet_id,
      sport,
      event_id,
      selection,
      bet_price,
      accepted_datetime_utc
    FROM 
      bet_transactions
  `;
  const params = [];

  if (startDate && endDate && startTime && endTime) {
    query += `
      WHERE 
        accepted_datetime_utc >= $1 AND accepted_datetime_utc <= $2
    `;
    params.push(`${startDate} ${startTime}`);
    params.push(`${endDate} ${endTime}`);
  } else if (startDate && endDate) {
    query += `
      WHERE 
        date >= $1 AND date <= $2
    `;
    params.push(startDate);
    params.push(endDate);
  }

  pool
    .query(query, params)
    .then((result) => {
      const bets = result.rows.map((row) => ({
        betId: row.bet_id,
        sportName: row.sport,
        eventId: row.event_id,
        selection: row.selection,
        betPrice: row.bet_price,
        acceptedDatetime: row.accepted_datetime_utc,
      }));
      res.json(bets);
    })
    .catch((err) => {
      console.error("Error executing bets by datetime range query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/top-winning-players", authenticatePassword, (req, res) => {
  const query = `
    SELECT 
      player_id,
      player_name,
      COUNT(*) AS total_bets,
      SUM(book_profit_gross) AS total_profit
    FROM 
      bet_transactions
    GROUP BY 
      player_id, 
      player_name
    ORDER BY 
      total_profit DESC
    LIMIT 10;
  `;

  pool
    .query(query)
    .then((result) => {
      const topPlayers = result.rows.map((row) => ({
        playerId: row.player_id,
        playerName: row.player_name,
        totalBets: row.total_bets,
        totalProfit: row.total_profit,
      }));
      res.json(topPlayers);
    })
    .catch((err) => {
      console.error("Error executing top winning players query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/bet-type-distribution", authenticatePassword, (req, res) => {
  const query = `
    SELECT 
      bet_type_id,
      bet_type,
      COUNT(*) AS count
    FROM 
      bet_transactions
    GROUP BY 
      bet_type_id, 
      bet_type;
  `;

  pool
    .query(query)
    .then((result) => {
      const betTypeCounts = result.rows.map((row) => ({
        betTypeId: row.bet_type_id,
        betType: row.bet_type,
        count: row.count,
      }));
      res.json(betTypeCounts);
    })
    .catch((err) => {
      console.error("Error executing bet type distribution query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/graph-options", authenticatePassword, (req, res) => {
  const query = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'bet_transactions'
  `;

  pool
    .query(query)
    .then((result) => {
      const columns = result.rows.map((row) => row.column_name);
      const yOptions = columns.filter(
        (column) => column !== "date" && column !== "datetime_utc"
      );
      const xOptions = columns.filter(
        (column) => column !== "date" && column !== "datetime_utc"
      );
      res.json({ yOptions, xOptions });
    })
    .catch((err) => {
      console.error("Error executing graph options query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/custom-graph", authenticatePassword, (req, res) => {
  const { yColumn, xColumns, startDate, startTime, endDate, endTime } =
    req.query;
  if (!yColumn || !xColumns) {
    return res.status(400).json({ error: "Missing required parameters" });
  }
  const xColumnsArray = xColumns.split(",");
  const selectColumns = [yColumn, ...xColumnsArray];

  let query = `
    SELECT 
      ${selectColumns
        .map((column, index) => `"${column}" AS "${column}"`)
        .join(", ")}
    FROM 
      bet_transactions
  `;

  const params = [];

  if (startDate && startTime && endDate && endTime) {
    query += `
      WHERE accepted_datetime_utc >= $1 AND accepted_datetime_utc <= $2
    `;
    params.push(`${startDate} ${startTime}`);
    params.push(`${endDate} ${endTime}`);
  } else if (startDate && startTime) {
    query += `
      WHERE accepted_datetime_utc >= $1
    `;
    params.push(`${startDate} ${startTime}`);
  }

  query += `
    LIMIT 250
  `;

  pool
    .query(query, params)
    .then((result) => {
      const data = result.rows.map((row) => {
        const dataPoint = {};
        selectColumns.forEach((column) => {
          dataPoint[column] = row[column];
        });
        return dataPoint;
      });
      res.json(data);
    })
    .catch((err) => {
      console.error("Error executing custom graph query:", err);
      res.status(500).json({ error: "Internal server error" });
    });
});

app.get("/api/custom-graph-paginated", authenticatePassword, (req, res) => {
  const { yColumn, xColumns, startDate, startTime, endDate, endTime, page = 1, pageSize = 250 } = req.query;
  
  if (!yColumn || !xColumns) {
    return res.status(400).json({ error: "Missing required parameters" });
  }

  const xColumnsArray = xColumns.split(",");
  const selectColumns = [yColumn, ...xColumnsArray];

  let query = `
    SELECT ${selectColumns.map((column, index) => `"${column}" AS "${column}"`).join(", ")}
    FROM bet_transactions
  `;
  const params = [];

  if (startDate && startTime && endDate && endTime) {
    query += ` WHERE accepted_datetime_utc >= $1 AND accepted_datetime_utc <= $2`;
    params.push(`${startDate} ${startTime}`);
    params.push(`${endDate} ${endTime}`);
  } else if (startDate && startTime) {
    query += ` WHERE accepted_datetime_utc >= $1`;
    params.push(`${startDate} ${startTime}`);
  }

  const offset = (page - 1) * pageSize;
  query += ` LIMIT ${pageSize} OFFSET ${offset}`;

  pool
    .query(query, params)
    .then((result) => {
      const data = result.rows.map((row) => {
        const dataPoint = {};
        selectColumns.forEach((column) => {
          dataPoint[column] = row[column];
        });
        return dataPoint;
      });

      const countQuery = `
        SELECT COUNT(*) AS total
        FROM bet_transactions
      `;
      pool.query(countQuery).then((countResult) => {
        const totalCount = countResult.rows[0].total;
        const totalPages = Math.ceil(totalCount / pageSize);

        res.json({
          data,
          currentPage: page,
          totalPages: totalPages,
        });
      });
    })
    .catch((err) => {
      console.error("Error executing custom graph query:", err);
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
