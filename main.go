package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

func main() {
	uri := flag.String("target_uri", "", "Target database URI to write data.")
	jsonFile := flag.String("json_file", "sample.json", "Path of the JSON file to read from.")
	level := flag.String("level", "info", "Log level to use from [ 'error', 'warn', 'info', 'debug' ].")
	flag.Parse()

	logCfg := log.Config{
		Format: "logfmt",
		Level:  *level,
	}
	if err := log.Init(logCfg); err != nil {
		panic(err)
	}

	pool := getPgxPool(uri, 1, 1)
	defer pool.Close()
	testConn(pool)

	file, err := os.Open(*jsonFile)
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to open file: %s", err.Error()))
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	txnCount := 0
	table_name := ""
	schema_name := ""
	column_values := [][]any{}
	row := []any{}
	txn_row_column := ""
	column_name := make(map[string][]string)
	refresh := func() {
		table_name = ""
		schema_name = ""
		column_values = column_values[:0]
		row = row[:0]
		txn_row_column = ""
		column_name = make(map[string][]string)
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		var stmt Stmt
		if err := json.Unmarshal(line, &stmt); err != nil {
			log.Error("msg", "error marshalling line", "line", string(line), "error", err.Error())
			os.Exit(1)
		}
		switch stmt.Action {
		case "B":
			refresh()
		case "I":
			schema_name = stmt.Message.Schema
			table_name = stmt.Message.Table
			row := make([]any, 0)
			row_column := []string{}
			for i := range stmt.Message.Columns {
				c := stmt.Message.Columns[i]
				row_column = append(row_column, c.Name)
				switch {
				case c.Type == "integer":
					v := c.Value.(float64)
					row = append(row, int32(v))
				case c.Type == "double precision":
					v := c.Value.(float64)
					row = append(row, v)
				case c.Type == "boolean":
					v := c.Value.(bool)
					row = append(row, v)
				case c.Type == "timestamp with time zone":
					v := c.Value.(string)
					var t pgtype.Timestamptz
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				case c.Type == "jsonb":
					v := c.Value.(string)
					var t pgtype.JSONB
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				case strings.HasPrefix(c.Type, "character varying"):
					v := c.Value.(string)
					var t pgtype.Varchar
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				case c.Type == "date":
					v := c.Value.(string)
					var t pgtype.Date
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				case c.Type == "text":
					v := c.Value.(string)
					var t pgtype.Text
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				case c.Type == "inet":
					v := c.Value.(string)
					var t pgtype.Inet
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				case c.Type == "iot_1.sensor_type":
					v := c.Value.(string)
					var t pgtype.GenericText
					if err := t.Scan(v); err != nil {
						panic(err)
					}
					row = append(row, t)
				}
			}
			column_values = append(column_values, row)
			txn_row_column = strings.Join(row_column, ",")
			column_name[txn_row_column] = row_column
		case "C":
			log.Debug("msg", "inserting", "table", fmt.Sprintf("%s.%s", schema_name, table_name), "column_names", txn_row_column, "num_rows", len(column_values))
			query := "insert into %s (%s) values "
			query = fmt.Sprintf(query, pgx.Identifier{schema_name, table_name}.Sanitize(), txn_row_column)

			// apply the rows
			counter := 1
			val := []interface{}{}
			for i := range column_values {
				query += " ( "
				for j := range column_values[i] {
					val = append(val, column_values[i][j])
					query += fmt.Sprintf("$%d", counter)
					counter++
					if j < len(column_values[i]) - 1 {
						query += ", "
					}
				}
				query += " ) "
				if i < len(column_values) - 1 {
					query += ","
				}
			}
			_, err := pool.Exec(context.Background(), query, val...)
			if err != nil {
				panic(err)
			}
			txnCount++
			log.Info("msg", "inserted rows", "txn_count", txnCount, "table", fmt.Sprintf("%s.%s", schema_name, table_name))
			refresh()
		case "K":
			// ignore
		}
	}
}

func getPgxPool(uri *string, min, max int32) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(*uri)
	if err != nil {
		log.Fatal("Error parsing config", err.Error())
	}
	cfg.MinConns = min
	cfg.MaxConns = max
	dbpool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Fatal("Unable to connect to database", err.Error())
	}
	return dbpool
}

func testConn(conn *pgxpool.Pool) bool {
	var t int
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&t); err != nil {
		panic(err)
	}
	log.Info("msg", "connected to the database")
	return true
}
