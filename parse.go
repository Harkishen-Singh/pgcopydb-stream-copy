package main

type Column struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type Message struct {
	Action  string   `json:"action"`
	Xid     int      `json:"xid"`
	Schema  string   `json:"schema,omitempty"`
	Table   string   `json:"table,omitempty"`
	Columns []Column `json:"columns,omitempty"`
}

type Stmt struct {
	Action    string  `json:"action"`
	Xid       string  `json:"xid"`
	Lsn       string  `json:"lsn"`
	Timestamp string  `json:"timestamp"`
	Message   Message `json:"message"`
}
