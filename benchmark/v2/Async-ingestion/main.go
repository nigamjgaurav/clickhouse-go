package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// /home/gaurav.nigam/workspace/gnGit/clickhouse-go/context.go

// func GetNativeConnection(settings clickhouse.Settings, tlsConfig *tls.Config,
// 	compression *clickhouse.Compression) (driver.Conn, error) {

// 	return conn, err

// }

// func Context(parent context.Context, options ...QueryOption) context.Context {
// 	opt := queryOptions(parent)
// 	for _, f := range options {
// 		f(&opt)
// 	}
// 	return context.WithValue(parent, _contextOptionKey, opt)
// }

func BatchInsert() error {

	// conn, err := GetNativeConnection(nil, nil, nil)
	port := 30934
	host := "10.15.162.151"

	// port := 9000
	// host := "127.0.0.1"

	settingVal := clickhouse.Settings{}
	settingVal["async_insert_busy_timeout_ms"] = 1_000
	settingVal["wait_for_async_insert"] = 1

	// async_insert=1,
	// wait_for_async_insert=1,
	// async_insert_busy_timeout_ms=30000,
	// async_insert_max_data_size=10000000,
	// async_insert_max_query_number=250,

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		return err
	}

	ctx := context.Background()

	// defer func() {
	// 	conn.Exec(ctx, "DROP TABLE default.example")
	// }()
	// conn.Exec(ctx, `DROP TABLE IF EXISTS default.example`)
	// const ddl = `
	// 	CREATE TABLE example (
	// 		  Col1 UInt64
	// 		, Col2 String
	// 		, Col3 Array(UInt8)
	// 		, Col4 DateTime
	// 	) ENGINE = Memory
	// `

	// if err := conn.Exec(ctx, ddl); err != nil {
	// 	return err
	// }
	for i := 0; i < 2; i++ {
		// if i%50000 == 0 {
		fmt.Printf("Inserting row# %d \n", i)
		// }
		fmt.Printf("%d, %v\n", i, time.Now())
		if err := conn.AsyncInsert(ctx, `INSERT INTO example VALUES (
			?, ?, ?, now()
		)`, true, i, "Golang SQL database driver", []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}); err != nil {
			return err
		}

		fmt.Printf("%d, %v\n", i, time.Now())
	}

	// for i := 0; i < 1000000; i++ {
	// 	var valueStrings []string

	// 	// Convert each value to its string representation
	// 	for j := 0; j < i; j++ {
	// 		valueStrings = append(valueStrings, fmt.Sprintf("%v", j))
	// 	}

	// 	// Join the values with commas and wrap them in parentheses
	// 	values = append(values, "("+strings.Join(valueStrings, ", ")+")")
	// }

	// // Join the values with commas and create the final SQL statement
	// sqlStatement := fmt.Sprintf("INSERT INTO %s (*) VALUES %s;", dbTableName, strings.Join(values, ", "))
	// // err := p.conn.AsyncInsert(ctx, sqlStatement, false)
	// fmt.Printf("Sql : %s\n", sqlStatement)
	// if err := nativeCon.AsyncInsert(ctx, sqlStatement, false, nil); err != nil {
	// 	fmt.Println(err)
	// }

	return nil
}

func main() {
	fmt.Println("Async insert starting...")
	BatchInsert()
	fmt.Println("Async insert completed...")
}
