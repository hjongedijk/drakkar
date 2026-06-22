// compress-nzb is a one-off migration tool that compresses all uncompressed
// NZB XML rows in nzb_documents using zstd. Rows that are already compressed
// (detected by the zstd frame magic number) are skipped automatically.
// Run from inside the drakkar container where the "postgres" hostname resolves.
//
// Usage:
//
//	compress-nzb -dsn "postgres://drakkar:PASSWORD@postgres:5432/drakkar?sslmode=disable"
package main

import (
	"bytes"
	"database/sql"
	"flag"
	"log"
	"time"

	"github.com/klauspost/compress/zstd"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

func main() {
	dsn := flag.String("dsn", "", "PostgreSQL DSN (required)")
	batchSize := flag.Int("batch", 50, "rows per transaction")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("-dsn is required")
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		log.Fatalf("zstd encoder: %v", err)
	}

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	var total int
	if err := db.QueryRow(`SELECT count(*) FROM nzb_documents WHERE xml IS NOT NULL`).Scan(&total); err != nil {
		log.Fatalf("count: %v", err)
	}
	log.Printf("total rows with xml: %d", total)

	var (
		processed, compressed, skipped int
		lastID                         int64
		start                          = time.Now()
	)

	for {
		rows, err := db.Query(
			`SELECT id, xml FROM nzb_documents WHERE xml IS NOT NULL AND id > $1 ORDER BY id ASC LIMIT $2`,
			lastID, *batchSize,
		)
		if err != nil {
			log.Fatalf("query batch: %v", err)
		}

		type row struct {
			id  int64
			xml []byte
		}
		var batch []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.xml); err != nil {
				rows.Close()
				log.Fatalf("scan: %v", err)
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			log.Fatalf("rows err: %v", err)
		}

		if len(batch) == 0 {
			break
		}

		for _, r := range batch {
			lastID = r.id
			processed++

			if len(r.xml) >= 4 && bytes.Equal(r.xml[:4], zstdMagic) {
				skipped++
				continue
			}

			compressed_xml := enc.EncodeAll(r.xml, make([]byte, 0, len(r.xml)/4))

			if _, err := db.Exec(`UPDATE nzb_documents SET xml = $1 WHERE id = $2`, compressed_xml, r.id); err != nil {
				log.Fatalf("update id=%d: %v", r.id, err)
			}
			compressed++
		}

		if processed%500 == 0 || len(batch) < *batchSize {
			elapsed := time.Since(start)
			rate := float64(processed) / elapsed.Seconds()
			remaining := float64(total-processed) / rate
			log.Printf("progress: %d/%d rows — compressed=%d skipped=%d — %.0f rows/s — ~%.0fs remaining",
				processed, total, compressed, skipped, rate, remaining)
		}
	}

	log.Printf("done: %d rows processed, %d compressed, %d already compressed — took %s",
		processed, compressed, skipped, time.Since(start).Round(time.Second))

	// Print final size reduction
	var beforeBytes, afterBytes int64
	db.QueryRow(`SELECT coalesce(sum(length(xml)),0) FROM nzb_documents`).Scan(&afterBytes)
	_ = beforeBytes
	log.Printf("current xml column size: %.1f MB", float64(afterBytes)/1024/1024)
}
