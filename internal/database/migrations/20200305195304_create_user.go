package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200305195304, Down20200305195304)
}

func Up20200305195304(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE users (
		id CHAR(14) NOT NULL,
		first_name VARCHAR(255) NOT NULL,
		last_name VARCHAR(255) DEFAULT NULL, -- optional field
		status VARCHAR(80) NOT NULL,
		approved_at INT(11) DEFAULT NULL,
		created_at INT(11) NOT NULL,
		updated_at INT(11) NOT NULL,
		deleted_at INT(11) DEFAULT NULL,
		PRIMARY KEY (id),
		KEY users_status_index (status),
		KEY users_created_at_index (created_at),
		KEY users_deleted_at_index (deleted_at)
	);`)

	return err
}

func Down20200305195304(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS users`)
	return err
}
