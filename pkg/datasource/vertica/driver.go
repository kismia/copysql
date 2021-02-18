package vertica

import (
	"context"
	"database/sql"
	"encoding/csv"
	"io"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/kismia/copysql/pkg/datasource"

	"github.com/vertica/vertica-sql-go"
)

const driverName = "vertica"

func init() {
	datasource.Register(driverName, &detectorFactory{})
}

type detectorFactory struct{}

func (f *detectorFactory) Create(parameters map[string]interface{}) (datasource.Driver, error) {
	return FromParameters(parameters)
}

type DriverParameters struct {
	Address  string
	Username string
	Password string
	Database string
}

type Driver struct {
	dsn        string
	connection *sql.DB
}

func FromParameters(parameters map[string]interface{}) (datasource.Driver, error) {
	params := DriverParameters{}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}

	return New(params), nil
}

func New(params DriverParameters) *Driver {
	return &Driver{
		dsn: driverName + "://" + params.Username + ":" + params.Password + "@" + params.Address + "/" + params.Database + "?sslmode=disable",
	}
}

func (d *Driver) Open() (err error) {
	d.connection, err = sql.Open(driverName, d.dsn)

	if err != nil {
		return err
	}

	return d.connection.Ping()
}

func (d *Driver) CopyFrom(r io.Reader, table string) error {
	vCtx := vertigo.NewVerticaContext(context.Background())
	err := vCtx.SetCopyInputStream(r)
	if err != nil {
		return errors.Wrap(err, "cannot read data to insert")
	}

	_, err = d.connection.ExecContext(vCtx, "COPY "+table+" FROM STDIN DELIMITER ',' ABORT ON ERROR")

	return err
}

func (d *Driver) CopyTo(w io.Writer, query string) error {
	rows, err := d.connection.Query(query)
	if err != nil {
		return errors.Wrap(err, "cannot execute query")
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return errors.Wrap(err, "cannot read table columns from "+driverName)
	}

	readColumns := make([]interface{}, len(columns))
	writeColumns := make([]sql.NullString, len(columns))

	for i := range writeColumns {
		readColumns[i] = &writeColumns[i]
	}

	csvWriter := csv.NewWriter(w)

	record := make([]string, len(columns))

	for rows.Next() {
		if err := rows.Scan(readColumns...); err != nil {
			return errors.Wrap(err, "an error occurred while reading from "+driverName)
		}

		for i := range writeColumns {
			record[i] = writeColumns[i].String
		}

		err = csvWriter.Write(record)
		if err != nil {
			return errors.Wrap(err, "an error occurred while reading from "+driverName)
		}
	}

	csvWriter.Flush()

	return rows.Err()
}

func (d *Driver) Close() error {
	return d.connection.Close()
}
