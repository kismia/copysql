package clickhouse

import (
	"encoding/csv"
	"io"

	"github.com/kismia/go-clickhouse"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/kismia/copysql/pkg/datasource"
)

const driverName = "clickhouse"

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
	cluster *clickhouse.Cluster
}

func FromParameters(parameters map[string]interface{}) (datasource.Driver, error) {
	params := DriverParameters{}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}

	return New(params), nil
}

func New(params DriverParameters) *Driver {
	dsn := "http://" + params.Username + ":" + params.Password + "@" + params.Address

	return &Driver{
		cluster: clickhouse.NewCluster(clickhouse.NewConn(dsn, clickhouse.NewHttpTransport(32))),
	}
}

func (d *Driver) Open() error {
	d.cluster.Check()

	if d.cluster.IsDown() {
		return errors.New("all clickhouse hosts down")
	}

	return nil
}

func (d *Driver) CopyFrom(r io.Reader, table string) error {
	query := clickhouse.BuildCSVInsert(table, r)
	return query.Exec(d.cluster.ActiveConn())
}

func (d *Driver) CopyTo(w io.Writer, query string) error {
	q := clickhouse.NewQuery(query)
	iter := q.Iter(d.cluster.ActiveConn())
	columns := iter.Columns()
	readColumns := make([]interface{}, len(columns))
	writeColumns := make([]string, len(columns))

	for i := range writeColumns {
		readColumns[i] = &writeColumns[i]
	}

	csvWriter := csv.NewWriter(w)

	for iter.Scan(readColumns...) {
		err := csvWriter.Write(writeColumns)
		if err != nil {
			return errors.Wrap(err, "an error occurred while reading from "+driverName)
		}
	}

	csvWriter.Flush()

	return iter.Error()
}

func (d *Driver) Close() error {
	return nil
}
