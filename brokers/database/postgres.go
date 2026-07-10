package database

type PostgresRepo struct {
	Host string
}

func NewPostgresConn() *PostgresRepo {
	return &PostgresRepo{
		Host: "200",
	}
}

func (p *PostgresRepo) GetMasterSymbolId()   {}
func (p *PostgresRepo) GetMasterSymbolName() {}
func (p *PostgresRepo) GetBrokerSymbolId()   {}
func (p *PostgresRepo) GetBrokerSymbolName() {}
