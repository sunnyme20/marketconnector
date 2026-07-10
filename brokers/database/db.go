package database

type Cfg struct {
	Driver string
}

type DB interface {
	GetMasterSymbolId()
	GetMasterSymbolName()
	GetBrokerSymbolId()
	GetBrokerSymbolName()
}

func NewDBConnection(cfg *Cfg) DB {
	switch cfg.Driver {
	case "postgres":
		return &PostgresRepo{}
	case "sqlite":
		return &SqliteRepo{}
	default:
		return nil
	}
}
