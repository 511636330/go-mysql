package mysql

import (
	"fmt"

	config "github.com/511636330/go-conf"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Model interface {
	GetConnection() string
}

var clients = make(map[string]*gorm.DB)

func GetClient(model Model) *gorm.DB {
	conn := model.GetConnection()
	if client, ok := clients[conn]; ok && client != nil {
		return client
	}
	return Connnect(model)
}

func Connnect(model Model) *gorm.DB {
	conn := model.GetConnection()
	dsn := GetMysqlDSN(conn)
	logMode := logger.Default.LogMode(logger.Error)
	logLevel := config.GetString(fmt.Sprintf("database.mysql.%s.log", conn), "silent")
	switch logLevel {
	case "silent":
		logMode = logger.Default.LogMode(logger.Silent)
	case "info":
		logMode = logger.Default.LogMode(logger.Info)
	case "error":
		logMode = logger.Default.LogMode(logger.Silent)
	case "warn":
		logMode = logger.Default.LogMode(logger.Warn)
	}
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       dsn,
		DefaultStringSize:         256,
		DisableDatetimePrecision:  true,
		DontSupportRenameIndex:    true,
		DontSupportRenameColumn:   true,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		Logger: logMode,
	})

	if err != nil {
		return nil
	}
	maxIdleConnections := config.GetInt(fmt.Sprintf("database.mysql.%s.max_idle_connections", conn), 100)
	maxOpenConnections := config.GetInt(fmt.Sprintf("database.mysql.%s.max_open_connections", conn), 25)
	maxIdleTime := config.GetDuration(fmt.Sprintf("database.mysql.%s.max_idle_seconds", conn), 3600)
	maxLifeTime := config.GetDuration(fmt.Sprintf("database.mysql.%s.max_life_seconds", conn), 24*3600)

	sqlDB, err := db.DB()

	if err != nil {
		return nil
	}

	sqlDB.SetConnMaxLifetime(maxLifeTime)
	sqlDB.SetConnMaxIdleTime(maxIdleTime)
	sqlDB.SetMaxIdleConns(maxIdleConnections)
	sqlDB.SetMaxOpenConns(maxOpenConnections)

	clients[conn] = db

	return db
}

func GetMysqlDSN(conn string) (dsn string) {
	username := config.GetString(fmt.Sprintf("database.mysql.%s.username", conn))
	password := config.GetString(fmt.Sprintf("database.mysql.%s.password", conn))
	host := config.GetString(fmt.Sprintf("database.mysql.%s.host", conn))
	port := config.GetString(fmt.Sprintf("database.mysql.%s.port", conn))
	database := config.GetString(fmt.Sprintf("database.mysql.%s.database", conn))
	charset := config.GetString(fmt.Sprintf("database.mysql.%s.charset", conn))
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True&loc=Local", username, password, host, port, database, charset)
	return
}
