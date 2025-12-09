package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tryy3/cozytemp-collect-data-service/config"
	"github.com/tryy3/cozytemp-service-helper/kafka"
	"github.com/tryy3/cozytemp-service-helper/models"
)

func main() {
	slog.SetDefault(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	)
	cfg := config.Load()

	slog.Info("Starting CozyTemp Collect Data Service")
	slog.Info("Kafka Brokers", "brokers", cfg.KafkaBrokers)
	slog.Info("Consumer Topic", "topic", cfg.KafkaConsumerTopic)
	slog.Info("Producer Topic", "topic", cfg.KafkaProducerTopic)
	slog.Info("Consumer Group", "group", cfg.KafkaConsumerGroup)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerTopic, cfg.KafkaConsumerGroup)
	defer consumer.Close()

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaProducerTopic)
	defer producer.Close()

	for {
		msg, err := consumer.FetchMessage(context.Background())
		if err != nil {
			slog.Error("Error reading message", "error", err)
			continue
		}

		var rawData models.RawDataEvent
		err = json.Unmarshal(msg.Value, &rawData)
		if err != nil {
			slog.Error("Error unmarshalling message", "error", err)
			continue
		}
		slog.Info("Raw Data", "data", rawData)

		calibrationData, err := collectData(cfg, rawData)
		if err != nil {
			slog.Error("Error collecting data", "error", err)
			continue
		}
		slog.Info("Calibration Data", "data", calibrationData)

		jsonData, err := json.Marshal(calibrationData)
		if err != nil {
			slog.Error("Error marshalling calibration data", "error", err)
			continue
		}
		err = producer.WriteMessage(context.Background(), []byte(calibrationData.NodeIdentifier), jsonData)
		if err != nil {
			slog.Error("Error producing message", "error", err)
			continue
		}
		slog.Info("Message produced successfully")

		err = consumer.CommitMessages(context.Background(), msg)
		if err != nil {
			slog.Error("Error committing message", "error", err)
			continue
		}
		slog.Info("Message committed successfully")
	}
}

func collectData(cfg *config.Config, rawData models.RawDataEvent) (*models.BeforeCalibrationDataEvent, error) {
	db, err := connectToPostgres(cfg)
	if err != nil {
		slog.Error("Error connecting to Postgres", "error", err)
		return nil, fmt.Errorf("error connecting to Postgres: %w", err)
	}
	defer db.Close()

	tx, err := db.Begin(context.Background())
	if err != nil {
		slog.Error("Error beginning transaction", "error", err)
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}
	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer tx.Rollback(context.Background())

	nodeID, err := findOrCreateNode(tx, rawData.NodeIdentifier)
	if err != nil {
		slog.Error("Error finding or creating node", "error", err)
		return nil, fmt.Errorf("error finding or creating node: %w", err)
	}
	if nodeID == uuid.Nil {
		slog.Error("Node ID is nil")
		return nil, fmt.Errorf("node ID is nil")
	}
	slog.Info("Node ID", "id", nodeID)

	sensorID, err := findOrCreateSensor(tx, nodeID, rawData.SensorIdentifier)
	if err != nil {
		slog.Error("Error finding or creating sensor", "error", err)
		return nil, fmt.Errorf("error finding or creating sensor: %w", err)
	}
	if sensorID == uuid.Nil {
		slog.Error("Sensor ID is nil")
		return nil, fmt.Errorf("sensor ID is nil")
	}
	slog.Info("Sensor ID", "id", sensorID)

	rawDataID, err := insertRawData(tx, sensorID, rawData.Temperature)
	if err != nil {
		slog.Error("Error inserting raw data", "error", err)
		return nil, fmt.Errorf("error inserting raw data: %w", err)
	}
	if rawDataID == uuid.Nil {
		slog.Error("Raw Data ID is nil")
		return nil, fmt.Errorf("raw Data ID is nil")
	}
	slog.Info("Raw Data ID", "id", rawDataID)

	err = tx.Commit(context.Background())
	if err != nil {
		slog.Error("Error committing transaction", "error", err)
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}
	return &models.BeforeCalibrationDataEvent{
		NodeIdentifier:   rawData.NodeIdentifier,
		NodeID:           nodeID,
		SensorIdentifier: rawData.SensorIdentifier,
		SensorID:         sensorID,
		RawDataID:        rawDataID,
		RawTemperature:   rawData.Temperature,
	}, nil
}

func connectToPostgres(cfg *config.Config) (*pgxpool.Pool, error) {
	dbconfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing Postgres URL: %w", err)
	}
	dbconfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxuuid.Register(conn.TypeMap())
		return nil
	}

	db, err := pgxpool.NewWithConfig(context.Background(), dbconfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Postgres: %w", err)
	}
	if err := db.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("error pinging Postgres: %w", err)
	}
	return db, nil
}

func findOrCreateNode(tx pgx.Tx, nodeIdentifier string) (uuid.UUID, error) {
	var nodeID uuid.UUID
	err := tx.QueryRow(context.Background(), "SELECT id FROM nodes WHERE identifier = $1", nodeIdentifier).Scan(&nodeID)
	if err == pgx.ErrNoRows {
		err = tx.QueryRow(context.Background(), "INSERT INTO nodes (identifier) VALUES ($1) RETURNING id", nodeIdentifier).Scan(&nodeID)
		if err != nil {
			return uuid.Nil, fmt.Errorf("error creating node: %w", err)
		}
		return nodeID, nil
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("error finding node: %w", err)
	}
	return nodeID, nil
}

func findOrCreateSensor(tx pgx.Tx, nodeID uuid.UUID, sensorIdentifier string) (uuid.UUID, error) {
	var sensorID uuid.UUID
	err := tx.QueryRow(context.Background(), "SELECT id FROM sensors WHERE \"parentNodeId\" = $1 AND identifier = $2", nodeID, sensorIdentifier).Scan(&sensorID)
	if err == pgx.ErrNoRows {
		err = tx.QueryRow(context.Background(), "INSERT INTO sensors (\"parentNodeId\", identifier) VALUES ($1, $2) RETURNING id", nodeID, sensorIdentifier).Scan(&sensorID)
		if err != nil {
			return uuid.Nil, fmt.Errorf("error creating sensor: %w", err)
		}
		return sensorID, nil
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("error finding sensor: %w", err)
	}
	return sensorID, nil
}

func insertRawData(tx pgx.Tx, sensorID uuid.UUID, temperature float64) (uuid.UUID, error) {
	var rawDataID uuid.UUID
	err := tx.QueryRow(context.Background(), "INSERT INTO raw_data (\"sensorId\", temperature) VALUES ($1, $2) RETURNING id", sensorID, temperature).Scan(&rawDataID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("error inserting raw data: %w", err)
	}
	return rawDataID, nil
}
