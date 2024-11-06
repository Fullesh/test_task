package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq" // Драйвер для PostgreSQL
	"log"
	"os/exec"
	"sync"
)

func enablePreparedTransaction(server string) error {
	db, err := sql.Open("postgres", server)
	if err != nil {
		return fmt.Errorf("Ошибка подключения к серверу %s: %v", server, err)
	}
	defer db.Close()

	// Set max_prepared_transactions to 8
	_, err = db.Exec("ALTER SYSTEM SET max_prepared_transactions = 8")
	if err != nil {
		return fmt.Errorf("Ошибка изменения max_prepared_transactions на сервере %s: %v", server, err)
	}

	// Reload the PostgreSQL configuration
	_, err = db.Exec("SELECT pg_reload_conf()")
	if err != nil {
		return fmt.Errorf("Ошибка перезагрузки конфигурации на сервере %s: %v", server, err)
	}

	fmt.Printf("Сервер %s успешно настроен для подготовленных транзакций.\n", server)
	return nil
}

func restartPostgresServer() error {
	cmd := exec.Command("pg_ctl", "-D", "D:\\TestDir\\Server_A", "stop")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Ошибка при остановке сервер: %v, вывод: %s", err, output)
	}
	cmdB := exec.Command("pg_ctl", "-D", "D:\\TestDir\\Server_B", "stop")
	outputB, err := cmdB.CombinedOutput()
	if err != nil {
		fmt.Printf("Ошибка при остановке сервер: %v, вывод: %s", err, outputB)
	}
	fmt.Println("Сервера успешно остановлены")
	cmdSA := exec.Command("pg_ctl", "-D", "D:\\TestDir\\Server_A", "-o", "-p33555", "start")
	if err := cmdSA.Start(); err != nil {
		return fmt.Errorf("Ошибка при запуске сервера: %v", err)
	}
	fmt.Println("Сервер А успешно поднялся")

	cmdSB := exec.Command("pg_ctl", "-D", "D:\\TestDir\\Server_B", "-o", "-p33556", "start")
	if err := cmdSB.Start(); err != nil {
		return fmt.Errorf("Ошибка при запуске сервера: %v", err)
	}
	fmt.Println("Сервер Б успешно поднялся")

	return nil
}

func createDataBase(server string) error {
	db, err := sql.Open("postgres", server)
	if err != nil {
		return fmt.Errorf("Ошибка подключения к серверу %s: %v", server, err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE database")
	if err != nil {
		return fmt.Errorf("Ошибка создания БД на сервере %s: %v", server, err)
	}
	fmt.Printf("Подключение к серверу %s успешно. БД 'database' создана \n", server)
	return nil
}

func createTables(server string) error {
	// Connect to the newly created database
	db, err := sql.Open("postgres", server+" dbname=database")
	if err != nil {
		return fmt.Errorf("Ошибка подключения к БД на сервере %s: %v", server, err)
	}
	defer db.Close()

	// Attempt to create the table
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS Data (id SERIAL PRIMARY KEY, value VARCHAR(100))")
	if err != nil {
		return fmt.Errorf("Ошибка при создании таблицы на сервере %s: %v", server, err)
	}
	fmt.Printf("Таблица 'Data' создана на сервере %s. \n", server)
	return nil
}

func dataFill(server string, wg *sync.WaitGroup) {
	db, err := sql.Open("postgres", server+" dbname=database")
	if err != nil {
		log.Fatalf("Ошибка при подключении к БД на сервере %s: %v", server, err)
	}
	defer db.Close()

	// Check if the server is the one with port 33555
	if server == "user=postgres host=localhost port=33555 sslmode=disable" {
		for i := 1; i < 10; i++ {
			_, err = db.Exec("INSERT INTO Data (value) VALUES ($1)", fmt.Sprintf("Value %d", i))
			if err != nil {
				log.Fatalf("Ошибка вставки данных на сервере %s: %v", server, err)
			}
		}
		fmt.Println("Таблица 'Data' заполнена данными на сервере А. \n")
	}
}

func transferDataWith2PC(serverA, serverB string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Подключение к обеим базам данных
	dbA, err := sql.Open("postgres", serverA+" dbname=database")
	if err != nil {
		log.Fatalf("Ошибка подключения к серверу A: %v", err)
	}
	defer dbA.Close()

	dbB, err := sql.Open("postgres", serverB+" dbname=database")
	if err != nil {
		log.Fatalf("Ошибка подключения к серверу B: %v", err)
	}
	defer dbB.Close()

	// Начало транзакций на обоих серверах
	txA, err := dbA.Begin()
	if err != nil {
		log.Fatalf("Ошибка начала транзакции на сервере A: %v", err)
	}

	txB, err := dbB.Begin()
	if err != nil {
		log.Fatalf("Ошибка начала транзакции на сервере B: %v", err)
	}

	// Фаза 1: Подготовка передачи данных
	rows, err := txA.Query("SELECT id, value FROM Data")
	if err != nil {
		txA.Rollback()
		txB.Rollback()
		log.Fatalf("Ошибка выборки данных на сервере A: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			txA.Rollback()
			txB.Rollback()
			log.Fatalf("Ошибка сканирования строки на сервере A: %v", err)
		}

		_, err = txB.Exec("INSERT INTO Data (id, value) VALUES ($1, $2)", id, value)
		if err != nil {
			txA.Rollback()
			txB.Rollback()
			log.Fatalf("Ошибка вставки данных на сервере B: %v", err)
		}
	}

	// Подготовка транзакций
	prepareTxA := "PREPARE TRANSACTION 'txA'"
	prepareTxB := "PREPARE TRANSACTION 'txB'"

	if _, err := txA.Exec(prepareTxA); err != nil {
		txA.Rollback()
		txB.Rollback()
		log.Fatalf("Ошибка подготовки транзакции на сервере A: %v", err)
	}

	if _, err := txB.Exec(prepareTxB); err != nil {
		txA.Rollback()
		txB.Rollback()
		log.Fatalf("Ошибка подготовки транзакции на сервере B: %v", err)
	}

	// Симуляция падения сервера A
	simulateCrash := false
	if simulateCrash {
		log.Println("Симуляция падения сервера A")
		_, _ = dbA.Exec("ROLLBACK PREPARED 'txA'")
		_, _ = dbB.Exec("ROLLBACK PREPARED 'txB'")
		return
	}

	// Фаза 2: Коммит подготовленных транзакций
	if _, err := dbA.Exec("COMMIT PREPARED 'txA'"); err != nil {
		_, _ = dbA.Exec("ROLLBACK PREPARED 'txA'")
		_, _ = dbB.Exec("ROLLBACK PREPARED 'txB'")
		log.Fatalf("Ошибка коммита подготовленной транзакции на сервере A: %v", err)
	}

	if _, err := dbB.Exec("COMMIT PREPARED 'txB'"); err != nil {
		_, _ = dbA.Exec("ROLLBACK PREPARED 'txA'")
		_, _ = dbB.Exec("ROLLBACK PREPARED 'txB'")
		log.Fatalf("Ошибка коммита подготовленной транзакции на сервере B: %v", err)
	}

	// Удаление данных с сервера A после успешной передачи
	if _, err := dbA.Exec("DROP TABLE Data"); err != nil {
		log.Fatalf("Ошибка удаления данных на сервере A: %v", err)
	}

	fmt.Println("Передача данных завершена успешно, данные на сервере A удалены.")
}

func createDataBaseNTables(server string, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := createDataBase(server); err != nil {
		log.Println(err)
		return
	}
	if err := createTables(server); err != nil {
		log.Println(err)
	}
	dataFill(server, wg)

}

func TransferData() {
	var wg sync.WaitGroup

	serverA := "user=postgres host=localhost port=33555 sslmode=disable"
	serverB := "user=postgres host=localhost port=33556 sslmode=disable"
	if err := enablePreparedTransaction(serverA); err != nil {
		log.Fatalf("Ошибка настройки сервера A: %v", err)
	}
	if err := enablePreparedTransaction(serverB); err != nil {
		log.Fatalf("Ошибка настройки сервера Б: %v", err)
	}

	if err := restartPostgresServer(); err != nil {
		log.Fatalf("Ошибка перезапуска серверов: %v", err)
	}

	simulateCrash := false
	fmt.Println("Hello World")
	var simulateCrashResponse string
	fmt.Print("Хотите ли вы иммитировать падние сервера А? (y/n): ")
	fmt.Scanln(&simulateCrashResponse)
	if simulateCrashResponse == "y" {
		simulateCrash = true
	}
	fmt.Println(simulateCrash)

	// Create databases and tables
	wg.Add(2)
	go createDataBaseNTables(serverA, &wg)
	go createDataBaseNTables(serverB, &wg)
	wg.Wait()

	// Transfer data using 2PC
	wg.Add(1)
	go transferDataWith2PC(serverA, serverB, &wg)
	wg.Wait()

	fmt.Println("Все задачи выполнены")
}
