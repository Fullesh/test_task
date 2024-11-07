package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq" // Драйвер для PostgreSQL
	"log"
	"sync"
)

//// Функция для изменения переменной max_prepared_transactions. server - данные для подключения к серверам
//func setPreparedTransaction(server string) error {
//
//	// Открываем подключение к серверу
//	db, err := sql.Open("postgres", server)
//	if err != nil {
//		return fmt.Errorf("Ошибка подключения к серверу %s: %v", server, err)
//	}
//	defer db.Close()
//
//	// Устанавливаем переменную max_prepared_transactions на 8
//	_, err = db.Exec("ALTER SYSTEM SET max_prepared_transactions = 8")
//	if err != nil {
//		return fmt.Errorf("Ошибка изменения max_prepared_transactions на сервере %s: %v", server, err)
//	}
//
//	// Перезагружаем конфигурацию
//	_, err = db.Exec("SELECT pg_reload_conf()")
//	if err != nil {
//		return fmt.Errorf("Ошибка перезагрузки конфигурации на сервере %s: %v", server, err)
//	}
//
//	fmt.Printf("Сервер %s успешно настроен для подготовленных транзакций.\n", server)
//	return nil
//}
//
//// Функция для перезапуска серверов после изменения переменной max_prepared_transactions
//func restartPostgresServer() error {
//	// Стоп сервера А
//	cmd := exec.Command("pg_ctl", "-D", "C:\\TestDir\\Server_A", "stop")
//	output, err := cmd.CombinedOutput()
//	if err != nil {
//		fmt.Printf("Ошибка при остановке сервер: %v, вывод: %s", err, output)
//	}
//
//	// Стоп сервера Б
//	cmdB := exec.Command("pg_ctl", "-D", "C:\\TestDir\\Server_B", "stop")
//	outputB, err := cmdB.CombinedOutput()
//	if err != nil {
//		fmt.Printf("Ошибка при остановке сервер: %v, вывод: %s", err, outputB)
//	}
//	fmt.Println("Сервера успешно остановлены")
//
//	// Старт сервера А
//	cmdSA := exec.Command("pg_ctl", "-D", "C:\\TestDir\\Server_A", "-o", "-p33555", "start")
//	if err := cmdSA.Start(); err != nil {
//		return fmt.Errorf("Ошибка при запуске сервера: %v", err)
//	}
//	isClusterRunning("C:\\TestDir\\Server_A")
//	fmt.Println("Сервер А успешно поднялся")
//
//	// Старт сервера Б
//	cmdSB := exec.Command("pg_ctl", "-D", "C:\\TestDir\\Server_B", "-o", "-p33556", "start")
//	if err := cmdSB.Start(); err != nil {
//		return fmt.Errorf("Ошибка при запуске сервера: %v", err)
//	}
//	isClusterRunning("C:\\TestDir\\Server_B")
//	fmt.Println("Сервер Б успешно поднялся")
//
//	return nil
//}

// Создание БД на серверах А и Б
func createDataBase(server string) error {
	db, err := sql.Open("postgres", server)
	if err != nil {
		return fmt.Errorf("Ошибка подключения к серверу %s: %v", server, err)
	}
	defer db.Close()

	fmt.Println("Выполняю команду CREATE DATABASE database \n")
	_, err = db.Exec("CREATE DATABASE database")
	if err != nil {
		return fmt.Errorf("Ошибка создания БД на сервере %s: %v", server, err)
	}
	fmt.Printf("Подключение к серверу %s успешно. БД 'database' создана \n", server)
	return nil
}

// Создание таблиц на серверах А и Б
func createTables(server string) error {
	// Подключаемся к созданной БД
	db, err := sql.Open("postgres", server+" dbname=database")
	if err != nil {
		return fmt.Errorf("Ошибка подключения к БД на сервере %s: %v", server, err)
	}
	defer db.Close()

	// Создаём таблицу Data
	fmt.Println("Выполняю команду CREATE TABLE IF NOT EXISTS Data (id SERIAL PRIMARY KEY, value VARCHAR(100)) \n")
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS Data (id SERIAL PRIMARY KEY, value VARCHAR(100))")
	if err != nil {
		return fmt.Errorf("Ошибка при создании таблицы на сервере %s: %v", server, err)
	}
	fmt.Printf("Таблица 'Data' создана на сервере %s. \n", server)
	return nil
}

// Наполнение данными только таблицы сервера А
func dataFill(server string, serverA string, wg *sync.WaitGroup) {
	db, err := sql.Open("postgres", server+" dbname=database")
	if err != nil {
		log.Fatalf("Ошибка при подключении к БД на сервере %s: %v", server, err)
	}
	defer db.Close()

	// Проверяем, что наполняем только сервер А
	if server == serverA {
		for i := 1; i < 10; i++ {
			fmt.Println("Выполняю команду INSERT INTO Data (value) VALUES ($1) \n")
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
	fmt.Println("Выполняю команду BEGIN на сервере А \n")
	if err != nil {
		log.Fatalf("Ошибка начала транзакции на сервере A: %v", err)
	}

	txB, err := dbB.Begin()
	fmt.Println("Выполняю команду BEGIN на сервере Б \n")
	if err != nil {
		log.Fatalf("Ошибка начала транзакции на сервере B: %v", err)
	}

	// Подготовка передачи данных
	fmt.Println("Выполняю команду DELETE FROM Data RETURNING id, value FROM Data \n")
	rows, err := txA.Query("DELETE FROM Data RETURNING id, value")
	if err != nil {
		txA.Rollback()
		txB.Rollback()
		log.Fatalf("Ошибка выборки данных на сервере A: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var value string
		fmt.Println("Выполняю сканирование на сервере А \n")
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

	fmt.Println("Выполняю команду PREPARE TRANSACTION 'txA' на сервере А \n")
	if _, err := txA.Exec(prepareTxA); err != nil {
		txA.Rollback()
		txB.Rollback()
		log.Fatalf("Ошибка подготовки транзакции на сервере A: %v", err)
	}

	fmt.Println("Выполняю команду PREPARE TRANSACTION 'txB' на сервере Б \n")
	if _, err := txB.Exec(prepareTxB); err != nil {
		txA.Rollback()
		txB.Rollback()
		log.Fatalf("Ошибка подготовки транзакции на сервере B: %v", err)
	}

	// Симуляция падения сервера A (не доделано)
	simulateCrash := false
	if simulateCrash {
		log.Println("Симуляция падения сервера A")
		_, _ = dbA.Exec("ROLLBACK PREPARED 'txA'")
		_, _ = dbB.Exec("ROLLBACK PREPARED 'txB'")
		return
	}

	// Коммит подготовленных транзакций
	fmt.Println("Выполняю команду COMMIT PREPARED 'txA' на сервере А \n")
	if _, err := dbA.Exec("COMMIT PREPARED 'txA'"); err != nil {
		fmt.Println("ОШИБКА. Выполняю команду ROLLBACK PREPARED 'txA' на сервере А \n")
		_, _ = dbA.Exec("ROLLBACK PREPARED 'txA'")
		fmt.Println("ОШИБКА. Выполняю команду ROLLBACK PREPARED 'txB' на сервере Б \n")
		_, _ = dbB.Exec("ROLLBACK PREPARED 'txB'")
		log.Fatalf("Ошибка коммита подготовленной транзакции на сервере A: %v", err)
	}

	fmt.Println("Выполняю команду COMMIT PREPARED 'txB' на сервере Б \n")
	if _, err := dbB.Exec("COMMIT PREPARED 'txB'"); err != nil {
		fmt.Println("ОШИБКА. Выполняю команду ROLLBACK PREPARED 'txA' на сервере А \n")
		_, _ = dbA.Exec("ROLLBACK PREPARED 'txA'")
		fmt.Println("ОШИБКА. Выполняю команду ROLLBACK PREPARED 'txB' на сервере Б \n")
		_, _ = dbB.Exec("ROLLBACK PREPARED 'txB'")
		log.Fatalf("Ошибка коммита подготовленной транзакции на сервере B: %v", err)
	}

	fmt.Println("Передача данных завершена успешно, данные на сервере A удалены.")
}

// Создание БД, таблиц и их наполнение вместе. server - данные серверов для подключения
func createDataBaseNTables(server string, serverA string, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := createDataBase(server); err != nil {
		log.Println(err)
		return
	}
	if err := createTables(server); err != nil {
		log.Println(err)
	}
	dataFill(server, serverA, wg)

}

// Основная функция
func TransferData() {
	var wg sync.WaitGroup

	// Запрос данных для подключения к кластеру сервера A
	var userA, passwordA, hostA, portA, sslA string
	fmt.Print("Введите имя пользователя для сервера A (оставьте пустым, если postgres): ")
	fmt.Scanln(&userA)
	fmt.Print("Введите пароль для сервера A (оставьте пустым, если не требуется): ")
	fmt.Scanln(&passwordA)
	fmt.Print("Введите host для сервера A (оставьте пустым, если localhost): ")
	fmt.Scanln(&hostA)
	fmt.Print("Введите port для сервера A (оставьте пустым если 5432): ")
	fmt.Scanln(&portA)
	fmt.Print("Использовать SSL для сервера A? (y/n) (оставьте пустым, если disabled): ")
	fmt.Scanln(&sslA)

	// Запрос данных для подключения к кластеру сервера B
	var userB, passwordB, hostB, portB, sslB string
	fmt.Print("Введите имя пользователя для сервера B (оставьте пустым, если postgres): ")
	fmt.Scanln(&userB)
	fmt.Print("Введите пароль для сервера B (оставьте пустым, если не требуется): ")
	fmt.Scanln(&passwordB)
	fmt.Print("Введите host для сервера B (оставьте пустым, если localhost): ")
	fmt.Scanln(&hostB)
	fmt.Print("Введите port для сервера B (оставьте пустым если 5432): ")
	fmt.Scanln(&portB)
	fmt.Print("Использовать SSL для сервера B? (оставьте пустым, если disabled): ")
	fmt.Scanln(&sslB)

	// Формирование строк подключения
	sslModeA := "disable"
	if sslA == "y" {
		sslModeA = "enable"
	}
	if hostA == "" {
		hostA = "localhost"
	}
	if userA == "" {
		userA = "postgres"
	}
	if portA == "" {
		portA = "5432"
	}
	serverA := fmt.Sprintf("user=%s password=%s host=%s port=%s sslmode=%s", userA, passwordA, hostA, portA, sslModeA)

	sslModeB := "disable"
	if sslB == "y" {
		sslModeB = "enable"
	}
	if hostB == "" {
		hostB = "localhost"
	}
	if userB == "" {
		userB = "postgres"
	}
	if portB == "" {
		portB = "5432"
	}
	serverB := fmt.Sprintf("user=%s password=%s host=%s port=%s sslmode=%s", userB, passwordB, hostB, portB, sslModeB)
	//if err := setPreparedTransaction(serverA); err != nil {
	//	log.Fatalf("Ошибка настройки сервера A: %v", err)
	//}
	//if err := setPreparedTransaction(serverB); err != nil {
	//	log.Fatalf("Ошибка настройки сервера Б: %v", err)
	//}
	//
	//if err := restartPostgresServer(); err != nil {
	//	log.Fatalf("Ошибка перезапуска серверов: %v", err)
	//}

	simulateCrash := false
	var simulateCrashResponse string
	//fmt.Print("Хотите ли вы иммитировать падние сервера А? (y/n): ")
	//fmt.Scanln(&simulateCrashResponse)
	if simulateCrashResponse == "y" {
		simulateCrash = true
	}
	fmt.Println(simulateCrash)

	// Заполнение таблиц
	wg.Add(2)
	go createDataBaseNTables(serverA, serverA, &wg)
	go createDataBaseNTables(serverB, serverA, &wg)
	wg.Wait()

	// Передача данных
	wg.Add(1)
	go transferDataWith2PC(serverA, serverB, &wg)
	wg.Wait()

	fmt.Println("Все задачи выполнены")
}
