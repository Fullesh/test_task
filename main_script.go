package main

import (
	"bytes"
	"fmt"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

func decodeOutput(ouput []byte) (string, error) {
	reader := transform.NewReader(bytes.NewReader(ouput), charmap.Windows1251.NewDecoder())
	decodeBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(decodeBytes), nil
}

func createCluser(clusterPath string) error {
	cmd := exec.Command("initdb", "-D", clusterPath, "--username=postgres")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("Ошибка при создании сервера: %v, вывод: %s", err, output)
	}
	fmt.Printf("Сервер успешно создан в: %s\n", clusterPath)
	return nil
}

func ClusterExsists(clusterPath string) bool {
	info, err := os.Stat(clusterPath)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func isClusterRunning(clusterPath string) (bool, error) {
	if !ClusterExsists(clusterPath) {
		return false, fmt.Errorf("Невозможно проверить статус сервера: он не существует по пути %s \n", clusterPath)
	}
	cmd := exec.Command("pg_ctl", "-D", clusterPath, "status")
	output, err := cmd.CombinedOutput()
	if err != nil {
		decodedOutput, decodeErr := decodeOutput(output)
		if decodeErr != nil {
			return false, fmt.Errorf("Ошибка при декодировании вывода: %v", decodeErr)
		}
		if strings.Contains(decodedOutput, "pg_ctl: сервер не работает") {
			return false, nil
		}
		fmt.Println(string(output))
		return false, fmt.Errorf("Ошибка при проверке статуса сервера: %v, вывод: %s", err, decodedOutput)
	}
	return true, nil
}

func StartCluster(clusterPath, host string, port int) error {
	if !ClusterExsists(clusterPath) {
		return fmt.Errorf("Невозможно запустить сервер: он не существует по пути %s", clusterPath)
	}
	running, err := isClusterRunning(clusterPath)
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("Невозможно запустить сервер: он уже запущен по пути %s", clusterPath)
	}
	cmd := exec.Command("pg_ctl", "-D", clusterPath, "-o", fmt.Sprintf("-p%d", port), "start")
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Ошибка при запуске сервера: %v", err)
	}
	fmt.Printf("Сервер %s по адресу: %s:%d успешно запущен.\n", clusterPath, host, port)
	return nil
}

func StopCluster(clusterPath, host string, port int) error {
	if !ClusterExsists(clusterPath) {
		return fmt.Errorf("Невозможно остановить сервер: он не существует по пути %s", clusterPath)
	}
	running, err := isClusterRunning(clusterPath)
	if err != nil {
		return err
	}
	if !running {
		return fmt.Errorf("Невозможно остановить сервер %s: он уже остановлен", clusterPath)
	}
	cmd := exec.Command("pg_ctl", "-D", clusterPath, "stop")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Ошибка при остановке сервер: %v, вывод: %s", err, output)
	}
	fmt.Printf("Сервер %s по адресу: %s:%d остановлен. \n", clusterPath, host, port)
	return nil
}

func deleteCluster(clusterPath string) error {
	if !ClusterExsists(clusterPath) {
		return fmt.Errorf("Невозможно удалить сервер: он не существует по пути: %s", clusterPath)
	}
	running, err := isClusterRunning(clusterPath)
	if err != nil {
		return err
	}
	if running {
		return fmt.Errorf("Невозможно удалить сервер: он запущен по пути %s", clusterPath)
	}

	err = os.RemoveAll(clusterPath)
	if err != nil {
		return fmt.Errorf("Ошибка при удалении сервера: %v", err)
	}
	fmt.Printf("Сервер успешно удалён по пути: %s\n", clusterPath)
	return nil
}

func main() {
	cluster1Path := "D:\\TestDir\\Server_A"
	cluster2Path := "D:\\TestDir\\Server_B"

	for {
		fmt.Println("\n Что вы хотите сделать?")
		fmt.Println("1) Проверить наличие тестовых серверов")
		fmt.Println("2) Удалить тестовые сервера")
		fmt.Println("3) Создать тестовые сервера")
		fmt.Println("4) Включить тестовые сервера")
		fmt.Println("5) Выключить тестовые сервера")
		fmt.Println("6) Проверить статус серверов")
		fmt.Println("7) Выполнить передачу данных между серверами А и Б")
		fmt.Println("8) Выход")
		var choice int
		fmt.Print("Введите номер действия: ")
		_, err := fmt.Scanf("%d", &choice)
		if err != nil {
			fmt.Println("Ошибка ввода, попробуйте число.")
			continue
		}
		switch choice {
		case 1:
			if ClusterExsists(cluster1Path) {
				fmt.Printf("Сервер А существует по пути %s \n", cluster1Path)
			} else {
				fmt.Printf("Сервер А не существует по пути %s \n", cluster1Path)
			}
			if ClusterExsists(cluster2Path) {
				fmt.Printf("Сервер Б существует по пути %s \n", cluster2Path)
			} else {
				fmt.Printf("Сервер Б не существует по пути %s \n", cluster2Path)
			}
		case 2:
			if err := deleteCluster(cluster1Path); err != nil {
				fmt.Println(err)
			}
			if err := deleteCluster(cluster2Path); err != nil {
				fmt.Println(err)
			}
		case 3:
			if ClusterExsists(cluster1Path) {
				fmt.Println("Сервер А существует, пропускаем \n")
			} else {
				fmt.Printf("Сервер А не существует, создаём \n")
				if err := createCluser(cluster1Path); err != nil {
					fmt.Println(err)
					return
				}
			}

			if ClusterExsists(cluster2Path) {
				fmt.Println("Сервер Б существует, пропускаем \n")
			} else {
				fmt.Println("Сервер Б не существует, создаём \n")
				if err := createCluser(cluster2Path); err != nil {
					fmt.Println(err)
					return
				}
			}
		case 4:
			if err := StartCluster(cluster1Path, "localhost", 33555); err != nil {
				fmt.Println(err)
			}
			if err := StartCluster(cluster2Path, "localhost", 33556); err != nil {
				fmt.Println(err)
			}
		case 5:
			if err := StopCluster(cluster1Path, "localhost", 33555); err != nil {
				fmt.Println(err)
			}
			if err := StopCluster(cluster2Path, "localhost", 33556); err != nil {
				fmt.Println(err)
			}
		case 6:
			running1, err := isClusterRunning(cluster1Path)
			if err != nil {
				fmt.Printf("Ошибка при проверке статуса сервера А: %v\n", err)
			} else if running1 {
				fmt.Println("Сервер А запущен.")
			} else {
				fmt.Println("Сервер А не запущен. ")
			}
			running2, err := isClusterRunning(cluster2Path)
			if err != nil {
				fmt.Printf("Ошибка при проверке статуса сервера Б: %v\n", err)
			} else if running2 {
				fmt.Println("Сервер Б запущен.")
			} else {
				fmt.Println("Сервер Б не запущен. ")
			}
		case 7:
			TransferData()
		case 8:
			fmt.Println("Выходим...")
			return
		default:
			fmt.Println("Некорректный выбор, попробуйте снова.")
		}
	}

}
