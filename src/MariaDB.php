<?php


namespace ManasahTech\Data\MariaDB;


use PDO;
use Exception;
use PDOException;

use ManasahTech\Contracts\Data\DatabaseDriver;
use ManasahTech\Contracts\Data\DatabaseConnection;
use ManasahTech\Container\Attributes\Injectable;
use ManasahTech\Container\Scopes\SingletonScope;


#[Injectable(scope: SingletonScope::class)]
class MariaDB implements DatabaseDriver
{
    protected ?PDO $pdo = null;

    public function connect(DatabaseConnection $connection)
    {
        try {
            $dsn = "mysql:host={$connection->host};dbname={$connection->dbname}";
            $this->pdo = new PDO($dsn, $connection->user, $connection->password);
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            throw new Exception("Connection failed: " . $e->getMessage());
        }
    }

    public function disconnect()
    {
        $this->pdo = null;
    }

    public function query(string $sql)
    {
        if ($this->pdo === null) {
            throw new Exception("Not connected to the database.");
        }

        try {
            $stmt = $this->pdo->query($sql);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            throw new Exception("Query failed: " . $e->getMessage());
        }
    }

    public function select(string $table, $args = array())
    {
        if ($this->pdo === null) {
            throw new Exception("Not connected to the database.");
        }

        $sql = "SELECT * FROM {$table}";
        if (!empty($args)) {
            $conditions = [];
            foreach ($args as $key => $value) {
                $conditions[] = "{$key} = :{$key}";
            }
            $sql .= " WHERE " . implode(" AND ", $conditions);
        }

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($args);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            throw new Exception("Select query failed: " . $e->getMessage());
        }
    }

    public function insert(string $table, $data)
    {
        if ($this->pdo === null) {
            throw new Exception("Not connected to the database.");
        }

        $columns = implode(", ", array_keys($data));
        $placeholders = implode(", ", array_map(function ($key) {
            return ":{$key}";
        }, array_keys($data)));

        $sql = "INSERT INTO {$table} ({$columns}) VALUES ({$placeholders})";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($data);
        } catch (PDOException $e) {
            throw new Exception("Insert query failed: " . $e->getMessage());
        }
    }

    public function update(string $table, $data, $args = array())
    {
        if ($this->pdo === null) {
            throw new Exception("Not connected to the database.");
        }

        $setParts = [];
        foreach ($data as $key => $value) {
            $setParts[] = "{$key} = :{$key}";
        }
        $setClause = implode(", ", $setParts);

        $conditionParts = [];
        foreach ($args as $key => $value) {
            $conditionParts[] = "{$key} = :condition_{$key}";
        }
        $conditionClause = implode(" AND ", $conditionParts);

        $sql = "UPDATE {$table} SET {$setClause} WHERE {$conditionClause}";

        try {
            $stmt = $this->pdo->prepare($sql);
            foreach ($data as $key => $value) {
                $stmt->bindValue(":{$key}", $value);
            }
            foreach ($args as $key => $value) {
                $stmt->bindValue(":condition_{$key}", $value);
            }
            $stmt->execute();
        } catch (PDOException $e) {
            throw new Exception("Update query failed: " . $e->getMessage());
        }
    }

    public function delete(string $table, $args = array())
    {
        if ($this->pdo === null) {
            throw new Exception("Not connected to the database.");
        }

        $conditions = [];
        foreach ($args as $key => $value) {
            $conditions[] = "{$key} = :{$key}";
        }
        $sql = "DELETE FROM {$table} WHERE " . implode(" AND ", $conditions);

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($args);
        } catch (PDOException $e) {
            throw new Exception("Delete query failed: " . $e->getMessage());
        }
    }
}

// just to notify the packagist.org