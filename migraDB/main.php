<?php

function exportToCSV($pdo, $tableName, $fileName) {
    $query = "SELECT * FROM $tableName";
    $stmt = $pdo->query($query);
    
    $fp = fopen($fileName, 'w');
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        fputcsv($fp, $row);
    }
    fclose($fp);
}

function importFromCSV($pdo, $tableName, $fileName) {
    try {
        $pdo->beginTransaction();
        
        $fp = fopen($fileName, 'r');
        $newRecordsCount = 0; // Contador para acompanhar o número de novos registros adicionados
        while (($data = fgetcsv($fp)) !== false) {
            // Verificar se o registro já existe na tabela de destino
            $queryCheck = "SELECT COUNT(*) FROM $tableName WHERE id = ?";
            $stmtCheck = $pdo->prepare($queryCheck);
            $stmtCheck->execute([$data[0]]); // Supondo que o ID está na primeira coluna
            $count = $stmtCheck->fetchColumn();
            
            if ($count == 0) {
                // Registro não existe, inserir na tabela de destino
                $placeholders = rtrim(str_repeat('?,', count($data)), ',');
                $queryInsert = "INSERT INTO $tableName VALUES ($placeholders)";
                $stmtInsert = $pdo->prepare($queryInsert);
                $stmtInsert->execute($data);
                $newRecordsCount++; // Incrementar o contador de novos registros
            }
        }
        fclose($fp);
        
        // Verificar se houve adição de registros novos
        if ($newRecordsCount > 0) {
            echo "Transferência de registros concluída. Foram adicionados $newRecordsCount registros novos.<br>";
        } else {
            // Verificar se a tabela está vazia após a importação
            $queryCount = "SELECT COUNT(*) FROM $tableName";
            $stmtCount = $pdo->query($queryCount);
            $totalRecords = $stmtCount->fetchColumn();
            
            if ($totalRecords == 0) {
                echo "Transferência de registros concluída. Nenhum registro foi encontrado para adicionar.<br>";
            } else {
                echo "Transferência de registros concluída. Nenhum registro novo foi adicionado.<br>";
            }
        }

        $pdo->commit();
    } catch(PDOException $e) {
        $pdo->rollBack();
        echo "Erro durante a importação: " . $e->getMessage();
    }
}

try {
    // Conexao banco origem
    $sourceHost = "localhost";
    $sourcePort = "5432";
    $sourceDBName = "php_api";
    $sourceUsername = "postgres";
    $sourcePassword = "HRp4SSbd7!";

    // Conexao banco destino
    $destinationHost = "localhost";
    $destinationPort = "5432";
    $destinationDBName = "migraDB_teste";
    $destinationUsername = "postgres";
    $destinationPassword = "HRp4SSbd7!";

    // Conectar ao banco de dados de origem
    $sourceDB = new PDO("pgsql:host=$sourceHost;port=$sourcePort;dbname=$sourceDBName", $sourceUsername, $sourcePassword);
    $sourceDB->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Conectar ao banco de dados de destino
    $destinationDB = new PDO("pgsql:host=$destinationHost;port=$destinationPort;dbname=$destinationDBName", $destinationUsername, $destinationPassword);
    $destinationDB->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Exportar dados da tabela de origem para um arquivo CSV temporário
    exportToCSV($sourceDB, "users", "data.csv");

    // Importar dados do arquivo CSV para a tabela de destino
    importFromCSV($destinationDB, "testedata", "data.csv");

    // Deletar registros do banco de origem após a transferência bem-sucedida
    $queryDelete = "DELETE FROM users";
    $sourceDB->exec($queryDelete);
    echo "Registros do banco de origem excluídos com sucesso.<br>";

} catch(PDOException $e) {
    echo "Erro: " . $e->getMessage();
}

?>