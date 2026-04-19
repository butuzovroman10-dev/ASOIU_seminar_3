namespace Seminar_3
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Text;

    internal class Program
    {
        static void Main(string[] args)
        {
            // Часть 1: Работа с SQLite
            const string dbFile = "developer.db";
            const string devCsv = "dev.csv";
            const string depCsv = "dep.csv";

            CreateDatabase(dbFile);
            LoadDatabase(dbFile, devCsv, depCsv);

            PrintData(dbFile, "dep");
            PrintData(dbFile, "dev");

            List<string[]> names = Projection(dbFile, "dev", "dev_name");
            Console.WriteLine("\n=== Результат Projection(dev, dev_name) ===");
            foreach (var name in names)
                Console.WriteLine(name[0]);

            List<string[]> rows = Where(dbFile, "dev", "dep_id", "2");
            Console.WriteLine("\n=== Результат Where(dev, dep_id, 2) ===");
            foreach (var row in rows)
                Console.WriteLine(string.Join(" | ", row));

            var (columns, joinRows) = Join(dbFile, "dev", "dep", "dep_id", "dep_id");
            Console.WriteLine("\n=== Результат Join(dev, dep, dep_id, dep_id) ===");
            Console.WriteLine(string.Join(" | ", columns));
            Console.WriteLine(new string('-', 80));
            foreach (var row in joinRows)
                Console.WriteLine(string.Join(" | ", row));

            var (gavgCols, gavgRows) = GroupAvg(dbFile, "dev", "dep_id", "dev_commits");
            Console.WriteLine("\n=== Результат GroupAvg(dev, dep_id, dev_commits) ===");
            Console.WriteLine(string.Join(" | ", gavgCols));
            Console.WriteLine(new string('-', 40));
            foreach (var row in gavgRows)
                Console.WriteLine(string.Join(" | ", row));

            Console.WriteLine("\n" + new string('=', 80));
            Console.WriteLine("Часть 2: Прототип реляционной СУБД на чистом C#");
            Console.WriteLine(new string('=', 80));

            if (args.Length > 0)
            {
                RunPrototype(args);
            }
            else
            {
                Console.WriteLine("Для запуска прототипа используйте команды:");
                Console.WriteLine("  projection <колонка>");
                Console.WriteLine("  where <колонка> <значение>");
                Console.WriteLine("  join <ключ_левой> <ключ_правой>");
                Console.WriteLine("  group_avg <колонка_группировки> <колонка_значений>");
                Console.WriteLine("\nПримеры:");
                Console.WriteLine("  echo \"dev_id;dep_id;dev_name;dev_commits\" > input.csv");
                Console.WriteLine("  echo \"1;1;Иванов;100\" >> input.csv");
                Console.WriteLine("  cat input.csv | Seminar_3.exe projection dev_name");
            }
        }

        static void RunPrototype(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.InputEncoding = Encoding.UTF8;

            string mode = args[0].ToLower();

            try
            {
                switch (mode)
                {
                    case "projection":
                        if (args.Length < 2)
                        {
                            Console.Error.WriteLine("Использование: projection <колонка>");
                            return;
                        }
                        var table = ReadCsv(Console.In, ';');
                        string columnName = args[1];
                        var result = ProjectionPure(table, columnName);
                        WriteCsv(Console.Out, result, ';');
                        break;

                    case "where":
                        if (args.Length < 3)
                        {
                            Console.Error.WriteLine("Использование: where <колонка> <значение>");
                            return;
                        }
                        table = ReadCsv(Console.In, ';');
                        string whereColumn = args[1];
                        string whereValue = args[2];
                        result = WherePure(table, whereColumn, whereValue);
                        WriteCsv(Console.Out, result, ';');
                        break;

                    case "join":
                        if (args.Length < 3)
                        {
                            Console.Error.WriteLine("Использование: join <ключ_левой> <ключ_правой>");
                            return;
                        }
                        var leftTable = ReadCsv(Console.In, ';');
                        var rightTable = ReadCsv(Console.In, ';');
                        string leftKey = args[1];
                        string rightKey = args[2];
                        var joinResult = JoinPure(leftTable, rightTable, leftKey, rightKey);
                        WriteCsv(Console.Out, joinResult, ';');
                        break;

                    case "group_avg":
                        if (args.Length < 3)
                        {
                            Console.Error.WriteLine("Использование: group_avg <колонка_группировки> <колонка_значений>");
                            return;
                        }
                        table = ReadCsv(Console.In, ';');
                        string groupColumn = args[1];
                        string valueColumn = args[2];
                        var groupResult = GroupAvgPure(table, groupColumn, valueColumn);
                        WriteCsv(Console.Out, groupResult, ';');
                        break;

                    default:
                        Console.Error.WriteLine($"Неизвестная команда: {mode}");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Ошибка: {ex.Message}");
            }
        }

        // ==================== Часть 1: Работа с SQLite ====================

        static void CreateDatabase(string dbPath)
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);

            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();

            var command = connection.CreateCommand();

            command.CommandText = @"
                CREATE TABLE dep (
                    dep_id INTEGER PRIMARY KEY,
                    dep_name TEXT NOT NULL
                );";
            command.ExecuteNonQuery();

            command.CommandText = @"
                CREATE TABLE dev (
                    dev_id INTEGER PRIMARY KEY,
                    dep_id INTEGER NOT NULL,
                    dev_name TEXT NOT NULL,
                    dev_commits INTEGER NOT NULL,
                    FOREIGN KEY (dep_id) REFERENCES dep (dep_id)
                );";
            command.ExecuteNonQuery();

            Console.WriteLine($"[OK] База данных {dbPath} сделана, таблицы dep и dev готовы.");
        }

        static void LoadDatabase(string dbPath, string devCsvPath, string depCsvPath)
        {
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();

            var depIds = new HashSet<int>();

            using (var transaction = connection.BeginTransaction())
            {
                var lines = File.ReadAllLines(depCsvPath);
                for (int i = 1; i < lines.Length; i++)
                {
                    var parts = lines[i].Split(';');
                    if (parts.Length < 2) continue;

                    int depId = int.Parse(parts[0]);
                    depIds.Add(depId);

                    var cmd = connection.CreateCommand();
                    cmd.CommandText = "INSERT INTO dep (dep_id, dep_name) VALUES (@id, @name);";
                    cmd.Parameters.AddWithValue("@id", depId);
                    cmd.Parameters.AddWithValue("@name", parts[1]);
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
                Console.WriteLine($"[OK] Загружено строк из {depCsvPath}: {lines.Length - 1}");
            }

            using (var transaction = connection.BeginTransaction())
            {
                var lines = File.ReadAllLines(devCsvPath);
                int loadedCount = 0;
                int skippedCount = 0;

                for (int i = 1; i < lines.Length; i++)
                {
                    var parts = lines[i].Split(';');
                    if (parts.Length < 4) continue;

                    int depId = int.Parse(parts[1]);

                    if (!depIds.Contains(depId))
                    {
                        skippedCount++;
                        Console.WriteLine($"[ПРЕДУПРЕЖДЕНИЕ] Пропущен разработчик {parts[2]}: dep_id={depId} не существует");
                        continue;
                    }

                    var cmd = connection.CreateCommand();
                    cmd.CommandText = "INSERT INTO dev (dev_id, dep_id, dev_name, dev_commits) VALUES (@id, @depId, @name, @commits);";
                    cmd.Parameters.AddWithValue("@id", int.Parse(parts[0]));
                    cmd.Parameters.AddWithValue("@depId", depId);
                    cmd.Parameters.AddWithValue("@name", parts[2]);
                    cmd.Parameters.AddWithValue("@commits", int.Parse(parts[3]));
                    cmd.ExecuteNonQuery();
                    loadedCount++;
                }
                transaction.Commit();
                Console.WriteLine($"[OK] Загружено строк из {devCsvPath}: {loadedCount}");
                if (skippedCount > 0)
                    Console.WriteLine($"[ПРЕДУПРЕЖДЕНИЕ] Пропущено строк: {skippedCount} (нет соответствующих отделов)");
            }
        }

        static void PrintData(string dbPath, string tableName)
        {
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();

            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} ORDER BY 1;";

            using var reader = cmd.ExecuteReader();

            int columnCount = reader.FieldCount;
            const int colWidth = 20;

            Console.WriteLine($"\n=== Данные из таблицы {tableName} ===");

            for (int i = 0; i < columnCount; i++)
                Console.Write($"{reader.GetName(i),-colWidth}");
            Console.WriteLine();
            Console.WriteLine(new string('-', colWidth * columnCount));

            while (reader.Read())
            {
                for (int i = 0; i < columnCount; i++)
                    Console.Write($"{reader.GetValue(i),-colWidth}");
                Console.WriteLine();
            }
        }

        static List<string[]> Projection(string dbPath, string tableName, string columnName)
        {
            var result = new List<string[]>();
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();
            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT {columnName} FROM {tableName} ORDER BY 1;";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add(new string[] { reader.GetValue(0).ToString()! });
            return result;
        }

        static List<string[]> Where(string dbPath, string tableName, string columnName, string value)
        {
            var result = new List<string[]>();
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();
            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} WHERE {columnName} = @value ORDER BY 1;";
            cmd.Parameters.AddWithValue("@value", value);
            using var reader = cmd.ExecuteReader();
            int colCount = reader.FieldCount;
            while (reader.Read())
            {
                var row = new string[colCount];
                for (int i = 0; i < colCount; i++)
                    row[i] = reader.GetValue(i).ToString()!;
                result.Add(row);
            }
            return result;
        }

        static (string[] columns, List<string[]> rows) Join(string dbPath, string table1, string table2, string key1, string key2)
        {
            var result = new List<string[]>();
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();
            var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT * 
                FROM {table1} 
                JOIN {table2} ON {table1}.{key1} = {table2}.{key2}
                ORDER BY 1;";
            using var reader = cmd.ExecuteReader();
            int colCount = reader.FieldCount;
            string[] columns = new string[colCount];
            for (int i = 0; i < colCount; i++)
                columns[i] = reader.GetName(i);
            while (reader.Read())
            {
                var row = new string[colCount];
                for (int i = 0; i < colCount; i++)
                    row[i] = reader.GetValue(i).ToString()!;
                result.Add(row);
            }
            return (columns, result);
        }

        static (string[] columns, List<string[]> rows) GroupAvg(string dbPath, string tableName, string groupByCol, string avgCol)
        {
            var result = new List<string[]>();
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();
            var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                SELECT {groupByCol}, AVG({avgCol}) 
                FROM {tableName} 
                GROUP BY {groupByCol}
                ORDER BY 1;";
            using var reader = cmd.ExecuteReader();
            int colCount = reader.FieldCount;
            string[] columns = new string[colCount];
            for (int i = 0; i < colCount; i++)
                columns[i] = reader.GetName(i);
            while (reader.Read())
            {
                var row = new string[colCount];
                for (int i = 0; i < colCount; i++)
                    row[i] = reader.GetValue(i).ToString()!;
                result.Add(row);
            }
            return (columns, result);
        }

        // ==================== Часть 2: Прототип на чистом C# ====================

        record CsvRow(string[] Fields);
        record CsvTable(string[] Headers, List<CsvRow> Rows);

        static CsvTable ReadCsv(TextReader reader, char separator)
        {
            string? headerLine = reader.ReadLine();
            if (headerLine is null)
                throw new InvalidOperationException("Входной поток пуст – нет строки заголовков.");

            string[] headers = headerLine.Split(separator);
            var rows = new List<CsvRow>();
            string? line;

            while ((line = reader.ReadLine()) is not null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                string[] parts = line.Split(separator);
                rows.Add(new CsvRow(parts));
            }

            return new CsvTable(headers, rows);
        }

        static void WriteCsv(TextWriter writer, CsvTable table, char separator)
        {
            writer.WriteLine(string.Join(separator, table.Headers));
            foreach (var row in table.Rows)
                writer.WriteLine(string.Join(separator, row.Fields));
        }

        static int FindColumnIndex(CsvTable table, string columnName)
        {
            int index = Array.IndexOf(table.Headers, columnName);
            if (index < 0)
                throw new ArgumentException(
                    $"Колонка «{columnName}» не найдена. " +
                    $"Доступные колонки: {string.Join(", ", table.Headers)}");
            return index;
        }

        static CsvTable ProjectionPure(CsvTable table, string columnName)
        {
            int colIndex = FindColumnIndex(table, columnName);
            string[] newHeaders = [columnName];
            var newRows = new List<CsvRow>();

            foreach (var row in table.Rows)
            {
                string[] fields = [row.Fields[colIndex]];
                newRows.Add(new CsvRow(fields));
            }

            return new CsvTable(newHeaders, newRows);
        }

        static CsvTable WherePure(CsvTable table, string columnName, string value)
        {
            int colIndex = FindColumnIndex(table, columnName);
            var newRows = new List<CsvRow>();

            foreach (var row in table.Rows)
            {
                if (row.Fields[colIndex] == value)
                    newRows.Add(row);
            }

            return new CsvTable(table.Headers, newRows);
        }

        static CsvTable JoinPure(CsvTable left, CsvTable right, string leftKey, string rightKey)
        {
            int leftKeyIndex = FindColumnIndex(left, leftKey);
            int rightKeyIndex = FindColumnIndex(right, rightKey);

            var newHeaders = new string[left.Headers.Length + right.Headers.Length];

            for (int i = 0; i < left.Headers.Length; i++)
                newHeaders[i] = left.Headers[i];

            for (int i = 0; i < right.Headers.Length; i++)
                newHeaders[left.Headers.Length + i] = right.Headers[i];

            var newRows = new List<CsvRow>();

            foreach (var leftRow in left.Rows)
            {
                foreach (var rightRow in right.Rows)
                {
                    if (leftRow.Fields[leftKeyIndex] == rightRow.Fields[rightKeyIndex])
                    {
                        var fields = new string[leftRow.Fields.Length + rightRow.Fields.Length];

                        for (int i = 0; i < leftRow.Fields.Length; i++)
                            fields[i] = leftRow.Fields[i];

                        for (int i = 0; i < rightRow.Fields.Length; i++)
                            fields[leftRow.Fields.Length + i] = rightRow.Fields[i];

                        newRows.Add(new CsvRow(fields));
                    }
                }
            }

            return new CsvTable(newHeaders, newRows);
        }

        static double Average(List<double> values)
        {
            double sum = 0;
            for (int i = 0; i < values.Count; i++)
                sum += values[i];
            return sum / values.Count;
        }

        static CsvTable GroupAvgPure(CsvTable table, string groupColumn, string valueColumn)
        {
            int groupIndex = FindColumnIndex(table, groupColumn);
            int valueIndex = FindColumnIndex(table, valueColumn);

            var groups = new Dictionary<string, List<double>>();

            foreach (var row in table.Rows)
            {
                string key = row.Fields[groupIndex];
                double value = double.Parse(row.Fields[valueIndex]);

                if (!groups.ContainsKey(key))
                    groups[key] = new List<double>();

                groups[key].Add(value);
            }

            string[] newHeaders = [groupColumn, "avg_" + valueColumn];
            var newRows = new List<CsvRow>();

            foreach (var pair in groups)
            {
                string avg = Average(pair.Value).ToString("F2");
                newRows.Add(new CsvRow([pair.Key, avg]));
            }

            return new CsvTable(newHeaders, newRows);
        }
    }
}