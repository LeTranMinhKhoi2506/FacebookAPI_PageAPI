using Microsoft.Data.Sqlite;
using System.Globalization;

namespace RetryService.Data
{
    public class RetryRepository
    {
        private readonly string _connectionString;
        private readonly ILogger<RetryRepository> _logger;

        public RetryRepository(IConfiguration configuration, ILogger<RetryRepository> logger)
        {
            _connectionString = configuration["Database:ConnectionString"] ?? "Data Source=retry_state.db";
            _logger = logger;
            InitializeDatabase();
        }

        private SqliteConnection CreateConnection() => new(_connectionString);

        private static string ToDbTime(DateTime value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

        private static DateTime FromDbTime(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return DateTime.UtcNow;
            }

            return DateTime.Parse(value, null, DateTimeStyles.RoundtripKind).ToUniversalTime();
        }

        private void InitializeDatabase()
        {
            try
            {
                using var connection = CreateConnection();
                connection.Open();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    CREATE TABLE IF NOT EXISTS RetryState (
                        CommandId TEXT PRIMARY KEY,
                        AttemptCount INTEGER NOT NULL DEFAULT 0,
                        NextRetryTime TEXT NOT NULL,
                        LastError TEXT,
                        FailedEventJson TEXT,
                        Status TEXT NOT NULL DEFAULT 'pending',
                        CreatedAt TEXT NOT NULL DEFAULT (datetime('now')),
                        UpdatedAt TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    CREATE INDEX IF NOT EXISTS idx_retry_status ON RetryState(Status);
                    CREATE INDEX IF NOT EXISTS idx_retry_nexttime ON RetryState(NextRetryTime);
                ";
                command.ExecuteNonQuery();
                _logger.LogInformation("SQLite schema initialized successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize SQLite schema.");
                throw;
            }
        }

        public async Task<Models.RetryState?> GetRetryStateAsync(string commandId)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    SELECT CommandId, AttemptCount, NextRetryTime, LastError, FailedEventJson, Status, CreatedAt, UpdatedAt
                    FROM RetryState
                    WHERE CommandId = $CommandId
                    LIMIT 1;";
                command.Parameters.AddWithValue("$CommandId", commandId);

                using var reader = await command.ExecuteReaderAsync();
                if (!await reader.ReadAsync())
                {
                    return null;
                }

                return new Models.RetryState
                {
                    CommandId = reader["CommandId"].ToString(),
                    AttemptCount = Convert.ToInt32(reader["AttemptCount"]),
                    NextRetryTime = FromDbTime(reader["NextRetryTime"]?.ToString()),
                    LastError = reader["LastError"]?.ToString(),
                    FailedEventJson = reader["FailedEventJson"]?.ToString(),
                    Status = reader["Status"]?.ToString(),
                    CreatedAt = FromDbTime(reader["CreatedAt"]?.ToString()),
                    UpdatedAt = FromDbTime(reader["UpdatedAt"]?.ToString())
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving retry state for CommandId: {CommandId}", commandId);
                return null;
            }
        }

        public async Task<List<Models.RetryState>> GetReadyForRetryAsync()
        {
            var states = new List<Models.RetryState>();
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    SELECT CommandId, AttemptCount, NextRetryTime, LastError, FailedEventJson, Status, CreatedAt, UpdatedAt
                    FROM RetryState
                    WHERE Status = 'pending' AND NextRetryTime <= $NowUtc
                    ORDER BY NextRetryTime ASC;";
                command.Parameters.AddWithValue("$NowUtc", ToDbTime(DateTime.UtcNow));

                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    states.Add(new Models.RetryState
                    {
                        CommandId = reader["CommandId"].ToString(),
                        AttemptCount = Convert.ToInt32(reader["AttemptCount"]),
                        NextRetryTime = FromDbTime(reader["NextRetryTime"]?.ToString()),
                        LastError = reader["LastError"]?.ToString(),
                        FailedEventJson = reader["FailedEventJson"]?.ToString(),
                        Status = reader["Status"]?.ToString(),
                        CreatedAt = FromDbTime(reader["CreatedAt"]?.ToString()),
                        UpdatedAt = FromDbTime(reader["UpdatedAt"]?.ToString())
                    });
                }

                return states;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving retry states ready for retry.");
                return states;
            }
        }

        public async Task<bool> UpsertPendingRetryAsync(Models.RetryState state)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();

                using (var existsCommand = connection.CreateCommand())
                {
                    existsCommand.Transaction = transaction;
                    existsCommand.CommandText = @"
                        SELECT Status
                        FROM RetryState
                        WHERE CommandId = $CommandId
                        LIMIT 1;";
                    existsCommand.Parameters.AddWithValue("$CommandId", state.CommandId ?? string.Empty);

                    var existingStatus = (await existsCommand.ExecuteScalarAsync())?.ToString();
                    if (existingStatus is "completed" or "dead_letter")
                    {
                        transaction.Rollback();
                        return false;
                    }
                }

                using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = @"
                    INSERT INTO RetryState (CommandId, AttemptCount, NextRetryTime, LastError, FailedEventJson, Status, CreatedAt, UpdatedAt)
                    VALUES ($CommandId, $AttemptCount, $NextRetryTime, $LastError, $FailedEventJson, 'pending', COALESCE((SELECT CreatedAt FROM RetryState WHERE CommandId = $CommandId), datetime('now')), datetime('now'))
                    ON CONFLICT(CommandId) DO UPDATE SET
                        AttemptCount = excluded.AttemptCount,
                        NextRetryTime = excluded.NextRetryTime,
                        LastError = excluded.LastError,
                        FailedEventJson = excluded.FailedEventJson,
                        Status = 'pending',
                        UpdatedAt = datetime('now');";

                command.Parameters.AddWithValue("$CommandId", state.CommandId ?? string.Empty);
                command.Parameters.AddWithValue("$AttemptCount", state.AttemptCount);
                command.Parameters.AddWithValue("$NextRetryTime", ToDbTime(state.NextRetryTime));
                command.Parameters.AddWithValue("$LastError", (object?)state.LastError ?? DBNull.Value);
                command.Parameters.AddWithValue("$FailedEventJson", (object?)state.FailedEventJson ?? DBNull.Value);

                await command.ExecuteNonQueryAsync();
                transaction.Commit();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error upserting retry state for CommandId: {CommandId}", state.CommandId);
                throw;
            }
        }

        public async Task UpdateRetryAttemptAsync(Models.RetryState state)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    UPDATE RetryState
                    SET AttemptCount = $AttemptCount,
                        NextRetryTime = $NextRetryTime,
                        LastError = $LastError,
                        FailedEventJson = $FailedEventJson,
                        Status = 'pending',
                        UpdatedAt = datetime('now')
                    WHERE CommandId = $CommandId;";

                command.Parameters.AddWithValue("$CommandId", state.CommandId ?? string.Empty);
                command.Parameters.AddWithValue("$AttemptCount", state.AttemptCount);
                command.Parameters.AddWithValue("$NextRetryTime", ToDbTime(state.NextRetryTime));
                command.Parameters.AddWithValue("$LastError", (object?)state.LastError ?? DBNull.Value);
                command.Parameters.AddWithValue("$FailedEventJson", (object?)state.FailedEventJson ?? DBNull.Value);

                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating retry attempt for CommandId: {CommandId}", state.CommandId);
                throw;
            }
        }

        public async Task MarkAsCompletedAsync(string commandId)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    UPDATE RetryState
                    SET Status = 'completed',
                        UpdatedAt = datetime('now')
                    WHERE CommandId = $CommandId;";
                command.Parameters.AddWithValue("$CommandId", commandId);
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking retry state as completed for CommandId: {CommandId}", commandId);
            }
        }

        public async Task MarkAsDeadLetterAsync(string commandId, string reason)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    UPDATE RetryState
                    SET Status = 'dead_letter',
                        LastError = $Reason,
                        UpdatedAt = datetime('now')
                    WHERE CommandId = $CommandId;";

                command.Parameters.AddWithValue("$CommandId", commandId);
                command.Parameters.AddWithValue("$Reason", reason);
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking retry state as dead letter for CommandId: {CommandId}", commandId);
            }
        }

        public async Task<int> GetPendingRetryCountAsync()
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM RetryState WHERE Status = 'pending';";
                var result = await command.ExecuteScalarAsync();
                return Convert.ToInt32(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving pending retry count.");
                return 0;
            }
        }
    }
}
