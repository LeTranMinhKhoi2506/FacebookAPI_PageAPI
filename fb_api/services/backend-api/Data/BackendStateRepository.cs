using Microsoft.Data.Sqlite;
using FacebookAPI___PageAPI.models;
using System.Globalization;

namespace FacebookAPI___PageAPI.Data
{
    public class BackendStateRepository
    {
        private readonly string _connectionString;
        private readonly ILogger<BackendStateRepository> _logger;

        public BackendStateRepository(IConfiguration configuration, ILogger<BackendStateRepository> logger)
        {
            _connectionString = configuration["Database:ConnectionString"] ?? "Data Source=backend_api.db";
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
                    CREATE TABLE IF NOT EXISTS CommentAuditLog (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        CommandId TEXT NOT NULL UNIQUE,
                        CommentId TEXT,
                        CommandType TEXT NOT NULL,
                        Message TEXT,
                        Sentiment TEXT,
                        Intent TEXT,
                        Status TEXT NOT NULL,
                        Error TEXT,
                        CreatedAt TEXT NOT NULL DEFAULT (datetime('now')),
                        UpdatedAt TEXT NOT NULL DEFAULT (datetime('now'))
                    );

                    CREATE TABLE IF NOT EXISTS CommentRateLimit (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        SenderId TEXT NOT NULL,
                        WindowStart TEXT NOT NULL,
                        Count INTEGER NOT NULL DEFAULT 0,
                        UpdatedAt TEXT NOT NULL DEFAULT (datetime('now')),
                        UNIQUE(SenderId, WindowStart)
                    );

                    CREATE INDEX IF NOT EXISTS idx_comment_audit_command ON CommentAuditLog(CommandId);
                    CREATE INDEX IF NOT EXISTS idx_comment_audit_comment ON CommentAuditLog(CommentId);
                    CREATE INDEX IF NOT EXISTS idx_rate_limit_sender ON CommentRateLimit(SenderId);
                ";
                command.ExecuteNonQuery();
                _logger.LogInformation("Backend SQLite schema initialized successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize backend SQLite schema.");
                throw;
            }
        }

        public async Task SaveAuditAsync(CommentAuditRecord record)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    INSERT INTO CommentAuditLog (CommandId, CommentId, CommandType, Message, Sentiment, Intent, Status, Error, CreatedAt, UpdatedAt)
                    VALUES ($CommandId, $CommentId, $CommandType, $Message, $Sentiment, $Intent, $Status, $Error, $CreatedAt, $UpdatedAt)
                    ON CONFLICT(CommandId) DO UPDATE SET
                        CommentId = excluded.CommentId,
                        CommandType = excluded.CommandType,
                        Message = excluded.Message,
                        Sentiment = excluded.Sentiment,
                        Intent = excluded.Intent,
                        Status = excluded.Status,
                        Error = excluded.Error,
                        UpdatedAt = excluded.UpdatedAt;";

                command.Parameters.AddWithValue("$CommandId", record.CommandId);
                command.Parameters.AddWithValue("$CommentId", (object?)record.CommentId ?? DBNull.Value);
                command.Parameters.AddWithValue("$CommandType", record.CommandType);
                command.Parameters.AddWithValue("$Message", (object?)record.Message ?? DBNull.Value);
                command.Parameters.AddWithValue("$Sentiment", (object?)record.Sentiment ?? DBNull.Value);
                command.Parameters.AddWithValue("$Intent", (object?)record.Intent ?? DBNull.Value);
                command.Parameters.AddWithValue("$Status", record.Status);
                command.Parameters.AddWithValue("$Error", (object?)record.Error ?? DBNull.Value);
                command.Parameters.AddWithValue("$CreatedAt", ToDbTime(record.CreatedAt));
                command.Parameters.AddWithValue("$UpdatedAt", ToDbTime(record.UpdatedAt));

                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving audit for CommandId: {CommandId}", record.CommandId);
                throw;
            }
        }

        public async Task<int> GetRateLimitCountAsync(string senderId, DateTime windowStart)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    SELECT COALESCE(SUM(Count), 0)
                    FROM CommentRateLimit
                    WHERE SenderId = $SenderId AND WindowStart = $WindowStart;";
                command.Parameters.AddWithValue("$SenderId", senderId);
                command.Parameters.AddWithValue("$WindowStart", ToDbTime(windowStart));

                var result = await command.ExecuteScalarAsync();
                return Convert.ToInt32(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading rate limit count for SenderId: {SenderId}", senderId);
                return 0;
            }
        }

        public async Task UpsertRateLimitCountAsync(string senderId, DateTime windowStart, int count)
        {
            try
            {
                using var connection = CreateConnection();
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    INSERT INTO CommentRateLimit (SenderId, WindowStart, Count, UpdatedAt)
                    VALUES ($SenderId, $WindowStart, $Count, datetime('now'))
                    ON CONFLICT(SenderId, WindowStart) DO UPDATE SET
                        Count = excluded.Count,
                        UpdatedAt = datetime('now');";
                command.Parameters.AddWithValue("$SenderId", senderId);
                command.Parameters.AddWithValue("$WindowStart", ToDbTime(windowStart));
                command.Parameters.AddWithValue("$Count", count);
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error upserting rate limit count for SenderId: {SenderId}", senderId);
                throw;
            }
        }

        public DateTime GetWindowStart(DateTime utcNow, int windowMinutes)
        {
            var truncated = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, utcNow.Minute, 0, DateTimeKind.Utc);
            return truncated.AddMinutes(-(truncated.Minute % windowMinutes));
        }
    }
}
