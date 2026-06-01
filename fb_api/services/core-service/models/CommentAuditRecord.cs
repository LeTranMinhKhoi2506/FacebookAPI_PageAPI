namespace CoreService.Models
{
    public class CommentAuditRecord
    {
        public string CommandId { get; set; } = string.Empty;
        public string? CommentId { get; set; }
        public string CommandType { get; set; } = string.Empty;
        public string? Message { get; set; }
        public string? Sentiment { get; set; }
        public string? Intent { get; set; }
        public string Status { get; set; } = string.Empty;
        public string? Error { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}
